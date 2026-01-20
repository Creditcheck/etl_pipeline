package multitable

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"etl/internal/config"
	"etl/internal/metrics"
	csvparser "etl/internal/parser/csv"
	jsonparser "etl/internal/parser/json"
	"etl/internal/transformer"
)

// ValidatedStream represents a streaming pipeline producing validated pooled rows.
//
// The caller must read from Rows until it is closed, then call Wait() to get any
// terminal error.
//
// Ownership: Every *transformer.Row received from Rows is owned by the caller and
// MUST be returned to the pool by calling r.Free() exactly once.
type ValidatedStream struct {
	Rows <-chan *transformer.Row
	Wait func() error

	// ParseErrorCount returns how many non-fatal parse errors were encountered while
	// streaming the input (CSV parse errors, JSON record decode errors, etc.).
	//
	// These errors do not stop the run; they are intended to be counted and emitted
	// as metrics/logs by the caller.
	ParseErrorCount func() uint64

	// ProcessedCount returns the number of rows that successfully parsed and
	// entered the transform stage.
	//
	// This is the multitable streaming equivalent of the single-table pipeline's
	// "processed" metric.
	ProcessedCount func() uint64

	// TransformRejectedCount returns the number of rows dropped by the coerce/
	// transform stage.
	//
	// This counter is incremented when transformer.TransformLoopRows rejects a row
	// (for example, when a value cannot be coerced into the requested type).
	TransformRejectedCount func() uint64

	// ValidateDroppedCount returns the number of rows dropped by the contract
	// validation stage.
	//
	// In lenient validation mode, invalid rows are dropped instead of failing the
	// run; this counter allows operators to track data quality over time.
	ValidateDroppedCount func() uint64
}

// streamMetricsKey is a private context key used to control whether
// StreamValidatedRows should emit metrics.RecordRow calls while streaming.
//
// Why context?
//   - StreamValidatedRows is used by both pass1 and pass2.
//   - The multitable engine reads the input file twice (two-pass design).
//   - We want row-level metrics (processed/parse_errors/validate_dropped/...) to
//     match the single-table pipeline semantics: count the input once per run.
//   - The engine can therefore enable metric emission only for pass2 by wrapping
//     the context passed to StreamValidatedRows.
//
// The key is unexported to avoid collisions with other packages.
type streamMetricsKey struct{}

// WithStreamRowMetrics returns a derived context that instructs StreamValidatedRows
// to emit row-level metrics (metrics.RecordRow) while streaming.
//
// When to use:
//   - Engine pass2 should enable row-level metrics.
//   - Engine pass1 should usually disable row-level metrics to avoid double
//     counting (because pass1 and pass2 both parse/validate the same file).
func WithStreamRowMetrics(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, streamMetricsKey{}, enabled)
}

func streamRowMetricsEnabled(ctx context.Context) bool {
	v := ctx.Value(streamMetricsKey{})
	b, _ := v.(bool)
	return b
}

// Dependency seams for unit testing.
//
// These variables are intentionally package-private so tests in this package can
// patch them without adding third-party mocking frameworks.
var (
	openSource = func(path string) (io.ReadCloser, error) { return os.Open(path) }

	// Signatures match the actual parser adapters used by the project.
	//   - CSV consumes an io.ReadCloser and closes it.
	//   - JSON consumes an io.Reader.
	streamCSVRows  = csvparser.StreamCSVRows
	streamJSONRows = jsonparser.StreamJSONRows

	buildCoerceSpec    = transformer.BuildCoerceSpecFromTypes
	validateSpecSanity = transformer.ValidateSpecSanity

	transformLoop = transformer.TransformLoopRows
	hashLoop      = transformer.HashLoopRows
	validateLoop  = transformer.ValidateLoopRows
)

// StreamValidatedRows streams file -> parse -> coerce -> (optional hash) -> validate into pooled rows.
//
// This function is allocation-conscious and does not buffer the whole file.
// It reuses existing project streaming components:
//   - csv.StreamCSVRows or json.StreamJSONRows
//   - transformer.TransformLoopRows (coerce)
//   - transformer.HashLoopRows (hash)
//   - transformer.ValidateLoopRows (contract validation)
//
// NOTE: The output rows must be freed by the consumer.
func StreamValidatedRows(ctx context.Context, cfg Pipeline, columns []string) (*ValidatedStream, error) {
	if err := validateStreamConfig(cfg); err != nil {
		return nil, err
	}

	rt := normalizeRuntime(cfg.Runtime)

	// Channel wiring is part of the public contract: downstream code expects
	// a single output channel that closes when the pipeline completes.
	//
	// We introduce a "tap" between parse and coerce to count "processed" rows.
	// This mirrors the single-table pipeline where processed == successfully parsed
	// rows entering the transform stage.
	rawRowCh := make(chan *transformer.Row, rt.ChannelBuffer)
	tapRawCh := make(chan *transformer.Row, rt.ChannelBuffer)
	coercedRowCh := make(chan *transformer.Row, rt.ChannelBuffer)

	// Optional: hash stage sits between coerce and validate.
	hasHash, hashSpec, err := hashSpecFromPipeline(cfg.Transform)
	if err != nil {
		return nil, err
	}

	hashOutCh := coercedRowCh
	if hasHash {
		hashOutCh = make(chan *transformer.Row, rt.ChannelBuffer)
	}

	validCh := make(chan *transformer.Row, rt.ChannelBuffer)

	// Parse errors are non-fatal. We count them and continue.
	var parseErrCount atomic.Uint64
	var processedCount atomic.Uint64
	var transformRejectedCount atomic.Uint64
	var validateDroppedCount atomic.Uint64

	emitRowMetrics := streamRowMetricsEnabled(ctx)

	// Terminal errors are fatal to the pipeline (I/O, context, misconfiguration).
	// We capture the first terminal error deterministically.
	var terminalErrOnce sync.Once
	var terminalErr error
	setTerminalErr := func(err error) {
		if err == nil {
			return
		}
		terminalErrOnce.Do(func() { terminalErr = err })
	}

	// Stage 1: parse -> rawRowCh.
	wgParse, err := startParseStage(ctx, cfg, columns, rawRowCh, &parseErrCount, setTerminalErr)
	if err != nil {
		return nil, err
	}

	// Stage 1.5: tap -> tapRawCh.
	//
	// The tap exists solely to count "processed" rows (successfully parsed rows).
	// It is implemented as a single goroutine because:
	//   - It performs O(1) work per row (an atomic increment and a channel send).
	//   - It preserves ordering (useful for deterministic tests).
	//   - It does not allocate per row.
	//
	// Ownership:
	//   - The tap does NOT take ownership of the pooled row; it forwards the pointer.
	//   - Downstream stages and the ultimate consumer still own/free rows.
	//
	// Closing behavior:
	//   - rawRowCh is closed by the parse stage.
	//   - tapRawCh is closed by this tap goroutine once rawRowCh is drained.
	wgTap := &sync.WaitGroup{}
	wgTap.Add(1)
	go func() {
		defer wgTap.Done()
		defer close(tapRawCh)
		for r := range rawRowCh {
			processedCount.Add(1)
			if emitRowMetrics {
				metrics.RecordRow(cfg.Job, "processed", 1)
			}
			select {
			case tapRawCh <- r:
			case <-ctx.Done():
				// On cancellation: do not re-pool
				r.Drop()
			}
		}
	}()

	// Stage 2: coerce -> coercedRowCh.
	wgCoerce, err := startCoerceStage(ctx, cfg, columns, tapRawCh, coercedRowCh, rt, &transformRejectedCount, emitRowMetrics)
	if err != nil {
		return nil, err
	}

	// Stage 3: optional hash -> hashOutCh.
	wgHash := startHashStage(ctx, cfg, columns, coercedRowCh, hashOutCh, hasHash, hashSpec)

	// Stage 4: validate -> validCh.
	wgValidate, err := startValidateStage(ctx, cfg, columns, hashOutCh, validCh, &validateDroppedCount, emitRowMetrics, rt.ValidateLogFirstN)
	if err != nil {
		return nil, err
	}

	wait := func() error {
		wgParse.Wait()
		wgTap.Wait()
		wgCoerce.Wait()
		wgHash.Wait()
		wgValidate.Wait()

		if terminalErr != nil {
			return terminalErr
		}
		// Context cancellation is often used as a control mechanism in tests and
		// in downstream orchestration. We treat context.Canceled as non-fatal.
		if err := ctx.Err(); err != nil && err != context.Canceled {
			return err
		}
		return nil
	}

	return &ValidatedStream{
		Rows: validCh,
		Wait: wait,
		ParseErrorCount: func() uint64 {
			return parseErrCount.Load()
		},
		ProcessedCount: func() uint64 {
			return processedCount.Load()
		},
		TransformRejectedCount: func() uint64 {
			return transformRejectedCount.Load()
		},
		ValidateDroppedCount: func() uint64 {
			return validateDroppedCount.Load()
		},
	}, nil
}

// validateStreamConfig validates the subset of Pipeline required for streaming.
//
// When to use:
//   - StreamValidatedRows should call this before starting any goroutines.
//   - Unit tests can use it directly to validate error messages deterministically.
//
// Errors:
//   - Returns a non-nil error when source is not a file or file path is empty.
//   - Returns a non-nil error when parser.kind is not "csv" or "json".
func validateStreamConfig(cfg Pipeline) error {
	if cfg.Source.Kind != "file" || cfg.Source.File == nil || cfg.Source.File.Path == "" {
		return fmt.Errorf("source.kind=file and source.file.path are required")
	}
	switch cfg.Parser.Kind {
	case "csv", "json":
		return nil
	default:
		return fmt.Errorf("parser.kind must be csv or json (got %q)", cfg.Parser.Kind)
	}
}

// normalizeRuntime applies defaults for runtime settings used by StreamValidatedRows.
//
// When to use:
//   - StreamValidatedRows uses this to ensure consistent buffering and worker counts.
//
// Edge cases:
//   - Non-positive values are replaced by defaults.
//   - The returned RuntimeConfig is a copy and does not mutate the input.
func normalizeRuntime(rt RuntimeConfig) RuntimeConfig {
	out := rt
	if out.ChannelBuffer <= 0 {
		out.ChannelBuffer = 256
	}
	if out.ReaderWorkers <= 0 {
		out.ReaderWorkers = 1
	}
	if out.TransformWorkers <= 0 {
		out.TransformWorkers = 1
	}
	return out
}

// startParseStage starts the parser goroutine and closes rawRowCh when complete.
//
// Ownership:
//   - startParseStage owns the lifetime of rawRowCh (it closes it exactly once).
//   - The parser owns rejected rows and frees them per the parser/transformer contract.
//
// Errors:
//   - Returns an error if the source file cannot be opened.
//   - Parser runtime errors are reported via setTerminalErr and surfaced by Wait().
func startParseStage(
	ctx context.Context,
	cfg Pipeline,
	columns []string,
	rawRowCh chan<- *transformer.Row,
	parseErrCount *atomic.Uint64,
	setTerminalErr func(error),
) (*sync.WaitGroup, error) {
	src, err := openSource(cfg.Source.File.Path)
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(rawRowCh)
		defer src.Close()

		onParseErr := func(_ int, err error) {
			if err == nil {
				return
			}
			parseErrCount.Add(1)

			// Metric emission is controlled by WithStreamRowMetrics.
			// See streamMetricsKey comment for why we do not always emit metrics.
			if streamRowMetricsEnabled(ctx) {
				metrics.RecordRow(cfg.Job, "parse_errors", 1)
			}
		}

		switch cfg.Parser.Kind {
		case "csv":
			if err := streamCSVRows(ctx, src, columns, cfg.Parser.Options, rawRowCh, onParseErr); err != nil {
				setTerminalErr(err)
			}
		case "json":
			if err := streamJSONRows(ctx, src, columns, cfg.Parser.Options, rawRowCh, onParseErr); err != nil {
				setTerminalErr(err)
			}
		default:
			// validateStreamConfig should have caught this; keep defensive.
			setTerminalErr(fmt.Errorf("unsupported parser.kind=%q", cfg.Parser.Kind))
		}
	}()

	return wg, nil
}

// startCoerceStage starts TransformLoopRows workers and closes coercedRowCh when complete.
//
// Errors:
//   - Returns an error when the generated coerce spec is incompatible with columns.
func startCoerceStage(
	ctx context.Context,
	cfg Pipeline,
	columns []string,
	in <-chan *transformer.Row,
	out chan<- *transformer.Row,
	rt RuntimeConfig,
	transformRejectedCount *atomic.Uint64,
	emitRowMetrics bool,
) (*sync.WaitGroup, error) {
	coerceSpec := buildCoerceSpec(
		coerceTypesFromPipeline(cfg.Transform),
		coerceLayoutFromPipeline(cfg.Transform),
		nil,
		nil,
	)
	if err := validateSpecSanity(columns, coerceSpec); err != nil {
		return nil, fmt.Errorf("coerce spec sanity: %w", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(rt.TransformWorkers)

	for i := 0; i < rt.TransformWorkers; i++ {
		go func() {
			defer wg.Done()
			transformLoop(
				ctx,
				columns,
				in,
				out,
				coerceSpec,
				func(_ int, _ string) {
					// TransformLoopRows owns rejected rows and frees them.
					// We only record a count/metric.
					if transformRejectedCount != nil {
						transformRejectedCount.Add(1)
					}
					if emitRowMetrics {
						metrics.RecordRow(cfg.Job, "transform_rejected", 1)
					}
				},
			)
		}()
	}

	// Close output after all coerce workers complete.
	go func() {
		wg.Wait()
		close(out)
	}()

	return wg, nil
}

// startHashStage starts an optional hash stage and returns its WaitGroup.
//
// When to use:
//   - StreamValidatedRows uses this to conditionally enable hashing.
//
// Edge cases:
//   - When enabled is false, returns a zero-value WaitGroup that is safe to Wait() on.
//   - When enabled is true, hashOutCh is closed by this stage.
func startHashStage(
	ctx context.Context,
	cfg Pipeline,
	columns []string,
	in <-chan *transformer.Row,
	hashOutCh chan<- *transformer.Row,
	enabled bool,
	spec transformer.HashSpec,
) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	if !enabled {
		return wg
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(hashOutCh)

		hashLoop(
			ctx,
			columns,
			in,
			hashOutCh,
			spec,
			func(_ int, _ string) {
				// HashLoopRows owns rejected rows and frees them.
			},
		)
	}()

	return wg
}

// startValidateStage starts the validate stage and closes validCh when complete.
//
// Errors:
//   - Returns any error produced while building validation inputs from transforms.
func startValidateStage(
	ctx context.Context,
	cfg Pipeline,
	columns []string,
	in <-chan *transformer.Row,
	validCh chan<- *transformer.Row,
	validateDroppedCount *atomic.Uint64,
	emitRowMetrics bool,
	logFirstN int,
) (*sync.WaitGroup, error) {
	requiredFields, typeMap, err := validateInputsFromPipeline(cfg.Transform)
	if err != nil {
		return nil, err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(validCh)

		// Debug: log the first N validation drop reasons (if enabled).
		// Uses a separate counter so operators can distinguish "first N" logging
		// from the overall validateDroppedCount metric.
		var logged atomic.Int64

		validateLoop(
			ctx,
			columns,
			requiredFields,
			typeMap,
			in,
			validCh,
			func(rowIndex int, reason string) {
				// ValidateLoopRows owns rejected rows and frees them.
				// We only record a count/metric.
				if logFirstN > 0 {
					n := logged.Add(1)
					if n <= int64(logFirstN) {
						// Keep logs reasonably bounded even if the validator returns
						// a very long message.
						const maxReason = 1000
						if len(reason) > maxReason {
							reason = reason[:maxReason] + "…"
						}
						log.Printf("stage=validate drop=%d row_index=%d reason=%s", n, rowIndex, reason)
					}
				}

				if validateDroppedCount != nil {
					validateDroppedCount.Add(1)
				}
				if emitRowMetrics {
					metrics.RecordRow(cfg.Job, "validate_dropped", 1)
				}
			},
		)
	}()

	return wg, nil
}

// coerceTypesFromPipeline extracts coerce.types from pipeline transforms.
func coerceTypesFromPipeline(ts []config.Transform) map[string]string {
	out := map[string]string{}
	for _, t := range ts {
		if t.Kind != "coerce" {
			continue
		}
		m := t.Options.StringMap("types")
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// coerceLayoutFromPipeline extracts coerce.layout from pipeline transforms.
func coerceLayoutFromPipeline(ts []config.Transform) string {
	for _, t := range ts {
		if t.Kind != "coerce" {
			continue
		}
		if s := t.Options.String("layout", ""); s != "" {
			return s
		}
	}
	return ""
}

// validateInputsFromPipeline returns the inputs expected by transformer.ValidateLoopRows:
//   - requiredFields: the set of required fields from the contract
//   - typeMap: field -> kind (e.g. "bigint","text","date") used by the validator
func validateInputsFromPipeline(ts []config.Transform) ([]string, map[string]string, error) {
	c, err := contractFromTransforms(ts) // defined in runner.go (same package)
	if err != nil || c == nil {
		return nil, nil, err
	}

	required := make([]string, 0, len(c.Fields))
	types := make(map[string]string, len(c.Fields))

	for _, f := range c.Fields {
		if f.Name == "" {
			continue
		}

		typ := strings.ToLower(strings.TrimSpace(f.Type))
		switch typ {
		case "":
			typ = "text"
		case "string":
			typ = "text"
		case "bitint":
			// Common typo we’ve already seen in configs; treat as bigint.
			typ = "bigint"
		}
		types[f.Name] = typ

		if f.Required {
			required = append(required, f.Name)
		}
	}

	sort.Strings(required)
	return required, types, nil
}

// hashSpecFromPipeline extracts a hash spec from pipeline transforms.
//
// Behavior:
//   - Uses the first transform with kind "hash".
//   - Requires options.target_field and a non-empty options.fields.
//   - Supports options.fields as either []string or []any (decoded from JSON).
//
// Errors:
//   - Returns an error when required options are missing.
//   - Returns an error when algorithm or encoding is unsupported by the streaming
//     implementation (sha256 + hex only).
func hashSpecFromPipeline(ts []config.Transform) (bool, transformer.HashSpec, error) {
	var spec transformer.HashSpec

	for _, t := range ts {
		if t.Kind != "hash" {
			continue
		}

		target := strings.TrimSpace(t.Options.String("target_field", ""))
		fields := t.Options.StringSlice("fields")
		if len(fields) == 0 {
			// Some configs use []any decoded from JSON.
			raw := t.Options.Any("fields")
			if rawSlice, ok := raw.([]any); ok {
				fields = make([]string, 0, len(rawSlice))
				for _, it := range rawSlice {
					s := strings.TrimSpace(fmt.Sprint(it))
					if s != "" {
						fields = append(fields, s)
					}
				}
			}
		}

		if target == "" || len(fields) == 0 {
			return false, transformer.HashSpec{}, fmt.Errorf("hash transform requires options.target_field and options.fields")
		}

		spec = transformer.HashSpec{
			TargetField:       target,
			Fields:            fields,
			Algorithm:         strings.ToLower(strings.TrimSpace(t.Options.String("algorithm", "sha256"))),
			Encoding:          strings.ToLower(strings.TrimSpace(t.Options.String("encoding", "hex"))),
			Separator:         t.Options.String("separator", "\x1f"),
			IncludeFieldNames: t.Options.Bool("include_field_names", false),
			TrimSpace:         t.Options.Bool("trim_space", false),
		}

		// Only sha256/hex are supported by the streaming implementation.
		if spec.Algorithm != "" && spec.Algorithm != "sha256" {
			return false, transformer.HashSpec{}, fmt.Errorf("hash algorithm %q not supported (want sha256)", spec.Algorithm)
		}
		if spec.Encoding != "" && spec.Encoding != "hex" {
			return false, transformer.HashSpec{}, fmt.Errorf("hash encoding %q not supported (want hex)", spec.Encoding)
		}

		return true, spec, nil
	}

	return false, transformer.HashSpec{}, nil
}
