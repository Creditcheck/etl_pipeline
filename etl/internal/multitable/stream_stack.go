package multitable

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"etl/internal/config"
	csvparser "etl/internal/parser/csv"
	jsonparser "etl/internal/parser/json"
	"etl/internal/schema"
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
}

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
	if cfg.Source.Kind != "file" || cfg.Source.File == nil || cfg.Source.File.Path == "" {
		return nil, fmt.Errorf("source.kind=file and source.file.path are required")
	}

	switch cfg.Parser.Kind {
	case "csv", "json":
		// ok
	default:
		return nil, fmt.Errorf("parser.kind must be csv or json (got %q)", cfg.Parser.Kind)
	}

	rt := cfg.Runtime
	if rt.ChannelBuffer <= 0 {
		rt.ChannelBuffer = 256
	}
	if rt.ReaderWorkers <= 0 {
		rt.ReaderWorkers = 1
	}
	if rt.TransformWorkers <= 0 {
		rt.TransformWorkers = 1
	}

	rawRowCh := make(chan *transformer.Row, rt.ChannelBuffer)
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

	// Terminal error capture for truly fatal issues (I/O, misconfig, ctx, etc.).
	var once sync.Once
	var terminalErr error
	setTerminalErr := func(err error) {
		if err == nil {
			return
		}
		once.Do(func() { terminalErr = err })
	}

	// 1) Start parser: stream -> rawRowCh.
	src, err := os.Open(cfg.Source.File.Path)
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}

	var wgParse sync.WaitGroup
	wgParse.Add(1)
	go func() {
		defer wgParse.Done()
		defer close(rawRowCh)
		defer src.Close()

		// Non-fatal parse errors (record-level). Count and continue.
		onParseErr := func(_ int, err error) {
			if err == nil {
				return
			}
			parseErrCount.Add(1)
			// Intentionally do NOT set terminalErr.
		}

		switch cfg.Parser.Kind {
		case "csv":
			if err := csvparser.StreamCSVRows(ctx, src, columns, cfg.Parser.Options, rawRowCh, onParseErr); err != nil {
				setTerminalErr(err)
			}
		case "json":
			if err := jsonparser.StreamJSONRows(ctx, src, columns, cfg.Parser.Options, rawRowCh, onParseErr); err != nil {
				setTerminalErr(err)
			}
		default:
			setTerminalErr(fmt.Errorf("unsupported parser.kind=%q", cfg.Parser.Kind))
		}
	}()

	// 2) Coerce stage: build spec from pipeline and run TransformLoopRows workers.
	coerceSpec := transformer.BuildCoerceSpecFromTypes(
		coerceTypesFromPipeline(cfg.Transform),
		coerceLayoutFromPipeline(cfg.Transform),
		nil,
		nil,
	)
	if err := transformer.ValidateSpecSanity(columns, coerceSpec); err != nil {
		return nil, fmt.Errorf("coerce spec sanity: %w", err)
	}

	var wgCoerce sync.WaitGroup
	wgCoerce.Add(rt.TransformWorkers)
	for i := 0; i < rt.TransformWorkers; i++ {
		go func() {
			defer wgCoerce.Done()
			transformer.TransformLoopRows(
				ctx,
				columns,
				rawRowCh,
				coercedRowCh,
				coerceSpec,
				func(_ int, _ string) {
					// TransformLoopRows owns rejected rows and frees them.
				},
			)
		}()
	}
	go func() {
		wgCoerce.Wait()
		close(coercedRowCh)
	}()

	// 3) Optional hash stage: coercedRowCh -> hashOutCh.
	var wgHash sync.WaitGroup
	if hasHash {
		wgHash.Add(1)
		go func() {
			defer wgHash.Done()
			defer close(hashOutCh)

			transformer.HashLoopRows(
				ctx,
				columns,
				coercedRowCh,
				hashOutCh,
				hashSpec,
				func(_ int, _ string) {
					// HashLoopRows owns rejected rows and frees them.
				},
			)
		}()
	}

	// 4) Validate stage: compile required fields + type map from contract.
	requiredFields, typeMap, err := validateInputsFromPipeline(cfg.Transform)
	if err != nil {
		return nil, err
	}

	var wgValidate sync.WaitGroup
	wgValidate.Add(1)
	go func() {
		defer wgValidate.Done()
		defer close(validCh)

		// If there is no contract, requiredFields/typeMap are nil and the validator
		// behaves like a passthrough (implementation-dependent). This matches existing
		// behavior in the repo.
		transformer.ValidateLoopRows(
			ctx,
			columns,
			requiredFields,
			typeMap,
			hashOutCh,
			validCh,
			func(_ int, _ string) {
				// ValidateLoopRows owns rejected rows and frees them.
			},
		)
	}()

	// Wait blocks until all stages complete, then returns any terminal error.
	wait := func() error {
		wgParse.Wait()
		wgCoerce.Wait()
		wgHash.Wait()
		wgValidate.Wait()

		if terminalErr != nil {
			return terminalErr
		}
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
	}, nil
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

// contractFromTransforms is intentionally NOT defined here.
// It already exists in internal/multitable/runner.go and returns *schema.Contract.
var _ *schema.Contract

// Silence unused import in case schema is only referenced via the var above.
var _ = log.New
