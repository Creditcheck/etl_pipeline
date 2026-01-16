package multitable

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"etl/internal/config"
	"etl/internal/parser/csv"
	"etl/internal/schema"
	"etl/internal/transformer"
)

// ValidatedStream represents a streaming pipeline producing validated pooled rows.
// The caller must read from Rows until it is closed, then call Wait() to get any
// terminal error.
//
// Ownership: Every *transformer.Row received from Rows is owned by the caller and
// MUST be returned to the pool by calling r.Free() exactly once.
type ValidatedStream struct {
	Rows <-chan *transformer.Row
	Wait func() error
}

// StreamValidatedRows streams file -> parse -> coerce -> validate -> (optional hash) into pooled rows.
//
// This function is allocation-conscious and does not buffer the whole file.
// It reuses existing project streaming components:
//   - csv.StreamCSVRows
//   - transformer.TransformLoopRows
//   - transformer.ValidateLoopRows
//   - transformer.HashLoopRows (optional stage; enabled when transform.kind == "hash")
//
// IMPORTANT: The hash transform may write to a synthetic target_field that is NOT
// present in the CSV. In that case, this function widens the pipeline column set
// (appending the new column name) and widens each row by appending a nil slot.
// The CSV parser still runs using the original CSV column list.
//
// NOTE: The output rows must be freed by the consumer.
func StreamValidatedRows(ctx context.Context, cfg Pipeline, columns []string) (*ValidatedStream, error) {
	if cfg.Source.Kind != "file" || cfg.Source.File == nil || cfg.Source.File.Path == "" {
		return nil, fmt.Errorf("source.kind=file and source.file.path are required")
	}
	if cfg.Parser.Kind != "csv" {
		return nil, fmt.Errorf("parser.kind must be csv")
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

	// Determine if a hash stage exists and whether it needs a synthetic output column.
	hashSpec, hashEnabled, err := hashSpecFromPipeline(cfg.Transform)
	if err != nil {
		return nil, err
	}

	// Parser columns are strictly the CSV fields (derived from header_map).
	parserCols := columns

	// Pipeline columns may include synthetic columns (e.g. row_hash).
	pipelineCols := columns
	needsWiden := false
	if hashEnabled {
		if !containsString(pipelineCols, hashSpec.TargetField) {
			// Synthesize the new target column at the end of the row.
			pipelineCols = append(copyStrings(pipelineCols), hashSpec.TargetField)
			needsWiden = true
		}
	}

	rawRowCh := make(chan *transformer.Row, rt.ChannelBuffer)     // parser -> widen (optional)
	widenedRowCh := make(chan *transformer.Row, rt.ChannelBuffer) // widen -> tap
	tapRawCh := make(chan *transformer.Row, rt.ChannelBuffer)     // tap -> coerce
	coercedRowCh := make(chan *transformer.Row, rt.ChannelBuffer) // coerce -> validate
	validCh := make(chan *transformer.Row, rt.ChannelBuffer)      // validate -> hash/output
	hashedCh := make(chan *transformer.Row, rt.ChannelBuffer)     // hash -> output (if enabled)

	// Terminal error capture (parser/setup).
	var once sync.Once
	var terminalErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		once.Do(func() { terminalErr = err })
	}

	// 1) Start CSV parser: stream -> rawRowCh (parserCols only).
	src, err := os.Open(cfg.Source.File.Path)
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}

	var wgParse sync.WaitGroup
	wgParse.Add(1)
	go func() {
		defer wgParse.Done()
		defer close(rawRowCh)

		// csv.StreamCSVRows closes src itself.
		if err := csv.StreamCSVRows(ctx, src, parserCols, cfg.Parser.Options, rawRowCh, func(line int, err error) {
			_ = line
			setErr(err)
		}); err != nil {
			setErr(err)
		}
	}()

	// 2) Optional widen stage: if we have synthetic columns, append nil slots.
	var wgWiden sync.WaitGroup
	wgWiden.Add(1)
	go func() {
		defer wgWiden.Done()
		defer close(widenedRowCh)

		if !needsWiden {
			// Passthrough with cancellation safety.
			for r := range rawRowCh {
				select {
				case widenedRowCh <- r:
				case <-ctx.Done():
					r.Free()
					return
				}
			}
			return
		}

		// We need to widen each row by exactly 1 column (the hash target).
		want := len(pipelineCols)
		have := len(parserCols)

		for r := range rawRowCh {
			select {
			case <-ctx.Done():
				r.Free()
				return
			default:
			}
			if r == nil {
				continue
			}
			if len(r.V) != have {
				// Unexpected row shape; drop to avoid panics downstream.
				r.Free()
				continue
			}

			// Append nil slots. (Right now we only ever add 1, but keep generic.)
			for len(r.V) < want {
				r.V = append(r.V, nil)
			}

			widenedRowCh <- r
		}
	}()

	// 3) Tap stage: forward widened rows to transform stage.
	var wgTap sync.WaitGroup
	wgTap.Add(1)
	go func() {
		defer wgTap.Done()
		defer close(tapRawCh)

		for r := range widenedRowCh {
			select {
			case tapRawCh <- r:
			case <-ctx.Done():
				r.Free()
				return
			}
		}
	}()

	// 4) Coerce stage: build spec from pipeline and run TransformLoopRows workers.
	// Coerce only affects known fields; synthetic columns are left untouched.
	coerceSpec := transformer.BuildCoerceSpecFromTypes(
		coerceTypesFromPipeline(cfg.Transform),
		coerceLayoutFromPipeline(cfg.Transform),
		nil,
		nil,
	)
	if err := transformer.ValidateSpecSanity(pipelineCols, coerceSpec); err != nil {
		return nil, fmt.Errorf("coerce spec sanity: %w", err)
	}

	var wgTransform sync.WaitGroup
	wgTransform.Add(rt.TransformWorkers)
	for i := 0; i < rt.TransformWorkers; i++ {
		go func() {
			defer wgTransform.Done()
			transformer.TransformLoopRows(
				ctx,
				pipelineCols,
				tapRawCh,
				coercedRowCh,
				coerceSpec,
				func(line int, reason string) {
					_ = line
					_ = reason
					// TransformLoopRows owns rejected rows and frees them.
				},
			)
		}()
	}
	go func() {
		wgTransform.Wait()
		close(coercedRowCh)
	}()

	// 5) Validate stage: compile required fields + type map from contract.
	requiredFields, typeMap, err := validateInputsFromPipeline(cfg.Transform)
	if err != nil {
		return nil, err
	}

	var wgValidate sync.WaitGroup
	wgValidate.Add(1)
	go func() {
		defer wgValidate.Done()
		defer close(validCh)

		transformer.ValidateLoopRows(
			ctx,
			pipelineCols,
			requiredFields,
			typeMap,
			coercedRowCh,
			validCh,
			func(line int, reason string) {
				_ = line
				_ = reason
				// ValidateLoopRows owns rejected rows and frees them.
			},
		)
	}()

	// 6) Optional hash stage: validate -> hash -> output.
	var (
		outRows <-chan *transformer.Row
		wgHash  sync.WaitGroup
	)

	if !hashEnabled {
		outRows = validCh
	} else {
		outRows = hashedCh

		wgHash.Add(rt.TransformWorkers)
		for i := 0; i < rt.TransformWorkers; i++ {
			go func() {
				defer wgHash.Done()
				transformer.HashLoopRows(
					ctx,
					pipelineCols,
					validCh,
					hashedCh,
					hashSpec,
					func(line int, reason string) {
						_ = line
						_ = reason
						// HashLoopRows owns rejected rows and frees them.
					},
				)
			}()
		}
		go func() {
			wgHash.Wait()
			close(hashedCh)
		}()
	}

	// Wait blocks until all stages complete, then returns any terminal error.
	wait := func() error {
		wgParse.Wait()
		wgWiden.Wait()
		wgTap.Wait()
		wgValidate.Wait()
		if hashEnabled {
			wgHash.Wait()
		}

		if terminalErr != nil {
			return terminalErr
		}
		if err := ctx.Err(); err != nil && err != context.Canceled {
			return err
		}
		return nil
	}

	return &ValidatedStream{
		Rows: outRows,
		Wait: wait,
	}, nil
}

// hashSpecFromPipeline extracts the hash configuration from the pipeline transform list.
// The hash transform is optional. If absent, (spec, false, nil) is returned.
//
// Supported options:
//   - target_field (string, required): the field/column to store the hash into.
//   - fields ([]string, required): ordered list of fields to hash.
//   - separator (string, optional): default "\x1f".
//   - include_field_names (bool, optional): default false.
//   - trim_space (bool, optional): default true.
//   - algorithm (string, optional): only "sha256" supported; default "sha256".
//   - encoding (string, optional): only "hex" supported; default "hex".
func hashSpecFromPipeline(ts []config.Transform) (transformer.HashSpec, bool, error) {
	var tr *config.Transform
	for i := range ts {
		if strings.EqualFold(strings.TrimSpace(ts[i].Kind), "hash") {
			tr = &ts[i]
			break
		}
	}
	if tr == nil {
		return transformer.HashSpec{}, false, nil
	}

	target := strings.TrimSpace(tr.Options.String("target_field", ""))
	if target == "" {
		return transformer.HashSpec{}, false, fmt.Errorf("hash transform: options.target_field is required")
	}

	fields := anyToStringSlice(tr.Options.Any("fields"))
	if len(fields) == 0 {
		return transformer.HashSpec{}, false, fmt.Errorf("hash transform: options.fields must be a non-empty []string")
	}

	alg := strings.TrimSpace(strings.ToLower(tr.Options.String("algorithm", "sha256")))
	enc := strings.TrimSpace(strings.ToLower(tr.Options.String("encoding", "hex")))
	if alg == "" {
		alg = "sha256"
	}
	if enc == "" {
		enc = "hex"
	}
	if alg != "sha256" {
		return transformer.HashSpec{}, false, fmt.Errorf("hash transform: unsupported algorithm %q (only sha256 supported)", alg)
	}
	if enc != "hex" {
		return transformer.HashSpec{}, false, fmt.Errorf("hash transform: unsupported encoding %q (only hex supported)", enc)
	}

	spec := transformer.HashSpec{
		TargetField:       target,
		Fields:            fields,
		Algorithm:         alg,
		Encoding:          enc,
		Separator:         tr.Options.String("separator", "\x1f"),
		IncludeFieldNames: tr.Options.Bool("include_field_names", false),
		TrimSpace:         tr.Options.Bool("trim_space", true),
	}

	return spec, true, nil
}

func anyToStringSlice(v any) []string {
	switch t := v.(type) {
	case nil:
		return nil
	case []string:
		out := make([]string, 0, len(t))
		for _, s := range t {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			s, ok := x.(string)
			if !ok {
				continue
			}
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func copyStrings(xs []string) []string {
	if len(xs) == 0 {
		return nil
	}
	out := make([]string, len(xs))
	copy(out, xs)
	return out
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
		}
		types[f.Name] = typ

		if f.Required {
			required = append(required, f.Name)
		}
	}

	return required, types, nil
}

// contractFromTransforms is intentionally NOT defined here.
// It already exists in internal/multitable/runner.go and returns *schema.Contract.
var _ *schema.Contract
