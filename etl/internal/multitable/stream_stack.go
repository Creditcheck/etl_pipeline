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

// StreamValidatedRows streams file -> parse -> coerce -> validate into pooled rows.
//
// This function is allocation-conscious and does not buffer the whole file.
// It reuses existing project streaming components:
//   - csv.StreamCSVRows
//   - transformer.TransformLoopRows
//   - transformer.ValidateLoopRows
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

	rawRowCh := make(chan *transformer.Row, rt.ChannelBuffer)
	tapRawCh := make(chan *transformer.Row, rt.ChannelBuffer)
	coercedRowCh := make(chan *transformer.Row, rt.ChannelBuffer)
	validCh := make(chan *transformer.Row, rt.ChannelBuffer)

	// Terminal error capture (parser/setup).
	var once sync.Once
	var terminalErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		once.Do(func() { terminalErr = err })
	}

	// 1) Start CSV parser: stream -> rawRowCh.
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
		if err := csv.StreamCSVRows(ctx, src, columns, cfg.Parser.Options, rawRowCh, func(line int, err error) {
			_ = line
			setErr(err)
		}); err != nil {
			setErr(err)
		}
	}()

	// 2) Tap stage: forward parser output to transform stage.
	// (This is a single fan-out. If you later want N transform workers, you can
	// switch to a worker pool that all read from tapRawCh; that’s what we do here.)
	var wgTap sync.WaitGroup
	wgTap.Add(1)
	go func() {
		defer wgTap.Done()
		defer close(tapRawCh)

		for r := range rawRowCh {
			select {
			case tapRawCh <- r:
			case <-ctx.Done():
				// Avoid leaks.
				r.Free()
				return
			}
		}
	}()

	// 3) Coerce stage: build spec from pipeline and run TransformLoopRows workers.
	coerceSpec := transformer.BuildCoerceSpecFromTypes(
		coerceTypesFromPipeline(cfg.Transform),
		coerceLayoutFromPipeline(cfg.Transform),
		nil,
		nil,
	)
	if err := transformer.ValidateSpecSanity(columns, coerceSpec); err != nil {
		return nil, fmt.Errorf("coerce spec sanity: %w", err)
	}

	var wgTransform sync.WaitGroup
	wgTransform.Add(rt.TransformWorkers)
	for i := 0; i < rt.TransformWorkers; i++ {
		go func() {
			defer wgTransform.Done()
			transformer.TransformLoopRows(
				ctx,
				columns,
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

		transformer.ValidateLoopRows(
			ctx,
			columns,
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

	// Wait blocks until all stages complete, then returns any terminal error.
	wait := func() error {
		wgParse.Wait()
		wgTap.Wait()
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
		// Validator expects "type" strings; keep as-is but normalize common aliases.
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
