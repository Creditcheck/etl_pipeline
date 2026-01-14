package multitable

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"etl/internal/config"
	"etl/internal/parser/csv"
	"etl/internal/schema"
	"etl/internal/transformer"
)

// StreamAndCollectRecords reuses the existing streaming stack:
// CSV -> coerce -> validate, then collects to []Record.
//
// It expects `columns` to be canonical field names that exist after header_map.
func StreamAndCollectRecords(ctx context.Context, cfg Pipeline, columns []string) ([]Record, error) {
	f, err := os.Open(cfg.Source.File.Path)
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer f.Close()

	rt := cfg.Runtime
	if rt.ChannelBuffer <= 0 {
		rt.ChannelBuffer = 256
	}
	transformWorkers := rt.TransformWorkers
	if transformWorkers <= 0 {
		transformWorkers = 1
	}

	rawRowCh := make(chan *transformer.Row, rt.ChannelBuffer)
	tapRawCh := make(chan *transformer.Row, rt.ChannelBuffer)
	coercedRowCh := make(chan *transformer.Row, rt.ChannelBuffer)
	validCh := make(chan *transformer.Row, rt.ChannelBuffer)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var parseOnce sync.Once
	var parseErr error
	onParseErr := func(line int, err error) {
		if err == nil {
			return
		}
		parseOnce.Do(func() {
			parseErr = fmt.Errorf("parse error at line %d: %w", line, err)
			cancel()
		})
	}

	// 1) Reader: stream CSV into pooled rows aligned to `columns`.
	var wgReaders sync.WaitGroup
	wgReaders.Add(1)
	go func() {
		defer wgReaders.Done()
		defer close(rawRowCh)

		_ = csv.StreamCSVRows(
			ctx,
			f,
			columns,
			cfg.Parser.Options, // now config.Options
			rawRowCh,
			onParseErr,
		)
	}()

	// 2) Tap: forward (keeps row lifetime semantics identical to main pipeline).
	var wgTap sync.WaitGroup
	wgTap.Add(1)
	go func() {
		defer wgTap.Done()
		defer close(tapRawCh)

		for r := range rawRowCh {
			select {
			case tapRawCh <- r:
			case <-ctx.Done():
				r.Free()
				return
			}
		}
	}()

	// 3) Transformers: build coerce spec from transform config (same as container.go).
	coerceSpec := transformer.BuildCoerceSpecFromTypes(
		coerceTypesFromPipeline(cfg.Transform),
		coerceLayoutFromPipeline(cfg.Transform),
		nil,
		nil,
	)

	if err := transformer.ValidateSpecSanity(columns, coerceSpec); err != nil {
		return nil, fmt.Errorf("coerce spec sanity: %w", err)
	}

	var wgTransformers sync.WaitGroup
	wgTransformers.Add(transformWorkers)
	for i := 0; i < transformWorkers; i++ {
		go func() {
			defer wgTransformers.Done()
			transformer.TransformLoopRows(
				ctx,
				columns,
				tapRawCh,
				coercedRowCh,
				coerceSpec,
				func(line int, reason string) {
					// lenient drop: just free behavior is handled by TransformLoopRows
					_ = line
					_ = reason
				},
			)
		}()
	}

	// Close coerced channel after reader/tap/transformers complete
	go func() {
		wgReaders.Wait()
		wgTap.Wait()
		wgTransformers.Wait()
		close(coercedRowCh)
	}()

	// 4) Validator: required fields + kind checks (same as container.go).
	required := requiredFromContractTransforms(cfg.Transform)
	colKinds := normalizedKindsFromTransforms(cfg.Transform)

	var wgValidator sync.WaitGroup
	wgValidator.Add(1)
	go func() {
		defer wgValidator.Done()
		defer close(validCh)

		transformer.ValidateLoopRows(
			ctx,
			columns,
			required,
			colKinds,
			coercedRowCh,
			validCh,
			func(line int, reason string) {
				// lenient drop
				_ = line
				_ = reason
			},
		)
	}()

	// 5) Collect typed, validated rows to []Record.
	var out []Record
	for r := range validCh {
		rec := make(Record, len(columns))
		for i, col := range columns {
			rec[col] = r.V[i]
		}
		out = append(out, rec)
		r.Free()
	}

	wgValidator.Wait()

	if parseErr != nil {
		return nil, parseErr
	}
	if err := ctx.Err(); err != nil && err != context.Canceled {
		return nil, err
	}

	return out, nil
}

// ---- helpers copied (in spirit) from container.go, but scoped to transforms ----

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

func coerceLayoutFromPipeline(ts []config.Transform) string {
	for _, t := range ts {
		if t.Kind == "coerce" {
			if s := t.Options.String("layout", ""); s != "" {
				return s
			}
		}
	}
	return "02.01.2006"
}

func requiredFromContractTransforms(ts []config.Transform) []string {
	for _, t := range ts {
		if t.Kind != "validate" {
			continue
		}
		raw := t.Options.Any("contract")
		if raw == nil {
			continue
		}
		b, err := json.Marshal(raw)
		if err != nil {
			continue
		}
		var c schema.Contract
		if err := json.Unmarshal(b, &c); err != nil {
			continue
		}
		var req []string
		for _, f := range c.Fields {
			if f.Required {
				req = append(req, f.Name)
			}
		}
		return req
	}
	return nil
}

// normalizedKindsFromTransforms mirrors the container.go behavior:
// coerce types provide a baseline, validate.contract types override.
func normalizedKindsFromTransforms(ts []config.Transform) map[string]string {
	out := make(map[string]string)

	for name, t := range coerceTypesFromPipeline(ts) {
		out[name] = normalizeKind(t)
	}

	for _, t := range ts {
		if t.Kind != "validate" {
			continue
		}
		raw := t.Options.Any("contract")
		if raw == nil {
			continue
		}
		b, err := json.Marshal(raw)
		if err != nil {
			continue
		}
		var c schema.Contract
		if err := json.Unmarshal(b, &c); err != nil {
			continue
		}
		for _, f := range c.Fields {
			out[f.Name] = normalizeKind(f.Type)
		}
	}

	return out
}

func normalizeKind(t string) string {
	s := strings.ToLower(t)
	switch s {
	case "bigint", "int8", "integer", "int4", "int2", "int":
		return "int"
	case "boolean", "bool":
		return "bool"
	case "date", "timestamp", "timestamptz":
		return "date"
	case "text", "string":
		return "string"
	default:
		return s
	}
}
