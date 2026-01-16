package json

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"etl/internal/config"
	"etl/internal/transformer"
)

// StreamJSONRows parses JSON from r according to parserOpts and streams records
// as *transformer.Row into 'out'.
//
// Streaming behavior:
//   - If the root is a JSON array, it streams each object element one-by-one.
//   - If the root is a JSON object and contains an array-of-objects field, it streams
//     the first such array field one-by-one (envelope pattern).
//   - If the root is a single object with no array-of-objects fields, it emits one record.
//
// parserOpts:
//   - header_map: map original key -> normalized key
//   - array_join_separator: used to flatten []string / []any(string) into a scalar string
func StreamJSONRows(
	ctx context.Context,
	r io.Reader,
	columns []string,
	parserOpts config.Options,
	out chan<- *transformer.Row,
	onParseErr func(line int, err error),
) error {
	dec := json.NewDecoder(r)
	dec.UseNumber() // avoids float64 allocations; downstream coerce can handle number parsing.

	headerMap := readHeaderMap(parserOpts)
	reverseHeaderMap := reverseHeaderMap(headerMap)

	sep := strings.TrimSpace(parserOpts.String("array_join_separator", ","))
	if sep == "" {
		sep = ","
	}

	line := 0

	emitObject := func(obj map[string]any) error {
		line++

		values := recordToRowJSONStreaming(obj, columns, reverseHeaderMap, sep)
		row := &transformer.Row{
			Line: line,
			V:    values,
		}

		select {
		case out <- row:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Peek the first token so we can stream arrays/envelopes without buffering.
	tok, err := dec.Token()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		if onParseErr != nil {
			onParseErr(0, err)
		}
		return fmt.Errorf("json: read first token: %w", err)
	}

	switch d := tok.(type) {
	case json.Delim:
		switch d {
		case '[':
			// Root array: stream each object.
			if err := streamArrayOfObjects(ctx, dec, emitObject, onParseErr, &line); err != nil {
				return err
			}
			// Consume closing ']'
			if end, err := dec.Token(); err != nil {
				return fmt.Errorf("json: read array end:: %w", err)
			} else if end != json.Delim(']') {
				return fmt.Errorf("json: expected array end ']', got %v", end)
			}
			// Optional trailing JSONL objects after the array.
			return streamTrailingObjects(ctx, dec, emitObject, onParseErr, &line)

		case '{':
			// Root object: either envelope (contains array-of-objects) or single record.
			streamed, singleObj, err := streamEnvelopeOrSingle(ctx, dec, emitObject, onParseErr, &line)
			if err != nil {
				return err
			}
			// Consume closing '}'
			if end, err := dec.Token(); err != nil {
				return fmt.Errorf("json: read object end: %w", err)
			} else if end != json.Delim('}') {
				return fmt.Errorf("json: expected object end '}', got %v", end)
			}

			if !streamed && singleObj != nil {
				// We parsed a single object record; emit it.
				if err := emitObject(singleObj); err != nil {
					return err
				}
			}

			// Optional trailing JSONL objects after the root object.
			return streamTrailingObjects(ctx, dec, emitObject, onParseErr, &line)

		default:
			return fmt.Errorf("json: unsupported root delimiter %q", d)
		}

	default:
		return fmt.Errorf("json: unsupported root token %T (want object or array)", tok)
	}
}

func streamTrailingObjects(
	ctx context.Context,
	dec *json.Decoder,
	emit func(map[string]any) error,
	onParseErr func(line int, err error),
	line *int,
) error {
	for {
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			if err == io.EOF {
				return nil
			}
			if onParseErr != nil {
				onParseErr(*line+1, err)
			}
			return fmt.Errorf("json: decode trailing object: %w", err)
		}
		if err := emit(obj); err != nil {
			return err
		}
	}
}

// streamArrayOfObjects streams elements of the current array (after '[' has been consumed).
// It expects each element to be an object (map[string]any). nil elements are skipped.
func streamArrayOfObjects(
	ctx context.Context,
	dec *json.Decoder,
	emit func(map[string]any) error,
	onParseErr func(line int, err error),
	line *int,
) error {
	for dec.More() {
		// Decode one element at a time (streaming).
		var raw any
		if err := dec.Decode(&raw); err != nil {
			if onParseErr != nil {
				onParseErr(*line+1, err)
			}
			return fmt.Errorf("json: decode array element: %w", err)
		}
		if raw == nil {
			continue
		}
		obj, ok := raw.(map[string]any)
		if !ok {
			err := fmt.Errorf("json: array element not an object (got %T)", raw)
			if onParseErr != nil {
				onParseErr(*line+1, err)
			}
			return err
		}
		if err := emit(obj); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// streamEnvelopeOrSingle walks a root object (after '{' has been consumed).
//
// If it finds the first field whose value is an array-of-objects, it streams that array
// and skips the rest of the object fields (envelope behavior).
//
// If it finds no array-of-objects, it builds a single object map and returns it so the caller
// can emit one record.
func streamEnvelopeOrSingle(
	ctx context.Context,
	dec *json.Decoder,
	emit func(map[string]any) error,
	onParseErr func(line int, err error),
	line *int,
) (streamed bool, single map[string]any, _ error) {
	single = make(map[string]any)

	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			if onParseErr != nil {
				onParseErr(*line+1, err)
			}
			return false, nil, fmt.Errorf("json: read object key: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return false, nil, fmt.Errorf("json: object key not a string (got %T)", keyTok)
		}

		// Read first token of the value so we can stream arrays without buffering.
		valTok, err := dec.Token()
		if err != nil {
			if onParseErr != nil {
				onParseErr(*line+1, err)
			}
			return false, nil, fmt.Errorf("json: read object value token: %w", err)
		}

		// If value is an array, try to stream as array-of-objects (envelope).
		if delim, ok := valTok.(json.Delim); ok && delim == '[' {
			// We only treat it as records if its elements are objects; stream one-by-one.
			// If a non-object element appears, we error out (matches old behavior).
			if err := streamArrayOfObjects(ctx, dec, emit, onParseErr, line); err != nil {
				return false, nil, err
			}
			// Consume closing ']'
			endTok, err := dec.Token()
			if err != nil {
				return false, nil, fmt.Errorf("json: read envelope array end: %w", err)
			}
			if endTok != json.Delim(']') {
				return false, nil, fmt.Errorf("json: expected ']' after envelope array, got %v", endTok)
			}

			// Skip remaining fields of the root object without decoding them into Go values.
			for dec.More() {
				// key
				if _, err := dec.Token(); err != nil {
					return true, nil, fmt.Errorf("json: skip envelope key: %w", err)
				}
				// value
				if err := skipNextValue(dec); err != nil {
					return true, nil, err
				}
			}

			return true, nil, nil
		}

		// Non-array value:
		// - If scalar, valTok already is the scalar.
		// - If object, we need to read and materialize it (rare for single-record case).
		// - If array (already handled), object, etc: consume remaining tokens for that value.
		val, err := materializeValueFromFirstToken(dec, valTok)
		if err != nil {
			if onParseErr != nil {
				onParseErr(*line+1, err)
			}
			return false, nil, err
		}
		single[key] = val
	}

	return false, single, nil
}

// skipNextValue skips the next JSON value from the decoder, without materializing it.
func skipNextValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("json: skip value token: %w", err)
	}
	return skipValueFromFirstToken(dec, tok)
}

func skipValueFromFirstToken(dec *json.Decoder, tok any) error {
	d, ok := tok.(json.Delim)
	if !ok {
		// scalar token; nothing else to consume
		return nil
	}

	switch d {
	case '{':
		for dec.More() {
			// key
			if _, err := dec.Token(); err != nil {
				return fmt.Errorf("json: skip object key: %w", err)
			}
			// value
			if err := skipNextValue(dec); err != nil {
				return err
			}
		}
		// consume '}'
		end, err := dec.Token()
		if err != nil {
			return fmt.Errorf("json: skip object end: %w", err)
		}
		if end != json.Delim('}') {
			return fmt.Errorf("json: expected '}', got %v", end)
		}
		return nil

	case '[':
		for dec.More() {
			if err := skipNextValue(dec); err != nil {
				return err
			}
		}
		// consume ']'
		end, err := dec.Token()
		if err != nil {
			return fmt.Errorf("json: skip array end: %w", err)
		}
		if end != json.Delim(']') {
			return fmt.Errorf("json: expected ']', got %v", end)
		}
		return nil

	default:
		return fmt.Errorf("json: unexpected delimiter %q", d)
	}
}

// materializeValueFromFirstToken builds a Go value for the current JSON value,
// given the first token has already been read.
//
// This is only used for the "single root object record" case, which should be small.
func materializeValueFromFirstToken(dec *json.Decoder, tok any) (any, error) {
	if d, ok := tok.(json.Delim); ok {
		switch d {
		case '{':
			m := make(map[string]any)
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, fmt.Errorf("json: read nested object key: %w", err)
				}
				k, ok := kt.(string)
				if !ok {
					return nil, fmt.Errorf("json: nested object key not string (got %T)", kt)
				}
				vt, err := dec.Token()
				if err != nil {
					return nil, fmt.Errorf("json: read nested object value token: %w", err)
				}
				v, err := materializeValueFromFirstToken(dec, vt)
				if err != nil {
					return nil, err
				}
				m[k] = v
			}
			end, err := dec.Token()
			if err != nil {
				return nil, fmt.Errorf("json: read nested object end: %w", err)
			}
			if end != json.Delim('}') {
				return nil, fmt.Errorf("json: expected '}', got %v", end)
			}
			return m, nil

		case '[':
			// Materialize small arrays in the single-record case.
			var arr []any
			for dec.More() {
				vt, err := dec.Token()
				if err != nil {
					return nil, fmt.Errorf("json: read nested array value token: %w", err)
				}
				v, err := materializeValueFromFirstToken(dec, vt)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			end, err := dec.Token()
			if err != nil {
				return nil, fmt.Errorf("json: read nested array end: %w", err)
			}
			if end != json.Delim(']') {
				return nil, fmt.Errorf("json: expected ']', got %v", end)
			}
			return arr, nil

		default:
			return nil, fmt.Errorf("json: unexpected delimiter %q", d)
		}
	}

	// scalar token
	return tok, nil
}

// readHeaderMap extracts header_map from parser options.
func readHeaderMap(opts config.Options) map[string]string {
	res := make(map[string]string)

	raw := opts.Any("header_map")
	if raw == nil {
		return res
	}

	switch m := raw.(type) {
	case map[string]string:
		for k, v := range m {
			res[k] = v
		}
	case map[string]any:
		for k, v := range m {
			if s, ok := v.(string); ok {
				res[k] = s
			}
		}
	default:
		// unsupported; leave empty
	}

	return res
}

// reverseHeaderMap builds normalized->original for lookup without per-record map copies.
func reverseHeaderMap(h map[string]string) map[string]string {
	out := make(map[string]string, len(h))
	for orig, norm := range h {
		if orig == "" || norm == "" {
			continue
		}
		out[norm] = orig
	}
	return out
}

// recordToRowJSONStreaming maps a JSON object into a []any aligned with columns.
// It uses reverseHeaderMap to look up original keys without copying the record map.
// It also flattens array-of-strings into a joined string.
func recordToRowJSONStreaming(obj map[string]any, columns []string, rev map[string]string, sep string) []any {
	row := make([]any, len(columns))
	for i, col := range columns {
		v, ok := obj[col]
		if !ok {
			if orig, ok2 := rev[col]; ok2 {
				v = obj[orig]
			} else {
				v = nil
			}
		}
		row[i] = normalizeScalarJSONValue(v, sep)
	}
	return row
}

// normalizeScalarJSONValue flattens array-of-strings to a joined string.
// Everything else passes through untouched.
func normalizeScalarJSONValue(v any, sep string) any {
	switch t := v.(type) {
	case nil:
		return nil

	case []string:
		if len(t) == 0 {
			return ""
		}
		return strings.Join(t, sep)

	case []any:
		if len(t) == 0 {
			return ""
		}
		ss := make([]string, 0, len(t))
		for _, it := range t {
			if it == nil {
				continue
			}
			s, ok := it.(string)
			if !ok {
				return v // mixed types; keep original
			}
			ss = append(ss, s)
		}
		if len(ss) == 0 {
			return ""
		}
		return strings.Join(ss, sep)

	default:
		return v
	}
}
