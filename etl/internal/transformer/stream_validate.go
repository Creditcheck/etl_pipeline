// Package transformer contains streaming validation utilities. This file hosts
// the *drain-safe* ValidateLoopRows stage, which enforces required non-NULL
// fields and drops invalid rows without ever returning early on context
// cancellation, thereby preventing upstream backpressure/hangs.
package transformer

import (
	"context"
	"etl/internal/transformer/builtin"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ValidateLoopRows performs lightweight, streaming validation over pooled rows.
//
// It enforces two classes of constraints:
//
//  1. Required fields: any column listed in 'required' must be non-nil.
//     (The caller is expected to derive this list from the validation contract.)
//
//  2. Type compatibility: any column present in 'colKinds' must have a Go
//     value that is compatible with the declared kind. The supported kinds are:
//
//     - "int"   : integer/bigint-compatible values (int, int64, numeric strings)
//     - "bool"  : boolean-compatible values (bool or string in truthy/falsy sets)
//     - "date"  : time.Time (non-zero) or empty string for nullable
//
// Rows that fail either check are dropped (fail-soft) and optionally reported
// via onReject. The function NEVER returns early on context cancellation; it
// drains the entire input channel, freeing each row, to avoid upstream hangs.
//
// Closing semantics:
//   - The caller is responsible for closing 'out' after this function returns.
func ValidateLoopRows(
	ctx context.Context,
	columns []string, // positional order (same as TransformLoopRows)
	required []string, // names of required columns
	colKinds map[string]string, // column name -> normalized kind ("int","bool","date",...)
	in <-chan *Row, // input rows (already coerced)
	out chan<- *Row, // output rows (valid only)
	onReject func(line int, reason string),
) {
	// Build column name -> index map.
	pos := make(map[string]int, len(columns))
	for i, c := range columns {
		pos[c] = i
	}

	// Precompute required column indexes.
	reqIx := make([]int, 0, len(required))
	for _, name := range required {
		if ix, ok := pos[name]; ok {
			reqIx = append(reqIx, ix)
		}
	}

	// Precompute type metadata per column index.
	type colMeta struct {
		kind string
	}
	colMetaByIdx := make([]colMeta, len(columns))
	for i, name := range columns {
		if kind, ok := colKinds[name]; ok {
			colMetaByIdx[i] = colMeta{kind: kind}
		}
	}

	for r := range in {
		// Even if ctx is canceled, we must *drain and Free*; do not return early.
		if r == nil || len(r.V) != len(columns) {
			if r != nil {
				r.Free()
			}
			continue
		}

		ok := true
		reason := ""

		// 1) Required-field check.
		for _, ix := range reqIx {
			if ix < 0 || ix >= len(r.V) || r.V[ix] == nil {
				ok = false
				reason = fmt.Sprintf("required field %q missing", columns[ix])
				break
			}
		}

		// 2) Type compatibility check for any contract-typed columns.
		if ok {
			for idx, meta := range colMetaByIdx {
				if meta.kind == "" {
					continue // no type constraint for this column
				}
				if idx < 0 || idx >= len(r.V) {
					continue
				}

				v := r.V[idx]
				if v == nil {
					// Nullable field: allowed for non-required columns.
					continue
				}

				switch meta.kind {
				case "int":
					if !isIntLike(v) {
						ok = false
						reason = fmt.Sprintf("field %q: %v not a valid int", columns[idx], v)
					}
				case "bool":
					if !isBoolLike(v) {
						ok = false
						reason = fmt.Sprintf("field %q: %v not a valid bool", columns[idx], v)
					}
				case "date":
					if !isDateLike(v) {
						ok = false
						reason = fmt.Sprintf("field %q: %v not a valid date", columns[idx], v)
					}
				default:
					// reason = fmt.Sprintf("field %q: %v is an unknown type", columns[idx], v)

					// Unknown/unsupported kind -> no extra check; DB/COPY will enforce.
				}

				if !ok {
					break
				}
			}
		}

		if !ok {
			if onReject != nil {
				onReject(r.Line, reason)
			}
			r.Free()
			continue
		}

		// Forward; the downstream stage is responsible for honoring ctx.
		//		out <- r
		select {
		case out <- r:
		case <-ctx.Done():
			r.Free()
		}

	}
}

// isIntLike reports whether v can be safely treated as an integer/bigint.
//
// It accepts:
//   - int/uint families
//   - float64 that is a whole number
//   - string that parses as base-10 int64
//   - empty string (treated as "no value"; required handled separately)
func isIntLike(v any) bool {
	switch t := v.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return true
	case float64:
		return t == float64(int64(t))
	case string:
		var s string
		// wrapped in HasEdgeSpace to save CPU cycles in hot-path
		if builtin.HasEdgeSpace(t) {
			s = strings.TrimSpace(t)
		} else {
			s = t
		}
		if s == "" {
			return true
		}
		_, err := strconv.ParseInt(s, 10, 64)
		return err == nil
	default:
		return false
	}
}

// Default truthy/falsy sets (lowercased). Includes Czech "ano"/"ne".
var (
	defaultTruthy = map[string]struct{}{
		"1": {}, "t": {}, "true": {}, "yes": {}, "y": {}, "ano": {},
	}
	defaultFalsy = map[string]struct{}{
		"0": {}, "f": {}, "false": {}, "no": {}, "n": {}, "ne": {},
	}
)

// isBoolLike reports whether v can be treated as a boolean. It accepts:
//
//   - bool values
//   - strings in the truthy/falsy sets (case-insensitive)
//   - empty string (treated as "no value"; required handled separately)
func isBoolLike(v any) bool {
	switch t := v.(type) {
	case bool:
		return true
	case string:
		var s string
		// wrapped in HasEdgeSpace to save CPU cycles in hot-path
		if builtin.HasEdgeSpace(t) {
			s = strings.ToLower(strings.TrimSpace(t))
		} else {
			s = strings.ToLower(t)
		}
		if s == "" {
			return true
		}
		if _, ok := defaultTruthy[s]; ok {
			return true
		}
		if _, ok := defaultFalsy[s]; ok {
			return true
		}
		return false
	default:
		return false
	}
}

// isDateLike reports whether v is compatible with a date/time column.
//
// We expect the coerce step to convert date fields to time.Time. For streaming,
// we accept non-zero time.Time or an empty string (nullable). Any other value
// is considered invalid.
func isDateLike(v any) bool {
	switch t := v.(type) {
	case time.Time:
		return !t.IsZero()
	case string:
		var s string
		// wrapped in HasEdgeSpace to save CPU cycles in hot-path
		if builtin.HasEdgeSpace(t) {
			s = strings.TrimSpace(t)
		} else {
			s = t
		}
		return s == "" // empty treated as "no value"; required handled above
	default:
		return false
	}
}
