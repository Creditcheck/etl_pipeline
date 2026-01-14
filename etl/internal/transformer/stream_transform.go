// Package transformer provides streaming, allocation-conscious transforms that
// convert parsed CSV rows ([]string) into database-ready rows ([]any) using a
// positional mapping derived from pipeline configuration.
//
// Design goals:
//   - No whole-file buffering; rows flow via channels.
//   - Avoid per-row map lookups; precompile a per-column coercion plan.
//   - Keep coercion fast and predictable; use zero-alloc date parsing where
//     possible (e.g., "02.01.2006").
//   - 12-factor friendly: all knobs are passed in by the caller.
//
// This file intentionally does not depend on Postgres; it just produces rows.
package transformer

import (
	"context"
	"etl/internal/transformer/builtin"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AnchorRule describes an optional "alignment anchor" used to salvage rows
// whose column values have shifted due to parse anomalies.
//
// If enabled, the transform stage:
//   - looks for AnchorRule.Value in any column
//   - if found in a column != AnchorRule.Field,
//     realigns the row so that the value lands in AnchorRule.Field
type AnchorRule struct {
	Field string // name of the target column (e.g. "zarazeni_vozidla")
	Value string // anchor value (e.g. "RSV")
}

// CoerceSpec describes how to coerce string fields into typed values.
// Typical source is the pipeline's "coerce" transform options.
type CoerceSpec struct {
	// Types maps column name -> target type: "int" | "bool" | "date" | "text".
	// Unrecognized or missing types default to "text" (pass-through).
	Types map[string]string
	// Layout is an optional default date layout (e.g., "02.01.2006").
	Layout string
	// Truthy/Falsy are optional custom boolean vocabularies. If empty,
	// a broad default set is used (including "ano"/"ne").
	Truthy []string
	Falsy  []string

	// Optional alignment anchor. Zero-value means disabled.
	Anchor *AnchorRule
}

// TransformLoopRows applies the compiled coercion plan (derived from spec) to
// each incoming pooled Row and forwards successfully coerced rows to 'out'.
//
// Semantics:
//
//   - For every row received on 'in', TransformLoopRows attempts to coerce each
//     column value in-place according to the provided CoerceSpec and the
//     positional 'columns' slice.
//   - If coercion for all columns succeeds, the row is sent downstream on 'out'
//     and ownership transfers to the next stage.
//   - If coercion fails for any column, the row is dropped (fail-soft), the row
//     is returned to the pool via r.Free(), and the error is reported through
//     onReject, if provided.
//   - The function never returns early on context cancellation. It drains the
//     'in' channel, freeing each row, to avoid upstream goroutine leaks.
//
// Parameters:
//
//   - ctx:      cancellation context for the overall pipeline. Cancellation
//     prevents further forwarding to 'out' but rows are still drained
//     from 'in' and freed.
//   - columns:  positional list of column names; len(columns) must match len(r.V).
//   - in:       upstream channel of pooled rows (typically from the tap stage).
//   - out:      downstream channel for successfully coerced rows (to validator).
//   - spec:     high-level coercion specification (types, date layout, etc.).
//   - onReject: optional callback invoked when a row is dropped due to coercion
//     failure. It receives the row's logical line number (if known)
//     and a human-readable reason string.
//
// Closing semantics:
//
//   - TransformLoopRows does not close 'out'; the caller must close it after all
//     transform goroutines have completed.
//   - All rows received from 'in' are eventually freed, regardless of ctx.
func TransformLoopRows(
	ctx context.Context,
	columns []string,
	in <-chan *Row,
	out chan<- *Row,
	spec CoerceSpec,
	onReject func(line int, reason string),
) {
	// buildCoercePlan is assumed to be your existing helper that compiles the
	// high-level CoerceSpec into a hot-path plan with per-column coerce funcs.
	//	plan := buildCoercePlan(columns, spec)

	plan := compilePlan(columns, spec)

	for r := range in {
		// Even if ctx is canceled, we must *drain and Free*; do not return early.
		if r == nil || len(r.V) != len(columns) {
			if r != nil {
				r.Free()
			}
			continue
		}

		// If the context has been cancelled, we no longer forward rows
		// downstream but we still drain and Free them to avoid backpressure.
		select {
		case <-ctx.Done():
			r.Free()
			continue
		default:
		}

		ok := true

		// Hot path: normalize/convert in place, avoiding extra allocations.
		for i := range columns {
			raw := r.V[i]
			if raw == nil {
				// Keep NULL as-is.
				continue
			}

			s, _ := raw.(string) // parser fills strings
			// Normalize edge whitespace only when necessary to save CPU.
			if builtin.HasEdgeSpace(s) {
				s = strings.TrimSpace(s)
			}

			// Empty string becomes NULL.
			if s == "" {
				r.V[i] = nil
				continue
			}

			if spec.Anchor != nil {
				applyAnchorRealignment(columns, r, spec.Anchor)
			}

			// Delegate to the compiled coerce plan for this column. The plan may
			// parse integers, dates, booleans, etc. in-place into r.V[i].
			if !plan.cols[i].coerce(&r.V[i], s) {
				ok = false
				if onReject != nil {
					onReject(
						r.Line,
						fmt.Sprintf("field %q: cannot coerce %q", columns[i], s),
					)
				}
				break
			}
		}

		if !ok {
			// Row failed coercion; drop it (fail-soft) and return to pool.
			r.Free()
			continue
		}

		// Successfully coerced row; forward to the next stage. The downstream
		// consumer is now responsible for calling r.Free() after persistence.
		out <- r
	}
}

// TransformLoop reads parsed rows from 'in', coerces them according to the
// provided columns (positional order) and CoerceSpec, and sends database-ready
// rows ([]any) to 'out'. The function returns when 'in' is closed or the
// context is canceled.
//
// Errors for individual rows are non-fatal: the row is dropped and processing
// continues. The caller can maintain counters externally (e.g., via a side
// channel) if they need observability for rejects.
//
// Performance notes:
//   - A per-column "plan" is compiled once to avoid per-row map lookups.
//   - Date parsing uses a zero-alloc fast path for the common CZ layout
//     "02.01.2006", falling back to time.Parse otherwise.
func TransformLoop(
	ctx context.Context,
	columns []string, // storage.Postgres.Columns (positional)
	in <-chan []string, // raw parsed values aligned to the same order
	out chan<- []any, // typed values aligned to columns
	spec CoerceSpec, // coercion rules
) {
	plan := compilePlan(columns, spec)

	for raw := range in {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if len(raw) != len(columns) {
			// Width mismatch: drop softly.
			continue
		}

		// NOTE: true pooling would require the loader to return rows.
		// Without that contract, we allocate one row slice per record to
		// avoid cross-goroutine reuse bugs.
		row := make([]any, len(columns))

		ok := true
		for i := range columns {
			// Trim once per field.
			field := strings.TrimSpace(raw[i])
			if !plan.cols[i].coerce(&row[i], field) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}

		select {
		case out <- row:
		case <-ctx.Done():
			return
		}
	}
}

// --- plan compilation ---------------------------------------------------------

// kind enumerates the coercion operation for a column.
type kind uint8

const (
	kindText kind = iota
	kindInt
	kindBool
	kindDate
)

type boolVocab struct {
	custom bool
	yes    map[string]struct{}
	no     map[string]struct{}
}

type datePlan struct {
	layout    string
	useCZFast bool // enable zero-alloc "02.01.2006" path
}

type colPlan struct {
	k  kind
	bv boolVocab
	dp datePlan
}

// compiledPlan holds a per-column coercion plan with inline function pointers.
// Using tiny closures here keeps the hot loop branchless and avoids map lookups.
type compiledPlan struct {
	cols []struct {
		coerce func(dst *any, s string) bool
	}
}

func compilePlan(columns []string, spec CoerceSpec) compiledPlan {
	cols := make([]struct {
		coerce func(dst *any, s string) bool
	}, len(columns))

	// Prepare bool vocab (case-insensitive) once.
	truthy := lowerSet(spec.Truthy)
	falsy := lowerSet(spec.Falsy)
	useCustomBools := len(truthy) > 0 || len(falsy) > 0

	// Normalize default type names and prebuild per-column coercers.
	for i, col := range columns {
		typ := strings.ToLower(spec.Types[col])
		if typ == "" {
			typ = "text"
		}

		switch typ {
		// Numeric.
		case "int", "bigint", "int64":
			// We store integers as int64 throughout the pipeline to match Postgres BIGINT
			// and avoid platform-dependent int widths.
			cols[i].coerce = func(dst *any, s string) bool {
				if s == "" {
					// Treat empty as invalid for int.
					return false
				}
				v, ok := toIntFast(s)
				if !ok {
					return false
				}
				*dst = v
				return true
			}

		case "bool":
			cols[i].coerce = func(dst *any, s string) bool {
				if s == "" {
					return false
				}
				if useCustomBools {
					ls := strings.ToLower(s)
					if _, ok := truthy[ls]; ok {
						*dst = true
						return true
					}
					if _, ok := falsy[ls]; ok {
						*dst = false
						return true
					}
					return false
				}
				v, err := strconv.ParseBool(s)
				if err != nil {
					return false
				}
				*dst = v
				return true
			}

		case "date":
			// (Existing date logic stays the same. Keeping your original implementation.)
			dp := datePlan{layout: spec.Layout}
			if dp.layout == "" {
				dp.layout = "02.01.2006"
			}
			dp.useCZFast = (dp.layout == "02.01.2006")

			cols[i].coerce = func(dst *any, s string) bool {
				if s == "" {
					return false
				}
				// Fast CZ date parsing for 02.01.2006
				if dp.useCZFast {
					if t, ok := parseCZDate(s); ok {
						*dst = t
						return true
					}
				}
				// Fallback: configured layout
				if t, err := time.Parse(dp.layout, s); err == nil {
					*dst = t
					return true
				}
				// Try ISO-8601 (date only) as a common alternative.
				if t, err := time.Parse("2006-01-02", s); err == nil {
					*dst = t
					return true
				}
				// Last resort: explicit CZ if not already tried.
				if !dp.useCZFast {
					if t, ok := parseCZDate(s); ok {
						*dst = t
						return true
					}
				}
				return false
			}

		// Text.
		case "text", "string":
			fallthrough
		default:
			// Pass-through trimmed string.
			cols[i].coerce = func(dst *any, s string) bool {
				*dst = s
				return true
			}
		}
	}

	return compiledPlan{cols: cols}
}

// --- helpers (fast, allocation-conscious) -------------------------------------

// lowerSet builds a lowercased membership set. Empty input returns nil to
// allow a quick "custom disabled" check.
func lowerSet(in []string) map[string]struct{} {
	if len(in) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(in))
	for _, s := range in {
		m[strings.ToLower(strings.TrimSpace(s))] = struct{}{}
	}
	return m
}

// toIntFast parses integers quickly and only falls back to float parsing when
// the field contains a '.' (supporting inputs like "42.0").
func toIntFast(s string) (int64, bool) {
	// Try fast int first.
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, true
	}
	// Only consider float fallback if there is a dot; avoids extra work.
	if strings.IndexByte(s, '.') >= 0 {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			if f == float64(int64(f)) {
				return int64(f), true
			}
		}
	}
	return 0, false
}

// toBoolFast resolves booleans with optional custom vocabularies.
// Returns (value, ok). Empty strings are invalid (ok=false) so that the row
// can be rejected by policy.
func toBoolFast(s string, custom bool, truthy, falsy map[string]struct{}) (bool, bool) {
	ls := strings.ToLower(s)
	if ls == "" {
		return false, false
	}
	if custom {
		if _, ok := truthy[ls]; ok {
			return true, true
		}
		if _, ok := falsy[ls]; ok {
			return false, true
		}
		return false, false
	}
	switch ls {
	case "1", "t", "true", "yes", "y", "ano":
		return true, true
	case "0", "f", "false", "no", "n", "ne":
		return false, true
	default:
		return false, false
	}
}

// parseCZDate implements a zero-allocation parser for "02.01.2006" (DD.MM.YYYY).
// It returns (time.Time, true) on success, or (zero, false) on any invalid input.
// This avoids time.Parse overhead in the common case.
func parseCZDate(s string) (time.Time, bool) {
	if len(s) < 10 || s[2] != '.' || s[5] != '.' {
		return time.Time{}, false
	}
	d1, d0 := s[0]-'0', s[1]-'0'
	m1, m0 := s[3]-'0', s[4]-'0'
	y3, y2, y1, y0 := s[6]-'0', s[7]-'0', s[8]-'0', s[9]-'0'
	if d1 > 9 || d0 > 9 || m1 > 9 || m0 > 9 || y3 > 9 || y2 > 9 || y1 > 9 || y0 > 9 {
		return time.Time{}, false
	}
	day := int(d1)*10 + int(d0)
	mon := int(m1)*10 + int(m0)
	year := int(y3)*1000 + int(y2)*100 + int(y1)*10 + int(y0)
	if mon < 1 || mon > 12 || day < 1 || day > 31 {
		return time.Time{}, false
	}
	return time.Date(year, time.Month(mon), day, 0, 0, 0, 0, time.UTC), true
}

// BuildCoerceSpecFromTypes constructs a CoerceSpec from a map[column]type and
// a date layout. This is a convenience for callers that already parsed the
// "coerce" transform options from the pipeline configuration.
func BuildCoerceSpecFromTypes(types map[string]string, layout string, truthy, falsy []string) CoerceSpec {
	cp := make(map[string]string, len(types))
	for k, v := range types {
		cp[k] = v
	}
	return CoerceSpec{
		Types:  cp,
		Layout: layout,
		Truthy: truthy,
		Falsy:  falsy,
	}
}

// ValidateSpecSanity performs basic validation of the given spec and columns.
// It returns an error if the coercion rules mention columns that are not in the
// provided positional 'columns' slice.
func ValidateSpecSanity(columns []string, spec CoerceSpec) error {
	pos := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		pos[c] = struct{}{}
	}
	for k := range spec.Types {
		if _, ok := pos[k]; !ok {
			return fmt.Errorf("coerce spec references unknown column %q", k)
		}
	}
	return nil
}

// applyAnchorRealignment repositions a known constant "anchor" so that its
// value appears in the designated column. This repairs common CSV shifts
// caused by stray delimiters or broken quotes.
//
// Behavior:
//   - If anchor disabled → do nothing
//   - Find the column index of Anchor.Field
//   - If row already has anchor.Value in that column → OK
//   - Otherwise find anchor.Value anywhere else → j
//   - Shift tail so r.V[j] becomes r.V[targetIdx]
func applyAnchorRealignment(columns []string, r *Row, ar *AnchorRule) {
	if r == nil || ar == nil {
		return
	}

	// Find anchor field index
	anchorIdx := -1
	for i, name := range columns {
		if name == ar.Field {
			anchorIdx = i
			break
		}
	}
	if anchorIdx < 0 {
		return
	}

	// Already correctly aligned?
	if s, ok := r.V[anchorIdx].(string); ok && s == ar.Value {
		return
	}

	// Find where the anchor value actually appears
	valueIdx := -1
	for i, v := range r.V {
		if s, ok := v.(string); ok && s == ar.Value {
			valueIdx = i
			break
		}
	}
	if valueIdx < 0 || valueIdx == anchorIdx {
		return
	}

	// Re-align: shift slice [anchorIdx:] so that valueIdx moves to anchorIdx.
	newV := make([]any, len(r.V))
	copy(newV[:anchorIdx], r.V[:anchorIdx])

	shift := valueIdx - anchorIdx
	for dst := anchorIdx; dst < len(r.V); dst++ {
		src := dst + shift
		if src >= anchorIdx && src < len(r.V) {
			newV[dst] = r.V[src]
		} else {
			newV[dst] = nil
		}
	}

	r.V = newV
}
