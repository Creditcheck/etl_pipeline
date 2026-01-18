package transformer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// HashSpec configures the streaming hash transform.
//
// The transform computes a deterministic hash over a set of fields from the
// current row and writes the resulting hash string into TargetField.
//
// The canonical string is built in the declared order of Fields. Field values
// are rendered in a stable, low-allocation way. By default, a unit-separator
// delimiter (0x1F) is used, which is unlikely to appear in real CSV data.
//
// Notes:
//   - Purely streaming and backend-agnostic.
//   - Hashes typed values (after coercion) for stability.
//   - Supports JSON arrays/objects (e.g. []any, map[string]any) deterministically.
type HashSpec struct {
	// TargetField is the destination column name (must exist in the active column set).
	TargetField string

	// Fields are the input column names hashed in order.
	Fields []string

	// Algorithm is currently only "sha256".
	Algorithm string

	// Encoding is currently only "hex".
	Encoding string

	// Separator is inserted between fields in the canonical string.
	Separator string

	// IncludeFieldNames toggles "field=value" vs just "value" per input.
	IncludeFieldNames bool

	// TrimSpace trims string inputs prior to hashing.
	TrimSpace bool
}

// HashLoopRows computes a hash per row and writes it into TargetField in-place.
//
// Semantics:
//   - Reads pooled *Row from 'in'.
//   - Computes a deterministic hash from spec.Fields.
//   - Writes the resulting string into r.V[targetIndex].
//   - Forwards the row to 'out'.
//   - On missing field indexes, rejects the row: calls onReject and Free()s it.
//
// Closing semantics:
//   - HashLoopRows does not close 'out'.
//   - On ctx cancellation, it drains 'in' and frees rows to avoid leaks.
func HashLoopRows(
	ctx context.Context,
	columns []string,
	in <-chan *Row,
	out chan<- *Row,
	spec HashSpec,
	onReject func(line int, reason string),
) {
	// Resolve indexes once to avoid per-row map allocations.
	targetIdx := indexOf(columns, spec.TargetField)
	if targetIdx < 0 {
		// Hard misconfiguration: drain and free so upstream doesn't hang.
		for r := range in {
			if r != nil {
				r.Free()
			}
		}
		return
	}

	fieldIdx := make([]int, len(spec.Fields))
	for i, name := range spec.Fields {
		fieldIdx[i] = indexOf(columns, name)
	}

	sep := spec.Separator
	if sep == "" {
		sep = "\x1f"
	}

	// Per-goroutine reusable builder/buffer to reduce allocations.
	var b strings.Builder
	var scratch [64]byte

	for r := range in {
		// Always free rows promptly on cancellation to avoid backpressure leaks.
		select {
		case <-ctx.Done():
			if r != nil {
				r.Free()
			}
			continue
		default:
		}

		if r == nil || len(r.V) != len(columns) {
			if r != nil {
				r.Free()
			}
			continue
		}

		// Validate required field indexes exist for this spec.
		reject := false
		for i, idx := range fieldIdx {
			if idx < 0 || idx >= len(r.V) {
				reject = true
				if onReject != nil {
					onReject(r.Line, fmt.Sprintf("hash: missing field %q", spec.Fields[i]))
				}
				break
			}
		}
		if reject {
			r.Free()
			continue
		}

		// Build canonical representation.
		b.Reset()
		for i, idx := range fieldIdx {
			if i > 0 {
				b.WriteString(sep)
			}
			if spec.IncludeFieldNames {
				b.WriteString(spec.Fields[i])
				b.WriteByte('=')
			}
			appendCanonicalValue(&b, r.V[idx], spec.TrimSpace, &scratch)
		}

		// Hash canonical string (sha256).
		sum := sha256.Sum256([]byte(b.String()))
		r.V[targetIdx] = hex.EncodeToString(sum[:])

		// Forward downstream (ownership transfers).
		out <- r
	}
}

func indexOf(cols []string, name string) int {
	if name == "" {
		return -1
	}
	for i, c := range cols {
		if c == name {
			return i
		}
	}
	return -1
}

// appendCanonicalValue writes a stable representation of v into b.
// It avoids fmt.Sprint for common types to reduce allocations.
// Supported types include those produced by the streaming coerce path AND
// JSON parser types (arrays/objects).
func appendCanonicalValue(b *strings.Builder, v any, trimSpace bool, scratch *[64]byte) {
	switch t := v.(type) {
	case nil:
		// Represent NULL explicitly so it affects the hash deterministically.
		b.WriteString("null")

	case string:
		if trimSpace && hasEdgeSpace(t) {
			t = strings.TrimSpace(t)
		}
		b.WriteString(t)

	case []byte:
		s := string(t)
		if trimSpace && hasEdgeSpace(s) {
			s = strings.TrimSpace(s)
		}
		b.WriteString(s)

	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}

	case int:
		b.Write(strconvAppendInt(scratch[:0], int64(t)))
	case int8:
		b.Write(strconvAppendInt(scratch[:0], int64(t)))
	case int16:
		b.Write(strconvAppendInt(scratch[:0], int64(t)))
	case int32:
		b.Write(strconvAppendInt(scratch[:0], int64(t)))
	case int64:
		b.Write(strconvAppendInt(scratch[:0], t))

	case uint:
		b.Write(strconvAppendUint(scratch[:0], uint64(t)))
	case uint8:
		b.Write(strconvAppendUint(scratch[:0], uint64(t)))
	case uint16:
		b.Write(strconvAppendUint(scratch[:0], uint64(t)))
	case uint32:
		b.Write(strconvAppendUint(scratch[:0], uint64(t)))
	case uint64:
		b.Write(strconvAppendUint(scratch[:0], t))

	case float32:
		b.WriteString(strconv.FormatFloat(float64(t), 'g', -1, 32))
	case float64:
		b.WriteString(strconv.FormatFloat(t, 'g', -1, 64))

	case time.Time:
		tt := t
		if !tt.IsZero() {
			tt = tt.UTC()
		}
		b.WriteString(tt.Format(time.RFC3339Nano))

	// JSON array cases: make the canonical form deterministic.
	case []string:
		// JSON encoding is stable for slices.
		enc, err := json.Marshal(t)
		if err != nil {
			// Extremely unlikely; fallback.
			b.WriteString(fmt.Sprint(t))
			return
		}
		b.Write(enc)

	case []any:
		enc, err := json.Marshal(t)
		if err != nil {
			b.WriteString(fmt.Sprint(t))
			return
		}
		b.Write(enc)

	// JSON object case: enforce stable key order before encoding.
	case map[string]any:
		appendCanonicalMap(b, t, trimSpace, scratch)

	default:
		// Fallback for uncommon types.
		b.WriteString(fmt.Sprint(t))
	}
}

func appendCanonicalMap(b *strings.Builder, m map[string]any, trimSpace bool, scratch *[64]byte) {
	if m == nil {
		b.WriteString("null")
		return
	}

	// Stable key order.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		// Key is always string.
		b.WriteString(strconv.Quote(k))
		b.WriteByte(':')

		// Value: reuse canonical rules. For nested maps/slices, this stays deterministic.
		appendCanonicalValue(b, m[k], trimSpace, scratch)
	}
	b.WriteByte('}')
}

func strconvAppendInt(dst []byte, v int64) []byte {
	if v == 0 {
		return append(dst, '0')
	}
	neg := v < 0
	if neg {
		v = -v
	}

	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return append(dst, buf[i:]...)
}

func strconvAppendUint(dst []byte, v uint64) []byte {
	if v == 0 {
		return append(dst, '0')
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return append(dst, buf[i:]...)
}

func hasEdgeSpace(s string) bool {
	if len(s) == 0 {
		return false
	}
	// This is the common fast-path check used elsewhere in the repo: avoid
	// TrimSpace when we can.
	return s[0] <= ' ' || s[len(s)-1] <= ' '
}
