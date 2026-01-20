package transformer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// HashSpec describes how to derive a row hash.
type HashSpec struct {
	Algorithm         string
	Encoding          string
	Fields            []string
	IncludeFieldNames bool
	Overwrite         bool
	Separator         string
	TargetField       string
	TrimSpace         bool
}

func HashLoopRows(
	ctx context.Context,
	columns []string,
	in <-chan *Row,
	out chan<- *Row,
	spec HashSpec,
	onReject func(line int, reason string),
) {
	targetIdx := indexOf(columns, spec.TargetField)
	if targetIdx < 0 {
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

	var b strings.Builder
	var scratch [64]byte

	for r := range in {
		// On cancellation: drain without re-pooling (prevents reuse races).
		select {
		case <-ctx.Done():
			if r != nil {
				r.Drop()
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

		sum := sha256.Sum256([]byte(b.String()))
		r.V[targetIdx] = hex.EncodeToString(sum[:])

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

func appendCanonicalValue(b *strings.Builder, v any, trimSpace bool, scratch *[64]byte) {
	switch t := v.(type) {
	case nil:
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

	default:
		// Fallback: stable-ish representation
		b.WriteString(fmt.Sprintf("%v", t))
	}
}

func hasEdgeSpace(s string) bool {
	if s == "" {
		return false
	}
	return s[0] == ' ' || s[len(s)-1] == ' ' || s[0] == '\t' || s[len(s)-1] == '\t'
}

func strconvAppendInt(dst []byte, v int64) []byte {
	return strconv.AppendInt(dst, v, 10)
}

func strconvAppendUint(dst []byte, v uint64) []byte {
	return strconv.AppendUint(dst, v, 10)
}
