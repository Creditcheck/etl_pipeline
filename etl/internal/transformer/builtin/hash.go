// Package builtin contains simple, reusable transformers used in the ETL.
package builtin

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"etl/pkg/records"
)

// Hash computes a deterministic SHA-256 hash from selected fields and writes it
// into a target field on each record.
//
// This is designed to create a stable, always-non-null dedupe key for fact rows,
// avoiding UNIQUE/ON CONFLICT behavior issues when some natural-key columns can
// be NULL (Postgres treats NULLs as distinct for UNIQUE constraints).
//
// JSON options expected by this transform (typical):
//
//	{
//	  "kind": "hash",
//	  "options": {
//	    "fields": ["pcv","typ","stav","cislo_protokolu","platnost_od","platnost_do"],
//	    "target_field": "row_hash",
//	    "include_field_names": true,
//	    "trim_space": true,
//	    "overwrite": true,
//	    "separator": "\u001f"
//	  }
//	}
//
// Behavior / canonicalization rules:
//   - Fields are concatenated in the given order using Separator.
//   - Missing or nil values are encoded as a single NUL byte (0x00) so missing
//     differs from empty-string.
//   - Common types are converted without fmt.Sprint for speed.
//   - time.Time values are encoded as RFC3339Nano in UTC.
//   - Output is a lowercase hex string (length 64).
type Hash struct {
	// Fields is the ordered list of input fields used to compute the hash.
	Fields []string

	// TargetField is where the computed hash is stored.
	TargetField string

	// IncludeFieldNames includes "field=value" in the canonical form.
	// This reduces accidental collisions when many fields are missing/empty.
	IncludeFieldNames bool

	// Separator used between field components in the canonical string.
	// If empty, defaults to ASCII Unit Separator (0x1f).
	Separator string

	// Overwrite controls whether an existing TargetField is replaced.
	// If false and TargetField exists, the record is left unchanged.
	Overwrite bool

	// TrimSpace trims leading/trailing ASCII whitespace for string/[]byte values
	// before hashing (using HasEdgeSpace to avoid work in the hot path).
	TrimSpace bool
}

// Apply computes hashes and mutates records in-place.
func (h Hash) Apply(in []records.Record) []records.Record {
	if len(in) == 0 {
		return in
	}
	if h.TargetField == "" || len(h.Fields) == 0 {
		return in
	}

	sep := h.Separator
	if sep == "" {
		sep = "\x1f"
	}

	for _, r := range in {
		if r == nil {
			continue
		}
		if !h.Overwrite {
			if _, exists := r[h.TargetField]; exists {
				continue
			}
		}

		sum := hashRecord(r, h.Fields, sep, h.IncludeFieldNames, h.TrimSpace)
		r[h.TargetField] = hex.EncodeToString(sum[:])
	}

	return in
}

func hashRecord(r records.Record, fields []string, sep string, includeNames bool, trimSpace bool) [sha256.Size]byte {
	var b strings.Builder

	// Heuristic: reduce reallocs for common short-ish fields.
	// If fields are long, Builder will grow as needed.
	b.Grow(len(fields) * 20)

	for i, f := range fields {
		if i > 0 {
			b.WriteString(sep)
		}
		if includeNames {
			b.WriteString(f)
			b.WriteByte('=')
		}

		v, ok := r[f]
		if !ok || v == nil {
			b.WriteByte('\x00')
			continue
		}

		appendCanonicalValue(&b, v, trimSpace)
	}

	return sha256.Sum256([]byte(b.String()))
}

// appendCanonicalValue appends a stable, canonical representation of v.
// It avoids fmt.Sprint for common types to reduce allocations.
func appendCanonicalValue(b *strings.Builder, v any, trimSpace bool) {
	switch t := v.(type) {
	case nil:
		b.WriteByte('\x00')

	case string:
		if trimSpace && HasEdgeSpace(t) {
			b.WriteString(strings.TrimSpace(t))
		} else {
			b.WriteString(t)
		}

	case []byte:
		if !trimSpace {
			b.Write(t)
			return
		}
		// If trimming is requested, we need string semantics.
		s := string(t)
		if HasEdgeSpace(s) {
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
		b.WriteString(strconv.Itoa(t))
	case int8:
		b.WriteString(strconv.FormatInt(int64(t), 10))
	case int16:
		b.WriteString(strconv.FormatInt(int64(t), 10))
	case int32:
		b.WriteString(strconv.FormatInt(int64(t), 10))
	case int64:
		b.WriteString(strconv.FormatInt(t, 10))

	case uint:
		b.WriteString(strconv.FormatUint(uint64(t), 10))
	case uint8:
		b.WriteString(strconv.FormatUint(uint64(t), 10))
	case uint16:
		b.WriteString(strconv.FormatUint(uint64(t), 10))
	case uint32:
		b.WriteString(strconv.FormatUint(uint64(t), 10))
	case uint64:
		b.WriteString(strconv.FormatUint(t, 10))

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
		b.WriteString(fmt.Sprint(t))
	}
}
