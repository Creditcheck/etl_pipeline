package builtin

import (
	"testing"
	"time"

	"etl/pkg/records"
)

func TestHash_Deterministic_WithTrim(t *testing.T) {
	h := Hash{
		Fields:            []string{"pcv", "cislo_protokolu", "platnost_od", "platnost_do"},
		TargetField:       "row_hash",
		IncludeFieldNames: true,
		TrimSpace:         true,
		Overwrite:         true,
	}

	r1 := records.Record{
		"pcv":             int64(14596725),
		"cislo_protokolu": " ABC-123 ",
		"platnost_od":     time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
		"platnost_do":     time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC),
	}
	r2 := records.Record{
		"pcv":             int64(14596725),
		"cislo_protokolu": "ABC-123",
		"platnost_od":     time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
		"platnost_do":     time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC),
	}

	h.Apply([]records.Record{r1})
	h.Apply([]records.Record{r2})

	s1, ok := r1["row_hash"].(string)
	if !ok || s1 == "" {
		t.Fatalf("expected row_hash string, got=%T val=%v", r1["row_hash"], r1["row_hash"])
	}
	if len(s1) != 64 {
		t.Fatalf("expected sha256 hex length 64, got %d (%q)", len(s1), s1)
	}

	s2 := r2["row_hash"].(string)
	if s1 != s2 {
		t.Fatalf("expected same hash after trimming; s1=%q s2=%q", s1, s2)
	}
}

func TestHash_ChangesWhenFieldChanges(t *testing.T) {
	h := Hash{
		Fields:            []string{"pcv", "cislo_protokolu"},
		TargetField:       "row_hash",
		IncludeFieldNames: true,
		Overwrite:         true,
	}

	a := records.Record{"pcv": int64(1), "cislo_protokolu": "A"}
	b := records.Record{"pcv": int64(1), "cislo_protokolu": "B"}

	h.Apply([]records.Record{a})
	h.Apply([]records.Record{b})

	if a["row_hash"] == b["row_hash"] {
		t.Fatalf("expected different hashes when inputs differ; both=%v", a["row_hash"])
	}
}

func TestHash_MissingVsEmptyDifferent(t *testing.T) {
	h := Hash{
		Fields:            []string{"pcv", "cislo_protokolu"},
		TargetField:       "row_hash",
		IncludeFieldNames: true,
		Overwrite:         true,
	}

	missing := records.Record{"pcv": int64(1)}
	empty := records.Record{"pcv": int64(1), "cislo_protokolu": ""}

	h.Apply([]records.Record{missing})
	h.Apply([]records.Record{empty})

	if missing["row_hash"] == empty["row_hash"] {
		t.Fatalf("expected different hashes for missing vs empty; got=%v", missing["row_hash"])
	}
}

func TestHash_OverwriteFalsePreservesExisting(t *testing.T) {
	h := Hash{
		Fields:      []string{"pcv"},
		TargetField: "row_hash",
		Overwrite:   false,
	}

	r := records.Record{
		"pcv":      int64(123),
		"row_hash": "preexisting",
	}

	h.Apply([]records.Record{r})

	if got := r["row_hash"]; got != "preexisting" {
		t.Fatalf("expected preexisting preserved, got=%v", got)
	}
}

func BenchmarkHashApply(b *testing.B) {
	h := Hash{
		Fields:            []string{"pcv", "typ", "stav", "kod_stk", "cislo_protokolu", "platnost_od", "platnost_do"},
		TargetField:       "row_hash",
		IncludeFieldNames: true,
		TrimSpace:         true,
		Overwrite:         true,
	}

	const n = 10_000
	recs := make([]records.Record, n)

	t0 := time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		recs[i] = records.Record{
			"pcv":             int64(10_000_000 + i),
			"typ":             " A ",
			"stav":            "OK",
			"kod_stk":         i % 200,
			"cislo_protokolu": " P-" + itoaBench(i) + " ",
			"platnost_od":     t0,
			"platnost_do":     t0.AddDate(1, 0, 0),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Apply(recs)
	}
}

func itoaBench(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [16]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + (i % 10))
		i /= 10
	}
	return string(buf[pos:])
}
