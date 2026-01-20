package probe

import (
	"reflect"
	"testing"

	"etl/internal/config"
	csvparser "etl/internal/parser/csv"
)

// TestNormalizeBackendKind verifies backend normalization behavior as implemented.
//
// Note: The current implementation defaults unknown/unsupported values to
// "postgres". It also does not treat "sqlite3" as an alias.
func TestNormalizeBackendKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     string
		expect string
	}{
		{"postgres canonical", "postgres", "postgres"},
		{"postgres casing", "Postgres", "postgres"},
		{"mssql canonical", "mssql", "mssql"},
		{"mssql casing", "MSSQL", "mssql"},
		{"sqlite canonical", "sqlite", "sqlite"},
		{"sqlite casing", "SQLite", "sqlite"},

		// Current behavior: unknown values fall back to postgres.
		{"unknown falls back to postgres", "oracle", "postgres"},

		// Current behavior: no sqlite3 alias support (falls back).
		{"sqlite3 falls back to postgres", "sqlite3", "postgres"},
		{"trim and lowercase", "  PoStGrEs  ", "postgres"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeBackendKind(tt.in)
			if got != tt.expect {
				t.Fatalf("normalizeBackendKind(%q) = %q, want %q", tt.in, got, tt.expect)
			}
		})
	}
}

// TestAutoSelectBreakouts verifies breakout field selection based on bounded uniqueness ratios.
// Selection must be deterministic and exclude "row_hash".
func TestAutoSelectBreakouts(t *testing.T) {
	t.Parallel()

	stats := sampleUniqueness{
		TotalRows: 100,
		PerColumnTotal: map[string]int{
			"id":       100,
			"shape":    100,
			"color":    100,
			"volume":   100,
			"row_hash": 100,
		},
		PerColumnDistinct: map[string]int{
			"id":       100,
			"shape":    3,
			"color":    4,
			"volume":   100,
			"row_hash": 100,
		},
		PerColumnCapped: map[string]bool{},
		ColumnOrder:     []string{"id", "shape", "color", "volume", "row_hash"},
	}

	got := autoSelectBreakouts(stats)
	want := []string{"shape", "color"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("autoSelectBreakouts() = %#v, want %#v", got, want)
	}
}

// TestBuildMultiTableFromSingle_SQLite verifies that SQLite configs never emit schema-qualified tables.
// This is a regression test for the `public.` bug.
func TestBuildMultiTableFromSingle_SQLite(t *testing.T) {
	t.Parallel()

	p := config.Pipeline{
		Job: "sample",
		Storage: config.Storage{
			Kind: "sqlite",
			DB: config.DBConfig{
				Table:   "sample",
				Columns: []string{"id", "shape", "row_hash"},
				DSN:     "file:test.db",
			},
		},
	}

	stats := sampleUniqueness{
		PerColumnTotal: map[string]int{
			"shape": 10,
		},
		PerColumnDistinct: map[string]int{
			"shape": 2,
		},
		ColumnOrder: []string{"shape"},
	}

	out := buildMultiTableFromSingle(p, stats, []string{"shape"})

	storage := out["storage"].(map[string]any)
	db := storage["db"].(map[string]any)
	tables := db["tables"].([]any)

	for _, tbl := range tables {
		name := tbl.(map[string]any)["name"].(string)
		if hasSchemaPrefix(name) {
			t.Fatalf("sqlite table name must not be schema-qualified: %q", name)
		}
	}
}

// TestBuildMultiTableFromSingle_Postgres verifies Postgres configs are schema-qualified under public.
func TestBuildMultiTableFromSingle_Postgres(t *testing.T) {
	t.Parallel()

	p := config.Pipeline{
		Job: "sample",
		Storage: config.Storage{
			Kind: "postgres",
			DB: config.DBConfig{
				Table:   "sample",
				Columns: []string{"id", "shape", "row_hash"},
				DSN:     "postgres://localhost",
			},
		},
	}

	stats := sampleUniqueness{
		PerColumnTotal: map[string]int{
			"shape": 10,
		},
		PerColumnDistinct: map[string]int{
			"shape": 2,
		},
		ColumnOrder: []string{"shape"},
	}

	out := buildMultiTableFromSingle(p, stats, []string{"shape"})

	storage := out["storage"].(map[string]any)
	db := storage["db"].(map[string]any)
	tables := db["tables"].([]any)

	foundPublic := false
	for _, tbl := range tables {
		name := tbl.(map[string]any)["name"].(string)
		if len(name) >= 7 && name[:7] == "public." {
			foundPublic = true
			break
		}
	}

	if !foundPublic {
		t.Fatalf("expected schema-qualified tables for postgres backend")
	}
}

// hasSchemaPrefix reports whether a table name contains a SQL schema prefix.
func hasSchemaPrefix(name string) bool {
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			return true
		}
	}
	return false
}

// -------------------- Benchmarks --------------------

func BenchmarkNormalizeBackendKind(b *testing.B) {
	inputs := []string{
		"postgres",
		"Postgres",
		"sqlite",
		"SQLite",
		"mssql",
		"MSSQL",
		"oracle",
		"sqlite3",
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = normalizeBackendKind(inputs[i%len(inputs)])
	}
}

func BenchmarkAutoSelectBreakouts(b *testing.B) {
	stats := sampleUniqueness{
		TotalRows: 1000,
		PerColumnTotal: map[string]int{
			"a": 1000,
			"b": 1000,
			"c": 1000,
			"d": 1000,
		},
		PerColumnDistinct: map[string]int{
			"a": 2,
			"b": 10,
			"c": 500,
			"d": 1000,
		},
		ColumnOrder: []string{"a", "b", "c", "d"},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = autoSelectBreakouts(stats)
	}
}

// TestStripHeaderBOM verifies that UTF-8 BOM is removed from the first header
// cell and that other headers are left unchanged.
//
// This matters because some real-world CSV exports (Excel, some CMS tools) emit
// UTF-8 BOMs, which would otherwise pollute the first column name and cause
// header_map mismatches.
func TestStripHeaderBOM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "empty",
			in:   nil,
			want: nil,
		},
		{
			name: "no_bom",
			in:   []string{"A", "B"},
			want: []string{"A", "B"},
		},
		{
			name: "bom_in_first_cell",
			in:   []string{"\uFEFFPČV", "Stát"},
			want: []string{"PČV", "Stát"},
		},
		{
			name: "bom_not_in_first_cell_only_first_stripped",
			in:   []string{"\uFEFFA", "\uFEFFB"},
			want: []string{"A", "\uFEFFB"},
		},
	}

	for _, tt := range tests {
		tt := tt // capture
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := append([]string(nil), tt.in...) // avoid mutating test vectors
			got = csvparser.StripHeaderBOM(got)

			if len(got) != len(tt.want) {
				t.Fatalf("len mismatch: got=%d want=%d got=%v want=%v", len(got), len(tt.want), got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("mismatch at %d: got=%q want=%q full got=%v want=%v", i, got[i], tt.want[i], got, tt.want)
				}
			}
		})
	}
}

// TestNormalizeFieldName_TransliteratesDiacritics verifies that field name
// normalization removes diacritics rather than dropping the entire character.
//
// This is required for Czech/Slovak datasets where headers may contain
// characters like Č, Ř, Á, etc. Without transliteration, normalization becomes
// lossy ("pčv" -> "pv"), which breaks stable schema naming.
func TestNormalizeFieldName_TransliteratesDiacritics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{in: "PČV", want: "pcv"},
		{in: "pčv", want: "pcv"},
		{in: "Stát", want: "stat"},
		{in: "Číslo-smlouvy", want: "cislo_smlouvy"},
		{in: "  Řízení / Test  ", want: "rizeni_test"},
	}

	for _, tt := range tests {
		tt := tt // capture
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			got := normalizeFieldName(tt.in)
			if got != tt.want {
				t.Fatalf("normalizeFieldName(%q)=%q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestBOMPlusNormalization verifies the real-world problematic case where a CSV
// header begins with a BOM and contains diacritics.
//
// Example: Excel UTF-8 BOM + Czech header "PČV" yields "\uFEFFPČV".
func TestBOMPlusNormalization(t *testing.T) {
	t.Parallel()

	headers := []string{"\uFEFFPČV", "Stát"}

	// Ensure probe follows parser behavior for BOM stripping.
	headers = csvparser.StripHeaderBOM(headers)

	if headers[0] != "PČV" {
		t.Fatalf("StripHeaderBOM did not strip BOM: got %q", headers[0])
	}

	// Ensure normalization produces stable ASCII identifiers.
	n0 := normalizeFieldName(headers[0])
	n1 := normalizeFieldName(headers[1])

	if n0 != "pcv" {
		t.Fatalf("normalized header[0]=%q want %q", n0, "pcv")
	}
	if n1 != "stat" {
		t.Fatalf("normalized header[1]=%q want %q", n1, "stat")
	}
}
