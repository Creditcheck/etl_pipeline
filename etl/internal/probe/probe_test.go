package probe

import (
	"reflect"
	"testing"

	"etl/internal/config"
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
