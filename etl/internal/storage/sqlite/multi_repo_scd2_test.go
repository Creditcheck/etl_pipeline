package sqlite

import (
	"strings"
	"testing"
	"time"

	"etl/internal/storage"
)

func boolPtr(v bool) *bool { return &v }

func TestBuildCreateSQL_SCD2CreatesHistoryTable(t *testing.T) {
	t.Parallel()

	spec := storage.TableSpec{
		Name:            "imports",
		AutoCreateTable: true,
		PrimaryKey:      &storage.PrimaryKeySpec{Name: "import_id", Type: "serial"},
		Columns: []storage.ColumnSpec{
			{Name: "vehicle_id", Type: "int", Nullable: boolPtr(false)},
			{Name: "country_id", Type: "int", Nullable: boolPtr(false)},
			{Name: "row_hash", Type: "text", Nullable: boolPtr(false)},
		},
		Load: storage.LoadSpec{
			Kind: "fact",
			History: &storage.HistorySpec{
				Enabled:         true,
				BusinessKey:     []string{"vehicle_id", "country_id"},
				ValidFromColumn: "valid_from",
				ValidToColumn:   "valid_to",
				ChangedAtColumn: "changed_at",
			},
		},
	}

	baseSQL, histSQL, err := buildCreateSQL(spec)
	if err != nil {
		t.Fatalf("buildCreateSQL: %v", err)
	}
	if !strings.Contains(baseSQL, "CREATE TABLE IF NOT EXISTS imports") {
		t.Fatalf("baseSQL missing CREATE TABLE: %q", baseSQL)
	}
	if histSQL == "" || !strings.Contains(histSQL, "CREATE TABLE IF NOT EXISTS imports_history") {
		t.Fatalf("expected history table DDL, got: %q", histSQL)
	}
}

func TestBuildCreateSQL_DoesNotDuplicateMetadataWhenExplicit(t *testing.T) {
	t.Parallel()

	spec := storage.TableSpec{
		Name:            "imports",
		AutoCreateTable: true,
		Columns: []storage.ColumnSpec{
			{Name: "vehicle_id", Type: "int", Nullable: boolPtr(false)},
			{Name: "country_id", Type: "int", Nullable: boolPtr(false)},
			{Name: "row_hash", Type: "text", Nullable: boolPtr(false)},
			// Explicit metadata columns.
			{Name: "valid_from", Type: "timestamptz", Nullable: boolPtr(false)},
			{Name: "valid_to", Type: "timestamptz", Nullable: boolPtr(true)},
			{Name: "changed_at", Type: "timestamptz", Nullable: boolPtr(false)},
		},
		Load: storage.LoadSpec{
			Kind: "fact",
			History: &storage.HistorySpec{
				Enabled:         true,
				BusinessKey:     []string{"vehicle_id", "country_id"},
				ValidFromColumn: "valid_from",
				ValidToColumn:   "valid_to",
				ChangedAtColumn: "changed_at",
			},
		},
	}

	baseSQL, histSQL, err := buildCreateSQL(spec)
	if err != nil {
		t.Fatalf("buildCreateSQL: %v", err)
	}

	// Each metadata column should appear exactly once.
	for _, col := range []string{`"valid_from"`, `"valid_to"`, `"changed_at"`} {
		if got := strings.Count(baseSQL, col); got != 1 {
			t.Fatalf("baseSQL duplicated %s (count=%d): %q", col, got, baseSQL)
		}
		if got := strings.Count(histSQL, col); got != 1 {
			t.Fatalf("histSQL duplicated %s (count=%d): %q", col, got, histSQL)
		}
	}
}

func TestEqualScalar_RowHashComparisons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    any
		b    any
		want bool
	}{
		{"same strings", "abc", "abc", true},
		{"string vs bytes same", "abc", []byte("abc"), true},
		{"bytes vs string same", []byte("abc"), "abc", true},
		{"different", "abc", "def", false},
		{"nil both", nil, nil, true},
		{"nil vs value", nil, "x", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := equalScalar(tt.a, tt.b); got != tt.want {
				t.Fatalf("equalScalar(%v,%v)=%v want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestBuildUpdateCurrentSQL_PlaceholdersAndArgsMatch(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 27, 10, 0, 0, 0, time.UTC)
	h := &storage.HistorySpec{
		Enabled:         true,
		BusinessKey:     []string{"vehicle_id", "country_id"},
		ValidFromColumn: "valid_from",
		ValidToColumn:   "valid_to",
		ChangedAtColumn: "changed_at",
	}

	columns := []string{"vehicle_id", "country_id", "row_hash"}
	incoming := []any{int64(1), int64(2), "aaa"}
	keyVals := []any{int64(1), int64(2)}

	sql, args := buildUpdateCurrentSQL("imports", columns, incoming, h, keyVals, now)

	// We expect:
	//   - len(columns) placeholders for fact columns
	//   - +1 for valid_from
	//   - +1 for changed_at
	//   - +len(keyVals) for WHERE
	//
	// valid_to is literal NULL so it adds no args.
	wantArgs := len(columns) + 2 + len(keyVals)
	if len(args) != wantArgs {
		t.Fatalf("args=%d want %d; sql=%q", len(args), wantArgs, sql)
	}
	if !strings.Contains(sql, `"valid_to" = NULL`) {
		t.Fatalf("expected valid_to=NULL in sql, got: %q", sql)
	}
}
