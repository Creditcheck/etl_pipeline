package postgres

import (
	"strings"
	"testing"

	"etl/internal/storage"
)

// boolPtr is a tiny helper to avoid repeating &[]bool literals in tests.
func boolPtr(v bool) *bool { return &v }

func TestBuildCreateSQL_NoHistory_CreatesBaseTable(t *testing.T) {
	t.Parallel()

	spec := storage.TableSpec{
		Name:            "public.countries",
		AutoCreateTable: true,
		PrimaryKey:      &storage.PrimaryKeySpec{Name: "country_id", Type: "serial"},
		Columns: []storage.ColumnSpec{
			{Name: "name", Type: "varchar(100)", Nullable: boolPtr(false)},
		},
		Constraints: []storage.ConstraintSpec{{Kind: "unique", Columns: []string{"name"}}},
		Load:        storage.LoadSpec{Kind: "dimension"},
	}

	schemaSQL, baseSQL, histSQL, err := buildCreateSQL(spec)
	if err != nil {
		t.Fatalf("buildCreateSQL: %v", err)
	}
	if schemaSQL == "" {
		t.Fatalf("expected schemaSQL to be non-empty for schema-qualified table")
	}
	if !strings.Contains(baseSQL, "CREATE TABLE IF NOT EXISTS public.countries") {
		t.Fatalf("baseSQL missing CREATE TABLE: %q", baseSQL)
	}
	if !strings.Contains(baseSQL, "country_id") || !strings.Contains(baseSQL, "PRIMARY KEY") {
		t.Fatalf("baseSQL missing primary key: %q", baseSQL)
	}
	if !strings.Contains(baseSQL, "\"name\"") || !strings.Contains(baseSQL, "varchar(100)") {
		t.Fatalf("baseSQL missing column definition: %q", baseSQL)
	}
	if !strings.Contains(baseSQL, "UNIQUE") {
		t.Fatalf("baseSQL missing UNIQUE constraint: %q", baseSQL)
	}
	if histSQL != "" {
		t.Fatalf("expected histSQL to be empty when history is disabled; got %q", histSQL)
	}
}

func TestBuildCreateSQL_WithHistory_CreatesHistoryTableWithoutBaseConstraints(t *testing.T) {
	t.Parallel()

	spec := storage.TableSpec{
		Name:            "public.customers",
		AutoCreateTable: true,
		Columns: []storage.ColumnSpec{
			{Name: "customer_id", Type: "bigint", Nullable: boolPtr(false)},
			{Name: "name", Type: "text", Nullable: boolPtr(false)},
		},
		Constraints: []storage.ConstraintSpec{{Kind: "unique", Columns: []string{"customer_id"}}},
		Load: storage.LoadSpec{
			Kind: "fact",
			History: &storage.HistorySpec{
				Enabled:         true,
				BusinessKey:     []string{"customer_id"},
				ValidFromColumn: "valid_from",
				ValidToColumn:   "valid_to",
				ChangedAtColumn: "changed_at",
			},
		},
	}

	_, baseSQL, histSQL, err := buildCreateSQL(spec)
	if err != nil {
		t.Fatalf("buildCreateSQL: %v", err)
	}
	if baseSQL == "" {
		t.Fatalf("expected baseSQL to be non-empty")
	}
	if histSQL == "" {
		t.Fatalf("expected histSQL to be non-empty when history is enabled")
	}

	// Base table should have the metadata columns.
	for _, col := range []string{"valid_from", "valid_to", "changed_at"} {
		if !strings.Contains(baseSQL, pgIdent(col)) {
			t.Fatalf("baseSQL missing metadata column %q: %q", col, baseSQL)
		}
	}

	// Base table should enforce uniqueness for the "one current per business key" invariant.
	if !strings.Contains(baseSQL, "UNIQUE") {
		t.Fatalf("baseSQL missing UNIQUE constraint: %q", baseSQL)
	}

	// History table must *not* include base-table uniqueness constraints; otherwise we
	// could not store multiple historical versions for a single business key.
	if strings.Contains(histSQL, "UNIQUE") {
		t.Fatalf("histSQL unexpectedly contains UNIQUE constraint: %q", histSQL)
	}

	// The history table should not contain a PRIMARY KEY derived from PrimaryKeySpec.
	// (The SCD2 insert logic does not write that column.)
	if strings.Contains(histSQL, "PRIMARY KEY") {
		t.Fatalf("histSQL unexpectedly contains PRIMARY KEY: %q", histSQL)
	}
}

func TestBuildInsertSQL_NoDedupe_NoOnConflict(t *testing.T) {
	t.Parallel()

	sql, args := buildInsertSQL(
		"public.imports",
		[]string{"vehicle_id", "country_id", "import_date"},
		[][]any{
			{int64(1), int64(1), nil},
			{int64(2), int64(3), "2026-01-01"},
		},
		nil,
	)

	if strings.Contains(sql, "ON CONFLICT") {
		t.Fatalf("expected no ON CONFLICT clause, got: %q", sql)
	}

	// 2 rows * 3 columns = 6 args
	if len(args) != 6 {
		t.Fatalf("expected 6 args, got %d", len(args))
	}

	// Spot-check placeholder numbering (must be stable for Exec()).
	if !strings.Contains(sql, "VALUES ($1, $2, $3), ($4, $5, $6)") {
		t.Fatalf("unexpected VALUES placeholders: %q", sql)
	}
}

func TestBuildInsertSQL_WithDedupe_AddsOnConflictDoNothing(t *testing.T) {
	t.Parallel()

	sql, args := buildInsertSQL(
		"public.imports",
		[]string{"vehicle_id", "country_id", "import_date"},
		[][]any{
			{int64(1), int64(1), nil},
			// Intentional duplicate business key to simulate input duplicates.
			{int64(1), int64(1), nil},
		},
		[]string{"vehicle_id", "country_id"},
	)

	// The critical behavior: idempotent insert for duplicates.
	if !strings.Contains(sql, "ON CONFLICT (\"vehicle_id\", \"country_id\") DO NOTHING") {
		t.Fatalf("expected ON CONFLICT DO NOTHING, got: %q", sql)
	}

	// 2 rows * 3 columns = 6 args
	if len(args) != 6 {
		t.Fatalf("expected 6 args, got %d", len(args))
	}
}

func TestBuildCreateSQL_WithHistory_ConfigAlreadyDefinesMetadataColumns_DoesNotDuplicate(t *testing.T) {
	t.Parallel()

	spec := storage.TableSpec{
		Name:            "public.imports",
		AutoCreateTable: true,
		PrimaryKey:      &storage.PrimaryKeySpec{Name: "import_id", Type: "serial"},
		Columns: []storage.ColumnSpec{
			{Name: "vehicle_id", Type: "int", Nullable: boolPtr(false)},
			{Name: "country_id", Type: "int", Nullable: boolPtr(false)},
			{Name: "import_date", Type: "date", Nullable: boolPtr(true)},

			// Explicit metadata columns as in the config patch.
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

	_, baseSQL, histSQL, err := buildCreateSQL(spec)
	if err != nil {
		t.Fatalf("buildCreateSQL: %v", err)
	}

	// The bug we are preventing: valid_from appearing twice in CREATE TABLE.
	//
	// We do a simple occurrence count on the identifier to ensure we didn't append
	// a second definition.
	if got := strings.Count(baseSQL, pgIdent("valid_from")); got != 1 {
		t.Fatalf("baseSQL duplicated valid_from (count=%d): %q", got, baseSQL)
	}
	if got := strings.Count(baseSQL, pgIdent("valid_to")); got != 1 {
		t.Fatalf("baseSQL duplicated valid_to (count=%d): %q", got, baseSQL)
	}
	if got := strings.Count(baseSQL, pgIdent("changed_at")); got != 1 {
		t.Fatalf("baseSQL duplicated changed_at (count=%d): %q", got, baseSQL)
	}

	// Same expectation for the history table DDL.
	if got := strings.Count(histSQL, pgIdent("valid_from")); got != 1 {
		t.Fatalf("histSQL duplicated valid_from (count=%d): %q", got, histSQL)
	}
}
