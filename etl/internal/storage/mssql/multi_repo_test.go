package mssql

import "testing"

func TestDedupeRowsByColumns_StableAndCorrect(t *testing.T) {
	// This test verifies "stable dedupe" behavior for fact inserts.
	//
	// Assumptions / context:
	//   1) The SQL Server backend uses a set-based INSERT ... WHERE NOT EXISTS strategy for idempotence.
	//   2) Unlike Postgres INSERT ... ON CONFLICT DO NOTHING, SQL Server statements do not inherently
	//      "collapse" duplicates inside the VALUES source. If the same dedupe key appears multiple times
	//      in the same batch and the target table does not yet contain that key, SQL Server can attempt
	//      multiple inserts and hit a UNIQUE constraint.
	//   3) To match Postgres behavior, the backend must keep exactly one row per dedupe key in each batch.
	//      When multiple source rows share a dedupe key, we keep the first occurrence (stable policy).
	//
	// The intent of this test is to guarantee correctness independent of any database connection.

	columns := []string{"country_id", "vehicle_id", "import_date"}
	dedupeCols := []string{"country_id", "vehicle_id"}

	// Duplicate key (46, 2049) appears multiple times. Only the first should remain.
	rows := [][]any{
		{int64(46), int64(2049), "2022-01-01"},
		{int64(46), int64(2049), "2022-01-02"}, // duplicate key, should be dropped
		{int64(47), int64(999), "2022-02-01"},
		{int64(46), int64(2049), "2022-03-01"}, // duplicate key, should be dropped
		{int64(46), int64(3000), "2022-04-01"},
	}

	got, err := dedupeRowsByColumns(rows, columns, dedupeCols)
	if err != nil {
		t.Fatalf("dedupeRowsByColumns returned error: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 rows after dedupe, got %d", len(got))
	}

	// Validate stable "keep first occurrence" behavior for (46,2049).
	if got[0][0] != int64(46) || got[0][1] != int64(2049) || got[0][2] != "2022-01-01" {
		t.Fatalf("first (46,2049) row not preserved; got=%v", got[0])
	}

	// The remaining rows should preserve original order of first occurrences.
	if got[1][0] != int64(47) || got[1][1] != int64(999) {
		t.Fatalf("unexpected second row; got=%v", got[1])
	}
	if got[2][0] != int64(46) || got[2][1] != int64(3000) {
		t.Fatalf("unexpected third row; got=%v", got[2])
	}
}

func TestDedupeRowsByColumns_MissingColumnErrors(t *testing.T) {
	// This test verifies defensive behavior: if the caller provides a dedupe column
	// that is not present in the insert column list, the function must return an error.
	//
	// Rationale:
	//   - A silent "best effort" would hide configuration drift between schema and pipeline config,
	//     leading to either incorrect idempotence (duplicates inserted) or runtime DB errors.

	columns := []string{"a", "b"}
	rows := [][]any{{1, 2}}

	_, err := dedupeRowsByColumns(rows, columns, []string{"missing"})
	if err == nil {
		t.Fatalf("expected error for missing dedupe column, got nil")
	}
}
