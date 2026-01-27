package mssql

import "testing"

func TestRowsEqual_UsesRowHashWhenPresent(t *testing.T) {
	columns := []string{"id", "row_hash", "v"}

	a := []any{int64(1), "hashA", "x"}
	b := []any{int64(1), "hashA", "DIFFERENT"} // should still be equal due to row_hash
	c := []any{int64(1), "hashB", "x"}

	if !rowsEqual(a, b, columns) {
		t.Fatalf("rowsEqual(a,b)=false, want true (same row_hash)")
	}
	if rowsEqual(a, c, columns) {
		t.Fatalf("rowsEqual(a,c)=true, want false (different row_hash)")
	}
}

func TestRowsEqual_FallsBackToFullCompareWithoutRowHash(t *testing.T) {
	columns := []string{"id", "v"}

	a := []any{int64(1), "x"}
	b := []any{int64(1), "x"}
	c := []any{int64(1), "y"}

	if !rowsEqual(a, b, columns) {
		t.Fatalf("rowsEqual(a,b)=false, want true (identical values)")
	}
	if rowsEqual(a, c, columns) {
		t.Fatalf("rowsEqual(a,c)=true, want false (different values)")
	}
}

func TestRowsEqual_FullCompareDetectsLengthMismatch(t *testing.T) {
	columns := []string{"a", "b"} // no row_hash -> full compare path

	a := []any{1, 2}
	b := []any{1, 2, 3}

	if rowsEqual(a, b, columns) {
		t.Fatalf("rowsEqual(length mismatch)=true, want false")
	}
}
