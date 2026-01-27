package postgres

import "testing"

func TestIndexOfColumn(t *testing.T) {
	t.Parallel()

	cols := []string{"a", "row_hash", "c"}
	got, ok := indexOfColumn(cols, "row_hash")
	if !ok {
		t.Fatalf("expected row_hash to be found")
	}
	if got != 1 {
		t.Fatalf("expected idx=1, got %d", got)
	}
}

func TestEqualScalar_StringVsBytes(t *testing.T) {
	t.Parallel()

	if !equalScalar("abc", []byte("abc")) {
		t.Fatalf("expected string and []byte with same content to be equal")
	}
	if equalScalar("abc", []byte("abd")) {
		t.Fatalf("expected different content to be not equal")
	}
}

func TestEqualScalar_NilHandling(t *testing.T) {
	t.Parallel()

	if !equalScalar(nil, nil) {
		t.Fatalf("expected nil == nil")
	}
	if equalScalar(nil, "x") {
		t.Fatalf("expected nil != non-nil")
	}
}
