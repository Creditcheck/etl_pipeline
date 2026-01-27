package postgres

import "testing"

func TestSCD2RowHashChangeDetection(t *testing.T) {
	tests := []struct {
		name         string
		currentHash  any
		newHash      any
		expectChange bool
	}{
		{"same string", "abc", "abc", false},
		{"string vs bytes same", "abc", []byte("abc"), false},
		{"different hash", "abc", "def", true},
		{"nil both", nil, nil, false},
		{"nil vs value", nil, "abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := !equalScalar(tt.currentHash, tt.newHash)
			if changed != tt.expectChange {
				t.Fatalf("expected change=%v got %v", tt.expectChange, changed)
			}
		})
	}
}

func TestBusinessKeyIndexing(t *testing.T) {
	cols := []string{"vehicle_id", "country_id", "row_hash"}
	idx := indexColumns(cols)
	keyIdx := indicesFor([]string{"vehicle_id", "country_id"}, idx)

	if keyIdx[0] != 0 || keyIdx[1] != 1 {
		t.Fatalf("business key indices incorrect: %v", keyIdx)
	}
}

func TestInsertDecisionUsingRowHash(t *testing.T) {
	columns := []string{"vehicle_id", "country_id", "row_hash"}
	rowHashIdx, ok := indexOfColumn(columns, "row_hash")
	if !ok {
		t.Fatal("row_hash not found")
	}

	current := []any{1, 2, "aaa"}
	incomingSame := []any{1, 2, "aaa"}
	incomingChanged := []any{1, 2, "bbb"}

	if equalScalar(current[rowHashIdx], incomingSame[rowHashIdx]) != true {
		t.Fatal("expected no change")
	}
	if equalScalar(current[rowHashIdx], incomingChanged[rowHashIdx]) != false {
		t.Fatal("expected change")
	}
}
