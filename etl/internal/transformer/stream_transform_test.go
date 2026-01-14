package transformer

import "testing"

func TestCompilePlan_BigIntCoercesToInt64(t *testing.T) {
	p := compilePlan([]string{"pcv"}, CoerceSpec{
		Types: map[string]string{"pcv": "bigint"},
	})

	var dst any
	ok := p.cols[0].coerce(&dst, "19108096")
	if !ok {
		t.Fatalf("coerce returned ok=false")
	}
	v, ok := dst.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T (%v)", dst, dst)
	}
	if v != 19108096 {
		t.Fatalf("expected 19108096, got %d", v)
	}
}

func TestCompilePlan_TextPreservesLeadingZeros(t *testing.T) {
	p := compilePlan([]string{"stat"}, CoerceSpec{
		Types: map[string]string{"stat": "text"},
	})

	var dst any
	ok := p.cols[0].coerce(&dst, "000")
	if !ok {
		t.Fatalf("coerce returned ok=false")
	}
	s, ok := dst.(string)
	if !ok {
		t.Fatalf("expected string, got %T (%v)", dst, dst)
	}
	if s != "000" {
		t.Fatalf("expected %q, got %q", "000", s)
	}
}
