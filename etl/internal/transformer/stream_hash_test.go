package transformer

import (
	"context"
	"testing"
)

func TestHashLoopRows_WritesDeterministicHexSHA256(t *testing.T) {
	columns := []string{"pcv", "kratky_text", "row_hash"}

	spec := HashSpec{
		TargetField: "row_hash",
		Fields:      []string{"pcv", "kratky_text"},
		Algorithm:   "sha256",
		Encoding:    "hex",
		Separator:   "\x1f",
		TrimSpace:   true,
	}

	in := make(chan *Row, 1)
	out := make(chan *Row, 1)

	// Note: we intentionally avoid Free() here by ensuring the row is accepted.
	r := &Row{
		Line: 1,
		V:    []any{int64(14596725), "testovací zpráva", nil},
	}
	in <- r
	close(in)

	ctx := context.Background()
	go func() {
		HashLoopRows(ctx, columns, in, out, spec, nil)
		close(out)
	}()

	got := <-out
	if got == nil {
		t.Fatalf("expected a row, got nil")
	}
	if got.V[2] == nil {
		t.Fatalf("expected row_hash to be set, got nil")
	}
	h1, ok := got.V[2].(string)
	if !ok {
		t.Fatalf("expected row_hash string, got %T", got.V[2])
	}
	if len(h1) != 64 {
		t.Fatalf("expected sha256 hex length 64, got %d (%q)", len(h1), h1)
	}

	// Run again with same input to verify determinism.
	in2 := make(chan *Row, 1)
	out2 := make(chan *Row, 1)
	r2 := &Row{
		Line: 2,
		V:    []any{int64(14596725), "testovací zpráva", nil},
	}
	in2 <- r2
	close(in2)

	go func() {
		HashLoopRows(ctx, columns, in2, out2, spec, nil)
		close(out2)
	}()

	got2 := <-out2
	h2 := got2.V[2].(string)

	if h1 != h2 {
		t.Fatalf("expected deterministic hash; h1=%q h2=%q", h1, h2)
	}
}

func TestHashLoopRows_IncludeFieldNamesChangesHash(t *testing.T) {
	columns := []string{"pcv", "kratky_text", "row_hash"}

	base := HashSpec{
		TargetField: "row_hash",
		Fields:      []string{"pcv", "kratky_text"},
		Algorithm:   "sha256",
		Encoding:    "hex",
		Separator:   "\x1f",
		TrimSpace:   true,
	}

	ctx := context.Background()

	run := func(includeNames bool) string {
		spec := base
		spec.IncludeFieldNames = includeNames

		in := make(chan *Row, 1)
		out := make(chan *Row, 1)

		in <- &Row{Line: 1, V: []any{int64(12855565), "alt. kola", nil}}
		close(in)

		go func() {
			HashLoopRows(ctx, columns, in, out, spec, nil)
			close(out)
		}()

		r := <-out
		return r.V[2].(string)
	}

	hA := run(false)
	hB := run(true)

	if hA == hB {
		t.Fatalf("expected different hashes when include_field_names changes; got same=%q", hA)
	}
}
