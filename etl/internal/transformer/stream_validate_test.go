package transformer

import (
	"context"
	"sync"
	"testing"
	"time"
)

/*
TestValidateLoopRows_AllValid verifies that rows containing all required fields
are forwarded unchanged and in order, and that no rejects are reported.

It uses an empty type map because ValidateLoopRows currently only enforces
non-nil / presence checks for required columns; type information is not needed
for this basic scenario.
*/
func TestValidateLoopRows_AllValid(t *testing.T) {
	columns := []string{"id", "name", "age"}
	required := []string{"id", "name"}
	types := map[string]string{} // type hints unused in this test

	in := make(chan *Row, 3)
	out := make(chan *Row, 3)

	// Produce three valid rows (all required fields present).
	for i := 0; i < 3; i++ {
		r := GetRow(len(columns))
		r.V[0] = "id"
		r.V[1] = "alice"
		r.V[2] = "42"
		in <- r
	}
	close(in)

	var rejects int
	errcb := func(_ int, _ string) { rejects++ }

	ValidateLoopRows(context.Background(), columns, required, types, in, out, errcb)

	if rejects != 0 {
		t.Fatalf("unexpected rejects: %d", rejects)
	}
	if got := len(out); got != 3 {
		t.Fatalf("forwarded %d rows; want 3", got)
	}

	// Clean up pooled rows.
	for i := 0; i < 3; i++ {
		(<-out).Free()
	}
}

/*
TestValidateLoopRows_MissingRequired verifies that rows missing any required
field are fail-soft dropped, reported via onReject, and not forwarded.

We create three rows:
  - row 1: valid
  - row 2: missing "name"
  - row 3: missing "id"

Only the first row should be forwarded; the other two should be rejected.
*/
func TestValidateLoopRows_MissingRequired(t *testing.T) {
	columns := []string{"id", "name"}
	required := []string{"id", "name"}
	types := map[string]string{} // type hints unused in this test

	in := make(chan *Row, 3)
	out := make(chan *Row, 3)

	// Row 1: valid
	r1 := GetRow(2)
	r1.V[0] = "id"
	r1.V[1] = "ok"
	in <- r1

	// Row 2: missing required "name"
	r2 := GetRow(2)
	r2.V[0] = "id"
	r2.V[1] = nil
	in <- r2

	// Row 3: missing required "id"
	r3 := GetRow(2)
	r3.V[0] = nil
	r3.V[1] = "ok"
	in <- r3

	close(in)

	var rejects int
	errcb := func(_ int, _ string) { rejects++ }

	ValidateLoopRows(context.Background(), columns, required, types, in, out, errcb)

	if rejects != 2 {
		t.Fatalf("rejects=%d; want 2", rejects)
	}
	if got := len(out); got != 1 {
		t.Fatalf("forwarded=%d; want 1", got)
	}
	(<-out).Free()
}

/*
TestValidateLoopRows_UnknownRequiredColumns verifies that required names not
present in the columns list are ignored. Only actual columns in 'columns'
should be enforced as required.

We declare required = {"a", "missing", "also_missing"}, but our input columns
are only {"a", "b"}. A row with non-nil 'a' should pass validation.
*/
func TestValidateLoopRows_UnknownRequiredColumns(t *testing.T) {
	columns := []string{"a", "b"}
	required := []string{"a", "missing", "also_missing"}
	types := map[string]string{} // type hints unused in this test

	in := make(chan *Row, 1)
	out := make(chan *Row, 1)

	r := GetRow(2)
	r.V[0] = "x"
	r.V[1] = "y"
	in <- r
	close(in)

	ValidateLoopRows(context.Background(), columns, required, types, in, out, nil)

	if got := len(out); got != 1 {
		t.Fatalf("forwarded=%d; want 1 (unknown required should be ignored)", got)
	}
	(<-out).Free()
}

/*
TestValidateLoopRows_BadRowLenAndNil ensures malformed rows (nil Row or slice
length mismatch) are drained and freed without panicking and are not forwarded.

We push:
  - a nil row
  - a row with length-1 value slice for 2 columns
  - a correct row

Only the correct row should be forwarded.
*/
func TestValidateLoopRows_BadRowLenAndNil(t *testing.T) {
	columns := []string{"c1", "c2"}
	required := []string{"c1"}
	types := map[string]string{} // type hints unused in this test

	in := make(chan *Row, 3)
	out := make(chan *Row, 3)

	// Nil row
	in <- nil

	// Wrong length row
	bad := GetRow(1)
	bad.V[0] = "x"
	in <- bad

	// Good row
	ok := GetRow(2)
	ok.V[0] = "x"
	ok.V[1] = "y"
	in <- ok

	close(in)

	ValidateLoopRows(context.Background(), columns, required, types, in, out, nil)

	if got := len(out); got != 1 {
		t.Fatalf("forwarded=%d; want 1", got)
	}
	(<-out).Free()
}

/*
TestValidateLoopRows_CancelStillDrains verifies ValidateLoopRows is drain-safe
and cancellation-aware with respect to forwarding.

Contract under test:

  - ValidateLoopRows must keep draining `in` even when ctx is canceled (so
    upstream producers are not blocked).
  - When ctx is canceled, ValidateLoopRows must not block attempting to forward
    valid rows to `out`. Instead it should select on ctx.Done() and discard the
    row (Drop) during shutdown unwinding.
  - Invalid rows must still be rejected and reported via onReject while draining.

This test pre-cancels ctx, then pushes 50 rows:
  - even index rows: valid
  - odd index rows: invalid (required field missing)

To make the forwarding behavior deterministic, `out` is unbuffered and there is
no receiver. Therefore `out <- r` is never ready, and the validate stage must
take the `<-ctx.Done()` branch for valid rows (resulting in zero forwarded).
*/
func TestValidateLoopRows_CancelStillDrains(t *testing.T) {
	columns := []string{"id"}
	required := []string{"id"}
	colKinds := map[string]string{} // no type checks in this test

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel to force ctx.Done() readiness

	in := make(chan *Row, 100)

	// Unbuffered out + no receiver => send is never ready => forwarding is impossible.
	out := make(chan *Row)

	const total = 50
	for i := 0; i < total; i++ {
		r := GetRow(1)
		if i%2 == 0 {
			r.V[0] = "ok"
		} else {
			r.V[0] = nil
		}
		in <- r
	}
	close(in)

	var rejects int
	done := make(chan struct{})

	go func() {
		ValidateLoopRows(ctx, columns, required, colKinds, in, out, func(_ int, _ string) { rejects++ })
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ValidateLoopRows did not return after ctx cancellation")
	}

	if rejects != total/2 {
		t.Fatalf("rejects=%d; want %d", rejects, total/2)
	}

	// Ensure nothing was forwarded.
	select {
	case r := <-out:
		if r != nil {
			r.Free()
		}
		t.Fatalf("forwarded unexpectedly; want 0")
	default:
		// ok
	}
}

/*
TestValidateLoopRows_BlockingOut verifies that ValidateLoopRows will block on
the 'out' channel when the consumer is slow, and still behaves correctly
(forwards valid rows).

We use an unbuffered 'out' channel and a slow consumer that sleeps between
receives. This also implicitly tests that ValidateLoopRows does not close 'out'
itself; the caller owns channel closing.
*/
func TestValidateLoopRows_BlockingOut(t *testing.T) {
	columns := []string{"a"}
	required := []string{"a"}
	types := map[string]string{} // type hints unused in this test

	in := make(chan *Row, 5)
	out := make(chan *Row) // unbuffered to force blocking

	// Produce three valid rows then close input.
	for i := 0; i < 3; i++ {
		r := GetRow(1)
		r.V[0] = "x"
		in <- r
	}
	close(in)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ValidateLoopRows(context.Background(), columns, required, types, in, out, nil)
	}()

	// Slow consumer: receive with delays.
	for i := 0; i < 3; i++ {
		select {
		case r := <-out:
			r.Free()
			time.Sleep(5 * time.Millisecond)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("timed out waiting for row")
		}
	}
	wg.Wait()
}

/*
BenchmarkValidateLoopRows_PassAll measures the cost of validating a large number
of rows when all rows are valid, and no callback is invoked.

We keep the callback nil and ensure that the consumer drains the 'out' channel
so that ValidateLoopRows is not artificially blocked by channel backpressure.
*/
func BenchmarkValidateLoopRows_PassAll(b *testing.B) {
	columns := []string{"c0", "c1", "c2", "c3", "c4", "c5"}
	required := []string{"c0", "c3"}
	types := map[string]string{} // type hints unused in this benchmark

	const N = 50_000
	makeRows := func() []*Row {
		rows := make([]*Row, N)
		for i := 0; i < N; i++ {
			r := GetRow(len(columns))
			for j := range columns {
				r.V[j] = "x"
			}
			rows[i] = r
		}
		return rows
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		in := make(chan *Row, N)
		out := make(chan *Row, N)
		rows := makeRows()
		for _, r := range rows {
			in <- r
		}
		close(in)

		// Drain consumer so ValidateLoopRows never blocks unduly.
		done := make(chan struct{})
		go func() {
			for r := range out {
				r.Free()
			}
			close(done)
		}()

		ValidateLoopRows(context.Background(), columns, required, types, in, out, nil)
		close(out)
		<-done
	}
}

/*
BenchmarkValidateLoopRows_DropHalf_NoCallback measures performance when roughly
half the rows are rejected due to missing required fields, with no onReject
callback installed.

This isolates the cost of the core validation logic (required field checks and
row dropping) without callback overhead.
*/
func BenchmarkValidateLoopRows_DropHalf_NoCallback(b *testing.B) {
	columns := []string{"c0", "c1", "c2"}
	required := []string{"c0", "c2"}
	types := map[string]string{} // type hints unused in this benchmark

	const N = 50_000
	makeRows := func() []*Row {
		rows := make([]*Row, N)
		for i := 0; i < N; i++ {
			r := GetRow(len(columns))
			// Required "c0" is always present.
			r.V[0] = "x"
			// Alternate valid/invalid by toggling "c2".
			if i%2 == 0 {
				r.V[2] = "y" // valid
			} else {
				r.V[2] = nil // invalid
			}
			rows[i] = r
		}
		return rows
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		in := make(chan *Row, N)
		out := make(chan *Row, N)
		for _, r := range makeRows() {
			in <- r
		}
		close(in)

		// Drain all forwarded rows.
		go func() {
			for r := range out {
				r.Free()
			}
		}()

		ValidateLoopRows(context.Background(), columns, required, types, in, out, nil)
		close(out)
	}
}

/*
BenchmarkValidateLoopRows_DropHalf_WithCallback is similar to
BenchmarkValidateLoopRows_DropHalf_NoCallback, but installs a lightweight
onReject callback.

This measures the additional overhead of invoking onReject for every rejected
row (roughly half the input) on top of the core validation logic.
*/
func BenchmarkValidateLoopRows_DropHalf_WithCallback(b *testing.B) {
	columns := []string{"c0", "c1", "c2"}
	required := []string{"c0", "c2"}
	types := map[string]string{} // type hints unused in this benchmark

	const N = 50_000
	makeRows := func() []*Row {
		rows := make([]*Row, N)
		for i := 0; i < N; i++ {
			r := GetRow(len(columns))
			r.V[0] = "x"
			if i%2 == 0 {
				r.V[2] = "y" // valid
			} else {
				r.V[2] = nil // invalid
			}
			rows[i] = r
		}
		return rows
	}

	onReject := func(_ int, _ string) {
		// Intentionally light-weight to focus on callback dispatch cost rather
		// than any heavy logging or string formatting.
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		in := make(chan *Row, N)
		out := make(chan *Row, N)
		for _, r := range makeRows() {
			in <- r
		}
		close(in)

		// Drain forwarded rows.
		go func() {
			for r := range out {
				r.Free()
			}
		}()

		ValidateLoopRows(context.Background(), columns, required, types, in, out, onReject)
		close(out)
	}
}
