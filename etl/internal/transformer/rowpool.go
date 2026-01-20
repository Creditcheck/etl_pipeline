// Package transformer provides streaming, allocation-conscious transforms.
// This file defines a pooled Row type used across parser → transformer → loader
// to significantly reduce heap churn and GC pressure.
package transformer

import "sync"

// Row is a pooled container holding a positional row for database COPY.
//
// Ownership contract:
//   - Exactly one goroutine "owns" a Row at a time.
//   - A Row may be passed downstream via channels (ownership transfer).
//   - The final consumer (typically loader) must call Free() AFTER it is fully
//     done with the Row (and anything referencing r.V).
//
// IMPORTANT:
//   - During ctx cancellation, drain-safe stages may still be running while the
//     parser is also unwinding. If canceled rows are returned to the pool, they
//     can be reused immediately and written concurrently with downstream reads.
//     That produces the races / corruption you've observed.
//
// Therefore:
//   - Use Free() only on the normal path.
//   - Use Drop() on cancellation paths (no re-pooling; allow GC to reclaim).
type Row struct {
	V    []any
	Line int // 1-based logical record number, if known
}

var rowPool sync.Pool

// GetRow returns a pooled Row with capacity for colCount fields and length set
// to colCount. All elements are zeroed for safety.
func GetRow(colCount int) *Row {
	if v := rowPool.Get(); v != nil {
		r := v.(*Row)
		if cap(r.V) < colCount {
			r.V = make([]any, colCount)
		}
		r.V = r.V[:colCount]
		for i := range r.V {
			r.V[i] = nil
		}
		r.Line = 0
		return r
	}
	return &Row{
		V:    make([]any, colCount),
		Line: 0,
	}
}

// Free returns the Row to the pool.
// Call this ONLY when you're sure no other goroutine can observe r or r.V.
func (r *Row) Free() {
	rowPool.Put(r)
}

// Drop discards the Row WITHOUT returning it to the pool.
//
// Use this on ctx-cancellation paths to prevent "canceled drain" from racing
// with upstream reuse of the same pooled Row.
func (r *Row) Drop() {
	// Help GC a bit on cancel paths (optional).
	// Avoid per-element nil for speed; releasing the slice header is enough.
	r.V = nil
	r.Line = 0
}
