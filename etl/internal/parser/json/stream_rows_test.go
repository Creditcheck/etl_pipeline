package json

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"etl/internal/config"
	"etl/internal/transformer"
)

// drainRows reads exactly n rows from out and returns them.
//
// This helper is intentionally deterministic: it blocks until it has read n items.
// Tests using drainRows must ensure the producer has completed or out has capacity.
func drainRows(t *testing.T, out <-chan *transformer.Row, n int) []*transformer.Row {
	t.Helper()
	got := make([]*transformer.Row, 0, n)
	for i := 0; i < n; i++ {
		r := <-out
		if r == nil {
			t.Fatalf("got nil row at index %d", i)
		}
		got = append(got, r)
	}
	return got
}

// readAllRows reads all rows from out until it is closed.
func readAllRows(out <-chan *transformer.Row) []*transformer.Row {
	var rows []*transformer.Row
	for r := range out {
		rows = append(rows, r)
	}
	return rows
}

// runStream runs StreamJSONRows in a goroutine, closes out when done, and returns
// (rows, err, parseErrCalls).
//
// Why this helper exists:
//   - StreamJSONRows streams rows asynchronously via a channel.
//   - Tests need to collect outputs and also observe returned errors and parse-error callbacks
//     without relying on sleeps.
//
// Edge cases:
//   - If StreamJSONRows returns early due to ctx cancellation, out is still closed.
//   - parseErrCalls captures (line, errString) to keep comparisons stable.
func runStream(
	ctx context.Context,
	input string,
	columns []string,
	opts config.Options,
	outBuf int,
) (rows []*transformer.Row, err error, parseErrCalls []string) {
	out := make(chan *transformer.Row, outBuf)

	onParseErr := func(line int, e error) {
		// Record only the stable parts of the callback: line number and error string.
		parseErrCalls = append(parseErrCalls, fmt.Sprintf("line=%d err=%s", line, e.Error()))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		err = StreamJSONRows(ctx, strings.NewReader(input), columns, opts, out, onParseErr)
		close(out)
	}()

	rows = readAllRows(out)
	<-done
	return rows, err, parseErrCalls
}

func TestStreamJSONRows_RootArray_StreamsObjectsAndTrailingJSONL(t *testing.T) {
	// This test verifies the "root array" streaming behavior and the optional
	// "trailing JSONL objects after the array" behavior.
	//
	// Contract:
	//   - Each object element in the root array becomes one emitted row.
	//   - nil elements are skipped.
	//   - After the closing ']', additional JSON objects in the stream are decoded
	//     and emitted as additional rows.
	//   - Line numbering increments per emitted record (1-based).
	//
	// Edge cases covered:
	//   - array element "null" is skipped
	//   - array-of-strings is flattened using the configured separator
	ctx := context.Background()
	input := `[
		{"a": 1, "b": ["x", "y"]},
		null,
		{"a": 2, "b": []}
	]
	{"a": 3, "b": ["z"]}`

	opts := config.Options{
		"array_join_separator": ",",
	}

	rows, err, parseCalls := runStream(ctx, input, []string{"a", "b"}, opts, 16)
	if err != nil {
		t.Fatalf("StreamJSONRows() err=%v, want nil", err)
	}
	if len(parseCalls) != 0 {
		t.Fatalf("onParseErr calls=%v, want none", parseCalls)
	}
	if len(rows) != 3 {
		t.Fatalf("rows.len=%d, want 3", len(rows))
	}

	// Row 1: a=1, b="x,y"
	if rows[0].Line != 1 {
		t.Fatalf("rows[0].Line=%d, want 1", rows[0].Line)
	}
	if got := mustJSONNumberString(t, rows[0].V[0]); got != "1" {
		t.Fatalf("rows[0].V[a]=%q, want %q", got, "1")
	}
	if got := rows[0].V[1]; got != "x,y" {
		t.Fatalf("rows[0].V[b]=%#v, want %q", got, "x,y")
	}

	// Row 2: a=2, b=""
	if rows[1].Line != 2 {
		t.Fatalf("rows[1].Line=%d, want 2", rows[1].Line)
	}
	if got := mustJSONNumberString(t, rows[1].V[0]); got != "2" {
		t.Fatalf("rows[1].V[a]=%q, want %q", got, "2")
	}
	if got := rows[1].V[1]; got != "" {
		t.Fatalf("rows[1].V[b]=%#v, want empty string", got)
	}

	// Row 3: trailing JSONL object: a=3, b="z"
	if rows[2].Line != 3 {
		t.Fatalf("rows[2].Line=%d, want 3", rows[2].Line)
	}
	if got := mustJSONNumberString(t, rows[2].V[0]); got != "3" {
		t.Fatalf("rows[2].V[a]=%q, want %q", got, "3")
	}
	if got := rows[2].V[1]; got != "z" {
		t.Fatalf("rows[2].V[b]=%#v, want %q", got, "z")
	}
}

func TestStreamJSONRows_RootObject_EnvelopeStreamsFirstArrayField(t *testing.T) {
	// This test verifies the "envelope" behavior for root objects:
	// if the root is an object and it contains an array value, StreamJSONRows
	// treats the *first* such array field as the record stream.
	//
	// Contract:
	//   - The first array field is streamed element-by-element.
	//   - Each element must be a JSON object; otherwise an error is returned.
	//   - After streaming the envelope array, the decoder skips the remaining fields.
	//   - After the closing '}', trailing JSONL objects are also decoded.
	ctx := context.Background()
	input := `{
		"meta": {"ignore": [1,2,3]},
		"records": [{"x": 1}, {"x": 2}],
		"other": {"deep": [{"k": "v"}], "n": 10}
	}
	{"x": 3}`

	rows, err, parseCalls := runStream(ctx, input, []string{"x"}, config.Options{}, 16)
	if err != nil {
		t.Fatalf("StreamJSONRows() err=%v, want nil", err)
	}
	if len(parseCalls) != 0 {
		t.Fatalf("onParseErr calls=%v, want none", parseCalls)
	}
	if len(rows) != 3 {
		t.Fatalf("rows.len=%d, want 3", len(rows))
	}
	for i := 0; i < 3; i++ {
		wantLine := i + 1
		if rows[i].Line != wantLine {
			t.Fatalf("rows[%d].Line=%d, want %d", i, rows[i].Line, wantLine)
		}
	}

	if got := mustJSONNumberString(t, rows[0].V[0]); got != "1" {
		t.Fatalf("rows[0].V[x]=%q, want 1", got)
	}
	if got := mustJSONNumberString(t, rows[1].V[0]); got != "2" {
		t.Fatalf("rows[1].V[x]=%q, want 2", got)
	}
	if got := mustJSONNumberString(t, rows[2].V[0]); got != "3" {
		t.Fatalf("rows[2].V[x]=%q, want 3", got)
	}
}

func TestStreamJSONRows_RootObject_SingleRecordMaterializesNestedValues(t *testing.T) {
	// This test verifies the "single object record" behavior:
	// if the root is an object and it contains no array fields,
	// StreamJSONRows emits exactly one record representing the object.
	//
	// IMPORTANT:
	// The parser treats the first array token inside the root object as an
	// "envelope" array candidate and will attempt to stream it as
	// array-of-objects. Therefore, this test must avoid root-level arrays.
	//
	// Contract:
	//   - Nested objects are materialized as map[string]any.
	//   - Numbers are decoded as json.Number (because the decoder uses UseNumber()).
	ctx := context.Background()
	input := `{
		"x": 1,
		"nested": {"y": 2},
		"flag": true
	}`

	rows, err, parseCalls := runStream(ctx, input, []string{"x", "nested", "flag"}, config.Options{}, 4)
	if err != nil {
		t.Fatalf("StreamJSONRows() err=%v, want nil", err)
	}
	if len(parseCalls) != 0 {
		t.Fatalf("onParseErr calls=%v, want none", parseCalls)
	}
	if len(rows) != 1 {
		t.Fatalf("rows.len=%d, want 1", len(rows))
	}
	if rows[0].Line != 1 {
		t.Fatalf("rows[0].Line=%d, want 1", rows[0].Line)
	}

	// x is json.Number("1")
	if got := mustJSONNumberString(t, rows[0].V[0]); got != "1" {
		t.Fatalf("x=%q, want 1", got)
	}

	// nested is map with y=json.Number("2")
	nested, ok := rows[0].V[1].(map[string]any)
	if !ok {
		t.Fatalf("nested type=%T, want map[string]any", rows[0].V[1])
	}
	if got := mustJSONNumberString(t, nested["y"]); got != "2" {
		t.Fatalf("nested.y=%q, want 2", got)
	}

	// flag is a bool scalar.
	if got := rows[0].V[2]; got != true {
		t.Fatalf("flag=%#v, want true", got)
	}
}

func TestStreamJSONRows_HeaderMap_MapsOriginalKeysToNormalizedColumns(t *testing.T) {
	// This test verifies header_map behavior:
	// header_map maps original JSON field names to normalized column names.
	// StreamJSONRows builds a reverse map so it can find original keys when the
	// normalized column is not present in the object.
	ctx := context.Background()
	input := `{"OrigName": "v"}`
	opts := config.Options{
		"header_map": map[string]any{
			"OrigName": "norm_name",
		},
	}

	rows, err, parseCalls := runStream(ctx, input, []string{"norm_name"}, opts, 1)
	if err != nil {
		t.Fatalf("StreamJSONRows() err=%v, want nil", err)
	}
	if len(parseCalls) != 0 {
		t.Fatalf("onParseErr calls=%v, want none", parseCalls)
	}
	if len(rows) != 1 {
		t.Fatalf("rows.len=%d, want 1", len(rows))
	}
	if got := rows[0].V[0]; got != "v" {
		t.Fatalf("rows[0].V[0]=%#v, want %q", got, "v")
	}
}

func TestStreamJSONRows_ContextCanceled_ReturnsContextErrorAndEmitsNothing(t *testing.T) {
	// This test verifies cancellation behavior:
	// if ctx is already canceled, StreamJSONRows should not block trying to send
	// into out; it should return ctx.Err().
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	input := `[{"a": 1}]`
	out := make(chan *transformer.Row) // unbuffered; send would block if attempted

	var gotParseErr []string
	onParseErr := func(line int, err error) {
		gotParseErr = append(gotParseErr, fmt.Sprintf("line=%d err=%s", line, err.Error()))
	}

	err := StreamJSONRows(ctx, strings.NewReader(input), []string{"a"}, config.Options{}, out, onParseErr)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
	if len(gotParseErr) != 0 {
		t.Fatalf("onParseErr calls=%v, want none", gotParseErr)
	}
}

func TestStreamJSONRows_ErrorPaths_UnsupportedRootsAndElementTypes(t *testing.T) {
	// This test verifies key error paths and ensures onParseErr is called with
	// meaningful line numbers for errors that occur while streaming records.
	tests := []struct {
		name            string
		input           string
		wantErrSubstr   string
		wantParseErrAny bool
		wantParseLine   string // substring like "line=1"
	}{
		{
			name:          "unsupported_root_token_null",
			input:         `null`,
			wantErrSubstr: "unsupported root token",
		},
		{
			name:            "unsupported_root_delimiter",
			input:           `(`,
			wantErrSubstr:   "read first token",
			wantParseErrAny: true,
			wantParseLine:   "line=0",
		},
		{
			name:            "array_element_not_object_calls_onParseErr",
			input:           `[1]`,
			wantErrSubstr:   "array element not an object",
			wantParseErrAny: true,
			wantParseLine:   "line=1",
		},
		{
			name:            "envelope_array_element_not_object_calls_onParseErr",
			input:           `{"records":[1]}`,
			wantErrSubstr:   "array element not an object",
			wantParseErrAny: true,
			wantParseLine:   "line=1",
		},
		{
			name:            "trailing_object_decode_error_calls_onParseErr",
			input:           `{"x":1} not-json`,
			wantErrSubstr:   "decode trailing object",
			wantParseErrAny: true,
			// First object emits line 1; trailing decode error uses line+1 => 2.
			wantParseLine: "line=2",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, err, parseCalls := runStream(ctx, tc.input, []string{"x"}, config.Options{}, 4)

			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("err=%q, want substring %q", err.Error(), tc.wantErrSubstr)
			}

			if tc.wantParseErrAny {
				if len(parseCalls) == 0 {
					t.Fatalf("expected onParseErr calls, got none")
				}
				if tc.wantParseLine != "" && !strings.Contains(parseCalls[0], tc.wantParseLine) {
					t.Fatalf("onParseErr[0]=%q, want substring %q (all=%v)", parseCalls[0], tc.wantParseLine, parseCalls)
				}
			} else if len(parseCalls) != 0 {
				t.Fatalf("unexpected onParseErr calls: %v", parseCalls)
			}
		})
	}
}

func TestNormalizeScalarJSONValue(t *testing.T) {
	// This test covers normalizeScalarJSONValue, which is used to flatten arrays
	// of strings into a joined scalar value.
	tests := []struct {
		name string
		in   any
		sep  string
		want any
	}{
		{name: "nil", in: nil, sep: ",", want: nil},
		{name: "string_array_joined", in: []string{"a", "b"}, sep: "|", want: "a|b"},
		{name: "string_array_empty", in: []string{}, sep: ",", want: ""},
		{name: "any_array_strings_joined", in: []any{"a", "b"}, sep: ",", want: "a,b"},
		{name: "any_array_all_nil_returns_empty", in: []any{nil, nil}, sep: ",", want: ""},
		{name: "any_array_mixed_types_returns_original", in: []any{"a", 1}, sep: ",", want: []any{"a", 1}},
		{name: "scalar_passthrough", in: true, sep: ",", want: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeScalarJSONValue(tc.in, tc.sep)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("normalizeScalarJSONValue(%#v,%q)=%#v, want %#v", tc.in, tc.sep, got, tc.want)
			}
		})
	}
}

func TestRecordToRowJSONStreaming_UsesReverseHeaderMapWithoutCopying(t *testing.T) {
	// This test verifies recordToRowJSONStreaming looks up values by normalized column
	// name and falls back to the original key via rev map.
	obj := map[string]any{
		"Orig": "v",
	}
	columns := []string{"norm", "missing"}
	rev := map[string]string{"norm": "Orig"}

	got := recordToRowJSONStreaming(obj, columns, rev, ",")
	if len(got) != 2 {
		t.Fatalf("len(row)=%d, want 2", len(got))
	}
	if got[0] != "v" {
		t.Fatalf("row[0]=%#v, want %q", got[0], "v")
	}
	if got[1] != nil {
		t.Fatalf("row[1]=%#v, want nil", got[1])
	}
}

// mustJSONNumberString extracts the string form of a json.Number.
//
// Tests in this file expect numeric values to be json.Number because the parser
// sets decoder.UseNumber(). If the type does not match, the test fails with a
// clear message.
func mustJSONNumberString(t *testing.T, v any) string {
	t.Helper()
	n, ok := v.(json.Number)
	if !ok {
		t.Fatalf("value type=%T, want json.Number", v)
	}
	return n.String()
}

// ---- Benchmarks ----

func BenchmarkNormalizeScalarJSONValue(b *testing.B) {
	// This benchmark measures the hot helper used per-field, per-row.
	// It avoids I/O and focuses on the normalization logic.
	cases := []struct {
		name string
		in   any
		sep  string
	}{
		{name: "nil", in: nil, sep: ","},
		{name: "slice_string_small", in: []string{"a", "b", "c"}, sep: ","},
		{name: "slice_any_strings_small", in: []any{"a", "b", "c"}, sep: ","},
		{name: "slice_any_mixed", in: []any{"a", 1, "c"}, sep: ","},
		{name: "scalar", in: true, sep: ","},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = normalizeScalarJSONValue(tc.in, tc.sep)
			}
		})
	}
}

func BenchmarkRecordToRowJSONStreaming(b *testing.B) {
	// This benchmark measures mapping a JSON object to a row with column alignment.
	// It is a core per-record operation in StreamJSONRows.
	obj := map[string]any{
		"a": json.Number("1"),
		"b": []any{"x", "y", "z"},
		"c": "s",
		"d": nil,
	}
	columns := []string{"a", "b", "c", "d", "missing"}
	rev := map[string]string{"missing": "c"} // forces fallback for one column

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = recordToRowJSONStreaming(obj, columns, rev, ",")
	}
}

func BenchmarkStreamJSONRows_RootArray(b *testing.B) {
	// This benchmark measures end-to-end parsing and row materialization for
	// a root JSON array, without I/O or external dependencies.
	//
	// It uses an in-memory reader and a buffered channel to avoid blocking.
	ctx := context.Background()
	input := `[{"a":1,"b":["x","y"]},{"a":2,"b":["z"]},{"a":3,"b":[]}]`
	columns := []string{"a", "b"}
	opts := config.Options{"array_join_separator": ","}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create a fresh decoder state per iteration.
		r := strings.NewReader(input)
		out := make(chan *transformer.Row, 8)

		// Drain outputs deterministically after the call.
		if err := StreamJSONRows(ctx, r, columns, opts, out, nil); err != nil {
			b.Fatalf("StreamJSONRows() err=%v", err)
		}
		close(out)
		for range out {
			// discard
		}
	}
}

// Compile-time sanity check: ensure the test file compiles even if io is otherwise unused.
var _ io.Reader
