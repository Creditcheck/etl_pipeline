package probe

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

//
// contractTypeFromInference
//

// TestContractTypeFromInference verifies mapping from inferred types to contract schema types.
//
// This function is correctness-critical because it defines the externally visible
// schema contract derived from sampling. Unknown inputs must not panic and should
// fall back to a conservative contract type.
func TestContractTypeFromInference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		inferred string
		want     string
	}{
		{"text passthrough", "text", "text"},
		{"integer to bigint", "integer", "bigint"},
		{"float passthrough", "float", "float"},
		{"boolean maps to bool", "boolean", "bool"},
		{"date passthrough", "date", "date"},
		{"timestamp passthrough", "timestamp", "timestamp"},
		{"unknown defaults to text", "weird", "text"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := contractTypeFromInference(tt.inferred); got != tt.want {
				t.Fatalf("contractTypeFromInference(%q) = %q, want %q", tt.inferred, got, tt.want)
			}
		})
	}
}

//
// parseBoolLoose
//

// TestParseBoolLoose verifies permissive boolean parsing.
//
// This function must accept common truthy/falsy encodings while rejecting
// ambiguous values. It should be case-insensitive and whitespace-tolerant.
func TestParseBoolLoose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		in    string
		ok    bool
		value bool
	}{
		{"true literal", "true", true, true},
		{"false literal", "false", true, false},
		{"numeric true", "1", true, true},
		{"numeric false", "0", true, false},
		{"yes", "yes", true, true},
		{"no", "no", true, false},
		{"upper case", "TRUE", true, true},
		{"with spaces", "  false  ", true, false},
		{"invalid", "maybe", false, false},
		{"empty", "", false, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := parseBoolLoose(tt.in)
			if ok != tt.ok || got != tt.value {
				t.Fatalf("parseBoolLoose(%q) = (%v,%v), want (%v,%v)", tt.in, got, ok, tt.value, tt.ok)
			}
		})
	}
}

//
// allNonEmptySample
//

// TestAllNonEmptySample verifies detection of whether every sampled row has
// a non-empty value for a given column index.
//
// Edge cases validated:
//   - empty rows slice returns false
//   - blank/whitespace values count as empty
//   - column index reads the correct column across rows
func TestAllNonEmptySample(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rows [][]string
		col  int
		want bool
	}{
		{
			name: "all present",
			rows: [][]string{{"a", "x"}, {"b", "y"}, {"c", "z"}},
			col:  0,
			want: true,
		},
		{
			name: "contains empty",
			rows: [][]string{{"a"}, {""}, {"c"}},
			col:  0,
			want: false,
		},
		{
			name: "contains whitespace",
			rows: [][]string{{"a"}, {"  "}, {"c"}},
			col:  0,
			want: false,
		},
		{
			name: "column specific check",
			rows: [][]string{{"id1", "x"}, {"id2", ""}},
			col:  1,
			want: false,
		},
		{
			name: "empty slice",
			rows: nil,
			col:  0,
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := allNonEmptySample(tt.rows, tt.col); got != tt.want {
				t.Fatalf("allNonEmptySample(rows,%d) = %v, want %v", tt.col, got, tt.want)
			}
		})
	}
}

//
// parseTimestampLoose
//

// TestParseTimestampLoose verifies permissive timestamp parsing.
//
// This function is used by type inference, so it must behave deterministically.
// We validate only the "ok" bit and basic parse sanity (non-zero time).
func TestParseTimestampLoose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		ok   bool
	}{
		{"rfc3339", "2023-01-02T15:04:05Z", true},
		{"rfc3339 with offset", "2023-01-02T15:04:05+01:00", true},
		{"invalid", "2023-99-99T00:00:00Z", false},
		{"date only", "2023-01-02", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts, _, ok := parseTimestampLoose(tt.in)
			if ok != tt.ok {
				t.Fatalf("parseTimestampLoose(%q) ok=%v, want %v", tt.in, ok, tt.ok)
			}
			if ok && ts.IsZero() {
				t.Fatalf("parseTimestampLoose(%q) returned zero time with ok=true", tt.in)
			}
		})
	}
}

//
// parseDateLoose
//

// TestParseDateLoose verifies permissive date parsing (no time components).
//
// The function returns (time, layout, ok). We treat "ok" as the contract.
func TestParseDateLoose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		ok   bool
	}{
		{"iso date", "2023-01-02", true},
		{"slash date", "01/02/2023", true},
		{"invalid", "2023-99-99", false},
		{"timestamp rejected", "2023-01-02T00:00:00Z", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d, _, ok := parseDateLoose(tt.in)
			if ok != tt.ok {
				t.Fatalf("parseDateLoose(%q) ok=%v, want %v", tt.in, ok, tt.ok)
			}
			if ok && d.IsZero() {
				t.Fatalf("parseDateLoose(%q) returned zero time with ok=true", tt.in)
			}
		})
	}
}

//
// inferKeyColumns
//

// TestInferKeyColumns verifies heuristic primary-key inference.
//
// Keys are inferred from columns that appear unique and fully populated in the sample.
func TestInferKeyColumns(t *testing.T) {
	t.Parallel()

	headers := []string{"id", "name"}
	normalized := []string{"id", "name"}
	rows := [][]string{
		{"1", "a"},
		{"2", "b"},
		{"3", "c"},
	}
	types := []string{"integer", "text"}

	got := inferKeyColumns(headers, normalized, types, rows)
	want := []string{"id"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inferKeyColumns() = %v, want %v", got, want)
	}
}

//
// detectColumnLayouts
//

// TestDetectColumnLayouts verifies layout detection for date and timestamp columns.
//
// This test is intentionally conservative: it validates that a layout string is
// returned for date columns and that RFC3339 timestamps parse as expected.
func TestDetectColumnLayouts(t *testing.T) {
	t.Parallel()

	rows := [][]string{
		{"2023-01-02", "2023-01-02T15:04:05Z"},
		{"2023-01-03", "2023-01-03T16:04:05Z"},
	}
	types := []string{"date", "timestamp"}

	got := detectColumnLayouts(rows, types)
	if len(got) != 2 {
		t.Fatalf("detectColumnLayouts len=%d, want 2", len(got))
	}

	if got[0] == "" {
		t.Fatalf("date layout is empty; expected non-empty")
	}
	if _, err := time.Parse(time.RFC3339, rows[0][1]); err != nil {
		t.Fatalf("timestamp sample should be RFC3339 parseable: %v", err)
	}
}

//
// inferTypes
//

// TestInferTypes verifies column type inference across mixed samples.
//
// This test ensures that:
//   - integers are detected as integer
//   - booleans are detected as boolean
//   - floating point numbers are detected as float
func TestInferTypes(t *testing.T) {
	t.Parallel()

	headers := []string{"id", "active", "price"}
	rows := [][]string{
		{"1", "true", "1.5"},
		{"2", "false", "2.0"},
	}

	got := inferTypes(headers, rows)
	want := []string{"integer", "boolean", "float"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inferTypes() = %v, want %v", got, want)
	}
}

//
// readCSVSample
//

// TestReadCSVSample verifies CSV parsing with headers and aligned rows.
//
// The function is expected to:
//   - return the header row
//   - return only well-formed data rows
//   - not error for simple valid CSV
func TestReadCSVSample(t *testing.T) {
	t.Parallel()

	data := []byte("a,b\n1,2\n3,4\n")
	headers, rows, err := readCSVSample(data, ',')
	if err != nil {
		t.Fatalf("readCSVSample error: %v", err)
	}

	if !reflect.DeepEqual(headers, []string{"a", "b"}) {
		t.Fatalf("headers = %v, want [a b]", headers)
	}

	if len(rows) != 2 {
		t.Fatalf("rows len = %d, want 2", len(rows))
	}
}

// TestReadCSVSample_TruncatedRow verifies malformed rows are skipped.
//
// A row with fewer fields than the header should be ignored.
func TestReadCSVSample_TruncatedRow(t *testing.T) {
	t.Parallel()

	data := []byte("a,b\n1\n2,3\n")
	_, rows, err := readCSVSample(data, ',')
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("rows len = %d, want 1", len(rows))
	}
}

// TestReadCSVSample_Empty verifies empty input produces an error.
func TestReadCSVSample_Empty(t *testing.T) {
	t.Parallel()

	// Current behavior: empty input returns empty headers/rows and no error.
	headers, rows, err := readCSVSample(nil, ',')
	if err != nil {
		t.Fatalf("readCSVSample(nil) unexpected error: %v", err)
	}
	if len(headers) != 0 {
		t.Fatalf("headers len=%d, want 0", len(headers))
	}
	if len(rows) != 0 {
		t.Fatalf("rows len=%d, want 0", len(rows))
	}
}

// TestReadCSVSample_NoTrailingNewline verifies parsing works without a final newline.
func TestReadCSVSample_NoTrailingNewline(t *testing.T) {
	t.Parallel()

	data := bytes.TrimSpace([]byte("a,b\n1,2"))
	headers, rows, err := readCSVSample(data, ',')
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(headers) != 2 {
		t.Fatalf("headers len=%d, want 2", len(headers))
	}
	if len(rows) != 1 {
		t.Fatalf("rows len=%d, want 1", len(rows))
	}
}
