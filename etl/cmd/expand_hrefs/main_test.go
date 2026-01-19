package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRun_StdinHappyPath verifies the command reads JSON from stdin when -file
// is not provided and emits the expected expanded URLs.
//
// This is the primary behavior of the utility.
// Edge cases covered in the same test:
//   - count contains spaces ("1 000")
//   - rows without count only emit href
//   - output is unique across all emitted lines
func TestRun_StdinHappyPath(t *testing.T) {
	t.Parallel()

	in := `[
  {"count":"101","href":"urlA"},
  {"href":"urlB"},
  {"count":"1 000","href":"urlC"},
  {"count":"101","href":"urlA"}
]`

	var stdout, stderr bytes.Buffer
	code := run([]string{"-count=25"}, strings.NewReader(in), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run() code=%d, want 0; stderr=%q", code, stderr.String())
	}

	// 101/25 = 4 => emit urlA + urlA/2/ + urlA/3/ + urlA/4/
	// urlB no count => urlB
	// 1000/25 = 40 => urlC + urlC/2/..urlC/40/
	// urlA repeated row => should not add duplicates.
	out := strings.Split(strings.TrimSpace(stdout.String()), "\n")

	// Spot-check key lines and dedupe.
	wantContains := []string{
		"urlA",
		"urlA/2/",
		"urlA/3/",
		"urlA/4/",
		"urlB",
		"urlC",
		"urlC/40/",
	}
	for _, w := range wantContains {
		if !containsLine(out, w) {
			t.Fatalf("output missing %q; got:\n%s", w, stdout.String())
		}
	}
	if countLine(out, "urlA") != 1 {
		t.Fatalf("expected urlA once; got:\n%s", stdout.String())
	}
	if countLine(out, "urlA/2/") != 1 {
		t.Fatalf("expected urlA/2/ once; got:\n%s", stdout.String())
	}
}

// TestRun_FileInput verifies the command reads JSON from a file when -file
// is provided.
//
// This test ensures the file branch is exercised without relying on external
// state; it uses a temp dir and file created by the test.
func TestRun_FileInput(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "in.json")
	if err := os.WriteFile(path, []byte(`[{"href":"urlB"}]`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"-file=" + path}, strings.NewReader("ignored"), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run() code=%d, want 0; stderr=%q", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "urlB" {
		t.Fatalf("stdout=%q, want %q", got, "urlB")
	}
}

// TestRun_InvalidCount verifies that an invalid count value results in a
// non-zero exit code and a helpful error message.
//
// This guards correctness of parsing and error propagation.
func TestRun_InvalidCount(t *testing.T) {
	t.Parallel()

	in := `[{"count":"not-a-number","href":"urlA"}]`

	var stdout, stderr bytes.Buffer
	code := run([]string{"-count=25"}, strings.NewReader(in), &stdout, &stderr)
	if code != 1 {
		t.Fatalf("run() code=%d, want 1; stderr=%q", code, stderr.String())
	}
	if stdout.Len() == 0 {
		// href is emitted before count parsing; existing behavior prints href first.
		// This is intentional and stable: it matches the spec "output the href" first.
		t.Fatalf("expected href to be printed before failure; stdout empty; stderr=%q", stderr.String())
	}
	if !strings.Contains(stderr.String(), "bad count") {
		t.Fatalf("stderr=%q, want substring %q", stderr.String(), "bad count")
	}
}

// TestRun_BadJSON verifies invalid JSON returns exit code 1.
//
// This ensures errors in decoding do not crash and are correctly surfaced.
func TestRun_BadJSON(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	code := run(nil, strings.NewReader("{not json"), &stdout, &stderr)
	if code != 1 {
		t.Fatalf("run() code=%d, want 1; stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "decode json") {
		t.Fatalf("stderr=%q, want substring %q", stderr.String(), "decode json")
	}
}

// TestRun_BadFlag verifies invalid flag usage returns exit code 2.
//
// This matches Go conventions where flag parse errors are usage errors.
func TestRun_BadFlag(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	code := run([]string{"-unknown-flag"}, strings.NewReader("[]"), &stdout, &stderr)
	if code != 2 {
		t.Fatalf("run() code=%d, want 2; stderr=%q", code, stderr.String())
	}
}

// TestRun_InvalidPerPage verifies -count<=0 returns exit code 2.
//
// This is a pure usage error and should not be treated as operational failure.
func TestRun_InvalidPerPage(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	code := run([]string{"-count=0"}, strings.NewReader("[]"), &stdout, &stderr)
	if code != 2 {
		t.Fatalf("run() code=%d, want 2; stderr=%q", code, stderr.String())
	}
}

// TestParseCount verifies parseCount accepts spaces as thousand separators.
//
// This is a hot-path helper and must be strict about invalid inputs while
// accommodating the "1 000" format.
func TestParseCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    int
		wantErr bool
	}{
		{"plain", "101", 101, false},
		{"with spaces", "1 000", 1000, false},
		{"trim spaces", "  25  ", 25, false},
		{"empty", "", 0, true},
		{"whitespace only", "   ", 0, true},
		{"not a number", "12x", 0, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseCount(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseCount(%q) err=nil, want error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCount(%q) err=%v, want nil", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("parseCount(%q)=%d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// TestWriteExpanded_SkipsEmptyHref verifies empty href values are ignored.
//
// This keeps output clean and avoids emitting blank lines.
func TestWriteExpanded_SkipsEmptyHref(t *testing.T) {
	t.Parallel()

	rows := []row{
		{Href: "  "},
		{Href: "urlA"},
	}
	var out bytes.Buffer
	if err := writeExpanded(&out, rows, 25); err != nil {
		t.Fatalf("writeExpanded() err=%v, want nil", err)
	}
	if got := strings.TrimSpace(out.String()); got != "urlA" {
		t.Fatalf("stdout=%q, want %q", got, "urlA")
	}
}

// containsLine reports whether lines contains target exactly.
//
// It is used by tests to avoid brittle full-output comparisons when
// the output is large (e.g., 40 pages).
func containsLine(lines []string, target string) bool {
	for _, s := range lines {
		if s == target {
			return true
		}
	}
	return false
}

// countLine counts occurrences of target in lines.
//
// It is used by tests to validate global deduplication behavior.
func countLine(lines []string, target string) int {
	n := 0
	for _, s := range lines {
		if s == target {
			n++
		}
	}
	return n
}

//
// Benchmarks
//

// BenchmarkParseCount measures the cost of parsing count strings with spaces.
//
// This is a hot-path helper when processing large lists.
func BenchmarkParseCount(b *testing.B) {
	inputs := []string{"101", "1 000", "25", "12 345 678"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = parseCount(inputs[i%len(inputs)])
	}
}

// BenchmarkWriteExpanded measures expansion performance without I/O.
//
// The benchmark uses an in-memory buffer and a fixed row set to focus on the
// core loop and dedup map behavior.
func BenchmarkWriteExpanded(b *testing.B) {
	rows := []row{
		{Href: "urlA", Count: "101"},    // 4 pages
		{Href: "urlB"},                  // 0 pages
		{Href: "urlC", Count: "1 000"},  // 40 pages
		{Href: "urlA", Count: "101"},    // duplicates
		{Href: "urlC", Count: "1 000"},  // duplicates
		{Href: "urlD", Count: "10 000"}, // 400 pages
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out bytes.Buffer
		if err := writeExpanded(&out, rows, 25); err != nil {
			b.Fatalf("writeExpanded() err=%v", err)
		}
	}
}
