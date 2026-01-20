package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// row is one input JSON element.
type row struct {
	Count string `json:"count"`
	Href  string `json:"href"`
}

// run is the testable entrypoint for this command.
//
// It parses args, reads JSON from either a file (via -file) or stdin, and writes
// deduplicated output URLs to stdout.
//
// Exit codes:
//   - 0 on success
//   - 1 on operational errors (I/O, JSON decoding, invalid count value)
//   - 2 on invalid CLI usage (bad flags)
//
// When to use: main() delegates to run so tests can drive the command without
// spawning subprocesses.
//
// Edge cases:
//   - Empty href values are ignored.
//   - Missing count prints only the href.
//   - Count strings may contain spaces (e.g. "1 000").
//   - Output is deduplicated across all emitted lines (href and href/page/).
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var filePath string
	var perPage int

	fs := flag.NewFlagSet("expand_hrefs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&filePath, "file", "", "path to input JSON file (reads stdin if omitted)")
	fs.IntVar(&perPage, "count", 25, "number of records per page")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if perPage <= 0 {
		fmt.Fprintln(stderr, "-count must be > 0")
		return 2
	}

	r := stdin
	if filePath != "" {
		f, err := os.Open(filePath)
		if err != nil {
			fmt.Fprintf(stderr, "open %q: %v\n", filePath, err)
			return 1
		}
		defer func() { _ = f.Close() }()
		r = f
	}

	rows, err := decodeRows(r)
	if err != nil {
		fmt.Fprintf(stderr, "decode json: %v\n", err)
		return 1
	}

	if err := writeExpanded(stdout, rows, perPage); err != nil {
		fmt.Fprintf(stderr, "expand: %v\n", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// decodeRows decodes an input JSON array into rows.
//
// Errors:
//   - returns an error if the input is not valid JSON or is not a JSON array of
//     objects compatible with row.
func decodeRows(r io.Reader) ([]row, error) {
	var rows []row
	dec := json.NewDecoder(r)
	if err := dec.Decode(&rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// writeExpanded writes the unique expanded URL set for rows to w.
//
// For each row:
//   - always emit href (if non-empty)
//   - if count is present, compute pages = count/perPage, and for page numbers > 1
//     emit href/<page>/
//
// Deduplication:
//   - ensures each emitted line appears at most once across the entire output.
//   - first occurrence wins; later duplicates are skipped.
//
// Errors:
//   - returns an error if count is present but cannot be parsed as an integer.
func writeExpanded(w io.Writer, rows []row, perPage int) error {
	seen := make(map[string]struct{}, len(rows)*2)

	printUnique := func(s string) error {
		if _, ok := seen[s]; ok {
			return nil
		}
		seen[s] = struct{}{}
		_, err := fmt.Fprintln(w, s)
		return err
	}

	for i, it := range rows {
		href := strings.TrimSuffix(strings.TrimSpace(it.Href), "/")
		if href == "" {
			continue
		}
		if err := printUnique(href); err != nil {
			return err
		}

		if strings.TrimSpace(it.Count) == "" {
			continue
		}

		total, err := parseCount(it.Count)
		if err != nil {
			return fmt.Errorf("row %d: bad count %q: %w", i, it.Count, err)
		}

		pages := total / perPage
		for p := 2; p <= pages; p++ {
			if err := printUnique(fmt.Sprintf("%s/%d/", href, p)); err != nil {
				return err
			}
		}
	}

	return nil
}

// parseCount parses a count string that may include spaces as thousand separators
// (e.g., "1 000") into an int.
//
// Errors:
//   - returns an error for empty strings or non-numeric values after space removal.
func parseCount(s string) (int, error) {
	s = strings.ReplaceAll(strings.TrimSpace(s), " ", "")
	if s == "" {
		return 0, fmt.Errorf("empty count")
	}
	return strconv.Atoi(s)
}
