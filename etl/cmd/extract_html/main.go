// Command extract-html reads HTML (from stdin, a URL, or a directory of files),
// applies extraction mappings, and prints JSON.
//
// Usage (stdin):
//
//	cat page.html | extract-html -mappings mappings.json
//
// Usage (fetch URL):
//
//	extract-html -url "https://example.com/page" -mappings mappings.json
//
// Usage (directory mode):
//
//	extract-html -dir "./pages" -mappings mappings.json
//
// Debug (print outer HTML blocks):
//
//	cat page.html | extract-html -selector "div#firmInfo"
//
// Debug (print text for selector matches):
//
//	cat page.html | extract-html -selector "#kontakty-firmy" -text
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"etl/internal/extracthtml"
)

func main() {
	os.Exit(run(
		context.Background(),
		os.Args[1:],
		os.Stdin,
		os.Stdout,
		os.Stderr,
		http.DefaultClient,
	))
}

// run is split out from main so we can unit test the command without spawning
// an OS process.
//
// It returns a Unix-style exit code:
//   - 0 for success
//   - 2 for usage/config errors
//   - 1 for operational/runtime errors
func run(
	ctx context.Context,
	args []string,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	httpClient *http.Client,
) int {
	fs := flag.NewFlagSet("extract-html", flag.ContinueOnError)
	fs.SetOutput(stderr)

	printCategoryURLs := fs.Bool(
		"printCategoryUrls",
		false,
		"Print paginated category URLs inferred from extracted count (25 per page). Requires -url.",
	)
	onlyText := fs.Bool("text", false, "Debug: print text blocks for -selector matches (not JSON)")
	debugSelector := fs.String("selector", "", "Debug: CSS selector to print matches for (not JSON)")
	mappingsPath := fs.String("mappings", "", "Path to mappings JSON file (required for JSON extraction)")
	urlFlag := fs.String("url", "", "Optional: fetch HTML from URL instead of stdin")
	timeout := fs.Duration("timeout", 20*time.Second, "Timeout for -url fetch")
	dirFlag := fs.String("dir", "", "Optional: directory containing HTML files to parse (one record per file)")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	loader := extracthtml.NewLoader(httpClient, *timeout)

	// Debug selector mode needs HTML input (stdin or url) but NOT mappings.
	if *debugSelector != "" {
		html, err := loader.Load(ctx, extracthtml.Input{
			URL:   *urlFlag,
			Stdin: stdin,
		})
		if err != nil {
			fmt.Fprintf(stderr, "load html: %v\n", err)
			return 1
		}

		if err := extracthtml.DebugPrintSelector(stdout, html, *debugSelector, *onlyText); err != nil {
			fmt.Fprintf(stderr, "debug selector: %v\n", err)
			return 1
		}
		return 0
	}

	// Mapping-driven mode (JSON output)
	if *mappingsPath == "" {
		fmt.Fprintf(stderr, "missing -mappings\n")
		return 2
	}

	mf, err := extracthtml.LoadMappingFile(*mappingsPath)
	if err != nil {
		fmt.Fprintf(stderr, "load mappings: %v\n", err)
		return 2
	}

	enc := json.NewEncoder(stdout)
	enc.SetEscapeHTML(false)

	// If requested, print pagination URLs instead of JSON extraction output.
	if *printCategoryURLs {
		if *urlFlag == "" {
			fmt.Fprintf(stderr, "-printCategoryUrls requires -url\n")
			return 2
		}

		html, err := loader.Load(ctx, extracthtml.Input{
			URL: *urlFlag,
		})
		if err != nil {
			fmt.Fprintf(stderr, "load html: %v\n", err)
			return 1
		}

		if err := extracthtml.PrintPagingURLs(stdout, *urlFlag, html, mf); err != nil {
			fmt.Fprintf(stderr, "printCategoryUrls: %v\n", err)
			return 1
		}
		return 0
	}

	// Directory mode: stream output as a single JSON array.
	if *dirFlag != "" {
		if err := extracthtml.StreamFromDir(stdout, *dirFlag, mf, enc); err != nil {
			fmt.Fprintf(stderr, "dir extract: %v\n", err)
			return 1
		}
		return 0
	}

	// Single input mode: stdin OR -url
	html, err := loader.Load(ctx, extracthtml.Input{
		URL:   *urlFlag,
		Stdin: stdin,
	})
	if err != nil {
		fmt.Fprintf(stderr, "load html: %v\n", err)
		return 1
	}

	// Record mode: output []object (one per record container)
	if mf.RecordSelector != "" {
		records, err := extracthtml.ExtractRecordsHTML(html, mf.RecordSelector, mf.Mappings)
		if err != nil {
			fmt.Fprintf(stderr, "extract records: %v\n", err)
			return 1
		}
		if err := enc.Encode(records); err != nil {
			fmt.Fprintf(stderr, "encode json: %v\n", err)
			return 1
		}
		return 0
	}

	// Single-object mode: output one object
	obj, err := extracthtml.ExtractOneHTML(html, mf.Mappings)
	if err != nil {
		fmt.Fprintf(stderr, "extract: %v\n", err)
		return 1
	}
	if err := enc.Encode(obj); err != nil {
		fmt.Fprintf(stderr, "encode json: %v\n", err)
		return 1
	}
	return 0
}
