//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"etl/internal/extracthtml"
)

// TestE2E_Strict_MappingsPopulateAcrossMultiplePages performs a strict end-to-end
// validation of mappings loaded from disk against real webpages.
//
// Strict behavior here means:
//
//   - The test fetches one or more URLs (serially, not in parallel).
//   - For each mapping's json_path, the test tallies how many pages produced a
//     non-empty value for that json_path.
//   - If ANY page yields a non-empty value for a given json_path, that json_path
//     is considered "working" (count > 0).
//   - The test fails if any json_path from mappings.json has a tally of 0 across
//     all tested pages.
//
// This mitigates fragility when some pages legitimately omit fields (e.g. no
// social links, missing logo, etc.) while still ensuring each extraction rule
// is functional on at least one real page.
//
// Run:
//
//	E2E=1 \
//	E2E_MAPPINGS_PATH="./mappings-detail-experimental.json" \
//	E2E_TARGET_URLS="https://REPLACE-1.example.com/x,https://REPLACE-2.example.com/y,https://REPLACE-3.example.com/z" \
//
// export E2E_TARGET_URLS='https://www.azet.sk/katalog/asistencne-sluzby_3/,https://www.azet.sk/katalog/auto-moto-internetove-obchody/,https://www.azet.sk/katalog/autoskoly/'
//
//	go test -tags=e2e ./...
func TestE2E_Strict_MappingsPopulateAcrossMultiplePages(t *testing.T) {
	if os.Getenv("E2E") != "1" {
		t.Skip("set E2E=1 to enable real network E2E tests")
	}

	mappingsPath := strings.TrimSpace(os.Getenv("E2E_MAPPINGS_PATH"))
	if mappingsPath == "" {
		mappingsPath = "mappings-detail-experimental.json"
	}

	rawURLs := strings.TrimSpace(os.Getenv("E2E_TARGET_URLS"))
	if rawURLs == "" {
		t.Skip("set E2E_TARGET_URLS to comma-separated real URLs (engineers will provide later)")
	}

	urls := splitCSV(rawURLs)
	if len(urls) == 0 {
		t.Skip("E2E_TARGET_URLS contained no usable URLs after trimming")
	}

	// Load mappings using the refactored internal module.
	mf, err := extracthtml.LoadMappingFile(mappingsPath)
	if err != nil {
		t.Fatalf("LoadMappingFile(%q): %v", mappingsPath, err)
	}
	if len(mf.Mappings) == 0 {
		t.Fatalf("mappings file %q had no mappings", mappingsPath)
	}

	// Tally "non-empty value observed" per json_path across all pages.
	tally := make(map[string]int)

	// Precompute distinct json_paths to ensure we measure coverage correctly
	// even when multiple mapping rows target the same json_path.
	paths := uniqueJSONPaths(mf.Mappings)

	// Use a dedicated HTTP client so E2E behavior is bounded and reproducible.
	// Note: Loader also applies a context timeout, but the client's Timeout
	// provides a second layer of safety.
	httpClient := &http.Client{Timeout: 30 * time.Second}
	loader := extracthtml.NewLoader(httpClient, 25*time.Second)

	// Serial fetch + extract. Do NOT parallelize: reduces flake and load on target.
	ctx := context.Background()
	for i, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" {
			t.Fatalf("url %d is empty after trimming", i+1)
		}

		// Fetch HTML without JS rendering for stability and determinism.
		htmlStr, err := loader.Load(ctx, extracthtml.Input{URL: u})
		if err != nil {
			t.Fatalf("Load(url[%d]=%q): %v", i+1, u, err)
		}

		// Apply mappings using the refactored extraction entrypoint.
		// This avoids depending on internal/unexported implementation details.
		out, err := extracthtml.ExtractOneHTML(htmlStr, mf.Mappings)
		if err != nil {
			t.Fatalf("ExtractOneHTML(url[%d]=%q): %v", i+1, u, err)
		}

		// Sanity: ensure output is JSON-serializable (a core contract of the tool).
		if _, err := json.Marshal(out); err != nil {
			t.Fatalf("json.Marshal(out) (url[%d]=%q): %v", i+1, u, err)
		}

		// Update tallies for any json_path present with non-empty value.
		for _, jp := range paths {
			if hasNonEmptyValue(out[jp]) {
				tally[jp]++
			}
		}
	}

	// Fail if any json_path never produced a non-empty value across all pages.
	var missing []string
	for _, jp := range paths {
		if tally[jp] == 0 {
			missing = append(missing, jp)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)

		var b strings.Builder
		b.WriteString("strict E2E failed: some json_path values were never populated across tested pages:\n")
		for _, jp := range missing {
			b.WriteString("  - " + jp + " (count=0)\n")
			for _, m := range mf.Mappings {
				if m.JSONPath != jp {
					continue
				}
				b.WriteString("      selector=" + m.Selector + " extract=" + m.Extract)
				if m.Attr != "" {
					b.WriteString(" attr=" + m.Attr)
				}
				if m.Match != "" {
					b.WriteString(" match=" + m.Match)
				}
				if m.All {
					b.WriteString(" all=true")
				}
				b.WriteString("\n")
			}
		}

		b.WriteString("\nTallies (non-empty observations across pages):\n")
		for _, jp := range paths {
			b.WriteString("  - " + jp + ": " + itoa(tally[jp]) + "\n")
		}

		b.WriteString("\nURLs tested (serial):\n")
		for i, u := range urls {
			b.WriteString("  - [" + itoa(i+1) + "] " + u + "\n")
		}

		t.Fatal(b.String())
	}
}

// splitCSV splits a comma-separated list into trimmed, non-empty entries.
//
// This helper is intentionally small and deterministic to keep E2E harness
// behavior easy to understand and debug.
func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

// uniqueJSONPaths returns a sorted list of unique JSONPath values from mappings.
//
// We dedupe because multiple mappings can intentionally write to the same json_path
// (e.g., alternate selectors for different page layouts).
func uniqueJSONPaths(mappings []extracthtml.Mapping) []string {
	seen := make(map[string]struct{}, len(mappings))
	var out []string
	for _, m := range mappings {
		if m.JSONPath == "" {
			continue
		}
		if _, ok := seen[m.JSONPath]; ok {
			continue
		}
		seen[m.JSONPath] = struct{}{}
		out = append(out, m.JSONPath)
	}
	sort.Strings(out)
	return out
}

// hasNonEmptyValue reports whether v should count as "populated" for E2E purposes.
//
// The extractor currently emits either:
//   - string
//   - []string
//
// This function treats other types as empty/unexpected to keep the E2E check strict
// and aligned with the tool’s output contract.
func hasNonEmptyValue(v any) bool {
	switch vv := v.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(vv) != ""
	case []string:
		for _, s := range vv {
			if strings.TrimSpace(s) != "" {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// itoa avoids importing strconv for a tiny amount of debug output.
//
// This implementation supports non-negative integers only, which is sufficient
// for indices and tallies in this test.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [32]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + (n % 10))
		n /= 10
	}
	return string(b[i:])
}
