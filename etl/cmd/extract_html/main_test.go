package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRun_StdinSingleObject verifies the "stdin + mappings" happy path.
//
// We test via run() (not main()) so the test is fast, deterministic,
// and does not require an OS-level subprocess.
func TestRun_StdinSingleObject(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	mappingsPath := filepath.Join(tmp, "mappings.json")

	// This mapping extracts the text inside <h1>.
	err := os.WriteFile(mappingsPath, []byte(`{
		"mappings": [
			{"selector":"h1","extract":"text","json_path":"title"}
		]
	}`), 0o600)
	if err != nil {
		t.Fatalf("write mappings: %v", err)
	}

	stdin := bytes.NewBufferString(`<html><body><h1>Hello</h1></body></html>`)
	var stdout, stderr bytes.Buffer

	code := run(
		context.Background(),
		[]string{"-mappings", mappingsPath},
		stdin,
		&stdout,
		&stderr,
		http.DefaultClient,
	)
	if code != 0 {
		t.Fatalf("run returned %d; stderr=%s", code, stderr.String())
	}

	var got map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout is not valid json: %v; out=%s", err, stdout.String())
	}
	if got["title"] != "Hello" {
		t.Fatalf("unexpected title: %#v", got["title"])
	}
}

// TestRun_DebugSelectorText verifies debug selector mode prints text (not JSON).
//
// This ensures we don't regress the debugging workflow, which is often
// used interactively when authoring mappings.
func TestRun_DebugSelectorText(t *testing.T) {
	t.Parallel()

	stdin := bytes.NewBufferString(`<div id="x">  A  </div><div id="x">B</div>`)
	var stdout, stderr bytes.Buffer

	code := run(
		context.Background(),
		[]string{"-selector", "div#x", "-text"},
		stdin,
		&stdout,
		&stderr,
		http.DefaultClient,
	)
	if code != 0 {
		t.Fatalf("run returned %d; stderr=%s", code, stderr.String())
	}

	// We expect two blocks with trimmed text, each separated by a blank line.
	out := stdout.String()
	if out != "A\n\nB\n\n" {
		t.Fatalf("unexpected debug output: %q", out)
	}
}

// TestRun_PrintCategoryURLs verifies -printCategoryUrls uses URL input and prints pages.
//
// We use httptest so the test does not hit real network.
func TestRun_PrintCategoryURLs(t *testing.T) {
	t.Parallel()

	// The page contains "count"=51 and href "/cat".
	// With per-page=25, expected pages are: /cat, /cat/2, /cat/3.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`
			<div class="rec">
			  <a class="href" href="/cat">Category</a>
			  <span class="count">(51)</span>
			</div>`))
	}))
	t.Cleanup(srv.Close)

	tmp := t.TempDir()
	mappingsPath := filepath.Join(tmp, "mappings.json")

	// Record mode: one record per ".rec".
	// We must output json_path "href" and "count" so pagination can work.
	err := os.WriteFile(mappingsPath, []byte(`{
		"record_selector": ".rec",
		"mappings": [
			{"selector":"a.href","extract":"attr","attr":"href","json_path":"href"},
			{"selector":"span.count","extract":"text","json_path":"count"}
		]
	}`), 0o600)
	if err != nil {
		t.Fatalf("write mappings: %v", err)
	}

	var stdout, stderr bytes.Buffer

	client := &http.Client{Timeout: 2 * time.Second}

	code := run(
		context.Background(),
		[]string{
			"-mappings", mappingsPath,
			"-url", srv.URL,
			"-printCategoryUrls",
		},
		bytes.NewBuffer(nil),
		&stdout,
		&stderr,
		client,
	)
	if code != 0 {
		t.Fatalf("run returned %d; stderr=%s", code, stderr.String())
	}

	// The command prints absolute URLs. The base is the server URL.
	want := srv.URL + "/cat\n" + srv.URL + "/cat/2\n" + srv.URL + "/cat/3\n"
	if stdout.String() != want {
		t.Fatalf("unexpected pagination output:\nwant=%q\ngot=%q", want, stdout.String())
	}
}
