package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestHelperProcess is a subprocess entrypoint used by tests.
//
// This pattern allows tests to execute main() and observe:
//   - process exit codes (including os.Exit),
//   - stdout/stderr output,
//
// without terminating the parent "go test" process.
//
// The parent test runs the current test binary with:
//
//	-test.run=TestHelperProcess
//
// and sets GO_WANT_HELPER_PROCESS=1.
//
// Any arguments after a literal "--" are treated as CLI args for the command.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	// Rebuild os.Args to contain only the command arguments passed after "--".
	args := os.Args
	i := 0
	for ; i < len(args); i++ {
		if args[i] == "--" {
			break
		}
	}
	if i < len(args) {
		os.Args = append([]string{args[0]}, args[i+1:]...)
	} else {
		// No args were provided; keep argv0 only.
		os.Args = []string{args[0]}
	}

	main()
	os.Exit(0)
}

// runCmd executes the command's main() in a subprocess and returns the captured
// stdout, stderr, and the process exit code.
//
// The subprocess is the current test binary, re-invoked with
// -test.run=TestHelperProcess, so it runs on all platforms supported by Go tests.
func runCmd(t *testing.T, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()

	cmdArgs := []string{"-test.run=TestHelperProcess", "--"}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err := cmd.Run()
	stdout = outBuf.String()
	stderr = errBuf.String()

	// Exit code handling: nil means exit 0.
	if err == nil {
		return stdout, stderr, 0
	}

	// For non-zero exits, Go returns *exec.ExitError.
	if ee, ok := err.(*exec.ExitError); ok {
		return stdout, stderr, ee.ExitCode()
	}

	// Unexpected error type (e.g., binary not runnable). Fail loudly.
	t.Fatalf("unexpected run error: %T: %v", err, err)
	return "", "", 1
}

func TestMain_ReportMode_SuppressesJSONAndPrintsReportToStdout(t *testing.T) {
	t.Parallel()

	// Create a small CSV file. The probe reads a bounded prefix, so we keep
	// the file tiny but representative (header + a few rows).
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "sample.csv")
	csv := strings.Join([]string{
		"id,category,value",
		"1,a,10",
		"2,a,11",
		"3,b,12",
		"4,b,13",
		"5,c,14",
		"",
	}, "\n")

	if err := os.WriteFile(csvPath, []byte(csv), 0o600); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	stdout, stderr, code := runCmd(
		t,
		"-url", csvPath,
		"-report=true",
		"-multitable=false",
		// keep other defaults
	)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d\nstderr:\n%s\nstdout:\n%s", code, stderr, stdout)
	}

	// In report mode, stdout should contain the report and MUST NOT contain JSON.
	if !strings.Contains(stdout, "uniqueness report:") {
		t.Fatalf("expected report header in stdout, got:\n%s", stdout)
	}
	if strings.Contains(stdout, "{") || strings.Contains(stdout, `"`+"storage"+`"`) {
		t.Fatalf("expected report-only output (no JSON), got stdout:\n%s", stdout)
	}

	// Report mode should not require stderr output.
	_ = stderr
}

func TestMain_DefaultMode_EmitsValidJSON(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	jsonPath := filepath.Join(tmpDir, "sample.json")

	// Provide a simple JSON array of objects (common case).
	// The probe uses a bounded sample; keep it small and valid.
	sample := `[{"a":"x","b":1},{"a":"y","b":2},{"a":"z","b":3}]`
	if err := os.WriteFile(jsonPath, []byte(sample), 0o600); err != nil {
		t.Fatalf("write json: %v", err)
	}

	stdout, stderr, code := runCmd(
		t,
		"-url", jsonPath,
		"-report=false",
		// allow default multitable=true; output still must be valid JSON
	)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d\nstderr:\n%s\nstdout:\n%s", code, stderr, stdout)
	}

	// stdout should be parseable JSON. We don't overfit to exact schema here,
	// because ProbeURLAny may emit either a pipeline struct or a multi-table map
	// depending on flags and future defaults.
	var v any
	if err := json.Unmarshal([]byte(stdout), &v); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
}

func TestMain_MissingURL_ExitsWith2AndPrintsMessage(t *testing.T) {
	t.Parallel()

	stdout, stderr, code := runCmd(t /* no args */)

	// The command explicitly os.Exit(2) when -url is missing.
	if code != 2 {
		t.Fatalf("expected exit code 2, got %d\nstderr:\n%s\nstdout:\n%s", code, stderr, stdout)
	}
	if !strings.Contains(stderr, "missing -url") {
		t.Fatalf("expected missing -url message on stderr, got:\n%s", stderr)
	}

	// Some usage output may also be printed; we don't require exact flag text.
}
