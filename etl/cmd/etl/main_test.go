package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"etl/internal/metrics/datadog"
	"etl/internal/multitable"
)

// fakeRunner is a deterministic runner used by CLI tests.
//
// It records the number of calls and the last config it received, and returns a
// configurable error.
//
// This fake is concurrency-safe so tests can run with -race even if the CLI
// plumbing changes to call the runner concurrently in the future.
type fakeRunner struct {
	err   error
	calls atomic.Int64

	mu      sync.Mutex
	lastCfg multitable.Pipeline
}

func (r *fakeRunner) Run(ctx context.Context, cfg multitable.Pipeline) error {
	_ = ctx // not asserted in these tests; contract is "ctx is passed through"
	r.calls.Add(1)
	r.mu.Lock()
	r.lastCfg = cfg
	r.mu.Unlock()
	return r.err
}

// fakeMetricsBackend is a deterministic metrics backend used by initMetrics tests.
type fakeMetricsBackend struct {
	closeErr error
	closed   atomic.Int64
}

func (b *fakeMetricsBackend) Close() error {
	b.closed.Add(1)
	return b.closeErr
}

func TestRunMain_UsageErrors(t *testing.T) {
	t.Parallel()

	// This test verifies the CLI's "usage error" contract:
	//   - exit code is 2
	//   - stderr contains a helpful message
	//   - no side effects occur (no file reads, no metrics init, no runner construction)
	tests := []struct {
		name            string
		args            []string
		wantCode        int
		wantStderrSub   string
		wantStdoutEmpty bool
	}{
		{
			name:            "missing_config_flag",
			args:            []string{},
			wantCode:        2,
			wantStderrSub:   "usage: etl -config",
			wantStdoutEmpty: true,
		},
		{
			name:            "empty_config_value",
			args:            []string{"-config", "   "},
			wantCode:        2,
			wantStderrSub:   "usage: etl -config",
			wantStdoutEmpty: true,
		},
		{
			name:            "unknown_flag_is_usage_error",
			args:            []string{"-nope"},
			wantCode:        2,
			wantStderrSub:   "flag provided but not defined",
			wantStdoutEmpty: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer

			// Each seam fatals if called, proving usage failures short-circuit
			// before any side effects occur.
			code := runMain(context.Background(), tc.args, &stdout, &stderr, appDeps{
				readFile: func(string) ([]byte, error) {
					t.Fatalf("readFile must not be called on usage errors")
					return nil, nil
				},
				unmarshal: func([]byte, any) error {
					t.Fatalf("unmarshal must not be called on usage errors")
					return nil
				},
				newRunner: func() runner {
					t.Fatalf("newRunner must not be called on usage errors")
					return &fakeRunner{}
				},
				initMetrics: func(context.Context, string, string) (func(), error) {
					t.Fatalf("initMetrics must not be called on usage errors")
					return func() {}, nil
				},
			})

			if code != tc.wantCode {
				t.Fatalf("exit code=%d, want %d; stderr=%q", code, tc.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderrSub) {
				t.Fatalf("stderr=%q, want contains %q", stderr.String(), tc.wantStderrSub)
			}
			if tc.wantStdoutEmpty && stdout.Len() != 0 {
				t.Fatalf("stdout=%q, want empty", stdout.String())
			}
		})
	}
}

func TestRunMain_ReadParseMetricsRun_FullFlow(t *testing.T) {
	t.Parallel()

	// This test validates:
	//   - error precedence (read -> parse -> initMetrics -> run)
	//   - runner is called only after metrics init succeeds
	//   - cleanup ownership: cleanup must run exactly once when initMetrics succeeds
	tests := []struct {
		name             string
		readErr          error
		unmarshalErr     error
		initMetricsErr   error
		runErr           error
		wantCode         int
		wantStderrSub    string
		wantStdout       string
		wantRunnerCalls  int64
		wantCleanupCalls int64
	}{
		{
			name:             "read_config_error",
			readErr:          errors.New("no such file"),
			wantCode:         1,
			wantStderrSub:    "read config:",
			wantRunnerCalls:  0,
			wantCleanupCalls: 0,
		},
		{
			name:             "parse_config_error",
			unmarshalErr:     errors.New("bad json"),
			wantCode:         1,
			wantStderrSub:    "parse config:",
			wantRunnerCalls:  0,
			wantCleanupCalls: 0,
		},
		{
			name:             "init_metrics_error",
			initMetricsErr:   errors.New("metrics unavailable"),
			wantCode:         1,
			wantStderrSub:    "init metrics:",
			wantRunnerCalls:  0,
			wantCleanupCalls: 0,
		},
		{
			name:             "runner_error_runs_cleanup",
			runErr:           errors.New("db failed"),
			wantCode:         1,
			wantStderrSub:    "run:",
			wantRunnerCalls:  1,
			wantCleanupCalls: 1,
		},
		{
			name:             "success",
			wantCode:         0,
			wantStdout:       "ok\n",
			wantRunnerCalls:  1,
			wantCleanupCalls: 1,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer
			fr := &fakeRunner{err: tc.runErr}

			var cleanupCalls atomic.Int64
			cleanup := func() { cleanupCalls.Add(1) }

			deps := appDeps{
				readFile: func(path string) ([]byte, error) {
					// Assumption: runMain passes through the -config value unchanged.
					if path != "cfg.json" {
						t.Fatalf("readFile path=%q, want %q", path, "cfg.json")
					}
					if tc.readErr != nil {
						return nil, tc.readErr
					}
					return []byte(`{"job":"job1"}`), nil
				},
				unmarshal: func(data []byte, v any) error {
					_ = data
					if tc.unmarshalErr != nil {
						return tc.unmarshalErr
					}
					// We avoid real json.Unmarshal here: this unit test verifies CLI orchestration,
					// not the encoding/json package.
					p, ok := v.(*multitable.Pipeline)
					if !ok {
						t.Fatalf("unmarshal target type=%T, want *multitable.Pipeline", v)
					}
					p.Job = "job1"
					return nil
				},
				initMetrics: func(ctx context.Context, jobName, backendName string) (func(), error) {
					_ = ctx
					// Assumption: job name is forwarded from config to metrics initialization.
					if jobName != "job1" {
						t.Fatalf("jobName=%q, want %q", jobName, "job1")
					}
					_ = backendName
					if tc.initMetricsErr != nil {
						return func() {}, tc.initMetricsErr
					}
					return cleanup, nil
				},
				newRunner: func() runner { return fr },
			}

			code := runMain(
				context.Background(),
				[]string{"-config", "cfg.json", "-metrics-backend", "none"},
				&stdout,
				&stderr,
				deps,
			)

			if code != tc.wantCode {
				t.Fatalf("exit code=%d, want %d; stderr=%q", code, tc.wantCode, stderr.String())
			}
			if tc.wantStderrSub != "" && !strings.Contains(stderr.String(), tc.wantStderrSub) {
				t.Fatalf("stderr=%q, want contains %q", stderr.String(), tc.wantStderrSub)
			}
			if tc.wantStdout != "" {
				if got := stdout.String(); got != tc.wantStdout {
					t.Fatalf("stdout=%q, want %q", got, tc.wantStdout)
				}
			} else if stdout.Len() != 0 {
				t.Fatalf("stdout=%q, want empty", stdout.String())
			}

			// Runner should only execute after config + metrics init succeed.
			if got := fr.calls.Load(); got != tc.wantRunnerCalls {
				t.Fatalf("runner calls=%d, want %d", got, tc.wantRunnerCalls)
			}

			// Cleanup must execute exactly once when initMetrics succeeded.
			if got := cleanupCalls.Load(); got != tc.wantCleanupCalls {
				t.Fatalf("cleanup calls=%d, want %d", got, tc.wantCleanupCalls)
			}
		})
	}
}

func TestInitMetrics_None_DoesNotMutateGlobalState(t *testing.T) {
	t.Parallel()

	// This test ensures the "none/noop" backend never calls setMetricsBackend.
	// That prevents surprising global state mutation in environments without metrics.
	oldSet := setMetricsBackend
	defer func() { setMetricsBackend = oldSet }()

	setMetricsBackend = func(any) {
		t.Fatalf("setMetricsBackend must not be called for none/noop")
	}

	cleanup, err := initMetrics(context.Background(), "job", "")
	if err != nil {
		t.Fatalf("initMetrics err=%v, want nil", err)
	}
	// Ownership rule: cleanup must always be non-nil and safe to call.
	if cleanup == nil {
		t.Fatalf("cleanup=nil, want non-nil")
	}
	cleanup()
}

func TestInitMetrics_Datadog_WiresBackendAndCloses(t *testing.T) {
	t.Parallel()

	// This test verifies the contract for the "datadog" backend:
	//   - backend construction is attempted once
	//   - the backend is wired into the global metrics package (via seam)
	//   - cleanup calls Close exactly once
	b := &fakeMetricsBackend{}

	var (
		newCalls atomic.Int64
		setCalls atomic.Int64
		gotOpts  datadog.Options
	)

	oldNew := newDatadogBackend
	oldSet := setMetricsBackend
	oldLog := logPrintf
	defer func() {
		newDatadogBackend = oldNew
		setMetricsBackend = oldSet
		logPrintf = oldLog
	}()

	newDatadogBackend = func(ctx context.Context, opts datadog.Options) (metricsBackend, error) {
		_ = ctx
		newCalls.Add(1)
		gotOpts = opts
		return b, nil
	}
	setMetricsBackend = func(any) { setCalls.Add(1) }

	// Close should not log on success; capture output to enforce that.
	var logged bytes.Buffer
	logPrintf = func(format string, v ...any) {
		fmt.Fprintf(&logged, format, v...)
	}

	cleanup, err := initMetrics(context.Background(), "jobA", "datadog")
	if err != nil {
		t.Fatalf("initMetrics err=%v, want nil", err)
	}
	if cleanup == nil {
		t.Fatalf("cleanup=nil, want non-nil")
	}

	// Assert option propagation: job name must be forwarded to the backend constructor.
	if gotOpts.JobName != "jobA" {
		t.Fatalf("datadog options JobName=%q, want %q", gotOpts.JobName, "jobA")
	}

	if newCalls.Load() != 1 {
		t.Fatalf("newDatadogBackend calls=%d, want 1", newCalls.Load())
	}
	if setCalls.Load() != 1 {
		t.Fatalf("setMetricsBackend calls=%d, want 1", setCalls.Load())
	}

	cleanup()
	if b.closed.Load() != 1 {
		t.Fatalf("backend closed=%d, want 1", b.closed.Load())
	}
	if logged.Len() != 0 {
		t.Fatalf("unexpected log output: %q", logged.String())
	}
}

func TestInitMetrics_Datadog_CloseErrorIsLogged(t *testing.T) {
	t.Parallel()

	// Close failures should be logged but should not panic or return errors
	// from cleanup (cleanup is best-effort flush).
	b := &fakeMetricsBackend{closeErr: errors.New("flush failed")}

	oldNew := newDatadogBackend
	oldSet := setMetricsBackend
	oldLog := logPrintf
	defer func() {
		newDatadogBackend = oldNew
		setMetricsBackend = oldSet
		logPrintf = oldLog
	}()

	newDatadogBackend = func(context.Context, datadog.Options) (metricsBackend, error) { return b, nil }
	setMetricsBackend = func(any) {}

	var logged bytes.Buffer
	logPrintf = func(format string, v ...any) {
		fmt.Fprintf(&logged, format, v...)
	}

	cleanup, err := initMetrics(context.Background(), "job", "dd")
	if err != nil {
		t.Fatalf("initMetrics err=%v, want nil", err)
	}
	cleanup()

	if b.closed.Load() != 1 {
		t.Fatalf("backend closed=%d, want 1", b.closed.Load())
	}
	if !strings.Contains(logged.String(), "metrics: datadog close error") {
		t.Fatalf("log=%q, want contains close error prefix", logged.String())
	}
	if !strings.Contains(logged.String(), "flush failed") {
		t.Fatalf("log=%q, want contains underlying error", logged.String())
	}
}

func TestInitMetrics_UnknownBackendErrors(t *testing.T) {
	t.Parallel()

	// Unknown backend should fail fast with a clear error message.
	cleanup, err := initMetrics(context.Background(), "job", "nope")
	if err == nil {
		t.Fatalf("initMetrics err=nil, want error")
	}
	// Even on error, cleanup must be non-nil and safe to call.
	if cleanup == nil {
		t.Fatalf("cleanup=nil, want non-nil")
	}
	cleanup()

	if !strings.Contains(err.Error(), "unknown metrics backend") {
		t.Fatalf("err=%q, want contains %q", err.Error(), "unknown metrics backend")
	}
	if !strings.Contains(err.Error(), "none|datadog") {
		t.Fatalf("err=%q, want contains %q", err.Error(), "none|datadog")
	}
}

// ---- Benchmarks ----

func BenchmarkRunMain_Success_NoIO(b *testing.B) {
	// Measures orchestration overhead of runMain excluding:
	//   - real file I/O
	//   - real JSON decoding
	//   - real metrics backend work
	//
	// This helps catch accidental allocation growth in CLI plumbing.
	ctx := context.Background()

	fr := &fakeRunner{}
	raw := []byte(`{"job":"job1"}`)

	deps := appDeps{
		readFile: func(string) ([]byte, error) { return raw, nil },
		unmarshal: func([]byte, any) error {
			// Keep decoding cheap to focus on runMain overhead.
			return nil
		},
		initMetrics: func(context.Context, string, string) (func(), error) {
			return func() {}, nil
		},
		newRunner: func() runner { return fr },
	}

	args := []string{"-config", "cfg.json", "-metrics-backend", "none"}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var stdout, stderr bytes.Buffer
		code := runMain(ctx, args, &stdout, &stderr, deps)
		if code != 0 {
			b.Fatalf("code=%d, stderr=%q", code, stderr.String())
		}
	}
}

func BenchmarkInitMetrics_None(b *testing.B) {
	// Measures overhead of the no-op backend path (should be near-zero allocations).
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cleanup, err := initMetrics(ctx, "job", "none")
		if err != nil {
			b.Fatalf("err=%v", err)
		}
		cleanup()
	}
}

func BenchmarkInitMetrics_Unknown(b *testing.B) {
	// Measures overhead of the unknown-backend error path.
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cleanup, err := initMetrics(ctx, "job", "nope")
		if err == nil {
			b.Fatalf("want error")
		}
		cleanup()
	}
}
