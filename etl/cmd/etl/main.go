package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"etl/internal/metrics"
	"etl/internal/metrics/datadog"
	"etl/internal/multitable"
	_ "etl/internal/storage/all"
)

// runner is the minimal contract the CLI needs from the pipeline runner.
//
// When to use:
//   - This interface exists to allow unit tests to inject a fake runner.
//   - Production uses multitable.NewDefaultRunner(), which satisfies this contract.
//
// Edge cases:
//   - Implementations should respect ctx cancellation and return promptly when ctx is done.
//
// Errors:
//   - Run returns an error on configuration validation failures, source/transform errors,
//     and storage errors.
type runner interface {
	Run(ctx context.Context, cfg multitable.Pipeline) error
}

// appDeps holds injected seams for the CLI.
//
// This struct exists to make the CLI unit-testable without real filesystem access,
// real metrics backends, or real ETL execution.
//
// When to use:
//   - Production uses defaultAppDeps().
//   - Tests provide deterministic fakes.
//
// Ownership:
//   - stdout and stderr writers are owned by the caller.
//   - If initMetrics succeeds, the returned cleanup function must be called exactly once.
type appDeps struct {
	// readFile loads the pipeline configuration file content.
	readFile func(path string) ([]byte, error)

	// unmarshal decodes configuration bytes into the provided value.
	unmarshal func(data []byte, v any) error

	// newRunner constructs the ETL runner.
	newRunner func() runner

	// initMetrics wires the metrics backend for this process and returns a cleanup func.
	//
	// Errors:
	//   - Returns an error if the backend cannot be initialized.
	initMetrics func(ctx context.Context, jobName, backendName string) (func(), error)
}

// defaultAppDeps returns the production dependencies.
func defaultAppDeps() appDeps {
	return appDeps{
		readFile:  os.ReadFile,
		unmarshal: json.Unmarshal,
		newRunner: func() runner { return multitable.NewDefaultRunner() },
		initMetrics: func(ctx context.Context, jobName, backendName string) (func(), error) {
			return initMetrics(ctx, jobName, backendName)
		},
	}
}

func main() {
	// Keep main small and deterministic: delegate to runMain for testability.
	code := runMain(context.Background(), os.Args[1:], os.Stdout, os.Stderr, defaultAppDeps())
	os.Exit(code)
}

// runMain runs the CLI with injected dependencies and returns the process exit code.
//
// When to use:
//   - Called by main().
//   - Called by unit tests to validate behavior without os.Exit.
//
// Exit codes:
//   - 0 on success.
//   - 1 on runtime errors (I/O, parsing, metrics init, pipeline run).
//   - 2 on usage errors (missing -config or invalid flags).
//
// Error precedence:
//   - Usage errors are detected before any I/O or runner construction.
//   - Metrics initialization happens after config parse but before running the pipeline.
//
// Output contract:
//   - Errors are written to stderr.
//   - On success, writes "ok\n" to stdout.
func runMain(ctx context.Context, args []string, stdout, stderr io.Writer, deps appDeps) int {
	var (
		cfgPath        string
		metricsBackend string
	)

	fs := flag.NewFlagSet("etl", flag.ContinueOnError)
	fs.SetOutput(stderr)

	fs.StringVar(&cfgPath, "config", "", "path to multi-table pipeline config JSON")
	fs.StringVar(&metricsBackend, "metrics-backend", "datadog", "metrics backend: none|datadog (or set METRICS_BACKEND)")
	if err := fs.Parse(args); err != nil {
		// flag package prints a message to stderr for us; treat it as a usage error.
		return 2
	}

	if strings.TrimSpace(cfgPath) == "" {
		fmt.Fprintln(stderr, "usage: etl -config path/to/pipeline.json")
		return 2
	}

	raw, err := deps.readFile(cfgPath)
	if err != nil {
		fmt.Fprintf(stderr, "read config: %v\n", err)
		return 1
	}

	var cfg multitable.Pipeline
	if err := deps.unmarshal(raw, &cfg); err != nil {
		fmt.Fprintf(stderr, "parse config: %v\n", err)
		return 1
	}

	closeMetrics, err := deps.initMetrics(ctx, cfg.Job, metricsBackend)
	if err != nil {
		fmt.Fprintf(stderr, "init metrics: %v\n", err)
		return 1
	}
	defer closeMetrics()

	r := deps.newRunner()
	if err := r.Run(ctx, cfg); err != nil {
		fmt.Fprintf(stderr, "run: %v\n", err)
		return 1
	}

	fmt.Fprintln(stdout, "ok")
	return 0
}

// ---- metrics wiring (package-private seams for unit tests) ----

// metricsBackend is the minimal Close() contract for backends that flush on shutdown.
type metricsBackend interface {
	Close() error
}

var (
	// newDatadogBackend is a seam for unit tests to avoid constructing real backends.
	newDatadogBackend = func(ctx context.Context, opts datadog.Options) (metricsBackend, error) {
		return datadog.NewBackend(ctx, opts)
	}

	// setMetricsBackend is a seam for unit tests to avoid mutating global metrics state.
	//
	// In production, metrics.SetBackend expects a value that implements metrics.Backend.
	// The Datadog backend does so; tests can override this function to avoid type coupling.
	setMetricsBackend = func(b any) {
		metrics.SetBackend(b.(metrics.Backend))
	}

	// logPrintf is a seam for unit tests to capture log output without racing on global logger.
	logPrintf = log.Printf
)

// initMetrics configures the global metrics backend used by the ETL.
//
// This function is intentionally implemented in the CLI package because backend selection
// is an environment-specific concern. Core pipeline packages should only record metrics,
// not choose where they go.
//
// Supported backends:
//   - none/noop: metrics calls become no-ops.
//   - datadog/dd: metrics are buffered and periodically flushed while the job runs.
//
// When to use:
//   - Call once at process startup, before running a pipeline.
//
// Edge cases:
//   - Empty backendName defaults to "none".
//   - Unknown backend names return an error.
//
// Errors:
//   - Returns an error if the requested backend cannot be constructed.
func initMetrics(ctx context.Context, jobName, backendName string) (func(), error) {
	name := strings.ToLower(strings.TrimSpace(backendName))
	if name == "" {
		name = "none"
	}

	switch name {
	case "none", "noop":
		// No-op backend is the default in internal/metrics.
		return func() {}, nil

	case "datadog", "dd":
		b, err := newDatadogBackend(ctx, datadog.Options{JobName: jobName})
		if err != nil {
			return func() {}, err
		}

		// Wire the backend into the global metrics package. This is intentionally
		// centralized here so core pipeline logic stays backend-agnostic.
		setMetricsBackend(b)

		return func() {
			// Close should flush; close errors are not fatal for the job result.
			if err := b.Close(); err != nil {
				logPrintf("metrics: datadog close error: %v", err)
			}
		}, nil

	default:
		return func() {}, fmt.Errorf("unknown metrics backend %q (want none|datadog)", backendName)
	}
}

/*
Behavior changes

- Removed Pushgateway plumbing from the CLI (flag, parameters, and error-message support list),
  because Pushgateway no longer exists in the repository. This eliminates a misleading, dead path.
- The usage line now matches the binary name in this package ("etl") consistently.
*/
