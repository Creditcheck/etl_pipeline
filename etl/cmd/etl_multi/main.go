package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"etl/internal/metrics"
	"etl/internal/metrics/datadog"
	"etl/internal/metrics/prompush"
	"etl/internal/multitable"
	_ "etl/internal/storage/all"
)

func main() {
	var cfgPath string
	var metricsBackend string
	var pushgatewayURL string
	flag.StringVar(&cfgPath, "config", "", "path to multi-table pipeline config JSON")
	//flag.StringVar(&metricsBackend, "metrics-backend", os.Getenv("METRICS_BACKEND"), "metrics backend: none|pushgateway|datadog (or set METRICS_BACKEND)")
	flag.StringVar(&metricsBackend, "metrics-backend", "datadog", "metrics backend: none|pushgateway|datadog (or set METRICS_BACKEND)")
	flag.StringVar(&pushgatewayURL, "pushgateway-url", "http://localhost:9091", "Prometheus Pushgateway base URL (or set PUSHGATEWAY_URL)")
	flag.Parse()

	if cfgPath == "" {
		fmt.Fprintln(os.Stderr, "usage: etl_multi -config path/to/pipeline.json")
		os.Exit(2)
	}

	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read config: %v\n", err)
		os.Exit(1)
	}

	var cfg multitable.Pipeline
	if err := json.Unmarshal(raw, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "parse config: %v\n", err)
		os.Exit(1)
	}

	// Metrics setup is intentionally done at the top-level CLI.
	//
	// Rationale:
	//   - The multitable engine should remain storage-agnostic and orchestration-agnostic.
	//   - The CLI is the correct place to choose the metrics backend (Prometheus Pushgateway,
	//     Datadog, or none) because it is environment-specific.
	//   - This mirrors the single-table cmd/etl runner: stage and record metrics are
	//     emitted from within the pipeline, but backend selection happens here.
	closeMetrics, err := initMetrics(context.Background(), cfg.Job, metricsBackend, pushgatewayURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init metrics: %v\n", err)
		os.Exit(1)
	}
	defer closeMetrics()

	r := multitable.NewDefaultRunner()
	if err := r.Run(context.Background(), cfg); err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("ok")
}

// initMetrics configures the global metrics backend used by the ETL.
//
// This function is intentionally verbose and heavily commented because the
// metrics wiring is a critical operational contract.
//
// Backends:
//   - none:       metrics calls become no-ops.
//   - pushgateway: metrics are recorded in-process and pushed once at exit.
//   - datadog:    metrics are buffered and periodically flushed while the job runs.
//
// The returned cleanup function MUST be deferred by the caller.
//
// Notes for maintainers (especially non-Go engineers):
//   - The rest of the codebase calls into internal/metrics via global functions
//     (metrics.RecordStep, metrics.RecordRow, metrics.RecordBatches).
//   - Those functions are safe to call even if no backend is configured.
//   - Selecting a backend here changes where those metrics go, without changing
//     any core pipeline logic.
func initMetrics(ctx context.Context, jobName, backendName, pushgatewayURL string) (func(), error) {
	name := strings.ToLower(strings.TrimSpace(backendName))
	if name == "" {
		// Keep the default behavior explicit. In many environments the ETL is
		// executed locally and metrics are not desired.
		name = "none"
	}

	switch name {
	case "none", "noop":
		// No-op backend is the default in internal/metrics.
		return func() {}, nil

	case "pushgateway", "prom", "prometheus":
		b, err := prompush.NewBackend(jobName, pushgatewayURL)
		if err != nil {
			return func() {}, err
		}
		metrics.SetBackend(b)

		// Pushgateway backends require an explicit flush at process end.
		// We also log flush errors so operators can detect networking/auth issues.
		return func() {
			if err := metrics.Flush(); err != nil {
				log.Printf("metrics: pushgateway flush error: %v", err)
			}
		}, nil

	case "datadog", "dd":
		// Datadog backend flushes periodically while the process runs and then once
		// more on shutdown.
		b, err := datadog.NewBackend(ctx, datadog.Options{JobName: jobName})
		if err != nil {
			return func() {}, err
		}
		metrics.SetBackend(b)
		return func() {
			if err := b.Close(); err != nil {
				log.Printf("metrics: datadog close error: %v", err)
			}
		}, nil

	default:
		return func() {}, fmt.Errorf("unknown metrics backend %q (want none|pushgateway|datadog)", backendName)
	}
}
