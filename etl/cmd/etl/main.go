package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"etl/internal/config"
	"etl/internal/metrics"
	"etl/internal/metrics/datadog"
	"etl/internal/metrics/prompush"

	// register all backends with the storage factory.
	// config specifies which to use but we need to build in support for all of them.
	_ "etl/internal/storage/all"
	//_ "etl/internal/storage/postgres"
)

// main is the entry point for the ETL binary. It loads the pipeline config,
// optionally initializes a metrics backend, and executes the streaming run.
func main() {
	var (
		cfgPath           string
		metricsBackendFlg string
		pushGatewayURLFlg string
		validate          bool
	)

	flag.StringVar(&cfgPath, "config", "configs/pipelines/sample.json", "pipeline config JSON path")
	flag.StringVar(&metricsBackendFlg, "metrics-backend", "pushgateway", "metrics backend to use (e.g. pushgateway, none)")
	flag.StringVar(&pushGatewayURLFlg, "pushgateway-url", "http://localhost:9091", "Pushgateway base URL (overrides env PUSHGATEWAY_URL)")
	flag.BoolVar(&validate, "validate", false, "validate the configuration and exit")
	verbose := flag.Bool("v", false, "enable verbose logs")

	flag.Parse()

	if !*verbose {
		// You could also adjust log flags here if you want quieter output.
		log.SetOutput(os.Stderr)
	}

	f, err := os.Open(cfgPath)
	if err != nil {
		fatalf("open config: %v", err)
	}
	defer f.Close()

	var p config.Pipeline
	if err := json.NewDecoder(f).Decode(&p); err != nil {
		fatalf("decode config: %v", err)
	}

	// Validate pipeline config.
	issues := config.ValidatePipeline(p)
	hasError := false
	for _, iss := range issues {
		fmt.Fprintf(os.Stderr, "%s: %s: %s\n", iss.Severity, iss.Path, iss.Message)
		if iss.Severity == config.SeverityError {
			hasError = true
		}
	}
	if hasError {
		log.Printf("Configuration is invalid: %v", cfgPath)
		os.Exit(1)
	}

	// If validate flag is set, only validate the configuration and exit
	if validate {
		// If validation succeeds, exit with success
		log.Printf("Configuration is valid: %v", cfgPath)
		os.Exit(0)
	}

	// Decide metrics backend: flag → env → default.
	backendName := metricsBackendFlg
	if backendName == "" {
		backendName = os.Getenv("METRICS_BACKEND")
	}
	switch backendName {
	case "pushgateway":
		// Decide Pushgateway URL: flag → env → default.
		gwURL := pushGatewayURLFlg
		if gwURL == "" {
			gwURL = os.Getenv("PUSHGATEWAY_URL")
		}
		if gwURL == "" {
			gwURL = "http://localhost:9091"
		}

		jobName := p.Job
		if jobName == "" {
			jobName = "etl_job"
		}

		b, err := prompush.NewBackend(jobName, gwURL)
		if err != nil {
			log.Printf("metrics: failed to init prom push backend: %v; using nop", err)
		} else {
			log.Printf("metrics: url=%v, backend=%v, job_name=%v", gwURL, backendName, jobName)
			metrics.SetBackend(b)
			defer func() {
				if err := metrics.Flush(); err != nil {
					log.Printf("metrics: flush error: %v", err)
				}
			}()
		}

	case "datadog":
		// Datadog backend:
		//   - buffers metrics and submits periodically (default once per minute)
		//   - submits one final time at shutdown (Close())
		//
		// This is generally more useful than “submit once per job” because:
		//   - you get an actual time series during long runs
		//   - monitors can evaluate continuously
		//   - dashboards look normal (not just a single spike at the end)

		ddCtx := context.Background()

		// Prefer the pipeline job name for the "job:<name>" tag.
		// Fall back to a stable default so the tag always exists.
		jobName := p.Job
		if jobName == "" {
			jobName = "etl_job"
		}

		// Optional extra tags provided via config/environment.
		// This complements the backend-enforced env:<...> tag.
		extraTags := datadog.ParseTagsCSV(os.Getenv("METRICS_TAGS"))

		// Create the backend. It starts its own periodic flush goroutine.
		b, err := datadog.NewBackend(ddCtx, datadog.Options{
			JobName:    jobName,
			Tags:       extraTags,
			FlushEvery: 60 * time.Second, // change here if you want a different default
		})
		if err != nil {
			log.Printf("metrics: failed to init datadog backend: %v; using nop", err)
		} else {
			log.Printf("metrics: backend=%v job_name=%v tags=%v", backendName, jobName, extraTags)
			metrics.SetBackend(b)

			// IMPORTANT:
			// Close() stops the periodic flush loop and then performs a final Flush().
			// This is the clean shutdown path for the Datadog backend.
			defer func() {
				if err := b.Close(); err != nil {
					log.Printf("metrics: datadog close/flush error: %v", err)
				}
			}()
		}

	case "", "none":
		// metrics disabled; nop backend remains
		if *verbose {
			log.Printf("metrics: disabled (backend=%q)", backendName)
		}

	default:
		log.Printf("metrics: unknown backend %q; metrics disabled", backendName)
	}

	ctx := context.Background()
	start := time.Now()

	if *verbose {
		log.Printf("pipeline: source=%s parser=%s storage=%s table=%s",
			p.Source.Kind, p.Parser.Kind, p.Storage.Kind, p.Storage.DB.Table)
	}

	if err := runStreamed(ctx, p); err != nil {
		log.Fatalf("%v", err)
	}

	if *verbose {
		log.Printf("completed in %s", time.Since(start).Truncate(time.Millisecond))
	}
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
