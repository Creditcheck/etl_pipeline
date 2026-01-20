// Command probe generates an ETL configuration by sampling an input dataset.
//
// This command is intended for quickly bootstrapping ETL configs from real input
// data without requiring the full dataset to be loaded. It reads a bounded
// prefix of the input (default 20KB), infers structure, and emits:
//
//   - A multi-table config object for cmd/etl
//
// Supported input formats are detected heuristically from the sample bytes:
// CSV, JSON, and XML.
//
// Output modes
//
//   - Default mode: prints JSON config to stdout.
//   - Report mode (-report): prints a uniqueness report to stdout and suppresses
//     config output. This makes the command convenient for interactive analysis
//     and for scripting (the output is purely text in report mode).
//
// Notes for operators
//
//   - The sample is bounded and may not be representative of the full dataset.
//     Treat inferred types and breakouts as starting points and review before
//     production use.
//   - For HTTP(S) sources, -allow-insecure controls TLS verification; disabling
//     verification is useful for internal/self-signed endpoints but should be
//     avoided in production environments.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"etl/internal/probe"
)

func main() {
	var (
		// flagURL is the URL or local filesystem path to the dataset.
		// The probe supports:
		//   - http:// and https:// URLs
		//   - file:// URLs
		//   - bare local paths (treated as file:// internally by the probe)
		flagURL = flag.String("url", "", "URL or path of the source file (CSV, XML, or JSON)")

		// flagBytes controls how many bytes are sampled from the start of the input.
		// Larger values can improve inference quality at the cost of slightly more
		// time and memory during probing.
		flagBytes = flag.Int("bytes", 20000, "Number of bytes to sample from the start of the file")

		// flagName is a human-friendly dataset name used for default table/job naming.
		// The probe normalizes this into a backend-safe identifier.
		flagName = flag.String("name", "dataset_name", "Dataset/connector name (used in table/job naming)")

		// flagJob is the logical job name recorded into the generated config.
		// When empty, it defaults to the normalized -name.
		flagJob = flag.String("job", "", "Job name; defaults to normalized -name when empty")

		// flagSave controls whether the sampled bytes are written next to the
		// current working directory, using [name].{csv,xml,json}.
		flagSave = flag.Bool("save", false, "Write sampled bytes to [name].{csv,xml,json} next to cwd")

		// flagPretty controls JSON indentation for config output.
		// Ignored in report mode (-report), because no JSON is printed.
		flagPretty = flag.Bool("pretty", true, "Pretty-print JSON output")

		// flagAllowInsecure controls TLS certificate verification for HTTP sources.
		// When true, the probe will skip TLS verification (useful for internal
		// endpoints with self-signed certs). Prefer false in production.
		flagAllowInsecure = flag.Bool("allow-insecure", true, "Allow insecure TLS")

		// flagBackend selects the storage backend used for defaults in the emitted
		// config. This affects table qualification and DSN templates.
		flagBackend = flag.String("backend", "postgres", "Storage backend: postgres|mssql|sqlite")

		// flagMultitable controls whether the probe emits a multi-table config
		// compatible with cmd/etl. When false, it emits a single-table
		// config.Pipeline which has been depreciated and is no longer used.
		flagMultitable = flag.Bool("multitable", true, "Emit a multi-table config usable by cmd/etl")

		// flagBreakout is an optional comma-separated list of fields to break out
		// into dimension tables when -multitable is true.
		//
		// If empty, the probe can auto-select breakout candidates using sample
		// uniqueness heuristics.
		flagBreakout = flag.String("breakout", "", "Comma-separated list of fields to break out into dimension tables (multi-table only)")

		// flagReport enables report mode. When true, the command prints a
		// human-readable uniqueness report and suppresses JSON config output.
		//
		// This makes it easier to inspect inferred uniqueness and dimension
		// breakout candidates without having to sift through JSON.
		flagReport = flag.Bool("report", false, "Print uniqueness report (suppresses JSON output)")
	)
	flag.Parse()

	// Validate required inputs early and exit with a usage hint.
	if strings.TrimSpace(*flagURL) == "" {
		fmt.Fprintln(os.Stderr, "missing -url")
		flag.Usage()
		os.Exit(2)
	}

	// Parse the optional breakout list. These are interpreted as column/field
	// names and should match the normalized names produced by the probe.
	var breakoutFields []string
	if s := strings.TrimSpace(*flagBreakout); s != "" {
		for _, p := range strings.Split(s, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				breakoutFields = append(breakoutFields, p)
			}
		}
	}

	// Bound the probe run. Probing should be fast and predictable; if the source
	// is slow/unreachable, we prefer to fail quickly rather than hang indefinitely.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Run the probe pipeline. ProbeURLAny returns:
	//   - out: a config object (single-table config.Pipeline or multi-table JSON)
	//   - rep: an optional uniqueness report (only populated when Report is true)
	out, rep, err := probe.ProbeURLAny(ctx, probe.Options{
		URL:              *flagURL,
		MaxBytes:         *flagBytes,
		Name:             *flagName,
		SaveSample:       *flagSave,
		Backend:          *flagBackend,
		AllowInsecureTLS: *flagAllowInsecure,
		Job:              *flagJob,

		Multitable:     *flagMultitable,
		BreakoutFields: breakoutFields,
		Report:         *flagReport,
	})
	if err != nil {
		log.Fatalf("probe: %v", err)
	}

	// Report mode: print the report and suppress JSON output.
	//
	// Rationale:
	//   - The report is intended for humans and scripts as a standalone artifact.
	//   - Emitting JSON alongside the report makes output noisy and harder to use.
	//   - If callers need both report + config, they can run the command twice or
	//     a future flag can explicitly request both.
	if *flagReport {
		if strings.TrimSpace(rep) == "" {
			// Always print a predictable line for scripts, even if the sample
			// contained no usable rows/columns.
			rep = "uniqueness: no rows sampled"
		}
		fmt.Fprintln(os.Stdout, rep)
		return
	}

	// Default mode: emit the generated config as JSON on stdout.
	enc := json.NewEncoder(os.Stdout)
	if *flagPretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(out); err != nil {
		log.Fatalf("encode config: %v", err)
	}
}
