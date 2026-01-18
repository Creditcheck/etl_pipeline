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
		flagURL           = flag.String("url", "", "URL or path of the source file (CSV, XML, or JSON)")
		flagBytes         = flag.Int("bytes", 20000, "Number of bytes to sample from the start of the file")
		flagName          = flag.String("name", "dataset_name", "Dataset/connector name (used in table/job naming)")
		flagJob           = flag.String("job", "", "Job name; defaults to normalized -name when empty")
		flagSave          = flag.Bool("save", false, "Write sampled bytes to [name].{csv,xml,json} next to cwd")
		flagPretty        = flag.Bool("pretty", true, "Pretty-print JSON output")
		flagAllowInsecure = flag.Bool("allow-insecure", true, "Allow insecure TLS")
		flagBackend       = flag.String("backend", "postgres", "Storage backend: postgres|mssql|sqlite")

		flagMultitable = flag.Bool("multitable", false, "Emit a multi-table config (cmd/etl_multi) instead of single-table (cmd/etl)")
		flagBreakout   = flag.String("breakout", "", "Comma-separated list of fields to break out into dimension tables (multi-table only)")
		flagReport     = flag.Bool("report", false, "Print uniqueness report to stderr (stdout remains valid JSON)")
	)
	flag.Parse()

	if strings.TrimSpace(*flagURL) == "" {
		fmt.Fprintln(os.Stderr, "missing -url")
		flag.Usage()
		os.Exit(2)
	}

	var breakoutFields []string
	if s := strings.TrimSpace(*flagBreakout); s != "" {
		for _, p := range strings.Split(s, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				breakoutFields = append(breakoutFields, p)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

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

	if *flagReport && rep != "" {
		fmt.Fprintln(os.Stderr, rep)
	}

	enc := json.NewEncoder(os.Stdout)
	if *flagPretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(out); err != nil {
		log.Fatalf("encode config: %v", err)
	}
}
