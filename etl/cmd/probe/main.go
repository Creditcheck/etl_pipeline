// Command probe generates an ETL configuration by sampling an input dataset.
//
// This command is intended for quickly bootstrapping ETL configs from real input
// data without requiring the full dataset to be loaded. It reads a bounded
// prefix of the input (default 20KB), infers structure, and emits either:
//
//   - A single-table config.Pipeline for cmd/etl, or
//   - A multi-table config object for cmd/etl_multi (when -multitable is set)
//
// Supported input formats are detected heuristically from the sample bytes:
// CSV, JSON, and XML.
//
// Output modes
//
//   - Default mode: prints JSON config to stdout.
//   - Report mode (-report): prints a uniqueness report to stdout and suppresses
//     config output. This makes the command convenient for interactive analysis
//     and scripting (the output is purely text in report mode).
//
// # DSN overrides
//
// The probe library generates a backend-appropriate default DSN string based on
// the selected backend kind ("postgres", "mssql", "sqlite"). This default is
// helpful for bootstrapping configs quickly.
//
// In real environments (Docker Compose, CI, staging), operators often need the
// generated config to point at an actual database without editing JSON by hand.
// This command therefore supports DSN overrides using either:
//
//   - -dsn "<dsn>"                     (highest priority)
//   - DSN="<dsn>"                      (full DSN via env var)
//   - DSN_HOST / DSN_PORT / DSN_USER / DSN_PASSWORD / DSN_DB
//     plus optional backend knobs:
//   - Postgres: DSN_SSLMODE (default: "disable")
//   - MSSQL:    DSN_ENCRYPT (default: "disable")
//     plus optional DSN_PARAMS for extra query parameters.
//
// Precedence rules are strict and deterministic:
//  1. -dsn flag
//  2. DSN env var
//  3. DSN_* component env vars
//
// The override applies to both single-table and multi-table configs.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"etl/internal/config"
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
		// compatible with cmd/etl_multi. When false, it emits a single-table
		// config.Pipeline compatible with cmd/etl.
		flagMultitable = flag.Bool("multitable", true, "Emit a multi-table config (cmd/etl_multi) instead of single-table (cmd/etl)")

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

		// flagDSN overrides the storage DSN in the generated config.
		// This is the highest precedence DSN override mechanism.
		//
		// Example:
		//   -dsn "postgresql://user:password@postgres:5432/testdb?sslmode=disable"
		flagDSN = flag.String("dsn", "", "Override storage DSN (highest priority). Example: postgresql://user:password@postgres:5432/testdb?sslmode=disable")
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

	// Optional DSN override:
	//   -dsn wins, then DSN env var, then DSN_* component env vars.
	//
	// This is a CLI-level feature: the probe library should keep providing safe
	// defaults, but operators should be able to target real databases without
	// modifying output JSON.
	backend := strings.ToLower(strings.TrimSpace(*flagBackend))
	dsnOverride, ok, err := resolveDSNOverride(backend, strings.TrimSpace(*flagDSN))
	if err != nil {
		log.Fatalf("dsn override: %v", err)
	}
	if ok {
		out, err = applyDSNOverride(out, dsnOverride)
		if err != nil {
			log.Fatalf("apply dsn override: %v", err)
		}
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

// resolveDSNOverride determines whether the generated config's storage DSN should
// be overridden, and returns the DSN if so.
//
// Precedence order (highest wins):
//  1. -dsn flag (explicit CLI override)
//  2. DSN environment variable (full DSN string)
//  3. Component env vars DSN_HOST / DSN_PORT / DSN_USER / DSN_PASSWORD / DSN_DB
//     plus backend-specific knobs:
//     - Postgres: DSN_SSLMODE (default "disable")
//     - MSSQL:    DSN_ENCRYPT (default "disable")
//     and optional extra query params DSN_PARAMS (no leading '?').
//
// If no override is configured, ok is false and the returned DSN is empty.
//
// This function intentionally does not read or parse the default DSN produced by
// the probe library. Instead, it constructs an override DSN from explicit inputs
// so behavior is predictable and easy to reason about in CI and containerized
// environments.
func resolveDSNOverride(backend, flagDSN string) (dsn string, ok bool, err error) {
	// 1) Flag override.
	if flagDSN != "" {
		return flagDSN, true, nil
	}

	// 2) Full DSN env override.
	if v := strings.TrimSpace(os.Getenv("DSN")); v != "" {
		return v, true, nil
	}

	// 3) Component env overrides.
	host := strings.TrimSpace(os.Getenv("DSN_HOST"))
	port := strings.TrimSpace(os.Getenv("DSN_PORT"))
	user := strings.TrimSpace(os.Getenv("DSN_USER"))
	pass := os.Getenv("DSN_PASSWORD") // allow spaces
	db := strings.TrimSpace(os.Getenv("DSN_DB"))

	// Optional knobs.
	params := strings.TrimSpace(os.Getenv("DSN_PARAMS"))
	sslmode := strings.TrimSpace(os.Getenv("DSN_SSLMODE"))   // postgres only
	encrypt := strings.TrimSpace(os.Getenv("DSN_ENCRYPT"))   // mssql only
	sqlitePath := strings.TrimSpace(os.Getenv("DSN_SQLITE")) // sqlite only (path or full DSN)

	// If none of the component env vars are present, no override.
	if host == "" && port == "" && user == "" && pass == "" && db == "" && params == "" && sslmode == "" && encrypt == "" && sqlitePath == "" {
		return "", false, nil
	}

	switch normalizeBackend(backend) {
	case "postgres":
		return buildPostgresDSN(host, port, user, pass, db, sslmode, params)
	case "mssql":
		return buildMSSQLDSN(host, port, user, pass, db, encrypt, params)
	case "sqlite":
		// For sqlite, allow DSN_SQLITE to fully override path/DSN. If it is empty,
		// fall back to etl.db in the working directory.
		return buildSQLiteDSN(sqlitePath, params)
	default:
		return "", false, fmt.Errorf("unsupported backend for DSN override: %q", backend)
	}
}

// applyDSNOverride mutates the output config object to set storage.db.dsn.
//
// The probe command can output either:
//   - config.Pipeline (single-table)
//   - map[string]any (multi-table JSON object)
//
// This function supports both forms and returns the updated object.
//
// The function fails if it cannot locate the DSN field or the config has an
// unexpected shape.
func applyDSNOverride(out any, dsn string) (any, error) {
	switch v := out.(type) {
	case config.Pipeline:
		v.Storage.DB.DSN = dsn
		return v, nil
	case *config.Pipeline:
		v.Storage.DB.DSN = dsn
		return v, nil
	case map[string]any:
		return applyDSNOverrideMulti(v, dsn)
	default:
		return out, fmt.Errorf("unsupported output type %T (expected config.Pipeline or map[string]any)", out)
	}
}

// applyDSNOverrideMulti applies a DSN override to the multi-table output shape.
//
// Expected path:
//
//	out["storage"].(map[string]any)["db"].(map[string]any)["dsn"] = dsn
//
// This is intentionally defensive: probe outputs are user-facing artifacts and
// should not panic on minor shape changes.
func applyDSNOverrideMulti(out map[string]any, dsn string) (map[string]any, error) {
	rawStorage, ok := out["storage"]
	if !ok {
		return out, errors.New("multi-table config: missing storage section")
	}
	storage, ok := rawStorage.(map[string]any)
	if !ok {
		return out, fmt.Errorf("multi-table config: storage has type %T, want map[string]any", rawStorage)
	}

	rawDB, ok := storage["db"]
	if !ok {
		return out, errors.New("multi-table config: missing storage.db section")
	}
	db, ok := rawDB.(map[string]any)
	if !ok {
		return out, fmt.Errorf("multi-table config: storage.db has type %T, want map[string]any", rawDB)
	}

	db["dsn"] = dsn
	storage["db"] = db
	out["storage"] = storage
	return out, nil
}

// normalizeBackend converts a user-specified backend string into one of the
// supported canonical values: "postgres", "mssql", "sqlite".
//
// This normalization mirrors behavior in the probe library, but is repeated here
// to keep DSN override logic fully self-contained at the CLI layer.
func normalizeBackend(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "postgres", "postgresql":
		return "postgres"
	case "mssql", "sqlserver":
		return "mssql"
	case "sqlite":
		return "sqlite"
	default:
		return "postgres"
	}
}

// buildPostgresDSN builds a Postgres DSN from component parts.
//
// Defaults:
//   - host: "postgres"
//   - port: "5432"
//   - user: "user"
//   - password: "password"
//   - db: "testdb"
//   - sslmode: "disable"
//
// The returned DSN uses the standard URL form:
//
//	postgresql://user:password@host:port/db?sslmode=disable&<params...>
func buildPostgresDSN(host, port, user, pass, db, sslmode, extraParams string) (string, bool, error) {
	if host == "" {
		host = "postgres"
	}
	if port == "" {
		port = "5432"
	}
	if user == "" {
		user = "user"
	}
	if pass == "" {
		pass = "password"
	}
	if db == "" {
		db = "testdb"
	}
	if sslmode == "" {
		sslmode = "disable"
	}

	u := &url.URL{
		Scheme: "postgresql",
		User:   url.UserPassword(user, pass),
		Host:   host + ":" + port,
		Path:   "/" + db,
	}

	q := u.Query()
	q.Set("sslmode", sslmode)
	appendRawParams(q, extraParams)
	u.RawQuery = q.Encode()

	return u.String(), true, nil
}

// buildMSSQLDSN builds a SQL Server DSN from component parts.
//
// Defaults:
//   - host: "mssql"
//   - port: "1433"
//   - user: "user"
//   - password: "password"
//   - db: "testdb"
//   - encrypt: "disable"
//
// The returned DSN uses the go-mssqldb compatible URL form:
//
//	sqlserver://user:password@host:port?database=testdb&encrypt=disable&<params...>
func buildMSSQLDSN(host, port, user, pass, db, encrypt, extraParams string) (string, bool, error) {
	if host == "" {
		host = "mssql"
	}
	if port == "" {
		port = "1433"
	}
	if user == "" {
		user = "user"
	}
	if pass == "" {
		pass = "password"
	}
	if db == "" {
		db = "testdb"
	}
	if encrypt == "" {
		encrypt = "disable"
	}

	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(user, pass),
		Host:   host + ":" + port,
		Path:   "",
	}

	q := u.Query()
	q.Set("database", db)
	q.Set("encrypt", encrypt)
	appendRawParams(q, extraParams)
	u.RawQuery = q.Encode()

	return u.String(), true, nil
}

// buildSQLiteDSN builds a SQLite DSN.
//
// SQLite DSN usage differs across drivers; in this codebase the probe defaults
// to:
//
//	file:etl.db?cache=shared&_fk=1
//
// To keep this override simple and predictable:
//   - DSN_SQLITE, if set, is treated as either a full DSN or a path.
//     If it contains ':' (e.g., "file:..."), it is treated as a full DSN.
//     Otherwise it is treated as a file path and converted to "file:<path>".
//   - If DSN_SQLITE is empty, we default to "etl.db" in the working directory.
//   - DSN_PARAMS, if present, is appended as query parameters.
func buildSQLiteDSN(sqliteOverride, extraParams string) (string, bool, error) {
	base := strings.TrimSpace(sqliteOverride)
	if base == "" {
		base = "etl.db"
	}

	// If the user supplied a DSN-like string (e.g., "file:etl.db?..."),
	// keep it as-is. Otherwise, treat it as a path.
	if strings.Contains(base, ":") {
		// Full DSN.
		if extraParams == "" {
			return base, true, nil
		}
		sep := "?"
		if strings.Contains(base, "?") {
			sep = "&"
		}
		return base + sep + extraParams, true, nil
	}

	// Path form.
	dsn := "file:" + base
	if extraParams != "" {
		dsn += "?" + extraParams
	}
	return dsn, true, nil
}

// appendRawParams appends raw query parameters provided via DSN_PARAMS.
//
// DSN_PARAMS is expected to be in standard URL query encoding without a leading
// '?' character. Example:
//
//	DSN_PARAMS="application_name=probe&connect_timeout=5"
//
// Invalid fragments are ignored conservatively (we skip empty keys/values).
func appendRawParams(q url.Values, raw string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return
	}

	// ParseQuery handles standard URL-encoded fragments (k=v&k2=v2).
	// It returns an error if the fragment is malformed.
	parsed, err := url.ParseQuery(raw)
	if err != nil {
		// Fail-soft: do not block config generation for optional params.
		return
	}

	for k, vals := range parsed {
		if strings.TrimSpace(k) == "" {
			continue
		}
		for _, v := range vals {
			q.Add(k, v)
		}
	}
}
