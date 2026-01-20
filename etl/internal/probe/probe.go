// Package probe implements dataset sampling, schema inference, and
// ETL pipeline generation.
//
// The probe package is responsible for:
//   - Fetching a bounded sample of input data (file:// or http(s)://)
//   - Detecting file format (CSV, JSON, XML)
//   - Inferring column names, types, and contracts
//   - Generating config.Pipeline objects for cmd/etl
//   - Optionally producing multi-table configs for cmd/etl_multi
//
// Design constraints:
//   - Sampling must be bounded in memory and time.
//   - All inference is best-effort and must never fail the probe run.
//   - Generated pipelines must be safe defaults and easy to refine manually.
//
// This package is intentionally dependency-light and side-effect free,
// except where explicitly documented (e.g., SaveSample).
package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"etl/internal/config"
	"etl/internal/datasource/file"
	"etl/internal/datasource/httpds"
	"etl/internal/inspect"
	xmlparser "etl/internal/parser/xml"
	"etl/internal/schema"
	"etl/pkg/records"
)

// Options control the sampling and output behavior.
//
// NOTE: This struct is used by both the legacy CSV-only Probe and the new
// ProbeURL, so new fields must remain optional for existing callers.
type Options struct {
	// URL to fetch.
	URL string
	// MaxBytes to sample from the start of the file.
	MaxBytes int
	// Delimiter (single rune). If zero, default ',' is used (CSV path only).
	Delimiter rune
	// Name used in config/table/file names (normalized later).
	Name string
	// OutputJSON toggles JSON config output; otherwise CSV summary is returned.
	// Used only by the legacy CSV Probe.
	OutputJSON bool
	// DatePreference influences tie-breakers for ambiguous numeric dates.
	// "auto" (default): use built-in preferences (DMY > ISO > MDY).
	// "eu": bias DMY stronger.
	// "us": bias MDY stronger.
	// Used only by the legacy CSV Probe; ProbeURL currently keeps "auto".
	DatePreference string
	// SaveSample, when true, writes the sampled bytes to a local file
	// named after Name (normalized). Extension is inferred by ProbeURL,
	// ".csv" for CSV and ".xml" for XML; legacy Probe always uses ".csv".
	SaveSample bool
	// Backend selects the storage backend: "postgres", "mssql", or "sqlite".
	// Used only by ProbeURL for ETL config generation. Legacy Probe ignores it.
	Backend string
	// AllowInsecureTLS, when true, skips TLS certificate verification for HTTP
	// downloads (useful for self-signed / internal endpoints).
	AllowInsecureTLS bool

	// Job is the logical job name for metrics/config.
	// Defaults to normalized Name when empty.
	Job string

	// Multitable toggles whether ProbeURLAny emits a multi-table config.
	Multitable bool

	// BreakoutFields is an optional list of column names to break out into dimension
	// tables when Multitable is true.
	//
	// If empty, ProbeURLAny will auto-select fields based on sample uniqueness.
	BreakoutFields []string

	// Report, when true, causes ProbeURLAny to return a text report summarizing
	// uniqueness per column (printed by the CLI to stderr).
	Report bool
}

// Result returns the rendered output (either CSV text or JSON text) and
// also exposes the headers for optional callers. This is used by the
// legacy CSV-only Probe.
type Result struct {
	// Rendered textual output for display/return (CSV lines or JSON with newline).
	Body []byte
	// Original header row (not normalized).
	Headers []string
	// Normalized header names (aligned with Headers).
	Normalized []string
}

// HTTPPeekFn is the overridable seam used to fetch the first N bytes.
type HTTPPeekFn func(ctx context.Context, url string, n int, insecure bool) ([]byte, error)

// httpPeekFn is a small overridable seam that the probe package uses to
// fetch the first N bytes from a URL. In production it is backed by the
// httpds.Client for HTTP/HTTPS URLs, and file.NewLocal for file:// URLs.
// Tests can replace it to avoid real I/O.
var httpPeekFn = func(ctx context.Context, url string, n int, insecure bool) ([]byte, error) {
	if n <= 0 {
		return nil, fmt.Errorf("peek: n must be > 0")
	}

	// Local filesystem: file://path/to/file
	if strings.HasPrefix(url, "file://") {
		path := strings.TrimPrefix(url, "file://")

		src := file.NewLocal(path)
		rc, err := src.Open(ctx)
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		lr := &io.LimitedReader{R: rc, N: int64(n)}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, lr); err != nil && err != io.EOF {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	// HTTP(S) path: use httpds client.
	client := httpds.NewClient(httpds.Config{
		InsecureSkipVerify: insecure,
	})
	return client.FetchFirstBytes(ctx, url, n)
}

// -----------------------------------------------------------------------------
// Legacy CSV-only probe (kept for cmd/csvparser and tests)
// -----------------------------------------------------------------------------

// Probe runs the sample+infer pipeline and produces the chosen output.
//
// It mirrors the original CLI pipeline:
//   - HTTP FetchFirstBytes (via httpds.Client) → cut to last newline
//   - readCSVSample   → headers+records (best-effort, skip bad/misaligned rows)
//   - inferTypes      → per-column types
//   - (optionally) build + pretty-print JSON config (internal jsonConfig shape)
//
// Probe runs the legacy CSV-only sampling and inference pipeline.
//
// It fetches a bounded byte sample from the configured URL, infers headers
// and column types, and returns either:
//   - A human-readable CSV summary, or
//   - A JSON configuration object (legacy format)
//
// This function exists for backward compatibility and SHOULD NOT be used
// for new ETL pipelines. New callers should prefer ProbeURLAny.
//
// Errors:
//   - Returns an error if the sample cannot be fetched or parsed.
//   - Inference errors are handled leniently and do not fail the probe.
func Probe(opt Options) (Result, error) {
	res := Result{}

	// Default delimiter if missing.
	delim := opt.Delimiter
	if delim == 0 {
		delim = ','
	}

	// Fetch sample bytes.
	n := opt.MaxBytes
	if n <= 0 {
		n = 20000
	}
	b, err := httpPeekFn(context.Background(), opt.URL, n, opt.AllowInsecureTLS)
	if err != nil {
		return res, fmt.Errorf("peek: %w", err)
	}

	// Cut sample at last newline to avoid a half-line record at the end.
	if i := bytes.LastIndexByte(b, '\n'); i > 0 {
		b = b[:i+1]
	}

	// Read CSV sample.
	headers, rows, err := readCSVSample(b, delim)
	if err != nil {
		return res, fmt.Errorf("read csv: %w", err)
	}
	res.Headers = headers

	// Normalize header names.
	normalized := make([]string, 0, len(headers))
	for _, h := range headers {
		n := truncateFieldName(normalizeFieldName(h))
		normalized = append(normalized, n)
	}
	res.Normalized = normalized

	// Type inference.
	types := inferTypes(headers, rows)
	colLayouts := detectColumnLayouts(rows, types)
	coerceLayout := chooseMajorityLayout(colLayouts, types)

	// Optional save sample.
	if opt.SaveSample {
		if err := writeSampleCSV(normalizeFieldName(opt.Name)+".csv", b); err != nil {
			return res, err
		}
	}

	// If OutputJSON is false: return a human-readable summary.
	if !opt.OutputJSON {
		res.Body = renderCSVSummary(headers, normalized, types, colLayouts, coerceLayout, rows)
		return res, nil
	}

	// JSON config output path (legacy jsonConfig shape).
	cfg := newJSONConfig(normalizeFieldName(opt.Name))
	cfg.Table = normalizeFieldName(opt.Name)
	cfg.Columns = normalized

	// Compute keyColumns heuristic.
	cfg.KeyColumns = inferKeyColumns(headers, normalized, types, rows)

	cfg.Coerce.Layout = coerceLayout
	cfg.Coerce.Types = make(map[string]string)
	for i := range headers {
		t := contractTypeFromInference(types[i])
		if t != "text" {
			cfg.Coerce.Types[normalized[i]] = t
		}
	}

	// Build contract.
	cfg.Contract.Name = normalizeFieldName(opt.Name)
	cfg.Contract.Fields = make([]schema.Field, 0, len(headers))
	for i, h := range headers {
		ct := contractTypeFromInference(types[i])

		f := schema.Field{
			Name: normalized[i],
			Type: ct,
		}
		if types[i] == "date" || types[i] == "timestamp" {
			if lay := colLayouts[i]; lay != "" {
				f.Layout = lay
			}
		}
		if types[i] == "boolean" {
			f.Truthy = []string{"1", "t", "true", "yes", "y"}
			f.Falsy = []string{"0", "f", "false", "no", "n"}
		}
		cfg.Contract.Fields = append(cfg.Contract.Fields, f)

		_ = h // keep for readability in loops
	}

	jb, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return res, err
	}
	res.Body = append(jb, '\n')
	return res, nil
}

// -----------------------------------------------------------------------------
// Unified probe → ETL pipeline config
// -----------------------------------------------------------------------------

type fileFormat int

const (
	formatUnknown fileFormat = iota
	formatCSV
	formatXML
	formatJSON
)

// this may be outdated: ... ProbeURLAny runs the probe pipeline and returns either a config.Pipeline
// (single-table) or a multi-table JSON object (as map[string]any).
//
// It ALSO returns an optional report string (empty unless Options.Report=true).
//
// Design notes:
//   - The generated config always includes a "row_hash" column computed from
//     all normalized fields (hash transform).
//   - In multi-table mode, ProbeURLAny can optionally break selected fields into
//     dimension tables; if fields aren't provided, it uses sample uniqueness.
// end...this may be outdated section

// ProbeURLAny runs the unified probe pipeline and returns either a single-table
// or multi-table ETL configuration.
//
// Behavior:
//   - Automatically detects CSV, JSON, or XML input.
//   - Generates a config.Pipeline with safe defaults.
//   - Always injects a deterministic row_hash column.
//   - Optionally emits a multi-table configuration when Multitable is true.
//
// Return values:
//   - out: either config.Pipeline (single-table) or map[string]any (multi-table)
//   - report: optional human-readable uniqueness report
//   - err: fatal probe errors only (sampling, parsing, config generation)
//
// Invariants:
//   - row_hash is always present and required in single-table output.
//   - The hash transform never includes row_hash in its own input fields.

func ProbeURLAny(ctx context.Context, opt Options) (out any, report string, err error) {
	p, sampleStats, err := probeURLWithStats(ctx, opt)
	if err != nil {
		return nil, "", err
	}

	// Always inject row_hash into single-table pipeline as well.
	injectRowHashIntoSingle(&p)

	if opt.Report {
		report = formatUniquenessReport(sampleStats)
	}

	if !opt.Multitable {
		return p, report, nil
	}

	mt := buildMultiTableFromSingle(p, sampleStats, opt.BreakoutFields)
	return mt, report, nil
}

// ProbeURL runs the sample+infer pipeline for the given URL and returns a
// config.Pipeline suitable for cmd/etl. It auto-detects CSV vs XML vs JSON
// based on the sampled bytes.
func ProbeURL(ctx context.Context, opt Options) (config.Pipeline, error) {
	p, _, err := probeURLWithStats(ctx, opt)
	if err != nil {
		return config.Pipeline{}, err
	}
	// Preserve historical behavior: ProbeURL itself does NOT inject row_hash.
	// New callers should use ProbeURLAny which always injects row_hash.
	return p, nil
}

func probeURLWithStats(ctx context.Context, opt Options) (config.Pipeline, sampleUniqueness, error) {
	if opt.MaxBytes <= 0 {
		opt.MaxBytes = 20000
	}
	if opt.Backend == "" {
		opt.Backend = "postgres"
	}
	if strings.TrimSpace(opt.Name) == "" {
		opt.Name = "dataset_name"
	}
	if strings.TrimSpace(opt.Job) == "" {
		opt.Job = normalizeFieldName(opt.Name)
	}

	// Fetch first bytes via httpPeekFn (supports file:// and http(s)://).
	peekURL := opt.URL
	if peekURL != "" && !strings.HasPrefix(peekURL, "http://") && !strings.HasPrefix(peekURL, "https://") && !strings.HasPrefix(peekURL, "file://") {
		peekURL = "file://" + peekURL
	}

	sample, err := httpPeekFn(ctx, peekURL, opt.MaxBytes, opt.AllowInsecureTLS)
	if err != nil {
		return config.Pipeline{}, sampleUniqueness{}, err
	}

	// Identify file type from sample.
	ff := sniffFormat(sample)

	// Optionally save sample to disk for inspection.
	if opt.SaveSample {
		if err := saveSample(opt.Name, ff, sample); err != nil {
			return config.Pipeline{}, sampleUniqueness{}, err
		}
	}

	// Build pipeline based on format.
	switch ff {
	case formatCSV:
		p, stats, err := probeCSVWithStats(sample, opt)
		if err != nil {
			return config.Pipeline{}, sampleUniqueness{}, err
		}
		p.Job = opt.Job
		return p, stats, nil

	case formatXML:
		p, stats, err := probeXMLWithStats(sample, opt)
		if err != nil {
			return config.Pipeline{}, sampleUniqueness{}, err
		}
		p.Job = opt.Job
		return p, stats, nil

	case formatJSON:
		p, stats, err := probeJSONWithStats(sample, opt)
		if err != nil {
			return config.Pipeline{}, sampleUniqueness{}, err
		}
		p.Job = opt.Job
		return p, stats, nil

	default:
		return config.Pipeline{}, sampleUniqueness{}, fmt.Errorf("unknown file format from sample")
	}
}

// sniffFormat infers the input file format from a byte sample.
// Detection is heuristic and intentionally conservative.
func sniffFormat(sample []byte) fileFormat {
	trim := bytes.TrimSpace(sample)
	if len(trim) == 0 {
		return formatUnknown
	}
	if trim[0] == '<' {
		return formatXML
	}
	if trim[0] == '{' || trim[0] == '[' {
		return formatJSON
	}
	return formatCSV
}

func saveSample(name string, ff fileFormat, sample []byte) error {
	base := normalizeFieldName(name)
	ext := ".txt"
	switch ff {
	case formatCSV:
		ext = ".csv"
	case formatXML:
		ext = ".xml"
	case formatJSON:
		ext = ".json"
	}
	return writeSampleCSV(base+ext, sample)
}

// -------------------- row_hash injection (single-table) --------------------

// injectRowHashIntoSingle mutates a single-table pipeline to ensure row_hash
// is consistently generated, validated, and stored.
//
// Specifically, it:
//   - Ensures row_hash exists in storage columns
//   - Ensures the validate contract requires row_hash
//   - Ensures a hash transform exists and runs before validation
//
// This function is idempotent and safe to call multiple times.
//
// IMPORTANT:
//   - row_hash MUST NOT be included in the hash input fields.
//   - The hash transform is deterministic across runs.
func injectRowHashIntoSingle(p *config.Pipeline) {
	if p == nil {
		return
	}

	// Ensure storage columns include row_hash (for single-table).
	if !containsString(p.Storage.DB.Columns, "row_hash") {
		p.Storage.DB.Columns = append(p.Storage.DB.Columns, "row_hash")
	}

	// Ensure validate contract includes required row_hash.
	contract, idx := findValidateContract(p.Transform)
	if contract != nil {
		if !contractHasField(*contract, "row_hash") {
			contract.Fields = append(contract.Fields, schema.Field{
				Name:     "row_hash",
				Type:     "text",
				Required: true,
			})
		} else {
			// If present but not required, upgrade to required.
			for i := range contract.Fields {
				if contract.Fields[i].Name == "row_hash" {
					contract.Fields[i].Required = true
				}
			}
		}
		// Write back into transform options (it’s stored as any).
		p.Transform[idx].Options["contract"] = *contract
	}

	// Ensure hash transform exists and is before validate.
	ensureHashTransform(p)
}

// ensureHashTransform ensures a hash transform exists and is correctly configured.
//
// The hash transform:
//   - Computes row_hash using all non-row_hash columns
//   - Uses a stable, deterministic configuration (sha256 + hex)
//   - Is inserted before validate when possible
//
// If a hash transform already exists, it is replaced to enforce invariants.
//
// Edge cases:
//   - If no validate transform exists, the hash transform is appended.
//   - If the pipeline is nil, the function is a no-op.
func ensureHashTransform(p *config.Pipeline) {
	if p == nil {
		return
	}

	// NOTE: the hash transform's input fields MUST NOT include the target field
	// itself (row_hash). Including it would make the hash value depend on its
	// previous value and can create unstable results across runs.
	cols := make([]string, 0, len(p.Storage.DB.Columns))
	for _, c := range p.Storage.DB.Columns {
		if c == "row_hash" {
			continue
		}
		cols = append(cols, c)
	}
	sort.Strings(cols)

	hashIdx := -1
	validateIdx := -1
	for i := range p.Transform {
		switch p.Transform[i].Kind {
		case "hash":
			hashIdx = i
		case "validate":
			validateIdx = i
		}
	}

	hashTransform := config.Transform{
		Kind: "hash",
		Options: config.Options{
			"target_field":        "row_hash",
			"fields":              cols,
			"include_field_names": true,
			"trim_space":          true,
			"separator":           "\u001f",
			"algorithm":           "sha256",
			"encoding":            "hex",
			"overwrite":           true,
		},
	}

	if hashIdx >= 0 {
		// Update existing hash transform to match our defaults.
		p.Transform[hashIdx] = hashTransform
	} else {
		// Insert before validate when possible, otherwise append.
		if validateIdx >= 0 {
			out := make([]config.Transform, 0, len(p.Transform)+1)
			out = append(out, p.Transform[:validateIdx]...)
			out = append(out, hashTransform)
			out = append(out, p.Transform[validateIdx:]...)
			p.Transform = out
		} else {
			p.Transform = append(p.Transform, hashTransform)
		}
	}
}

func findValidateContract(ts []config.Transform) (*schema.Contract, int) {
	for i := range ts {
		if ts[i].Kind != "validate" {
			continue
		}
		raw := ts[i].Options.Any("contract")
		if raw == nil {
			return nil, -1
		}
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, -1
		}
		var c schema.Contract
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, -1
		}
		return &c, i
	}
	return nil, -1
}

func contractHasField(c schema.Contract, name string) bool {
	for _, f := range c.Fields {
		if f.Name == name {
			return true
		}
	}
	return false
}

func containsString(ss []string, v string) bool {
	for _, s := range ss {
		if s == v {
			return true
		}
	}
	return false
}

// -------------------- uniqueness report + selection --------------------

const distinctCapPerColumn = 10000

// computeUniquenessFromCSVSample computes bounded per-column uniqueness statistics
// from a sampled CSV dataset.
//
// This helper is used by ProbeURLAny when Options.Report is enabled for CSV inputs.
// It scans the sampled rows and estimates how "unique" each column is, which is
// useful for:
//   - generating a human-readable uniqueness report (formatUniquenessReport), and
//   - auto-selecting breakout/dimension candidates in multi-table configs
//     (autoSelectBreakouts), when the user has not specified BreakoutFields.
//
// # What it measures
//
// For each column, this function computes:
//   - PerColumnTotal[col]:
//     the number of sampled rows where the column has a "meaningful" value.
//     A value is meaningful if strings.TrimSpace(value) != "".
//     This is the correct denominator for uniqueness ratios.
//   - PerColumnDistinct[col]:
//     the number of distinct meaningful values observed for the column,
//     bounded by distinctCapPerColumn.
//   - PerColumnCapped[col]:
//     whether distinct counting was capped for this column.
//
// TotalRows is also tracked as the number of sampled rows processed overall,
// but is informational only. It MUST NOT be used as the denominator for ratios.
// Ratios should always use PerColumnTotal for the given column.
//
// # Memory and runtime safety
//
// Distinct counting is bounded by distinctCapPerColumn per column.
// Once the cap is reached for a column:
//   - stats.PerColumnCapped[col] is set to true, and
//   - the backing distinct set for that column is dropped (set to nil)
//     to release memory and prevent further growth.
//
// This ensures the probe remains safe even when a sampled column has extremely
// high cardinality (e.g., UUIDs, row-level IDs).
//
// Input requirements and edge cases
//
//   - rows is expected to be a rectangular matrix where each record has the
//     same number of fields as normalizedCols.
//   - If a row has a mismatched field count, it is skipped.
//     (Probe sampling is best-effort; bad rows must not fail the run.)
//   - If rows is empty or normalizedCols is empty, an empty stats object is returned.
//   - Empty / whitespace-only values are treated as missing and do not contribute
//     to totals or distinct counts.
//
// # Ordering guarantees
//
// stats.ColumnOrder is a stable copy of normalizedCols. This ordering is used
// by the reporting and breakout-selection logic to ensure deterministic output.
//
// # Errors
//
// This function never returns an error. Any malformed input is handled by
// skipping rows/values as described above.
//
// Note: normalizedCols must match the column order of the sample rows.
// The caller is responsible for ensuring alignment.
func computeUniquenessFromCSVSample(rows [][]string, normalizedCols []string) sampleUniqueness {
	stats := sampleUniqueness{
		TotalRows: 0,

		// PerColumnTotal is required for meaningful uniqueness ratios and reporting.
		// It counts only rows where the column had a meaningful value.
		PerColumnTotal: make(map[string]int, len(normalizedCols)),

		PerColumnDistinct: make(map[string]int, len(normalizedCols)),
		PerColumnCapped:   make(map[string]bool, len(normalizedCols)),
		ColumnOrder:       append([]string(nil), normalizedCols...),
	}

	if len(rows) == 0 || len(normalizedCols) == 0 {
		return stats
	}

	sets := make([]map[string]struct{}, len(normalizedCols))
	for i := range sets {
		sets[i] = make(map[string]struct{})
	}

	for _, r := range rows {
		if len(r) != len(normalizedCols) {
			continue
		}
		stats.TotalRows++

		for i := range normalizedCols {
			col := normalizedCols[i]

			// Only count rows where this column has a meaningful value.
			v := strings.TrimSpace(r[i])
			if v == "" {
				continue
			}

			// Increment the per-column denominator.
			stats.PerColumnTotal[col]++

			// Distinct tracking (bounded).
			if stats.PerColumnCapped[col] {
				continue
			}
			sets[i][v] = struct{}{}
			if len(sets[i]) >= distinctCapPerColumn {
				stats.PerColumnCapped[col] = true
				sets[i] = nil
			}
		}
	}

	for i, col := range normalizedCols {
		if stats.PerColumnCapped[col] {
			stats.PerColumnDistinct[col] = distinctCapPerColumn
			continue
		}
		stats.PerColumnDistinct[col] = len(sets[i])
	}

	return stats
}

func stringifyScalarForUniq(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case []byte:
		return strings.TrimSpace(string(t))
	case float64:
		// encoding/json default number type
		return strings.TrimSpace(fmt.Sprintf("%.0f", t))
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		// Sample-bounded; acceptable fallback.
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

// sampleUniqueness captures bounded distinct-count stats for a sample.
//
// IMPORTANT: "row counts" for uniqueness are *per column*, not global.
// A column should only count a row toward its denominator when the column
// has a meaningful (non-nil / non-empty) value in that row.
type sampleUniqueness struct {
	// TotalRows is the number of sampled records processed overall.
	// This is informational only; it MUST NOT be used as the denominator
	// for per-column uniqueness ratios.
	TotalRows int

	// PerColumnTotal counts, per column, how many sampled rows had a
	// meaningful value for that column (non-nil / non-empty after stringification).
	PerColumnTotal map[string]int

	// PerColumnDistinct holds bounded distinct counts per column, keyed by normalized column name.
	PerColumnDistinct map[string]int

	// PerColumnCapped indicates that distinct counting was capped for a column.
	PerColumnCapped map[string]bool

	// ColumnOrder is stable order of normalized columns.
	ColumnOrder []string
}

func computeUniquenessFromJSONRecords(recs []records.Record, columns []string) sampleUniqueness {
	stats := sampleUniqueness{
		// TotalRows is informational (records seen), not a ratio denominator.
		TotalRows: 0,

		// PerColumnTotal is the correct denominator source for uniqueness ratios.
		PerColumnTotal: make(map[string]int, len(columns)),

		PerColumnDistinct: make(map[string]int, len(columns)),
		PerColumnCapped:   make(map[string]bool, len(columns)),
		ColumnOrder:       append([]string(nil), columns...),
	}
	if len(recs) == 0 || len(columns) == 0 {
		return stats
	}

	// sets holds distinct values per column until the cap is reached.
	// Once capped, we stop tracking the set for that column.
	sets := make(map[string]map[string]struct{}, len(columns))
	for _, c := range columns {
		sets[c] = make(map[string]struct{})
	}

	for _, r := range recs {
		// Track how many records we examined (informational only).
		stats.TotalRows++

		for _, c := range columns {
			// If we've already capped this column, skip any further tracking.
			if stats.PerColumnCapped[c] {
				continue
			}

			// Only count rows where this field is present and meaningful.
			v, ok := r[c]
			if !ok || v == nil {
				continue
			}

			// Convert to a canonical scalar string representation and treat
			// empty results as "missing" for uniqueness purposes.
			s := stringifyScalarForUniq(v)
			if s == "" {
				continue
			}

			// Increment the per-column denominator:
			// this row has a value for column c.
			stats.PerColumnTotal[c]++

			// Add to distinct set (bounded).
			sets[c][s] = struct{}{}
			if len(sets[c]) >= distinctCapPerColumn {
				// Mark capped and drop the set to bound memory.
				stats.PerColumnCapped[c] = true
				delete(sets, c)
			}
		}
	}

	// Finalize distinct counts per column, respecting caps.
	for _, c := range columns {
		if stats.PerColumnCapped[c] {
			stats.PerColumnDistinct[c] = distinctCapPerColumn
			continue
		}
		stats.PerColumnDistinct[c] = len(sets[c])
	}

	return stats
}

// autoSelectBreakouts heuristically selects dimension breakout candidates
// based on column uniqueness ratios.
//
// Selection rules:
//   - Excludes row_hash unconditionally.
//   - Uses per-column denominators (rows where the column had a value).
//   - Prefers columns with low-to-moderate uniqueness.
//   - Caps selection at a small, fixed number for safety.
//
// The returned slice is deterministic and stable across runs.
func autoSelectBreakouts(stats sampleUniqueness) []string {
	// We need per-column denominators to compute meaningful ratios.
	if len(stats.PerColumnTotal) == 0 {
		return nil
	}

	type cand struct {
		col   string
		ratio float64
	}

	cands := make([]cand, 0, len(stats.ColumnOrder))
	for _, col := range stats.ColumnOrder {
		if col == "row_hash" {
			continue
		}

		dist := stats.PerColumnDistinct[col]
		if dist <= 0 {
			continue
		}

		// Use the per-column denominator: only rows where col had a value.
		den := stats.PerColumnTotal[col]
		if den <= 0 {
			continue
		}

		r := float64(dist) / float64(den)
		if r > 0.90 {
			continue
		}
		cands = append(cands, cand{col: col, ratio: r})
	}

	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].ratio == cands[j].ratio {
			return cands[i].col < cands[j].col
		}
		return cands[i].ratio < cands[j].ratio
	})

	const maxBreakouts = 5
	out := make([]string, 0, maxBreakouts)
	for _, c := range cands {
		out = append(out, c.col)
		if len(out) >= maxBreakouts {
			break
		}
	}
	return out
}

func formatUniquenessReport(stats sampleUniqueness) string {
	// If we didn't sample anything meaningful, report that explicitly.
	if stats.TotalRows <= 0 {
		return "uniqueness: no rows sampled"
	}

	type row struct {
		Col    string
		Dist   int
		Ratio  float64
		Capped bool
		Den    int
	}

	rows := make([]row, 0, len(stats.ColumnOrder))
	for _, col := range stats.ColumnOrder {
		d := stats.PerColumnDistinct[col]

		// Use per-column denominator (rows where col has a value).
		den := stats.PerColumnTotal[col]
		if den <= 0 {
			// No values observed for this column; omit from report.
			continue
		}

		r := float64(d) / float64(den)
		rows = append(rows, row{
			Col:    col,
			Dist:   d,
			Ratio:  r,
			Capped: stats.PerColumnCapped[col],
			Den:    den,
		})
	}

	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Ratio == rows[j].Ratio {
			return rows[i].Col < rows[j].Col
		}
		return rows[i].Ratio < rows[j].Ratio
	})

	var b strings.Builder

	// TotalRows is informational; ratio denominators are per-column totals.
	fmt.Fprintf(&b, "uniqueness report:\tsampled_rows=%d\n", stats.TotalRows)
	fmt.Fprintf(&b, "%-15s\t%-7s\t%-7s\tratio\tcapped\n", "col", "unique", "rows")

	//	fmt.Fprintf(&b, "%-15s\t%-7s\tratio\tcapped\n", "col", "unique")
	for _, r := range rows {
		fmt.Fprintf(
			&b,
			"%-15s\t%-7d\t%d\t%.1f%%\t%t\n",
			r.Col,
			r.Dist,
			r.Den,
			r.Ratio*100,
			r.Capped,
		)
	}

	return strings.TrimRight(b.String(), "\n")
}

// Multi-table configs need backend-specific table qualification.
// - Postgres: schema-qualified under public.
// - MSSQL: schema-qualified under dbo.
// - SQLite: no schema prefix.
func qualifyTable(backend, base string) string {

	base = strings.TrimSpace(base)
	if base == "" {
		return base
	}
	switch backend {
	case "postgres":
		return "public." + base
	case "mssql":
		return "dbo." + base
	default: // sqlite and others
		return base
	}
}

// -------------------- multi-table conversion --------------------

// buildMultiTableFromSingle converts a single-table pipeline into the multi-table
// config JSON structure used by cmd/etl_multi.
//
// We return map[string]any to avoid depending on internal/storage TableSpec types
// in this probe utility.
func buildMultiTableFromSingle(p config.Pipeline, stats sampleUniqueness, breakout []string) map[string]any {
	backend := normalizeBackendKind(p.Storage.Kind)
	dataset := normalizeFieldName(p.Job)
	if dataset == "" {
		dataset = normalizeFieldName(p.Storage.DB.Table)
	}
	if dataset == "" {
		dataset = "dataset"
	}

	// Determine breakouts: user-specified > auto.
	breakoutSet := make(map[string]struct{})
	for _, b := range breakout {
		b = strings.TrimSpace(b)
		if b == "" {
			continue
		}
		breakoutSet[b] = struct{}{}
	}
	if len(breakoutSet) == 0 {
		for _, col := range autoSelectBreakouts(stats) {
			breakoutSet[col] = struct{}{}
		}
	}

	// Ensure we never breakout row_hash itself.
	delete(breakoutSet, "row_hash")

	// Build dimension tables for breakouts.
	dimTables := make([]map[string]any, 0, len(breakoutSet))
	factColumns := make([]map[string]any, 0, len(p.Storage.DB.Columns)+len(breakoutSet))
	factFromRows := make([]map[string]any, 0, len(p.Storage.DB.Columns)+len(breakoutSet))

	// Sort breakouts for deterministic output.
	breakouts := make([]string, 0, len(breakoutSet))
	for b := range breakoutSet {
		breakouts = append(breakouts, b)
	}
	sort.Strings(breakouts)

	for _, b := range breakouts {
		dimTableName := qualifyTable(p.Storage.Kind, fmt.Sprintf("%s_%s", dataset, b))
		pkName := fmt.Sprintf("%s_id", b)

		// Use varchar(512) as requested for non-JSON-capable backends.
		valType := "varchar(512)"

		dim := map[string]any{
			"name":              dimTableName,
			"auto_create_table": true,
			"primary_key": map[string]any{
				"name": pkName,
				"type": "serial",
			},
			"columns": []any{
				map[string]any{"name": b, "type": valType, "nullable": false},
			},
			"constraints": []any{
				map[string]any{"kind": "unique", "columns": []any{b}},
			},
			"load": map[string]any{
				"kind": "dimension",
				"from_rows": []any{
					map[string]any{"target_column": b, "source_field": b},
				},
				"conflict":  map[string]any{"target_columns": []any{b}, "action": "do_nothing"},
				"returning": []any{pkName, b},
				"cache": map[string]any{
					"key_column":   b,
					"value_column": pkName,
					"prewarm":      false,
				},
			},
		}
		dimTables = append(dimTables, dim)

		// Add FK column to fact table.
		factColumns = append(factColumns, map[string]any{
			"name":       pkName,
			"type":       "int",
			"nullable":   true,
			"references": fmt.Sprintf("%s(%s)", dimTableName, pkName),
		})
		factFromRows = append(factFromRows, map[string]any{
			"target_column": pkName,
			"lookup": map[string]any{
				"table":      dimTableName,
				"match":      map[string]any{b: b},
				"return":     pkName,
				"on_missing": "insert",
			},
		})
	}

	// Add row_hash early.
	factColumns = append(factColumns, map[string]any{"name": "row_hash", "type": "text", "nullable": false})
	factFromRows = append(factFromRows, map[string]any{"target_column": "row_hash", "source_field": "row_hash"})

	// Add remaining scalar columns (skip breakout fields; they’re represented by FK ids now).
	for _, c := range p.Storage.DB.Columns {
		if c == "row_hash" {
			continue
		}
		if _, isBreakout := breakoutSet[c]; isBreakout {
			continue
		}
		factColumns = append(factColumns, map[string]any{"name": c, "type": "text", "nullable": true})
		factFromRows = append(factFromRows, map[string]any{"target_column": c, "source_field": c})
	}

	factTableName := qualifyTable(p.Storage.Kind, dataset)
	fact := map[string]any{
		"name":              factTableName,
		"auto_create_table": true,
		"primary_key": map[string]any{
			"name": dataset + "_id",
			"type": "serial",
		},
		"columns": factColumns,
		"constraints": []any{
			map[string]any{"kind": "unique", "columns": []any{"row_hash"}},
		},
		"load": map[string]any{
			"kind": "fact",
			"dedupe": map[string]any{
				"conflict_columns": []any{"row_hash"},
				"action":           "do_nothing",
			},
			"from_rows": factFromRows,
		},
	}

	tables := make([]any, 0, len(dimTables)+1)
	for _, d := range dimTables {
		tables = append(tables, d)
	}
	tables = append(tables, fact)

	// Rebuild storage section for multi_table.
	storage := map[string]any{
		"kind": backend,
		"db": map[string]any{
			"dsn":    p.Storage.DB.DSN,
			"mode":   "multi_table",
			"tables": tables,
		},
	}

	out := map[string]any{
		"job":       p.Job,
		"source":    p.Source,
		"parser":    p.Parser,
		"transform": p.Transform,
		"storage":   storage,
		"runtime": map[string]any{
			"reader_workers":                     p.Runtime.ReaderWorkers,
			"transform_workers":                  p.Runtime.TransformWorkers,
			"loader_workers":                     p.Runtime.LoaderWorkers,
			"batch_size":                         p.Runtime.BatchSize,
			"channel_buffer":                     p.Runtime.ChannelBuffer,
			"dedupe_dimension_keys":              false,
			"dedupe_dimension_keys_within_batch": false,
		},
	}
	return out
}

// -------------------- probeCSV/probeJSON/probeXML WithStats --------------------

// probeCSVWithStats builds a single-table ETL pipeline config from a sampled CSV
// input and also computes bounded uniqueness statistics for reporting and
// multi-table breakout selection.
//
// This function exists as the CSV-specific entry in the unified probe pipeline.
// It is used by probeURLWithStats after the file format has been detected as CSV.
//
// What it does
//
//  1. Sample normalization (safety):
//     The input sample is truncated to the last newline ('\n') to avoid
//     processing a partial trailing record. This prevents producing a
//     misaligned row slice from an incomplete line.
//
//  2. CSV sample parsing:
//     It parses the sample into:
//     - headers: the raw CSV header row (un-normalized)
//     - rows:    sampled records, each aligned with the header column order
//
//     Parsing is best-effort: malformed rows may be skipped by readCSVSample,
//     but fatal CSV parsing errors are returned.
//
//  3. Pipeline generation:
//     It delegates to probeCSV to construct a config.Pipeline containing:
//     - file source
//     - csv parser options (header map, expected fields, trim behavior)
//     - coerce + validate transform chain
//     - backend-specific storage defaults
//
//  4. Uniqueness statistics:
//     It computes per-column uniqueness statistics (distinct counts + ratios)
//     from the sampled rows. These stats are later used to:
//     - render a human-readable uniqueness report (formatUniquenessReport), and
//     - auto-select dimension breakouts in multi-table mode (autoSelectBreakouts).
//
// Important invariants
//
//   - Column alignment matters: uniqueness computation assumes that the i-th value
//     in each row corresponds to the i-th entry in the provided normalized column
//     slice. This function therefore derives the normalized column list directly
//     from the sampled header order to ensure index alignment.
//
// Errors
//
//   - Returns an error if CSV parsing fails (readCSVSample).
//   - Returns an error if probeCSV fails to generate a pipeline config.
//   - Uniqueness computation never fails; it is bounded and best-effort.
//
// Performance and memory behavior
//
//   - Uniqueness counting is bounded per column by distinctCapPerColumn.
//   - Parsing and uniqueness are limited to the sampled rows only; this function
//     never reads the full dataset.
func probeCSVWithStats(sample []byte, opt Options) (config.Pipeline, sampleUniqueness, error) {
	// Cut sample at last newline to avoid a half-line record at the end.
	if i := bytes.LastIndexByte(sample, '\n'); i > 0 {
		sample = sample[:i+1]
	}

	headers, rows, err := readCSVSample(sample, ',')
	if err != nil {
		return config.Pipeline{}, sampleUniqueness{}, fmt.Errorf("parse csv sample: %w", err)
	}

	p, err := probeCSV(sample, opt)
	if err != nil {
		return config.Pipeline{}, sampleUniqueness{}, err
	}

	// Compute uniqueness using the normalized header order derived from the
	// sampled rows, not p.Storage.DB.Columns.
	//
	// Why:
	//   - readCSVSample returns row slices ordered exactly as the CSV header.
	//   - probeCSV produces p.Storage.DB.Columns from normalized header names,
	//     which SHOULD match that order, but the storage column list may diverge
	//     in the future (e.g., if a caller injects columns, reorders columns, or
	//     applies mapping logic).
	//   - Uniqueness stats depend on column index alignment between rows[i] and
	//     normalizedCols[i]. Using the sampled header order is the safest source
	//     of truth.
	normalizedCols := make([]string, 0, len(headers))
	for _, h := range headers {
		normalizedCols = append(normalizedCols, truncateFieldName(normalizeFieldName(h)))
	}

	stats := computeUniquenessFromCSVSample(rows, normalizedCols)
	return p, stats, nil
}

func probeJSONWithStats(sample []byte, opt Options) (config.Pipeline, sampleUniqueness, error) {
	// Decode sample JSON records (best-effort, no dependency on internal/parser/json).
	recs, err := sampleJSONRecords(sample, 5000)
	if err != nil {
		return config.Pipeline{}, sampleUniqueness{}, fmt.Errorf("decode json sample: %w", err)
	}
	if len(recs) == 0 {
		// Still return a minimal pipeline; stats empty.
		p, err := probeJSON(sample, opt, nil, nil, nil)
		return p, sampleUniqueness{}, err
	}

	// Flatten records into single-level maps.
	flat := make([]records.Record, 0, len(recs))
	for _, r := range recs {
		m := make(records.Record)
		flattenJSONRecord("", records.Record(r), m)
		flat = append(flat, m)
	}

	// Infer columns from flattened records.
	headers := inferHeadersFromJSON(flat)

	// Normalize column names.
	normalizedCols := make([]string, 0, len(headers))
	normByHeader := make(map[string]string, len(headers))
	for _, h := range headers {
		n := truncateFieldName(normalizeFieldName(h))
		normByHeader[h] = n
		normalizedCols = append(normalizedCols, n)
	}

	// Build pipeline based on inferred columns.
	p, err := probeJSON(sample, opt, headers, normalizedCols, normByHeader)
	if err != nil {
		return config.Pipeline{}, sampleUniqueness{}, err
	}

	// Canonicalize flat records to normalized keys for uniqueness stats.
	canon := make([]records.Record, 0, len(flat))
	for _, r := range flat {
		out := make(records.Record, len(r))
		for k, v := range r {
			if mapped, ok := normByHeader[k]; ok && mapped != "" {
				out[mapped] = v
			} else {
				out[k] = v
			}
		}
		canon = append(canon, out)
	}

	stats := computeUniquenessFromJSONRecords(canon, p.Storage.DB.Columns)
	return p, stats, nil
}

func probeXMLWithStats(sample []byte, opt Options) (config.Pipeline, sampleUniqueness, error) {
	p, err := probeXML(sample, opt)
	if err != nil {
		return config.Pipeline{}, sampleUniqueness{}, err
	}

	// For XML we do not compute uniqueness (structure-only).
	stats := sampleUniqueness{
		TotalRows:         0,
		PerColumnDistinct: make(map[string]int),
		PerColumnCapped:   make(map[string]bool),
		ColumnOrder:       append([]string(nil), p.Storage.DB.Columns...),
	}
	return p, stats, nil
}

// -------------------- JSON helpers (flattening, header inference) --------------------

func inferHeadersFromJSON(recs []records.Record) []string {
	set := make(map[string]struct{})
	for _, r := range recs {
		for k := range r {
			set[k] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// expandJSONRecords heuristically unwraps a "records-like" envelope without
// hard-coding field names. If there is exactly one top-level record and it
// contains one or more fields that are array-of-object, it picks the largest
// such array and treats its elements as the actual records.
//
// If no such array-of-object field exists, it returns the input unchanged.
func expandJSONRecords(in []records.Record) []records.Record {
	if len(in) != 1 {
		return in
	}
	r := in[0]

	type candidate struct {
		recs []records.Record
	}

	var best candidate

	for _, v := range r {
		arr, ok := v.([]any)
		if !ok || len(arr) == 0 {
			continue
		}

		objs := make([]records.Record, 0, len(arr))
		for _, elem := range arr {
			m, ok := elem.(map[string]any)
			if !ok {
				objs = nil
				break
			}
			objs = append(objs, records.Record(m))
		}
		if len(objs) == 0 {
			continue
		}

		if len(objs) > len(best.recs) {
			best.recs = objs
		}
	}

	if len(best.recs) > 0 {
		return best.recs
	}
	return in
}

func flattenJSONRecord(prefix string, in records.Record, out records.Record) {
	for k, v := range in {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		flattenJSONValue(key, v, out)
	}
}

func flattenJSONValue(key string, v any, out records.Record) {
	switch t := v.(type) {
	case map[string]any:
		flattenJSONRecord(key, records.Record(t), out)
	default:
		out[key] = v
	}
}

// -------------------- probeJSON / probeCSV / probeXML --------------------

func probeJSON(sample []byte, opt Options, headers, normalizedCols []string, normByHeader map[string]string) (config.Pipeline, error) {
	// Build contract fields: all text by default (user can refine).
	fields := make([]schema.Field, 0, len(headers))
	for _, h := range headers {
		fields = append(fields, schema.Field{
			Name: normByHeader[h],
			Type: "text",
		})
	}

	// Build pipeline.
	var p config.Pipeline

	// Source: suggest a local file path; user will edit as needed.
	p.Source.Kind = "file"
	p.Source.File.Path = normalizeFieldName(opt.Name) + ".json"

	// Runtime: conservative defaults; user may tune later.
	p.Runtime = config.RuntimeConfig{
		ReaderWorkers:    1,
		TransformWorkers: 1,
		LoaderWorkers:    1,
		BatchSize:        512,
		ChannelBuffer:    256,
	}

	// Parser config: allow arrays and carry a header_map so the streaming
	// JSON parser can map original JSON field names onto normalized columns,
	// just like the CSV path does.
	p.Parser.Kind = "json"
	p.Parser.Options = config.Options{
		"allow_arrays": true,
		"header_map":   buildHeaderMap(headers, normByHeader),
	}

	contract := schema.Contract{
		Name:   normalizeFieldName(opt.Name),
		Fields: fields,
	}
	validateOpt := config.Options{
		"policy":   "lenient",
		"contract": contract,
	}
	validate := config.Transform{
		Kind:    "validate",
		Options: validateOpt,
	}
	p.Transform = []config.Transform{validate}

	// Storage: backend-specific defaults.
	backend := normalizeBackendKind(opt.Backend)
	p.Storage.Kind = backend
	p.Storage.DB = defaultDBConfigForBackend(backend, opt.Name, normalizedCols)

	return p, nil
}

func probeCSV(sample []byte, opt Options) (config.Pipeline, error) {
	// Cut sample at last newline to avoid a half-line record at the end.
	if i := bytes.LastIndexByte(sample, '\n'); i > 0 {
		sample = sample[:i+1]
	}

	headers, rows, err := readCSVSample(sample, ',')
	if err != nil {
		return config.Pipeline{}, fmt.Errorf("parse csv sample: %w", err)
	}

	types := inferTypes(headers, rows)
	colLayouts := detectColumnLayouts(rows, types)
	coerceLayout := chooseMajorityLayout(colLayouts, types)

	// Build contract fields and normalized column names.
	fields := make([]schema.Field, 0, len(headers))
	normalizedCols := make([]string, 0, len(headers))
	normByHeader := make(map[string]string, len(headers))

	for _, h := range headers {
		n := truncateFieldName(normalizeFieldName(h))
		normByHeader[h] = n
		normalizedCols = append(normalizedCols, n)
	}

	requiredCounter := 0
	for i, h := range headers {
		ct := contractTypeFromInference(types[i])
		f := schema.Field{
			Name:     normByHeader[h],
			Type:     ct,
			Required: false,
		}

		// Heuristic: first integer column with no empties is required.
		if types[i] == "integer" && allNonEmptySample(rows, i) && requiredCounter == 0 {
			f.Required = true
			requiredCounter++
		}

		// Layout for date/timestamp columns.
		if types[i] == "date" || types[i] == "timestamp" {
			if lay := colLayouts[i]; lay != "" {
				f.Layout = lay
			}
		}

		// Truthy/falsy for booleans.
		if types[i] == "boolean" {
			f.Truthy = []string{"1", "t", "true", "yes", "y"}
			f.Falsy = []string{"0", "f", "false", "no", "n"}
		}

		fields = append(fields, f)
	}

	// Build coerce types map (skip explicit text).
	coerceTypes := make(map[string]string, len(headers))
	for i, h := range headers {
		t := contractTypeFromInference(types[i])
		if t != "text" {
			coerceTypes[normByHeader[h]] = t
		}
	}

	// Build pipeline.
	var p config.Pipeline

	// Source: suggest a local file path; user will edit as needed.
	p.Source.Kind = "file"
	p.Source.File.Path = normalizeFieldName(opt.Name) + ".csv"

	// Runtime: conservative defaults; user may tune later.
	p.Runtime = config.RuntimeConfig{
		ReaderWorkers:    1,
		TransformWorkers: 2,
		LoaderWorkers:    1,
		BatchSize:        512,
		ChannelBuffer:    256,
	}

	// Parser config.
	p.Parser.Kind = "csv"
	p.Parser.Options = config.Options{
		"has_header":      true,
		"comma":           ",",
		"trim_space":      true,
		"expected_fields": len(headers),
		"header_map":      buildHeaderMap(headers, normByHeader),
	}

	// Transform chain: coerce then validate.
	coerceOpt := config.Options{
		"layout": coerceLayout,
		"types":  coerceTypes,
	}
	coerce := config.Transform{
		Kind:    "coerce",
		Options: coerceOpt,
	}

	contract := schema.Contract{
		Name:   normalizeFieldName(opt.Name),
		Fields: fields,
	}
	validateOpt := config.Options{
		"policy":   "lenient",
		"contract": contract,
	}
	validate := config.Transform{
		Kind:    "validate",
		Options: validateOpt,
	}
	p.Transform = []config.Transform{coerce, validate}

	// Storage: backend-specific defaults.
	backend := normalizeBackendKind(opt.Backend)
	p.Storage.Kind = backend
	p.Storage.DB = defaultDBConfigForBackend(backend, opt.Name, normalizedCols)

	return p, nil
}

func probeXML(sample []byte, opt Options) (config.Pipeline, error) {
	// Guess record tag via discovery on the sample.
	rt, err := inspect.GuessRecordTag(bytes.NewReader(sample))
	if err != nil {
		return config.Pipeline{}, fmt.Errorf("guess record tag: %w", err)
	}
	if rt == "" {
		return config.Pipeline{}, fmt.Errorf("guess record tag: empty result")
	}

	// Discover structure using the same record tag.
	rep, err := inspect.Discover(bytes.NewReader(sample), rt)
	if err != nil {
		return config.Pipeline{}, fmt.Errorf("discover xml structure: %w", err)
	}

	// Starter XML parser config.
	xmlCfg := inspect.StarterConfigFrom(rep)
	xmlCfg.RecordTag = rt

	// Build a deterministic column list from fields + lists.
	cols := inferColumnsFromConfig(xmlCfg)

	// Build contract fields: all text, optional (user can refine).
	fields := make([]schema.Field, 0, len(cols))
	for _, name := range cols {
		fields = append(fields, schema.Field{
			Name: name,
			Type: "text",
		})
	}

	// Build pipeline.
	var p config.Pipeline

	p.Source.Kind = "file"
	p.Source.File.Path = normalizeFieldName(opt.Name) + ".xml"

	p.Runtime = config.RuntimeConfig{
		ReaderWorkers:    1,
		TransformWorkers: 2,
		LoaderWorkers:    1,
		BatchSize:        5000,
		ChannelBuffer:    1000,
	}

	p.Parser.Kind = "xml"

	// Embed the XML parser config as a nested object inside parser.options.
	var xmlOpt map[string]any
	{
		b, err := xmlparser.MarshalConfigJSON(xmlCfg, false)
		if err != nil {
			return config.Pipeline{}, fmt.Errorf("marshal xml config: %w", err)
		}
		if err := json.Unmarshal(b, &xmlOpt); err != nil {
			return config.Pipeline{}, fmt.Errorf("unmarshal xml config: %w", err)
		}
	}
	p.Parser.Options = config.Options{
		"xml_config": xmlOpt,
	}

	contract := schema.Contract{
		Name:   normalizeFieldName(opt.Name),
		Fields: fields,
	}
	validateOpt := config.Options{
		"policy":   "lenient",
		"contract": contract,
	}
	p.Transform = []config.Transform{
		{
			Kind:    "validate",
			Options: validateOpt,
		},
	}

	backend := normalizeBackendKind(opt.Backend)
	p.Storage.Kind = backend
	p.Storage.DB = defaultDBConfigForBackend(backend, opt.Name, cols)

	return p, nil
}

func inferColumnsFromConfig(cfg xmlparser.Config) []string {
	cols := make([]string, 0, len(cfg.Fields)+len(cfg.Lists))
	for name := range cfg.Fields {
		cols = append(cols, name)
	}
	for name := range cfg.Lists {
		cols = append(cols, name)
	}
	sort.Strings(cols)
	return cols
}

func buildHeaderMap(headers []string, normByHeader map[string]string) map[string]string {
	out := make(map[string]string, len(headers))
	for _, h := range headers {
		out[h] = normByHeader[h]
	}
	return out
}

func normalizeBackendKind(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
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

func defaultDBConfigForBackend(backend, name string, columns []string) config.DBConfig {
	tableBase := normalizeFieldName(name)
	switch backend {
	case "mssql":
		return config.DBConfig{
			DSN:             "sqlserver://user:password@0.0.0.0:1433?database=testdb",
			Table:           "dbo." + tableBase,
			Columns:         columns,
			KeyColumns:      []string{},
			DateColumn:      "",
			AutoCreateTable: true,
		}
	case "sqlite":
		return config.DBConfig{
			DSN:             "file:etl.db?cache=shared&_fk=1",
			Table:           tableBase,
			Columns:         columns,
			KeyColumns:      []string{},
			DateColumn:      "",
			AutoCreateTable: true,
		}
	default: // postgres
		return config.DBConfig{
			DSN:             "postgresql://user:password@0.0.0.0:5432/testdb?sslmode=disable",
			Table:           "public." + tableBase,
			Columns:         columns,
			KeyColumns:      []string{},
			DateColumn:      "",
			AutoCreateTable: true,
		}
	}
}

// ----------------------------------------------------------------------------
// Normalization helpers (used by both legacy + unified probes)
// ----------------------------------------------------------------------------

// truncateFieldName enforces backend identifier length limits while
// preserving UTF-8 validity.
func truncateFieldName(s string) string {
	const maxLen = 63
	if len(s) <= maxLen {
		return s
	}
	// Ensure we cut on UTF-8 boundary.
	b := []byte(s)
	if len(b) <= maxLen {
		return s
	}
	cut := maxLen
	for cut > 0 && !utf8.Valid(b[:cut]) {
		cut--
	}
	if cut <= 0 {
		return s[:maxLen]
	}
	return string(b[:cut])
}

// normalizeFieldName converts an arbitrary input string into a safe,
// lowercase identifier suitable for column and table names.
func normalizeFieldName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// Simple ASCII-ish normalization:
	//  - lower
	//  - replace whitespace with underscore
	//  - remove non [a-z0-9_]
	s = strings.ToLower(s)

	var b strings.Builder
	b.Grow(len(s))

	lastUnderscore := false
	for _, r := range s {
		if r == ' ' || r == '-' || r == '.' || r == '/' || r == '\\' || r == ':' || r == ';' {
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
			continue
		}

		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
			lastUnderscore = (r == '_')
			continue
		}

		// Drop everything else.
	}

	out := strings.Trim(b.String(), "_")
	return out
}

func sampleJSONRecords(sample []byte, maxRecords int) ([]map[string]any, error) {
	if maxRecords <= 0 {
		maxRecords = 1000
	}

	dec := json.NewDecoder(bytes.NewReader(sample))

	// Decode first top-level value.
	var root any
	if err := dec.Decode(&root); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	out := make([]map[string]any, 0, minInt(maxRecords, 128))
	emit := func(m map[string]any) {
		if m == nil || len(out) >= maxRecords {
			return
		}
		out = append(out, m)
	}

	switch v := root.(type) {
	case []any:
		for _, it := range v {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			emit(m)
			if len(out) >= maxRecords {
				return out, nil
			}
		}

	case map[string]any:
		// Envelope support: find first array-of-objects field.
		if slice := findObjectSliceJSON(v); slice != nil {
			for _, m := range slice {
				emit(m)
				if len(out) >= maxRecords {
					return out, nil
				}
			}
		} else {
			emit(v)
		}
	default:
		// Unsupported root; return empty.
	}

	// NDJSON / multiple top-level objects.
	for len(out) < maxRecords {
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			if err == io.EOF {
				break
			}
			// Stop sampling on error; return what we have.
			break
		}
		emit(obj)
	}

	return out, nil
}

func findObjectSliceJSON(root map[string]any) []map[string]any {
	for _, v := range root {
		rawSlice, ok := v.([]any)
		if !ok || len(rawSlice) == 0 {
			continue
		}
		objects := make([]map[string]any, 0, len(rawSlice))
		valid := true
		for _, elem := range rawSlice {
			if elem == nil {
				continue
			}
			m, ok := elem.(map[string]any)
			if !ok {
				valid = false
				break
			}
			objects = append(objects, m)
		}
		if valid && len(objects) > 0 {
			return objects
		}
	}
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
