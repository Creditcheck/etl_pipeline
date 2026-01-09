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
	jsonparser "etl/internal/parser/json"
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
// httpds.Client, but tests can replace it to avoid real HTTP traffic.
//var httpPeekFn HTTPPeekFn = func(ctx context.Context, url string, n int, insecure bool) ([]byte, error) {
//	client := httpds.NewClient(httpds.Config{
//		InsecureSkipVerify: insecure,
//	})
//	return client.FetchFirstBytes(ctx, url, n)
//}

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
func Probe(opt Options) (Result, error) {
	res := Result{}

	// Default delimiter if missing.
	delim := opt.Delimiter
	if delim == 0 {
		delim = ','
	}

	ctx := context.Background()

	// Fetch first N bytes using the shared HTTP datasource client.
	data, err := httpPeekFn(ctx, opt.URL, opt.MaxBytes, opt.AllowInsecureTLS)
	if err != nil {
		return res, err
	}
	// Cut to last newline boundary to avoid partial CSV record at the end.
	if i := bytes.LastIndexByte(data, '\n'); i > 0 {
		data = data[:i+1]
	}

	// Optionally write sampled CSV to disk.
	if opt.SaveSample {
		fname := normalizeFieldName(opt.Name) + ".csv"
		if err := writeSampleCSV(fname, data); err != nil {
			return Result{}, err
		}
	}

	// Parse sample (CSV-only).
	headers, records, err := readCSVSample(data, delim)
	if err != nil {
		return res, err
	}
	res.Headers = headers

	// Infer types.
	types := inferTypes(headers, records)

	// DatePreference is currently a no-op; the scoring already prefers DMY.
	switch opt.DatePreference {
	case "eu":
		// Existing scoring already favors DMY; no additional changes required.
	case "us":
		// MDY bias would require alternative dateLayoutPreference; left as-is.
	default:
		// "auto" → use current default in detectColumnLayouts.
	}

	// Produce output.
	if opt.OutputJSON {
		cfg := buildJSONConfig(opt.Name, headers, records, types, delim)
		body, err := printJSONConfig(cfg)
		if err != nil {
			return res, err
		}
		res.Body = body

		// Collect normalized names in the same order as headers.
		norm := make([]string, len(headers))
		for i, h := range headers {
			norm[i] = normalizeFieldName(h)
		}
		res.Normalized = norm
		return res, nil
	}

	// CSV-like text (header,normalized,type per line).
	var buf bytes.Buffer
	for i, h := range headers {
		fmt.Fprintf(&buf, "%s,%s,%s\n", h, normalizeFieldName(h), types[i])
	}
	res.Body = buf.Bytes()

	// Normalized names in CSV case, too.
	norm := make([]string, len(headers))
	for i, h := range headers {
		norm[i] = normalizeFieldName(h)
	}
	res.Normalized = norm
	return res, nil
}

// DecodeDelimiter converts a user-supplied string into a single rune delimiter.
func DecodeDelimiter(s string) rune {
	if s == "" {
		return ','
	}
	r, _ := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return ','
	}
	return r
}

// OrderedMap preserves insertion order when marshaled as a JSON object.
// This is used by the legacy JSON config generator (csvprobe-style).
type OrderedMap struct {
	Pairs []KV
}

// KV is a single key/value entry.
type KV struct {
	Key   string
	Value string
}

// MarshalJSON emits the pairs as a JSON object in the original order.
// Keys/values are individually json-escaped to stay safe for diacritics, etc.
func (om OrderedMap) MarshalJSON() ([]byte, error) {
	if len(om.Pairs) == 0 {
		return []byte(`{}`), nil
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, p := range om.Pairs {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, _ := json.Marshal(p.Key)
		vb, _ := json.Marshal(p.Value)
		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// jsonConfig models the emitted JSON structure for the legacy csvprobe-style
// config (NOT the ETL config.Pipeline shape).
//
// The type itself lives here; buildJSONConfig/printJSONConfig live in
// internal/probe/main.go and use this type as their return value.
type jsonConfig struct {
	Source struct {
		Kind string `json:"kind"`
		File struct {
			Path string `json:"path"`
		} `json:"file"`
	} `json:"source"`

	Runtime runtimeConfig `json:"runtime"`

	Parser struct {
		Kind    string `json:"kind"`
		Options struct {
			HasHeader      bool       `json:"has_header"`
			Comma          string     `json:"comma"`
			TrimSpace      bool       `json:"trim_space"`
			ExpectedFields int        `json:"expected_fields"`
			HeaderMap      OrderedMap `json:"header_map"`
		} `json:"options"`
	} `json:"parser"`

	Transform []any `json:"transform"`

	Storage struct {
		Kind string `json:"kind"`
		DB   struct {
			DSN             string   `json:"dsn"`
			Table           string   `json:"table"`
			Columns         []string `json:"columns"`
			KeyColumns      []string `json:"key_columns"`
			AutoCreateTable bool     `json:"auto_create_table"`
		} `json:"db"`
	} `json:"storage"`
}

// runtimeConfig controls ingestion parallelism and buffering for the legacy
// CSV config generator. The ETL runtime config is config.RuntimeConfig.
type runtimeConfig struct {
	ReaderWorkers    int `json:"reader_workers"`
	TransformWorkers int `json:"transform_workers"`
	LoaderWorkers    int `json:"loader_workers"`
	BatchSize        int `json:"batch_size"`
	ChannelBuffer    int `json:"channel_buffer"`
}

// -----------------------------------------------------------------------------
// New unified ETL-config probe: ProbeURL (CSV + XML → config.Pipeline)
// -----------------------------------------------------------------------------

// fileFormat represents a coarse file type inferred from a small sample.
type fileFormat int

const (
	formatUnknown fileFormat = iota
	formatCSV
	formatXML
	formatJSON
)

// ProbeURL runs the sample+infer pipeline for the given URL and returns a
// config.Pipeline suitable for cmd/etl. It auto-detects CSV vs XML based on
// the sampled bytes.
//
// The resulting pipeline is intentionally conservative:
//   - JSON: ??
//   - CSV: coerce/validate chain with inferred types and a shared date layout.
//   - XML: validate contract with type="text" by default, plus a starter XML
//     parser config; coerce is omitted for now (strings remain as strings).
func ProbeURL(ctx context.Context, opt Options) (config.Pipeline, error) {
	if opt.MaxBytes <= 0 {
		opt.MaxBytes = 20000
	}
	if opt.Backend == "" {
		opt.Backend = "postgres"
	}

	// set a default job name
	job := opt.Job
	if job == "" {
		if opt.Name != "" {
			job = normalizeFieldName(opt.Name)
		} else {
			job = "etl_job"
		}
	}

	sample, err := httpPeekFn(ctx, opt.URL, opt.MaxBytes, opt.AllowInsecureTLS)
	if err != nil {
		return config.Pipeline{}, fmt.Errorf("fetch sample: %w", err)
	}

	ft := detectFormat(sample)
	if opt.SaveSample {
		if err := writeSampleFile(opt.Name, ft, sample); err != nil {
			return config.Pipeline{}, fmt.Errorf("save sample: %w", err)
		}
	}

	var p config.Pipeline

	switch ft {
	case formatXML:
		p, err = probeXML(sample, opt)
	case formatJSON:
		p, err = probeJSON(sample, opt)
	case formatCSV, formatUnknown:
		// Unknown: default to CSV; worst case, user edits config manually.
		p, err = probeCSV(sample, opt)
	default:
		return config.Pipeline{}, fmt.Errorf("unhandled format %v", ft)
	}
	if err != nil {
		return config.Pipeline{}, err
	}

	// Ensure Job is set on the resulting pipeline.
	if p.Job == "" {
		p.Job = job
	}

	return p, nil
}

// detectFormat applies a very small heuristic on the sampled bytes to decide
// whether the input looks like JSON, XML or CSV. It does not attempt to be perfect;
// the result is only a hint for initial config generation.
func detectFormat(sample []byte) fileFormat {
	s := bytes.TrimSpace(sample)
	if len(s) == 0 {
		return formatUnknown
	}

	// JSON: first non-space char is { or [
	if s[0] == '{' || s[0] == '[' {
		return formatJSON
	}

	// XML detection
	ls := bytes.ToLower(s)
	if bytes.HasPrefix(ls, []byte("<?xml")) || s[0] == '<' {
		return formatXML
	}
	// If there's an obvious XML tag early on, treat as XML.
	if bytes.Contains(s[:min(len(s), 256)], []byte("<")) && bytes.Contains(s[:min(len(s), 256)], []byte(">")) {
		return formatXML
	}
	return formatCSV
}

// writeSampleFile writes the raw sample bytes to a local file with an inferred
// extension based on the detected format.
func writeSampleFile(name string, ft fileFormat, data []byte) error {
	base := normalizeFieldName(name)
	ext := ".bin"
	switch ft {
	case formatCSV:
		ext = ".csv"
	case formatXML:
		ext = ".xml"
	case formatJSON:
		ext = ".json"
	}
	return writeSampleCSV(base+ext, data)
}

// probeJSON builds a config.Pipeline for a JSON source.
//
// It is intentionally structural and does NOT hard-code field names:
//   - It decodes JSON into records using internal/parser/json.
//   - If there is a single top-level record containing one or more
//     array-of-object fields, it picks the largest such array and treats
//     its elements as the actual records (generic envelope handling).
//   - It flattens nested objects into dotted paths (e.g., "user.id"),
//     similar in spirit to how the XML config generator flattens structure.
//   - It then reuses the CSV inference helpers to detect types/layouts and
//     build coerce/validate transforms.
func probeJSON(sample []byte, opt Options) (config.Pipeline, error) {
	// Decode JSON records (object, array-of-objects, or JSONL/NDJSON via your
	// jsonparser implementation).
	recs, err := jsonparser.DecodeAll(bytes.NewReader(sample), jsonparser.Options{
		AllowArrays: true,
	})
	if err != nil {
		return config.Pipeline{}, fmt.Errorf("parse json sample: %w", err)
	}
	if len(recs) == 0 {
		return config.Pipeline{}, fmt.Errorf("parse json sample: no records found")
	}

	// Structural envelope handling:
	// If we have exactly one record with one or more array-of-object fields,
	// pick the largest such array and treat its elements as the records.
	recs = expandJSONRecords(recs)

	// Flatten nested objects so that child fields become columns, analogous to
	// how XML discovery produces flattened field names.
	flatRecs := make([]records.Record, len(recs))
	for i, r := range recs {
		flat := make(records.Record)
		flattenJSONRecord("", r, flat)
		flatRecs[i] = flat
	}

	// Derive headers as the union of flattened keys, with deterministic order.
	headerSet := make(map[string]struct{})
	for _, r := range flatRecs {
		for k := range r {
			headerSet[k] = struct{}{}
		}
	}
	headers := make([]string, 0, len(headerSet))
	for k := range headerSet {
		headers = append(headers, k)
	}
	sort.Strings(headers)

	// Build [][]string rows in header order from flattened records.
	rows := make([][]string, len(flatRecs))
	for i, r := range flatRecs {
		row := make([]string, len(headers))
		for j, h := range headers {
			row[j] = fmt.Sprint(r[h]) // inference works over string patterns
		}
		rows[i] = row
	}

	// Reuse CSV type/layout inference.
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

	// Build pipeline (mirrors probeCSV, but with "json" parser and .json path).
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

// probeCSV builds a config.Pipeline for a CSV source using the existing CSV
// sampling + type/layout inference heuristics.
func probeCSV(sample []byte, opt Options) (config.Pipeline, error) {
	// Cut sample at last newline to avoid a half-line record at the end.
	if i := bytes.LastIndexByte(sample, '\n'); i > 0 {
		sample = sample[:i+1]
	}

	// Use the existing CSV sampler: headers + rows as [][]string.
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
		TransformWorkers: 1,
		LoaderWorkers:    1,
		BatchSize:        1,
		ChannelBuffer:    1000,
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

// probeXML builds a config.Pipeline for an XML source. It uses discovery to
// build an xmlparser.Config and infers the columns from fields+lists. Types
// default to "text" and can be hand-edited later.
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
		TransformWorkers: 1,
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

// inferColumnsFromConfig builds a deterministic column list from the XML
// parser config. It combines field and list names and sorts them.
func inferColumnsFromConfig(cfg xmlparser.Config) []string {
	cols := make([]string, 0, len(cfg.Fields)+len(cfg.Lists))
	for name := range cfg.Fields {
		cols = append(cols, name)
	}
	for name := range cfg.Lists {
		cols = append(cols, name)
	}
	// deterministic order for config diffs
	if len(cols) > 1 {
		for i := 0; i < len(cols)-1; i++ {
			for j := i + 1; j < len(cols); j++ {
				if cols[j] < cols[i] {
					cols[i], cols[j] = cols[j], cols[i]
				}
			}
		}
	}
	return cols
}

// buildHeaderMap constructs the parser.options.header_map object from the
// original CSV headers and their normalized names.
func buildHeaderMap(headers []string, normByHeader map[string]string) map[string]string {
	out := make(map[string]string, len(headers))
	for _, h := range headers {
		out[h] = normByHeader[h]
	}
	return out
}

// normalizeBackendKind normalizes user-provided backend names.
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

// defaultDBConfigForBackend builds a DBConfig with backend-specific defaults.
// DSNs are placeholders that users are expected to edit.
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

// min returns the smaller of a and b.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

// flattenJSONRecord flattens nested JSON structures into dotted-path keys,
// e.g. { "user": { "id": 1, "name": "x" } } becomes:
//
//	"user.id" = 1
//	"user.name" = "x"
//
// Arrays are left as-is (the entire []any is stored), and will be stringified
// later for inference; this keeps the logic simple and matches the ETL’s
// "text first" bias for complex types.
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
		// Nested object → recurse.
		for k2, v2 := range t {
			subKey := key + "." + k2
			flattenJSONValue(subKey, v2, out)
		}
	case records.Record:
		// In case our decoder ever yields records.Record directly.
		flattenJSONRecord(key, t, out)
	default:
		// Arrays and scalars are kept as-is; arrays will later be stringified
		// when building rows for inference.
		out[key] = v
	}
}
