// Package pipeline contains the streaming ETL execution logic.
//
// It wires together CSV/XML reading, transformation, validation, and batched
// loading into the configured storage backend in a fully streaming fashion.

// Package main wires the ETL pipeline end-to-end for both legacy (buffered)
// and streaming (channel-based, batched) execution modes. This file keeps the
// CLI layer thin: it depends only on storage-agnostic interfaces and never
// imports database drivers or backend-specific packages directly.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"etl/internal/config"
	"etl/internal/datasource/file"
	"etl/internal/metrics"
	"etl/internal/parser"

	csvparser "etl/internal/parser/csv"
	jsonparser "etl/internal/parser/json"

	//	xmlparser "etl/internal/parser/xml"

	"etl/internal/schema"
	"etl/internal/storage"

	"etl/internal/transformer"
	"etl/internal/transformer/builtin"
)

// RowErr describes an error associated with a specific row/line in the pipeline.
type RowErr struct {
	Line int
	Err  error
}

const (
	thisMany = 3
)

// counters holds cross-goroutine statistics for the streaming pipeline.
//
// All fields are updated atomically; use the helper methods when possible
// instead of manipulating counters directly.
type counters struct {
	job              string       // metrics label; set once per run
	processed        atomic.Int64 // rows entering the coerce stage
	parseErrors      atomic.Int64 // rows that failed parsing (CSV/XML)
	validateRejects  atomic.Int64 // rows rejected by validation
	transformRejects atomic.Int64 // rows dropped by transform/coerce
	coerced          atomic.Int64 // rows successfully leaving the transform/coerce stage
	inserted         atomic.Int64 // rows successfully inserted via COPY
	batches          atomic.Int64 // COPY batches successfully flushed
}

// runtimeConfig contains the resolved concurrency and buffering configuration
// for a streaming run. Values are derived from the pipeline spec with optional
// environment variable overrides (12-factor style).
type runtimeConfig struct {
	readerWorkers int
	transformers  int
	loaderWorkers int
	batchSize     int
	bufferSize    int
}

type Repository = storage.Repository

//// Repository is the minimal interface of the storage backend used by
//// runStreamed. It is satisfied by storage.Repository.
//type Repository interface {
//	CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error)
//	Exec(ctx context.Context, query string, args ...any) error
//	Close() error
//}

// Function variables used to introduce test seams.
// In production these point to real implementations; tests can override them.
var (
	newRepositoryFn = func(ctx context.Context, cfg storage.Config) (Repository, error) {
		return storage.New(ctx, cfg)
	}

	openSourceFn = openSource

	streamCSVRowsFn = csvparser.StreamCSVRows

	// streamXMLRowsFn provides a test seam for the XML streaming adapter.
	// In production it points to xmlparser.StreamXMLRows.
	//	streamXMLRowsFn = xmlparser.StreamXMLRows

	// streamJSONRowsFn provides a test seam for the JSON streaming adapter.
	// In production it points to jsonparser.StreamJSONRows.
	streamJSONRowsFn = jsonparser.StreamJSONRows
)

// runStreamed executes a full CSV/JSON/XML → coerce → validate → storage pipeline
// in a streaming, batched, and concurrent fashion.
//
// Bad rows are dropped before the database (fail-soft semantics), while
// parse/validation errors are aggregated and summarized at the end. The
// function also emits per-batch progress logs and end-of-run summary stats.
//
// Stats reported:
//
//   - processed:       rows that entered the coerce stage (parsed successfully)
//   - parse_errors:    lines the CSV/XML reader could not parse
//   - validate_dropped: rows dropped by required-field validation
//   - inserted:        rows successfully flushed to the database
//   - batches:         number of COPY batches flushed
//
// Concurrency model:
//
//	Reader (CSV/XML; usually 1)
//	     → tap (counts "processed")
//	     → N Transformers (coerce, in-place on pooled rows)
//	     → Validator (required fields; fail-soft)
//	     → Loader (COPY in batches)
//
// Back-pressure is enforced via bounded channels so that peak memory stays
// around O(batchSize + bufferSize). A fatal loader error cancels the context
// and drains/frees remaining rows to avoid leaks or deadlocks.
func runStreamed(ctx context.Context, spec config.Pipeline) (err error) {
	start := time.Now()
	defer func() {
		metrics.RecordStep(spec.Job, "run_streamed", err, time.Since(start))
	}()

	rt := newRuntimeConfig(spec)

	log.Printf(
		"stream runtime: readers=%d transformers=%d loaders=%d batch=%d buffer=%d",
		rt.readerWorkers, rt.transformers, rt.loaderWorkers, rt.batchSize, rt.bufferSize,
	)
	log.Printf("connecting to DB with DSN: %s", spec.Storage.DB.DSN)

	repo, repoErr := initRepository(ctx, spec)
	if repoErr != nil {
		err = repoErr
		return err
	}
	defer repo.Close()

	if spec.Storage.DB.AutoCreateTable {
		if err := storage.EnsureTableFromPipeline(ctx, spec, repo); err != nil {
			err = fmt.Errorf("apply DDL: %w", err)
			return err
		}
	}

	// Abstract COPY behind repository for testability.
	copyFn := func(ctx context.Context, columns []string, rows [][]any) (int64, error) {
		return repo.CopyFrom(ctx, columns, rows)
	}

	// Cancellation: any fatal loader error cancels upstream work.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Shared stats and error aggregators.
	// var stats counters
	stats := counters{job: spec.Job}

	parseAgg := newErrAgg(thisMany)     // aggregated parse errors (first N messages)
	transformAgg := newErrAgg(thisMany) // aggregated transform/coerce errors (first N messages)
	validateAgg := newErrAgg(thisMany)  // aggregated validation errors (first N messages)

	errCh := make(chan RowErr, 256)

	// Pooled row channels forming the pipeline.
	rawRowCh := make(chan *transformer.Row, rt.bufferSize)     // parser → tap
	tapRawCh := make(chan *transformer.Row, rt.bufferSize)     // tap → transformers
	coercedRowCh := make(chan *transformer.Row, rt.bufferSize) // coerce tap → validate
	validCh := make(chan *transformer.Row, rt.bufferSize)      // validate → loader

	// 1) Error logger goroutine (generic stage errors).
	var wgErr sync.WaitGroup
	wgErr.Add(1)
	go func() {
		defer wgErr.Done()
		log.Printf("loader: started with batchSize=%d columns=%d", rt.batchSize, len(spec.Storage.DB.Columns))
		for e := range errCh {
			if e.Err == nil {
				continue
			}
			if e.Line > 0 {
				log.Printf("row %d: %v", e.Line, e.Err)
				continue
			}
			log.Printf("stage error: %v", e.Err)
		}
	}()

	// 2) Reader(s): CSV/XML → pooled rows.
	var wgReaders sync.WaitGroup
	wgReaders.Add(rt.readerWorkers)

	for i := 0; i < rt.readerWorkers; i++ {
		go func() {
			var stageErr error
			startReader := time.Now()
			defer func() {
				metrics.RecordStep(spec.Job, "reader", stageErr, time.Since(startReader))
				wgReaders.Done()
			}()

			onParseErr := func(_ int, err error) {
				if err == nil {
					return
				}
				parseAgg.add(err.Error())
				stats.IncParseError()

				//				stats.parseErrors.Add(1)
				//				metrics.RecordRow(spec.Job, "parse_errors", 1)

			}
			src, err := openSourceFn(ctx, spec)
			if err != nil {
				stageErr = fmt.Errorf("source open: %w", err)
				errCh <- RowErr{Err: fmt.Errorf("source open: %w", err)}
				return
			}
			defer src.Close()

			switch spec.Parser.Kind {
			case "csv":
				if err := streamCSVRowsFn(
					ctx,
					src,
					spec.Storage.DB.Columns,
					spec.Parser.Options,
					rawRowCh,
					onParseErr,
				); err != nil {
					errCh <- RowErr{Err: err}
				}

				//			case "xml":
				//				// XML streaming path: uses internal/parser/xml.StreamXMLRows, which
				//				// consumes parser.options as an xml.Config JSON and emits *transformer.Row
				//				// records aligned with storage.DB.Columns.
				//
				//				// If the pipeline uses the nested "xml_config" shape (as produced by ProbeURL
				//				// and by your JSON config), unwrap it so StreamXMLRows sees the Config directly.
				//				xmlOpts := spec.Parser.Options
				//				if raw := xmlOpts.Any("xml_config"); raw != nil {
				//					if m, ok := raw.(map[string]any); ok {
				//						xmlOpts = config.Options(m)
				//					}
				//				}
				//
				//				if err := streamXMLRowsFn(
				//					ctx,
				//					src,
				//					spec.Storage.DB.Columns,
				//					xmlOpts,
				//					rawRowCh,
				//					onParseErr,
				//				); err != nil {
				//					errCh <- RowErr{Err: err}
				//				}

			case "json":
				// JSON streaming path: uses internal/parser/json.StreamJSONRows, which
				// consumes parser.options (e.g. allow_arrays, flatten) and emits
				// *transformer.Row records aligned with storage.DB.Columns.
				if err := streamJSONRowsFn(
					ctx,
					src,
					spec.Storage.DB.Columns,
					spec.Parser.Options,
					rawRowCh,
					onParseErr,
				); err != nil {
					errCh <- RowErr{Err: err}
				}

			default:
				errCh <- RowErr{Err: fmt.Errorf("unsupported parser.kind=%s", spec.Parser.Kind)}
			}
		}()
	}

	// 3) Tap: count processed rows and forward to transformers.
	var wgTap sync.WaitGroup
	wgTap.Add(1)
	go func() {
		defer wgTap.Done()
		defer close(tapRawCh)

		for r := range rawRowCh {
			//			stats.processed.Add(1)
			//			metrics.RecordRow(spec.Job, "processed", 1)
			stats.IncProcessed()

			select {
			case tapRawCh <- r:
			case <-ctx.Done():
				// Context cancelled: free the row and exit to avoid leaks.
				r.Free()
				return
			}
		}
	}()

	// 4) Transformers: coerce in place on pooled rows.
	var wgTransformers sync.WaitGroup
	wgTransformers.Add(rt.transformers)

	coerceSpec := transformer.BuildCoerceSpecFromTypes(
		coerceTypesFromSpec(spec),
		coerceLayoutFromSpec(spec),
		nil,
		nil,
	)

	// Attach anchor salvage rule if configured
	if ar := anchorRuleFromSpec(spec); ar != nil {
		log.Printf("DEBUG: YES")
		coerceSpec.Anchor = ar
	}

	if err := transformer.ValidateSpecSanity(spec.Storage.DB.Columns, coerceSpec); err != nil {
		return fmt.Errorf("coerce spec sanity: %w", err)
	}

	for i := 0; i < rt.transformers; i++ {
		go func() {
			var stageErr error
			startTransformer := time.Now()
			defer func() {
				metrics.RecordStep(spec.Job, "transformers", stageErr, time.Since(startTransformer))
			}()

			defer wgTransformers.Done()
			transformer.TransformLoopRows(
				ctx,
				spec.Storage.DB.Columns,
				tapRawCh,
				coercedRowCh,
				coerceSpec,
				func(line int, reason string) {
					// Transform/coerce-level drop; count and aggregate a few examples.
					if line > 0 {
						transformAgg.add(fmt.Sprintf("line=%d: %s", line, reason))
					} else {
						transformAgg.add(fmt.Sprintf("reason: %v, i=%v", reason, i))
					}
					stats.IncTransformRejected()
					// stats.transformRejects.Add(1)
					// metrics.RecordRow(spec.Job, "transform_rejects", 1)
				},
			)
		}()
	}

	// 5) Close channels after all upstream stages complete.
	go func() {
		wgReaders.Wait()
		close(rawRowCh)

		wgTap.Wait()
		wgTransformers.Wait()
		close(coercedRowCh)
	}()

	// 6) Validator: drop rows with missing required fields and type mismatches.
	required := requiredFromContract(spec)
	colKinds := normalizedKindsFromSpec(spec)

	var wgValidator sync.WaitGroup
	wgValidator.Add(1)
	go func() {
		var stageErr error
		startValidator := time.Now()
		defer func() {
			metrics.RecordStep(spec.Job, "validator", stageErr, time.Since(startValidator))
		}()

		defer wgValidator.Done()
		defer close(validCh)

		transformer.ValidateLoopRows(
			ctx,
			spec.Storage.DB.Columns,
			required,
			colKinds,
			coercedRowCh,
			validCh,
			func(line int, reason string) {
				if line > 0 {
					validateAgg.add(fmt.Sprintf("line=%d: %s", line, reason))
				} else {
					validateAgg.add(reason)
				}
				stats.IncValidateDropped()

				//				stats.validateRejects.Add(1)
				//				metrics.RecordRow(spec.Job, "validate_rejects", 1)
			},
		)
	}()

	// 7) Loader(s): batch rows and COPY to storage, always freeing rows.
	var stageErr error
	startLoader := time.Now()
	var wgLoaders sync.WaitGroup
	wgLoaders.Add(rt.loaderWorkers)

	loaderCfg := loaderConfig{
		ctx:        ctx,
		cancel:     cancel,
		validCh:    validCh,
		errCh:      errCh,
		copyFn:     copyFn,
		batchSize:  rt.batchSize,
		columns:    spec.Storage.DB.Columns,
		stats:      &stats,
		clockNowFn: time.Now, // small seam for benchmark tests if needed
	}

	for i := 0; i < rt.loaderWorkers; i++ {
		go runLoader(loaderCfg, &wgLoaders)
	}

	// Wait for validator and loaders; then stop error logger.
	wgValidator.Wait()
	wgLoaders.Wait()
	defer func() {
		metrics.RecordStep(spec.Job, "loader", stageErr, time.Since(startLoader))
	}()

	close(errCh)
	wgErr.Wait()

	logParseAndValidationSummaries(parseAgg, transformAgg, validateAgg)
	logGlobalSummary(&stats)

	return nil
}

// newRuntimeConfig resolves the runtime configuration for a streaming run
// using the pipeline spec and environment-variable fallbacks.
//
// The pickInt / getenvInt helpers are assumed to be already defined in this
// package.
func newRuntimeConfig(spec config.Pipeline) runtimeConfig {
	return runtimeConfig{
		readerWorkers: pickInt(spec.Runtime.ReaderWorkers, getenvInt("ETL_READER_WORKERS", 1)),
		transformers:  pickInt(spec.Runtime.TransformWorkers, getenvInt("ETL_TRANSFORM_WORKERS", 4)),
		loaderWorkers: pickInt(spec.Runtime.LoaderWorkers, getenvInt("ETL_LOADER_WORKERS", 1)), // 1 writer per table is usually best
		batchSize:     pickInt(spec.Runtime.BatchSize, getenvInt("ETL_BATCH_SIZE", 10000)),
		bufferSize:    pickInt(spec.Runtime.ChannelBuffer, getenvInt("ETL_CH_BUFFER", 4096)),
	}
}

// initRepository constructs the storage repository from the pipeline spec and
// returns a backend-agnostic Repository.
//
// It also logs basic connection details for diagnostics.
func initRepository(ctx context.Context, spec config.Pipeline) (Repository, error) {
	repo, err := newRepositoryFn(ctx, storage.Config{
		Kind:       spec.Storage.Kind,
		DSN:        spec.Storage.DB.DSN,
		Table:      spec.Storage.DB.Table,
		Columns:    spec.Storage.DB.Columns,
		KeyColumns: spec.Storage.DB.KeyColumns,
		DateColumn: spec.Storage.DB.DateColumn,
	})
	if err != nil {
		return nil, fmt.Errorf("init repo: %w", err)
	}
	return repo, nil
}

// ensureTableExists optionally creates the storage table if AutoCreateTable
// is enabled on the pipeline spec.
//
// It uses the DDL helper to infer the table definition and generate a CREATE
// TABLE statement.
func ensureTableExists(ctx context.Context, repo Repository, spec config.Pipeline) error {
	log.Printf("auto-create table enabled for %s", spec.Storage.DB.Table)

	if spec.Storage.DB.AutoCreateTable {
		if err := storage.EnsureTableFromPipeline(ctx, spec, repo); err != nil {
			return fmt.Errorf("apply DDL: %w", err)
		}
	}
	log.Printf("table ensured: %s", spec.Storage.DB.Table)
	return nil
}

// loaderConfig contains all dependencies required by a loader worker.
type loaderConfig struct {
	ctx        context.Context
	cancel     context.CancelFunc
	validCh    <-chan *transformer.Row
	errCh      chan<- RowErr
	copyFn     func(context.Context, []string, [][]any) (int64, error)
	batchSize  int
	columns    []string
	stats      *counters
	clockNowFn func() time.Time
}

// runLoader consumes validated rows, batches them, and calls COPY via the
// configured repository. It always frees rows (whether due to success,
// cancellation, or errors) and updates stats and per-batch logs.
//
// Any fatal COPY error is reported on errCh and causes ctx cancellation.
func runLoader(cfg loaderConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	start := cfg.clockNowFn()

	// bVals holds the values for COPY; bRows tracks row objects for Free().
	bVals := make([][]any, 0, cfg.batchSize)
	bRows := make([]*transformer.Row, 0, cfg.batchSize)

	flush := func() error {
		if len(bVals) == 0 {
			return nil
		}

		if _, err := cfg.copyFn(cfg.ctx, cfg.columns, bVals); err != nil {
			// COPY failed for this batch: log a concise summary and a small sample
			// of the offending rows, but do not panic if column/value lengths
			// mismatch (that can happen in tests or misconfigurations).
			if len(bRows) > 0 {
				first := bRows[0]
				last := bRows[len(bRows)-1]

				log.Printf(
					"loader: COPY failed for batch (rows=%d, approx line range=%d-%d): %v",
					len(bRows),
					first.Line,
					last.Line,
					err,
				)

				// If the number of values differs from the number of columns, note it
				// explicitly but do not crash.
				if len(cfg.columns) != len(first.V) {
					log.Printf(
						"loader: column/value length mismatch: columns=%d values=%d",
						len(cfg.columns),
						len(first.V),
					)
				}

				// Log a few sample rows from the failing batch to aid diagnosis.
				maxSamples := 5
				if len(bRows) < maxSamples {
					maxSamples = len(bRows)
				}
				for i := 0; i < maxSamples; i++ {
					r := bRows[i]
					log.Printf(
						"loader: failing batch sample row idx=%d line=%d",
						i,
						r.Line,
					)

					for colIdx, val := range r.V {
						colName := "<unknown>"
						if colIdx < len(cfg.columns) {
							colName = cfg.columns[colIdx]
						}

						log.Printf(
							"loader details:   col_idx=%d col=%s go_type=%T value=%v",
							colIdx,
							colName,
							val,
							val,
						)
					}
				}
			} else {
				log.Printf("loader: COPY failed for empty batch: %v", err)
			}

			// Propagate the error back to the caller; the caller will push it onto
			// errCh, cancel the context, and drain remaining rows.
			return err
		}
		// Success path: free rows and update stats as before.
		for _, r := range bRows {
			r.Free()
		}
		inserted := int64(len(bVals))

		//cfg.stats.inserted.Add(int64(len(bVals)))
		cfg.stats.AddInserted(inserted)

		//		metrics.RecordRow(cfg.jobName, "inserted", inserted)

		batchNum := cfg.stats.batches.Add(1)
		cfg.stats.IncBatches()

		duration := cfg.clockNowFn().Sub(start)
		totalInserted := cfg.stats.inserted.Load()
		rate := int64(float64(totalInserted) / duration.Seconds())

		if batchNum%1 == 0 {
			log.Printf(
				"batch=%d rps=%d inserted=%d total_inserted=%d elapsed=%s",
				batchNum,
				rate,
				len(bVals),
				totalInserted,
				duration.Truncate(time.Millisecond),
			)
		}
		//		metrics.RecordBatches(cfg.jobName, 1)

		bVals = bVals[:0]
		bRows = bRows[:0]

		return nil
	}

	for {
		select {
		case <-cfg.ctx.Done():
			// Context cancelled (e.g. other loader failed): drain and free all remaining rows.
			for r := range cfg.validCh {
				r.Free()
			}
			return

		case r, ok := <-cfg.validCh:
			if !ok {
				// No more rows: flush last batch and exit.
				if err := flush(); err != nil {
					cfg.errCh <- RowErr{Err: err}
					cfg.cancel()
				}
				return
			}

			bVals = append(bVals, r.V)
			bRows = append(bRows, r)

			if len(bVals) >= cfg.batchSize {
				if err := flush(); err != nil {
					// Fatal DB error: report, cancel, then drain & free remaining rows.
					cfg.errCh <- RowErr{Err: err}
					cfg.cancel()
					for r := range cfg.validCh {
						r.Free()
					}
					return
				}
			}
		}
	}
}

// logParseAndValidationSummaries prints aggregated parse, transform, and
// validation errors. Only the first N unique messages (per errAgg) are shown.
func logParseAndValidationSummaries(parseAgg, transformAgg, validateAgg *errAgg) {
	if parseAgg.count > 0 {
		log.Printf("parse errors: %d (showing first %d)", parseAgg.count, len(parseAgg.first))
		for i, s := range parseAgg.first {
			log.Printf("  #%03d: %s", i+1, s)
		}
	}
	if transformAgg.count > 0 {
		log.Printf("transform rejects: %d (showing first %d)", transformAgg.count, len(transformAgg.first))
		for i, s := range transformAgg.first {
			log.Printf("  #%03d: %s", i+1, s)
		}
	}
	if validateAgg.count > 0 {
		log.Printf("validation rejects: %d (showing first %d)", validateAgg.count, len(validateAgg.first))
		for i, s := range validateAgg.first {
			log.Printf("  #%03d: %s", i+1, s)
		}
	}
}

// logGlobalSummary prints final aggregated statistics for the run.
//
// Invariants for data rows (excluding headers) are:
//
//	processed + parse_errors == total_data_rows
//	processed == inserted + validate_dropped + transform_dropped
//
// where transform_dropped is derived as the number of rows that entered the
// transform/validate/loader pipeline but did not reach storage and were not
// explicitly rejected by validation.
//
// transformRejects counts rows that TransformLoopRows dropped explicitly due
// to coercion/transform errors (a subset of transform_dropped).
func logGlobalSummary(c *counters) {
	processed := c.processed.Load()
	parseErrs := c.parseErrors.Load()
	validateDropped := c.validateRejects.Load()
	transformRejected := c.transformRejects.Load()
	inserted := c.inserted.Load()
	batches := c.batches.Load()

	// Derived: rows that entered the pipeline but were neither validated nor
	// inserted. This includes transform rejects plus any rows dropped in the
	// loader under cancellation.
	transformDropped := processed - validateDropped - inserted

	log.Printf(
		"summary: processed=%d parse_errors=%d validate_dropped=%d transform_rejected=%d transform_dropped=%d inserted=%d batches=%d",
		processed,
		parseErrs,
		validateDropped,
		transformRejected,
		transformDropped,
		inserted,
		batches,
	)
	//	if transformDropped >= 0 {
	//		metrics.RecordRow(job, "transform_dropped", transformDropped)
	//	}

	// Optional sanity check during development to ensure conservation.
	totalDataRows := processed + parseErrs
	accounted := parseErrs + validateDropped + transformDropped + inserted
	if accounted != totalDataRows {
		log.Printf(
			"WARNING: row accounting mismatch: total=%d accounted=%d (delta=%d)",
			totalDataRows,
			accounted,
			totalDataRows-accounted,
		)
	}
}

// ////////////////////////////////////////////////////////////////////////////////////////
// refactor end
// ////////////////////////////////////////////////////////////////////////////////////////
// openSource is retained for legacy paths (parser.Parse usage).
func openSource(ctx context.Context, spec config.Pipeline) (io.ReadCloser, error) {
	switch spec.Source.Kind {
	case "file":
		return file.NewLocal(spec.Source.File.Path).Open(ctx)
	default:
		return nil, fmt.Errorf("unsupported source.kind=%s", spec.Source.Kind)
	}
}

// buildParser maps parser configuration into a concrete parser implementation.
func buildParser(p config.Parser) (parser.Parser, error) {
	switch p.Kind {
	case "csv":
		opt := csvparser.Options{
			HasHeader:            p.Options.Bool("has_header", true),
			Comma:                p.Options.Rune("comma", ','),
			TrimSpace:            p.Options.Bool("trim_space", true),
			ExpectedFields:       p.Options.Int("expected_fields", 0),
			HeaderMap:            p.Options.StringMap("header_map"),
			StreamScrubLikvidaci: p.Options.Bool("stream_scrub_likvidaci", true),
		}
		return csvparser.NewParser(opt), nil

		//	case "json":
		//		// Map config.Options onto jsonparser.Options.
		//		jopt := jsonparser.Options{
		//			AllowArrays: p.Options.Bool("allow_arrays", true),
		//			// Add more flags here if your json parser exposes them, e.g.:
		//			// Flatten:     p.Options.Bool("flatten", true),
		//			// RootPath:    p.Options.String("root_path", ""),
		//		}
		//		return jsonparser.NewParser(jopt), nil
	default:
		return nil, fmt.Errorf("unsupported parser.kind=%s", p.Kind)
	}
}

// normalizedKindsFromSpec returns a mapping of column name -> normalized kind
// for the streaming validator.
//
// Source of truth:
//  1. "coerce" transform types (coerceTypesFromSpec)
//  2. inline validate.contract field types (via decodeInlineContract)
//
// When both specify a type for the same field, the contract type wins.
// Types are normalized to a small set ("int","bool","date","string",...)
// for use by the streaming validator.
//
// This function never returns nil; an empty map means "no type constraints".
func normalizedKindsFromSpec(p config.Pipeline) map[string]string {
	out := make(map[string]string)

	// 1) Types declared on the coerce transform, e.g.:
	//    "types": { "rok_vyroby": "bigint" }
	for name, t := range coerceTypesFromSpec(p) {
		out[name] = normalizeKind(t) // local helper (defined below)
	}

	// 2) Types declared in the inline validate.contract. These override
	//    coerce types when both are present.
	for _, t := range p.Transform {
		if t.Kind != "validate" {
			continue
		}
		contract, err := decodeInlineContract(t.Options)
		if err != nil {
			// Invalid contract is a configuration bug; we log and keep running
			// with the coerce types only.
			log.Printf("validate contract decode: %v", err)
			continue
		}
		for _, f := range contract.Fields {
			out[f.Name] = normalizeKind(f.Type)
		}
	}

	return out
}

// normalizeKind maps pipeline/contract type strings onto coarse kinds used by
// the streaming validator.
//
// The mapping is kept deliberately small to make the ValidateLoopRows switch
// simple and predictable. It mirrors builtin.normalizeKind, but is kept local
// to avoid exporting internal symbols across packages.
func normalizeKind(t string) string {
	s := strings.ToLower(t)
	switch s {
	case "bigint", "int8", "integer", "int4", "int2", "int":
		return "int"
	case "boolean", "bool":
		return "bool"
	case "date", "timestamp", "timestamptz":
		return "date"
	case "text", "string":
		return "string"
	default:
		return s
	}
}

// decodeInlineContract retrieves and decodes the inline validation contract
// from a transform's options. It round-trips through JSON to the schema type.
func decodeInlineContract(opt config.Options) (schema.Contract, error) {
	raw := opt.Any("contract")
	if raw == nil {
		return schema.Contract{}, fmt.Errorf("validate.options.contract is required")
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return schema.Contract{}, fmt.Errorf("contract marshal: %w", err)
	}
	var c schema.Contract
	if err := json.Unmarshal(b, &c); err != nil {
		return schema.Contract{}, fmt.Errorf("contract unmarshal: %w", err)
	}
	if c.Name == "" {
		c.Name = "inline"
	}
	return c, nil
}

// buildTransformers constructs the transformer chain from configuration.
func buildTransformers(ts []config.Transform) (transformer.Chain, error) {
	var c transformer.Chain
	for _, t := range ts {
		switch t.Kind {
		case "validate":
			contract, err := decodeInlineContract(t.Options)
			if err != nil {
				return nil, err
			}
			var rejectCount int
			v := builtin.Validate{
				Contract:   contract,
				DateLayout: t.Options.String("date_layout", ""),
				Policy:     t.Options.String("policy", "lenient"),
				Reject: func(r builtin.RejectedRow) {
					rejectCount++
					if rejectCount <= thisMany {
						log.Printf("validate reject: %s | raw=%v", r.Reason, r.Raw)
					}
					if rejectCount == thisMany+1 {
						log.Printf("... additional rejections suppressed ...")
					}
				},
			}
			c = append(c, v)
		case "normalize":
			c = append(c, builtin.Normalize{})
		case "dedupe":
			c = append(c, builtin.DeDup{
				Keys:         t.Options.StringSlice("keys"),
				Policy:       t.Options.String("policy", "keep-last"),
				PreferFields: t.Options.StringSlice("prefer_fields"),
			})
		case "coerce":
			c = append(c, builtin.Coerce{
				Types:  t.Options.StringMap("types"),
				Layout: t.Options.String("layout", "02.01.2006"),
			})
		case "require":
			c = append(c, builtin.Require{
				Fields: t.Options.StringSlice("fields"),
			})
		default:
			return nil, fmt.Errorf("unsupported transformer.kind=%s", t.Kind)
		}
	}
	if c == nil {
		c = transformer.Chain{}
	}
	return c, nil
}

// ----------------------------------------------------------------------------
// Streaming, concurrent path with pooled rows
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// Small helpers
// ----------------------------------------------------------------------------

// getenvInt reads an int from environment, returning def when unset/invalid.
func getenvInt(k string, def int) int {
	if s := os.Getenv(k); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return def
}

// pickInt chooses the first positive value 'a', otherwise returns 'b'.
func pickInt(a, b int) int {
	if a > 0 {
		return a
	}
	return b
}

// coerceTypesFromSpec extracts a union of "types" maps from the transform list.
// If no coerce step exists, it returns an empty map (text passthrough).
func coerceTypesFromSpec(p config.Pipeline) map[string]string {
	out := map[string]string{}
	for _, t := range p.Transform {
		if t.Kind == "coerce" {
			m := t.Options.StringMap("types")
			for k, v := range m {
				out[k] = v
			}
		}
	}
	return out
}

// coerceLayoutFromSpec returns the first "coerce.layout" encountered, or a
// sensible default for CZ (02.01.2006) when unspecified.
func coerceLayoutFromSpec(p config.Pipeline) string {
	for _, t := range p.Transform {
		if t.Kind == "coerce" {
			if s := t.Options.String("layout", ""); s != "" {
				return s
			}
		}
	}
	return "02.01.2006"
}

// splitFQN converts "schema.table" into {"schema","table"}; used in some places.
func splitFQN(fqn string) []string {
	parts := strings.Split(fqn, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// errAgg aggregates errors
type errAgg struct {
	mu      sync.Mutex
	limit   int
	count   int
	first   []string
	buckets map[string]int
}

func newErrAgg(limit int) *errAgg {
	return &errAgg{limit: limit, buckets: make(map[string]int)}
}
func (a *errAgg) add(msg string) {
	a.mu.Lock()
	a.buckets[msg]++
	if a.count < a.limit {
		a.first = append(a.first, msg)
	}
	a.count++
	a.mu.Unlock()
}

func requiredFromContract(p config.Pipeline) []string {
	for _, t := range p.Transform {
		if t.Kind == "validate" {
			if raw := t.Options.Any("contract"); raw != nil {
				b, _ := json.Marshal(raw)
				var c schema.Contract
				if json.Unmarshal(b, &c) == nil {
					var req []string
					for _, f := range c.Fields {
						if f.Required {
							req = append(req, f.Name)
						}
					}
					return req
				}
			}
		}
	}
	return nil
}

// anchorRuleFromSpec reads {"field": "...", "value": "..."} from
// transform.options.use_anchor.
func anchorRuleFromSpec(p config.Pipeline) *transformer.AnchorRule {
	for _, t := range p.Transform {
		if t.Kind != "coerce" {
			continue
		}
		raw := t.Options.Any("use_anchor")
		if raw == nil {
			continue
		}
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		field, _ := m["field"].(string)
		value, _ := m["value"].(string)

		if field == "" || value == "" {
			continue
		}

		return &transformer.AnchorRule{Field: field, Value: value}
	}
	return nil
}

// IncProcessed increments the count of successfully parsed rows that
// have entered the processing pipeline.
//
// This represents rows that were read successfully by the parser
// and handed off for transformation.
func (c *counters) IncProcessed() {
	c.processed.Add(1)
	metrics.RecordRow(c.job, "processed", 1)
}

// IncParseError increments the count of rows that failed during the
// parsing stage (CSV/XML read failures).
//
// These rows never enter the transformation pipeline.
func (c *counters) IncParseError() {
	c.parseErrors.Add(1)
	metrics.RecordRow(c.job, "parse_errors", 1)
}

// IncValidateDropped increments the count of rows that were rejected
// by validation.
//
// These rows were well-formed but missing required fields or had
// invalid values according to the contract.
func (c *counters) IncValidateDropped() {
	c.validateRejects.Add(1)
	metrics.RecordRow(c.job, "validate_dropped", 1)
}

// IncTransformRejected increments the count of rows that were rejected
// during the transform/coerce stage.
//
// These rows failed type coercion or other transform rules.
func (c *counters) IncTransformRejected() {
	c.transformRejects.Add(1)
	metrics.RecordRow(c.job, "transform_rejected", 1)
}

// AddInserted increments the count of rows successfully written to
// the storage backend.
//
// n represents the number of rows flushed as part of a batch.
func (c *counters) AddInserted(n int64) {
	if n <= 0 {
		return
	}
	c.inserted.Add(n)
	metrics.RecordRow(c.job, "inserted", n)
}

// IncBatches increments the count of database batches successfully
// flushed to the storage backend.
//
// This is typically tied to COPY or bulk insert operations.
func (c *counters) IncBatches() {
	c.batches.Add(1)
	metrics.RecordBatches(c.job, 1)
}
