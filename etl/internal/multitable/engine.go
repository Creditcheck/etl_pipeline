package multitable

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"etl/internal/config"
	"etl/internal/metrics"
	"etl/internal/storage"
	"etl/internal/transformer"
	"etl/internal/transformer/builtin"
)

// Logger is the minimal logging interface used by the multitable engine.
// *log.Logger satisfies this interface.
type Logger interface {
	Printf(format string, v ...any)
}

// StreamFn is a seam for providing validated row streams.
//
// When to use:
//   - Unit tests: inject a deterministic stream without file I/O or parsers.
//   - Alternate runtimes: route streams from other sources.
//
// Errors:
//   - Implementations should return a non-nil error for fatal setup failures.
//   - Row-level parse/validation errors should be reflected by stream.Wait()
//     semantics (consistent with StreamValidatedRows).
type StreamFn func(ctx context.Context, cfg Pipeline, columns []string) (*ValidatedStream, error)

// Engine2Pass implements Option 2 (two-pass streaming):
//   - Pass 1: stream validated rows and ensure dimension keys exist (batching, no global dedupe).
//   - Pass 2: stream again, resolve IDs per batch, insert facts per batch.
//
// This avoids buffering all rows in memory and avoids huge in-memory key sets.
type Engine2Pass struct {
	Repo   storage.MultiRepository
	Logger Logger

	// Stream is an optional seam to make Engine2Pass unit-testable.
	// When nil, StreamValidatedRows is used.
	Stream StreamFn
}

// EngineRunStats are the high-level counters emitted by the multitable engine.
//
// These are intentionally aligned with cmd/etl single-table counters so that
// dashboards and operators can reason about both pipeline types consistently.
//
// Definitions:
//   - Processed: successfully parsed rows entering the transform stage.
//   - ParseErrors: non-fatal parse/decode errors during parsing.
//   - TransformRejected: rows rejected during the coerce/transform stage.
//   - ValidateDropped: rows dropped by contract validation (lenient policy).
//   - Inserted: total fact rows inserted.
//   - Batches: number of fact insert batches.
type EngineRunStats struct {
	Processed         uint64
	ParseErrors       uint64
	TransformRejected uint64
	ValidateDropped   uint64
	Inserted          uint64
	Batches           uint64
}

// Run executes the two-pass streaming plan.
func (e *Engine2Pass) Run(ctx context.Context, cfg Pipeline, columns []string) error {
	if e.Repo == nil {
		return fmt.Errorf("engine: Repo is required")
	}

	logf := e.logger()
	lenient := isLenient(cfg.Transform)

	plan, err := buildIndexedPlan(cfg, columns)
	if err != nil {
		return err
	}

	ddlStart := time.Now()
	err = e.Repo.EnsureTables(ctx, plan.AllTables())
	metrics.RecordStep(cfg.Job, "ddl", err, time.Since(ddlStart))
	if err != nil {
		return err
	}
	logf("stage=ddl ok duration=%s", durMS(ddlStart))

	// Pass 1: ensure dimensions (streaming).
	pass1Start := time.Now()
	err = e.ensureDimensionsStreaming(ctx, cfg, columns, plan)
	metrics.RecordStep(cfg.Job, "pass1_ensure_dims", err, time.Since(pass1Start))
	if err != nil {
		return err
	}
	logf("stage=pass1_ensure_dims ok duration=%s", durMS(pass1Start))

	// Pass 2: load facts (streaming).
	pass2Start := time.Now()
	stats, err := e.loadFactsStreaming(ctx, cfg, columns, plan, lenient)
	metrics.RecordStep(cfg.Job, "pass2_load_facts", err, time.Since(pass2Start))
	if err != nil {
		return err
	}
	logf("stage=pass2_load_facts ok duration=%s", durMS(pass2Start))

	logf(
		"summary: processed=%d parse_errors=%d transform_rejected=%d validate_dropped=%d inserted=%d batches=%d",
		stats.Processed,
		stats.ParseErrors,
		stats.TransformRejected,
		stats.ValidateDropped,
		stats.Inserted,
		stats.Batches,
	)

	return nil
}

func (e *Engine2Pass) logger() func(format string, v ...any) {
	if e.Logger == nil {
		l := log.New(discardWriter{}, "", 0)
		return l.Printf
	}
	return e.Logger.Printf
}

// stream returns the validated row stream implementation used by Engine2Pass.
//
// In production, this constructs the default file->parse->transform->validate
// streaming pipeline (StreamValidatedRows). In unit tests or alternate runtimes,
// Engine2Pass.Stream may be provided to inject a deterministic stream.
//
// Errors:
//
//   - Returns any fatal initialization error from the stream provider.
//   - Row-level parse/transform/validation issues are reflected via stream counters
//     and/or stream.Wait (consistent with StreamValidatedRows).
func (e *Engine2Pass) stream(ctx context.Context, cfg Pipeline, columns []string) (*ValidatedStream, error) {
	if e.Stream != nil {
		return e.Stream(ctx, cfg, columns)
	}
	return StreamValidatedRows(ctx, cfg, columns)
}

func durMS(start time.Time) time.Duration { return time.Since(start).Truncate(time.Millisecond) }

// ensureDimensionsStreaming streams validated rows and calls EnsureDimensionKeys in batches.
// It does not attempt global dedupe (which is memory-expensive and usually pointless for
// high-cardinality keys).
func (e *Engine2Pass) ensureDimensionsStreaming(
	ctx context.Context,
	cfg Pipeline,
	columns []string,
	plan indexedPlan,
) error {
	logf := e.logger()

	batchSize := cfg.Runtime.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	// Per-dimension pending typed values.
	pending := make(map[string][]any, len(plan.Dimensions))
	seenRows := 0

	// Optional within-batch dedupe (bounded).
	dedupeWithinBatch := cfg.Runtime.DedupeDimensionKeysWithinBatch

	stream, err := e.stream(ctx, cfg, columns)
	if err != nil {
		return err
	}

	flushDim := func(dim indexedDimension) error {
		keys := pending[dim.Table.Name]
		if len(keys) == 0 {
			return nil
		}

		// Detach keys from pending slice before any dedupe rewrite.
		pending[dim.Table.Name] = pending[dim.Table.Name][:0]
		if cap(pending[dim.Table.Name]) > batchSize*4 {
			// Drop oversized backing array to reduce retained heap.
			pending[dim.Table.Name] = make([]any, 0, batchSize)
		}

		if dedupeWithinBatch && len(keys) > 1 {
			keys = dedupeTypedKeys(keys)
		}

		conflictCols := []string{dim.KeyColumn}
		if dim.Table.Load.Conflict != nil && len(dim.Table.Load.Conflict.TargetColumns) > 0 {
			conflictCols = dim.Table.Load.Conflict.TargetColumns
		}

		if err := e.Repo.EnsureDimensionKeys(ctx, dim.Table.Name, dim.KeyColumn, keys, conflictCols); err != nil {
			return err
		}
		return nil
	}

	flushAll := func() error {
		for _, dim := range plan.Dimensions {
			if err := flushDim(dim); err != nil {
				return err
			}
		}
		return nil
	}

	// Ensure slices are initialized with batch capacity.
	for _, dim := range plan.Dimensions {
		pending[dim.Table.Name] = make([]any, 0, batchSize)
	}

	// Stream rows, enqueue dim keys, flush when any dim hits batchSize.
	for r := range stream.Rows {
		seenRows++
		for _, dim := range plan.Dimensions {
			if dim.SourceIndex < 0 {
				continue
			}
			v := r.V[dim.SourceIndex]
			if normalizeKey(v) == "" {
				continue
			}
			pending[dim.Table.Name] = append(pending[dim.Table.Name], typedBindValue(v))
			if len(pending[dim.Table.Name]) >= batchSize {
				if err := flushDim(dim); err != nil {
					r.Free()
					return err
				}
			}
		}
		r.Free()
	}

	if err := flushAll(); err != nil {
		return err
	}
	if stream.Wait != nil {
		if err := stream.Wait(); err != nil {
			return err
		}
	}

	logf("stage=pass1_rows seen_rows=%d", seenRows)
	return nil
}

// loadFactsStreaming performs pass 2 of the two-pass multitable pipeline.
//
// It streams validated rows again, resolves dimension foreign keys per-batch,
// and inserts fact rows into the target tables using the repository backend.
//
// Concurrency model:
//
//   - A single producer goroutine reads from the validated stream and groups
//     rows into batches pushed onto a jobs channel.
//   - One or more loader workers consume batches, prewarm dimension lookups,
//     and execute fact inserts.
//
// Cancellation & pooling:
//
//   - The function is drain-safe: it does not abruptly stop reading from the
//     upstream validated stream on ctx cancellation. Instead, it cancels work
//     via ctx and continues draining to avoid goroutine leaks.
//   - To prevent reuse races with pooled *transformer.Row values during
//     cancellation unwinding, rows observed on cancellation paths are discarded
//     via r.Drop() (not returned to the sync.Pool). Rows on the normal path are
//     returned to the pool via r.Free() after all downstream work completes.
//
// Stats:
//
//   - Row-level counters (processed/parse_errors/transform_rejected/validate_dropped)
//     are sourced from the returned ValidatedStream counters.
//   - Inserted/batches are tracked locally from repository insert results.
//
// Closing semantics:
//
//   - Does not close any external channels. It consumes stream.Rows until closed,
//     and waits on stream.Wait (if non-nil) before returning.
//   - Returns the first fatal error encountered (via repository calls or stream.Wait).
func (e *Engine2Pass) loadFactsStreaming(
	ctx context.Context,
	cfg Pipeline,
	columns []string,
	plan indexedPlan,
	lenient bool,
) (EngineRunStats, error) {
	logf := e.logger()
	var stats EngineRunStats

	batchSize := cfg.Runtime.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	loaderWorkers := cfg.Runtime.LoaderWorkers
	if loaderWorkers <= 0 {
		loaderWorkers = 1
	}

	debug := cfg.Runtime.DebugTimings

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	errCh := make(chan error, 1)
	setErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
			cancel(err)
		default:
		}
	}

	// Only pass2 should emit row-level metrics, but counters always work.
	ctx = WithStreamRowMetrics(ctx, true)
	stream, err := e.stream(ctx, cfg, columns)
	if err != nil {
		return stats, err
	}

	var insertedTotal atomic.Uint64
	var batchesTotal atomic.Uint64

	type batchJob struct {
		rows []*transformer.Row
	}
	jobs := make(chan batchJob, loaderWorkers*2)

	// Producer: read validated rows and bundle into batches.
	var producerWG sync.WaitGroup
	producerWG.Add(1)
	go func() {
		defer producerWG.Done()
		defer close(jobs)

		batch := make([]*transformer.Row, 0, batchSize)

		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			job := batchJob{rows: batch}
			select {
			case jobs <- job:
				batch = make([]*transformer.Row, 0, batchSize)
				return true
			case <-ctx.Done():
				// On cancellation: do not re-pool
				for _, r := range batch {
					r.Drop()
				}
				return false
			}
		}

		for r := range stream.Rows {
			if ctx.Err() != nil {
				r.Drop()
				continue
			}
			batch = append(batch, r)
			if len(batch) >= batchSize {
				if ok := flush(); !ok {
					return
				}
			}
		}
		_ = flush()
	}()

	// workerFn consumes fact batch jobs, prewarms dimension lookups for the batch,
	// inserts fact rows, and returns input rows to the pool on the normal path.
	//
	// Cancellation & pooling:
	//   - On ctx cancellation, rows are discarded via Drop() to avoid pooled-row reuse
	//     races during cancellation unwinding.
	//   - On the normal path, rows are returned to the pool via Free() after all
	//     inserts are complete for the batch.
	workerFn := func(workerID int) {
		cache := make(map[string]map[string]int64, len(plan.Dimensions))

		for job := range jobs {
			if ctx.Err() != nil {
				for _, r := range job.rows {
					r.Drop()
				}
				continue
			}

			startBatch := time.Now()

			if err := e.prewarmBatchLookups(ctx, plan, job.rows, cache); err != nil {
				for _, r := range job.rows {
					if ctx.Err() != nil {
						r.Drop()
					} else {
						r.Free()
					}
				}
				setErr(err)
				continue
			}

			for _, fact := range plan.Facts {
				inserted, dropped, err := e.insertFactBatch(ctx, cfg.Job, fact, job.rows, cache, lenient)
				if err != nil {
					for _, r := range job.rows {
						if ctx.Err() != nil {
							r.Drop()
						} else {
							r.Free()
						}
					}
					setErr(err)
					break
				}
				if inserted > 0 {
					insertedTotal.Add(uint64(inserted))
				}
				if inserted > 0 || dropped > 0 {
					batchesTotal.Add(1)
				}
			}

			// Normal path: return to pool
			if ctx.Err() != nil {
				for _, r := range job.rows {
					r.Drop()
				}
			} else {
				for _, r := range job.rows {
					r.Free()
				}
			}

			if debug {
				logf("debug: worker=%d batch=%d duration=%s", workerID, len(job.rows), time.Since(startBatch).Truncate(time.Millisecond))
			}
		}
	}

	// START
	//workerFn := func(workerID int) {
	//	cache := make(map[string]map[string]int64, len(plan.Dimensions))

	//	for job := range jobs {
	//		if ctx.Err() != nil {
	//			for _, r := range job.rows {
	//				r.Drop()
	//			}
	//			continue
	//		}

	//		startBatch := time.Now()

	//		if err := e.prewarmBatchLookups(ctx, plan, job.rows, cache); err != nil {
	//			for _, r := range job.rows {
	//				if ctx.Err() != nil {
	//					r.Drop()
	//				} else {
	//					r.Free()
	//				}
	//			}
	//			setErr(err)
	//			continue
	//		}

	//		for _, fact := range plan.Facts {
	//			inserted, dropped, err := e.insertFactBatch(ctx, fact, job.rows, cache, lenient)
	//			if err != nil {
	//				for _, r := range job.rows {
	//					if ctx.Err() != nil {
	//						r.Drop()
	//					} else {
	//						r.Free()
	//					}
	//				}
	//				setErr(err)
	//				break
	//			}

	//			if inserted > 0 {
	//				insertedTotal.Add(uint64(inserted))
	//			}
	//			// Count batches that did any work (inserted or dropped)
	//			if inserted > 0 || dropped > 0 {
	//				batchesTotal.Add(1)
	//			}
	//		}

	//		// Release rows
	//		if ctx.Err() != nil {
	//			for _, r := range job.rows {
	//				r.Drop()
	//			}
	//		} else {
	//			for _, r := range job.rows {
	//				r.Free()
	//			}
	//		}

	//		if debug {
	//			logf("debug: worker=%d batch=%d duration=%s", workerID, len(job.rows), time.Since(startBatch).Truncate(time.Millisecond))
	//		}
	//	}
	//}
	// END

	var workersWG sync.WaitGroup
	workersWG.Add(loaderWorkers)
	for i := 0; i < loaderWorkers; i++ {
		go func(id int) {
			defer workersWG.Done()
			workerFn(id)
		}(i + 1)
	}

	producerWG.Wait()
	workersWG.Wait()

	// Wait for upstream pipeline (parse/transform/validate) to finish and report terminal errors.
	if stream.Wait != nil {
		if err := stream.Wait(); err != nil {
			setErr(err)
		}
	}

	select {
	case err := <-errCh:
		return stats, err
	default:
	}

	// Populate stats from stream counters + loader counters.
	if stream.ProcessedCount != nil {
		stats.Processed = stream.ProcessedCount()
	}
	if stream.ParseErrorCount != nil {
		stats.ParseErrors = stream.ParseErrorCount()
	}
	if stream.TransformRejectedCount != nil {
		stats.TransformRejected = stream.TransformRejectedCount()
	}
	if stream.ValidateDroppedCount != nil {
		stats.ValidateDropped = stream.ValidateDroppedCount()
	}
	stats.Inserted = insertedTotal.Load()
	stats.Batches = batchesTotal.Load()

	return stats, nil
}

// prewarmBatchLookups fetches only the dimension keys needed by this batch.
// It avoids "SELECT *" table scans.
//
// IMPORTANT: for lookup prewarm we pass STRING keys (normalized) to SelectKeyValueByKeys
// so cache keys are consistent across []byte vs string sources and mocks/backends.
func (e *Engine2Pass) prewarmBatchLookups(
	ctx context.Context,
	plan indexedPlan,
	batch []*transformer.Row,
	cache map[string]map[string]int64,
) error {
	// Collect needed keys per dimension table for this batch (exclude already cached keys).
	needed := make(map[string]map[string]struct{})

	for _, fact := range plan.Facts {
		for _, col := range fact.Columns {
			if col.Lookup == nil {
				continue
			}
			dimTable := col.Lookup.Table
			matchIdx := col.Lookup.MatchFieldIndex
			if matchIdx < 0 {
				continue
			}

			for _, r := range batch {
				v := r.V[matchIdx]
				nk := normalizeKey(v)
				if nk == "" {
					continue
				}
				if cm := cache[dimTable]; cm != nil {
					if _, ok := cm[nk]; ok {
						continue
					}
				}
				m := needed[dimTable]
				if m == nil {
					m = make(map[string]struct{})
					needed[dimTable] = m
				}
				m[nk] = struct{}{}
			}
		}
	}

	// Bulk fetch per dimension table.
	for _, dim := range plan.Dimensions {
		m := needed[dim.Table.Name]
		if len(m) == 0 {
			continue
		}

		keys := make([]any, 0, len(m))
		for k := range m {
			keys = append(keys, k) // normalized string key
		}

		kv, err := e.Repo.SelectKeyValueByKeys(ctx, dim.Table.Name, dim.KeyColumn, dim.ValueColumn, keys)
		if err != nil {
			return err
		}

		cm := cache[dim.Table.Name]
		if cm == nil {
			cm = make(map[string]int64, len(kv))
			cache[dim.Table.Name] = cm
		}
		for k, v := range kv {
			cm[k] = v
		}
	}

	return nil
}

// insertFactBatch builds and inserts a batch of fact rows for a single fact table.
//
// For each input *transformer.Row in the batch, it produces an output row matching
// the fact's TargetColumns by copying source fields and/or resolving dimension IDs
// from the provided lookup cache.
//
// Leniency:
//   - If lenient is true, rows with missing lookup keys or lookup misses are dropped
//     (counted as dropped) rather than producing an error.
//   - If lenient is false, missing lookup keys or lookup misses are fatal.
//
// Dedupe / deadlock avoidance:
//   - If ON CONFLICT / dedupe is enabled for the fact table, output rows are sorted
//     by conflict keys to impose a stable lock acquisition order in Postgres.
//
// Metrics:
//   - Emits loader-side counters via metrics.RecordRow(job, kind, n) so etl.records.total
//     includes inserted rows and conflict/dropped visibility (not just "processed").
//
// Return values:
//   - inserted: number of rows reported as inserted by the repository backend.
//   - dropped:  number of rows dropped due to lenient lookup policy.
//   - error:    non-nil on fatal lookup failures or repository insert errors.
func (e *Engine2Pass) insertFactBatch(
	ctx context.Context,
	job string,
	fact indexedFact,
	batch []*transformer.Row,
	cache map[string]map[string]int64,
	lenient bool,
) (inserted int, dropped int, _ error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}

	cols := fact.TargetColumns
	outRows := make([][]any, 0, len(batch))

	for _, r := range batch {
		rowOut := make([]any, len(fact.Columns))
		drop := false

		for i, c := range fact.Columns {
			if c.Lookup != nil {
				dimTable := c.Lookup.Table
				matchIdx := c.Lookup.MatchFieldIndex
				key := normalizeKey(r.V[matchIdx])
				if key == "" {
					if c.Nullable {
						rowOut[i] = nil
						continue
					}
					if lenient {
						drop = true
						break
					}
					return inserted, dropped, fmt.Errorf("fact %s: empty lookup key table=%s", fact.Table.Name, dimTable)
				}

				cm := cache[dimTable]
				id, ok := int64(0), false
				if cm != nil {
					id, ok = cm[key]
				}
				if !ok {
					if lenient {
						drop = true
						break
					}
					return inserted, dropped, fmt.Errorf("fact %s: lookup miss table=%s key=%q", fact.Table.Name, dimTable, key)
				}

				rowOut[i] = id
				continue
			}

			if c.SourceFieldIndex >= 0 {
				rowOut[i] = r.V[c.SourceFieldIndex]
				continue
			}
			rowOut[i] = nil
		}

		if drop {
			dropped++
			continue
		}
		outRows = append(outRows, rowOut)
	}

	// Emit drop metric (optional but useful).
	if dropped > 0 {
		metrics.RecordRow(job, "fact_dropped", int64(dropped))
	}

	if len(outRows) == 0 {
		return 0, dropped, nil
	}

	// Stable lock acquisition order when ON CONFLICT is enabled (reduces deadlocks).
	if len(fact.DedupeIndices) > 0 && len(outRows) > 1 {
		sort.SliceStable(outRows, func(i, j int) bool {
			a := outRows[i]
			b := outRows[j]
			for _, idx := range fact.DedupeIndices {
				ka := normalizeKey(a[idx])
				kb := normalizeKey(b[idx])
				if ka == kb {
					continue
				}
				return ka < kb
			}
			return false
		})
	}

	affected, err := e.Repo.InsertFactRows(ctx, fact.Table, fact.Table.Name, cols, outRows, fact.DedupeColumns)

	if err == nil && affected == 0 {
		log.Printf("debug: op=insert all_conflicts table=%s rows=%d", fact.Table.Name, len(outRows))
		metrics.RecordRow(job, "all_conflicts", int64(len(outRows)))
	}

	if err != nil {
		// Attach high-signal context: table, columns, and a typed sample row.
		bad := ""
		if len(outRows) > 0 {
			// If you want: locate "row_hash" index generically; for now just sample first row.
			bad = fmtRowForCols(cols, outRows[0])
		}
		return 0, dropped, fmt.Errorf(
			"insert facts failed table=%s cols=%v bad=%s: %w",
			fact.Table.Name, cols, bad, err,
		)
	}

	if affected > 0 {
		metrics.RecordRow(job, "inserted", int64(affected))
	}

	return int(affected), dropped, nil
}

// dedupeTypedKeys performs bounded dedupe for a slice of typed values.
// It uses normalizeKey for keying but preserves the first typed value.
func dedupeTypedKeys(in []any) []any {
	seen := make(map[string]struct{}, len(in))
	out := make([]any, 0, len(in))
	for _, v := range in {
		k := normalizeKey(v)
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return out
}

// ---- Plan compilation with column indices (avoid per-row map allocs) ----

type indexedPlan struct {
	Dimensions []indexedDimension
	Facts      []indexedFact
}

func (p indexedPlan) AllTables() []storage.TableSpec {
	out := make([]storage.TableSpec, 0, len(p.Dimensions)+len(p.Facts))
	for _, d := range p.Dimensions {
		out = append(out, d.Table)
	}
	for _, f := range p.Facts {
		out = append(out, f.Table)
	}
	return out
}

type indexedDimension struct {
	Table       storage.TableSpec
	SourceIndex int

	KeyColumn   string
	ValueColumn string
}

type indexedFact struct {
	Table         storage.TableSpec
	TargetColumns []string
	DedupeColumns []string

	// DedupeIndices are the positional indices (into TargetColumns/out rows)
	// corresponding to DedupeColumns.
	DedupeIndices []int

	Columns []indexedFactColumn
}

type indexedFactColumn struct {
	TargetColumn     string
	SourceFieldIndex int
	Nullable         bool
	Lookup           *indexedLookup
}

type indexedLookup struct {
	Table           string
	MatchFieldIndex int
}

func buildIndexedPlan(cfg Pipeline, columns []string) (indexedPlan, error) {
	colIndex := indexColumns(columns)

	var plan indexedPlan

	for _, t := range cfg.Storage.DB.Tables {
		switch t.Load.Kind {
		case "dimension":
			plan.Dimensions = append(plan.Dimensions, compileDimension(t, colIndex))

		case "fact":
			f, err := compileFact(t, colIndex)
			if err != nil {
				return plan, err
			}
			plan.Facts = append(plan.Facts, f)

		default:
			return plan, fmt.Errorf("table %s: unknown load kind %q", t.Name, t.Load.Kind)
		}
	}

	sort.SliceStable(plan.Dimensions, func(i, j int) bool { return plan.Dimensions[i].Table.Name < plan.Dimensions[j].Table.Name })
	sort.SliceStable(plan.Facts, func(i, j int) bool { return plan.Facts[i].Table.Name < plan.Facts[j].Table.Name })

	return plan, nil
}

func indexColumns(columns []string) map[string]int {
	m := make(map[string]int, len(columns))
	for i, c := range columns {
		m[c] = i
	}
	return m
}

func compileDimension(t storage.TableSpec, colIndex map[string]int) indexedDimension {
	srcField := ""
	targetKeyCol := ""
	if len(t.Load.FromRows) > 0 {
		srcField = t.Load.FromRows[0].SourceField
		targetKeyCol = t.Load.FromRows[0].TargetColumn
	}

	srcIdx := -1
	if srcField != "" {
		if i, ok := colIndex[srcField]; ok {
			srcIdx = i
		}
	}

	keyCol := targetKeyCol
	valCol := ""
	if t.Load.Cache != nil {
		keyCol = t.Load.Cache.KeyColumn
		valCol = t.Load.Cache.ValueColumn
	}

	return indexedDimension{
		Table:       t,
		SourceIndex: srcIdx,
		KeyColumn:   keyCol,
		ValueColumn: valCol,
	}
}

func compileFact(t storage.TableSpec, colIndex map[string]int) (indexedFact, error) {
	f := indexedFact{Table: t}

	nullableByColumn := mapNullableByColumn(t.Columns)

	for _, fr := range t.Load.FromRows {
		f.TargetColumns = append(f.TargetColumns, fr.TargetColumn)

		col := indexedFactColumn{
			TargetColumn:     fr.TargetColumn,
			SourceFieldIndex: -1,
			Nullable:         nullableByColumn[fr.TargetColumn],
		}

		if fr.Lookup != nil {
			matchField := pickLookupMatchField(fr.Lookup.Match)
			matchIdx := -1
			if matchField != "" {
				if i, ok := colIndex[matchField]; ok {
					matchIdx = i
				}
			}
			col.Lookup = &indexedLookup{
				Table:           fr.Lookup.Table,
				MatchFieldIndex: matchIdx,
			}
		} else if fr.SourceField != "" {
			if i, ok := colIndex[fr.SourceField]; ok {
				col.SourceFieldIndex = i
			}
		}

		f.Columns = append(f.Columns, col)
	}

	if t.Load.Dedupe != nil && len(t.Load.Dedupe.ConflictColumns) > 0 {
		f.DedupeColumns = append(f.DedupeColumns, t.Load.Dedupe.ConflictColumns...)
	}

	// Precompute the output row indices for the dedupe columns.
	if len(f.DedupeColumns) > 0 && len(f.TargetColumns) > 0 {
		idx := make(map[string]int, len(f.TargetColumns))
		for i, c := range f.TargetColumns {
			idx[c] = i
		}
		for _, c := range f.DedupeColumns {
			if i, ok := idx[c]; ok {
				f.DedupeIndices = append(f.DedupeIndices, i)
			}
		}
	}

	return f, nil
}

func mapNullableByColumn(cols []storage.ColumnSpec) map[string]bool {
	out := make(map[string]bool, len(cols))
	for _, c := range cols {
		nullable := false
		if c.Nullable != nil {
			nullable = *c.Nullable
		}
		out[c.Name] = nullable
	}
	return out
}

// pickLookupMatchField chooses a stable match field from a lookup match map.
func pickLookupMatchField(match map[string]string) string {
	if len(match) == 0 {
		return ""
	}
	keys := make([]string, 0, len(match))
	for k := range match {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return match[keys[0]]
}

// isLenient returns true if any validate transform has policy "lenient".
func isLenient(ts []config.Transform) bool {
	for _, t := range ts {
		if t.Kind != "validate" {
			continue
		}
		p := strings.ToLower(strings.TrimSpace(fmt.Sprint(t.Options.Any("policy"))))
		if p == "lenient" {
			return true
		}
	}
	return false
}

// normalizeKey produces a stable string form used for in-memory caches.
func normalizeKey(v any) string {
	switch t := v.(type) {
	case nil:
		return ""

	case string:
		if builtin.HasEdgeSpace(t) {
			return strings.TrimSpace(t)
		}
		return t

	case []byte:
		s := string(t)
		if builtin.HasEdgeSpace(s) {
			return strings.TrimSpace(s)
		}
		return s

	case bool:
		if t {
			return "true"
		}
		return "false"

	case int:
		return strconv.Itoa(t)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)

	case uint:
		return strconv.FormatUint(uint64(t), 10)
	case uint8:
		return strconv.FormatUint(uint64(t), 10)
	case uint16:
		return strconv.FormatUint(uint64(t), 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint64:
		return strconv.FormatUint(t, 10)

	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)

	default:
		val := fmt.Sprint(v)
		if builtin.HasEdgeSpace(val) {
			return strings.TrimSpace(val)
		}
		return val
	}
}

func typedBindValue(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		if builtin.HasEdgeSpace(t) {
			return strings.TrimSpace(t)
		}
		return t
	case []byte:
		// bytes.TrimSpace returns a sub-slice; for most parser implementations this is fine.
		// If you see retention of huge buffers here, we can add a "copy on trim" option.
		return bytes.TrimSpace(t)
	default:
		return v
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (n int, err error) { return len(p), nil }

// fmtAny formats a value with its Go type, truncating long strings/bytes.
func fmtAny(v any) string {
	if v == nil {
		return "nil"
	}

	switch x := v.(type) {
	case string:
		const max = 200
		if len(x) > max {
			return fmt.Sprintf("%q(string,len=%d)", x[:max]+"…", len(x))
		}
		return fmt.Sprintf("%q(string)", x)
	case []byte:
		const max = 64
		if len(x) > max {
			return fmt.Sprintf("%x([]byte,len=%d)", x[:max], len(x))
		}
		return fmt.Sprintf("%x([]byte)", x)
	default:
		// Covers int/int64/float64/bool/time.Time/etc.
		return fmt.Sprintf("%v(%T)", v, v)
	}
}

func fmtRowForCols(cols []string, row []any) string {
	n := len(cols)
	if len(row) < n {
		n = len(row)
	}
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(cols[i])
		b.WriteByte('=')
		b.WriteString(fmtAny(row[i]))
	}
	if len(row) > n {
		b.WriteString(", …")
	}
	b.WriteByte(']')
	return b.String()
}
