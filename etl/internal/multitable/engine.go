package multitable

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"etl/internal/config"
	"etl/internal/storage"
	"etl/internal/transformer"
	"etl/internal/transformer/builtin"
)

// Logger is the minimal logging interface used by the multitable engine.
// *log.Logger satisfies this interface.
type Logger interface {
	Printf(format string, v ...any)
}

// Engine2Pass implements Option 2 (two-pass streaming):
//   - Pass 1: stream validated rows and ensure dimension keys exist (batching, no global dedupe).
//   - Pass 2: stream again, resolve IDs per batch, insert facts per batch.
//
// This avoids buffering all rows in memory and avoids huge in-memory key sets.
type Engine2Pass struct {
	Repo   storage.MultiRepository
	Logger Logger
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
	if err := e.Repo.EnsureTables(ctx, plan.AllTables()); err != nil {
		return err
	}
	logf("stage=ddl ok duration=%s", durMS(ddlStart))

	// Pass 1: ensure dimensions (streaming).
	pass1Start := time.Now()
	if err := e.ensureDimensionsStreaming(ctx, cfg, columns, plan); err != nil {
		return err
	}
	logf("stage=pass1_ensure_dims ok duration=%s", durMS(pass1Start))

	// Pass 2: load facts (streaming).
	pass2Start := time.Now()
	if err := e.loadFactsStreaming(ctx, cfg, columns, plan, lenient); err != nil {
		return err
	}
	logf("stage=pass2_load_facts ok duration=%s", durMS(pass2Start))

	return nil
}

func (e *Engine2Pass) logger() func(format string, v ...any) {
	if e.Logger == nil {
		l := log.New(discardWriter{}, "", 0)
		return l.Printf
	}
	return e.Logger.Printf
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

	stream, err := StreamValidatedRows(ctx, cfg, columns)
	if err != nil {
		return err
	}

	flushDim := func(dim indexedDimension) error {
		keys := pending[dim.Table.Name]
		if len(keys) == 0 {
			return nil
		}
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
	if err := stream.Wait(); err != nil {
		return err
	}

	logf("stage=pass1_rows seen_rows=%d", seenRows)
	return nil
}

// loadFactsStreaming streams validated rows again and inserts facts in batches.
// Dimension ID resolution is done per batch using SelectKeyValueByKeys.
//
// Performance note:
// For SQL Server specifically, many small inserts on a single connection tend to be
// single-core and slow. To drive higher throughput (and multiple SQL Server schedulers),
// this stage supports configurable loader worker concurrency.
func (e *Engine2Pass) loadFactsStreaming(ctx context.Context, cfg Pipeline, columns []string, plan indexedPlan, lenient bool) error {
	logf := e.logger()

	batchSize := cfg.Runtime.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	loaderWorkers := cfg.Runtime.LoaderWorkers
	if loaderWorkers <= 0 {
		loaderWorkers = 1
	}

	debug := cfg.Runtime.DebugTimings

	stream, err := StreamValidatedRows(ctx, cfg, columns)
	if err != nil {
		return err
	}

	// Producer: batches of pooled rows. Ownership transfers to the consumer worker,
	// which must Free() each row exactly once.
	batchCh := make(chan []*transformer.Row, loaderWorkers*2)

	// Worker errors: first error cancels the operation.
	var (
		errOnce sync.Once
		runErr  error
	)
	setErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() { runErr = err })
	}

	// Worker pool: per-worker cache to avoid locks.
	var wg sync.WaitGroup
	wg.Add(loaderWorkers)
	for w := 0; w < loaderWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()

			// cache[dimTable][normalizedKey] = id
			cache := make(map[string]map[string]int64, len(plan.Dimensions))

			for batch := range batchCh {
				if runErr != nil {
					// Drain + free quickly if we already failed.
					for _, r := range batch {
						r.Free()
					}
					continue
				}

				start := time.Now()
				err := e.processFactBatch(ctx, plan, batch, cache, lenient, debug, logf)
				dur := time.Since(start)

				// Always free owned rows.
				for _, r := range batch {
					r.Free()
				}

				if err != nil {
					setErr(err)
					if debug {
						logf("stage=pass2_batch worker=%d status=error duration=%s err=%v", workerID, dur.Truncate(time.Millisecond), err)
					}
				} else if debug {
					logf("stage=pass2_batch worker=%d status=ok duration=%s rows=%d", workerID, dur.Truncate(time.Millisecond), len(batch))
				}
			}
		}(w)
	}

	// Producer loop: build batches and hand off to workers.
	seenRows := 0
	batch := make([]*transformer.Row, 0, batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		out := batch
		batch = make([]*transformer.Row, 0, batchSize)

		select {
		case batchCh <- out:
		case <-ctx.Done():
			for _, r := range out {
				r.Free()
			}
			setErr(ctx.Err())
		}
	}

	for r := range stream.Rows {
		if runErr != nil {
			r.Free()
			continue
		}

		seenRows++
		batch = append(batch, r)
		if len(batch) >= batchSize {
			flush()
		}
	}
	flush()

	close(batchCh)
	wg.Wait()

	if err := stream.Wait(); err != nil {
		return err
	}
	if runErr != nil {
		return runErr
	}

	logf("stage=pass2_rows seen_rows=%d loader_workers=%d", seenRows, loaderWorkers)
	return nil
}

func (e *Engine2Pass) processFactBatch(
	ctx context.Context,
	plan indexedPlan,
	batch []*transformer.Row,
	cache map[string]map[string]int64,
	lenient bool,
	debug bool,
	logf func(format string, v ...any),
) error {
	if len(batch) == 0 {
		return nil
	}

	// Time the lookup resolution separately, to make bottlenecks obvious.
	lookupStart := time.Now()
	if err := e.resolveBatchLookups(ctx, plan, batch, cache); err != nil {
		return err
	}
	if debug {
		logf("stage=pass2_resolve_lookups batch_rows=%d duration=%s", len(batch), time.Since(lookupStart).Truncate(time.Millisecond))
	}

	for _, fact := range plan.Facts {
		start := time.Now()
		inserted, ds, err := e.insertFactBatch(ctx, fact, batch, cache, lenient, debug)
		dur := time.Since(start).Truncate(time.Millisecond)

		if err != nil {
			return err
		}

		droppedTotal := ds.TotalDropped()
		if debug {
			logf(
				"stage=pass2_insert_fact table=%s batch_rows=%d inserted=%d dropped_total=%d dropped_lookup_miss=%d dropped_empty_key=%d dropped_conflict=%d duration=%s",
				fact.Table.Name,
				len(batch),
				inserted,
				droppedTotal,
				ds.LookupMiss,
				ds.EmptyKey,
				ds.Conflict,
				dur,
			)
			for _, s := range ds.Samples {
				logf("stage=fact_drop_sample table=%s %s", fact.Table.Name, s)
			}
		} else if lenient && droppedTotal > 0 {
			logf(
				"stage=fact_batch table=%s inserted=%d dropped_total=%d dropped_lookup_miss=%d dropped_empty_key=%d dropped_conflict=%d",
				fact.Table.Name,
				inserted,
				droppedTotal,
				ds.LookupMiss,
				ds.EmptyKey,
				ds.Conflict,
			)
		}
	}

	return nil
}

// resolveBatchLookups fetches (key->id) mappings needed by this batch for all lookup tables.
func (e *Engine2Pass) resolveBatchLookups(
	ctx context.Context,
	plan indexedPlan,
	batch []*transformer.Row,
	cache map[string]map[string]int64,
) error {
	// needed[dimTable][normalizedKey] = typedValue
	needed := make(map[string]map[string]any)

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
					m = make(map[string]any)
					needed[dimTable] = m
				}
				if _, exists := m[nk]; !exists {
					m[nk] = typedBindValue(v)
				}
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
		for _, v := range m {
			keys = append(keys, v)
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

func (e *Engine2Pass) insertFactBatch(
	ctx context.Context,
	fact indexedFact,
	batch []*transformer.Row,
	cache map[string]map[string]int64,
	lenient bool,
	debug bool,
) (inserted int, ds dropStats, _ error) {
	cols := fact.TargetColumns

	outRows := make([][]any, 0, len(batch))
	for _, r := range batch {
		rowOut := make([]any, 0, len(fact.Columns))
		drop := false
		dropSample := ""

		for _, c := range fact.Columns {
			if c.Lookup != nil {
				dimTable := c.Lookup.Table
				matchIdx := c.Lookup.MatchFieldIndex
				key := normalizeKey(r.V[matchIdx])
				if key == "" {
					if lenient {
						ds.EmptyKey++
						drop = true
						if debug && dropSample == "" {
							dropSample = fmt.Sprintf("reason=empty_lookup_key lookup_table=%s", dimTable)
						}
						break
					}
					return inserted, ds, fmt.Errorf("fact %s: empty lookup key table=%s", fact.Table.Name, dimTable)
				}

				var (
					id int64
					ok bool
				)
				if cm := cache[dimTable]; cm != nil {
					id, ok = cm[key]
				}
				if !ok {
					if lenient {
						ds.LookupMiss++
						drop = true
						if debug && dropSample == "" {
							dropSample = fmt.Sprintf("reason=lookup_miss lookup_table=%s key=%q", dimTable, key)
						}
						break
					}
					return inserted, ds, fmt.Errorf("fact %s: lookup miss table=%s key=%q", fact.Table.Name, dimTable, key)
				}
				rowOut = append(rowOut, id)
				continue
			}

			if c.SourceFieldIndex >= 0 {
				rowOut = append(rowOut, r.V[c.SourceFieldIndex])
				continue
			}
			rowOut = append(rowOut, nil)
		}

		if drop {
			if debug && dropSample != "" {
				ds.AddSample(dropSample)
			}
			continue
		}
		outRows = append(outRows, rowOut)
	}

	if len(outRows) == 0 {
		return 0, ds, nil
	}

	affected, err := e.Repo.InsertFactRows(ctx, fact.Table.Name, cols, outRows, fact.DedupeColumns)
	if err != nil {
		return 0, ds, err
	}
	inserted = int(affected)

	// If a dedupe/conflict target is configured, some rows may be skipped by the
	// database (e.g. ON CONFLICT DO NOTHING). We cannot observe the exact reason
	// without backend-specific features, but we can reliably compute the delta.
	if len(fact.DedupeColumns) > 0 {
		conflict := len(outRows) - inserted
		if conflict < 0 {
			conflict = 0
		}
		ds.Conflict += conflict
	}

	return inserted, ds, nil
}

// dropStats tracks why rows were dropped before insertion (lenient mode) and why
// inserts resulted in fewer affected rows than attempted (conflict dedupe).
//
// IMPORTANT: "Conflict" is inferred as (attempted - affected) when dedupe columns
// are configured. This is backend-agnostic and works across Postgres/MSSQL/SQLite.
type dropStats struct {
	LookupMiss int
	EmptyKey   int
	Conflict   int

	// Samples contains a small number of representative drop reasons (debug only).
	Samples []string
}

func (d dropStats) TotalDropped() int {
	return d.LookupMiss + d.EmptyKey + d.Conflict
}

func (d *dropStats) AddSample(s string) {
	const maxSamples = 5
	if d == nil {
		return
	}
	if len(d.Samples) >= maxSamples {
		return
	}
	d.Samples = append(d.Samples, s)
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
	Columns       []indexedFactColumn
}

type indexedFactColumn struct {
	TargetColumn     string
	SourceFieldIndex int
	Lookup           *indexedLookup
}

type indexedLookup struct {
	Table           string
	MatchFieldIndex int
}

func buildIndexedPlan(cfg Pipeline, columns []string) (indexedPlan, error) {
	colIndex := make(map[string]int, len(columns))
	for i, c := range columns {
		colIndex[c] = i
	}

	var plan indexedPlan

	for _, t := range cfg.Storage.DB.Tables {
		switch t.Load.Kind {
		case "dimension":
			src := ""
			targetKeyCol := ""
			if len(t.Load.FromRows) > 0 {
				src = t.Load.FromRows[0].SourceField
				targetKeyCol = t.Load.FromRows[0].TargetColumn
			}

			srcIdx := -1
			if src != "" {
				if i, ok := colIndex[src]; ok {
					srcIdx = i
				}
			}

			keyCol := targetKeyCol
			valCol := ""
			if t.Load.Cache != nil {
				keyCol = t.Load.Cache.KeyColumn
				valCol = t.Load.Cache.ValueColumn
			}

			plan.Dimensions = append(plan.Dimensions, indexedDimension{
				Table:       t,
				SourceIndex: srcIdx,
				KeyColumn:   keyCol,
				ValueColumn: valCol,
			})

		case "fact":
			f := indexedFact{Table: t}

			for _, fr := range t.Load.FromRows {
				f.TargetColumns = append(f.TargetColumns, fr.TargetColumn)
				col := indexedFactColumn{
					TargetColumn:     fr.TargetColumn,
					SourceFieldIndex: -1,
				}

				if fr.Lookup != nil {
					matchField := ""
					for _, v := range fr.Lookup.Match {
						matchField = v
						break
					}
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

			plan.Facts = append(plan.Facts, f)

		default:
			return plan, fmt.Errorf("table %s: unknown load kind %q", t.Name, t.Load.Kind)
		}
	}

	sort.SliceStable(plan.Dimensions, func(i, j int) bool { return plan.Dimensions[i].Table.Name < plan.Dimensions[j].Table.Name })
	sort.SliceStable(plan.Facts, func(i, j int) bool { return plan.Facts[i].Table.Name < plan.Facts[j].Table.Name })

	return plan, nil
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
		if builtin.HasEdgeSpace(string(t)) {
			return strings.TrimSpace(string(t))
		}
		return string(t)
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
		return bytes.TrimSpace(t)
	default:
		return v
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (n int, err error) { return len(p), nil }
