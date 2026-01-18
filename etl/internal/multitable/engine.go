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

// stream returns the validated row stream implementation.
//
// When to use:
//   - Production: uses StreamValidatedRows (file->parse->transform->validate).
//   - Tests: inject Engine2Pass.Stream to provide deterministic rows.
//
// Errors:
//   - Returns any fatal initialization error from the stream provider.
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

	// Cancellation model:
	// - Any worker error cancels derived context with a cause.
	// - Producer stops early and frees owned rows promptly.
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
			// First error wins.
		}
	}

	stream, err := e.stream(ctx, cfg, columns)
	if err != nil {
		return err
	}

	// Producer: batches of pooled rows. Ownership transfers to the consumer worker,
	// which must Free() each row exactly once.
	batchCh := make(chan []*transformer.Row, loaderWorkers*2)

	// Worker pool: per-worker cache to avoid locks.
	var wg sync.WaitGroup
	wg.Add(loaderWorkers)
	for w := 0; w < loaderWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()

			// cache[dimTable][normalizedKey] = id
			cache := make(map[string]map[string]int64, len(plan.Dimensions))

			for batch := range batchCh {
				// If canceled, drain and free quickly.
				select {
				case <-ctx.Done():
					for _, r := range batch {
						r.Free()
					}
					continue
				default:
				}

				start := time.Now()
				err := e.processFactBatch(ctx, plan, batch, cache, lenient, debug, logf)
				dur := time.Since(start).Truncate(time.Millisecond)

				// Always free owned rows.
				for _, r := range batch {
					r.Free()
				}

				if err != nil {
					setErr(err)
					if debug {
						logf("stage=pass2_batch worker=%d status=error duration=%s err=%v", workerID, dur, err)
					}
					continue
				}
				if debug {
					logf("stage=pass2_batch worker=%d status=ok duration=%s rows=%d", workerID, dur, len(batch))
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
			// Producer still owns out when send fails; free it.
			for _, r := range out {
				r.Free()
			}
		}
	}

	for r := range stream.Rows {
		select {
		case <-ctx.Done():
			// Context canceled: producer must free incoming rows.
			r.Free()
			continue
		default:
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

	// Prefer terminal stream error unless worker error already captured.
	if err := stream.Wait(); err != nil {
		// If we were canceled due to a worker error, surface that first.
		select {
		case werr := <-errCh:
			return werr
		default:
		}
		return err
	}

	select {
	case werr := <-errCh:
		return werr
	default:
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

	lookupStart := time.Now()
	if err := e.resolveBatchLookups(ctx, plan, batch, cache); err != nil {
		return err
	}
	if debug {
		logf("stage=pass2_resolve_lookups batch_rows=%d duration=%s", len(batch), time.Since(lookupStart).Truncate(time.Millisecond))
	}

	for _, fact := range plan.Facts {
		start := time.Now()
		inserted, dropped, err := e.insertFactBatch(ctx, fact, batch, cache, lenient)
		dur := time.Since(start).Truncate(time.Millisecond)

		if err != nil {
			return err
		}

		if debug {
			logf(
				"stage=pass2_insert_fact table=%s batch_rows=%d inserted=%d dropped=%d duration=%s",
				fact.Table.Name, len(batch), inserted, dropped, dur,
			)
		} else if lenient && dropped > 0 {
			logf("stage=fact_batch table=%s inserted=%d dropped=%d", fact.Table.Name, inserted, dropped)
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
	needed := make(map[string]map[string]any, len(plan.Dimensions))

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

	if len(outRows) == 0 {
		return 0, dropped, nil
	}

	affected, err := e.Repo.InsertFactRows(ctx, fact.Table.Name, cols, outRows, fact.DedupeColumns)
	if err != nil {
		return 0, dropped, err
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
	Columns       []indexedFactColumn
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

// buildIndexedPlan compiles a runtime plan for streaming execution.
//
// It converts column names into integer indices so the engine can read values from
// []*transformer.Row without per-row map allocations.
//
// Errors:
//   - Returns an error for unknown table load kinds.
//   - Does not validate storage schema correctness beyond what is needed to build indices.
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
//
// The storage schema represents lookup match as a map (db key column -> source field). :contentReference[oaicite:3]{index=3}
// Map iteration order is random, so selecting an arbitrary value would make plan
// compilation nondeterministic and brittle for tests.
//
// Behavior:
//   - If match is empty, returns "".
//   - Otherwise returns the source field corresponding to the lexicographically
//     smallest db key column.
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
//
// Hot-path rules:
//   - Avoid fmt.Sprint for common primitive types (it allocates heavily).
//   - Preserve current trim semantics for strings/[]byte.
//   - Treat nil/empty as "".
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
		// Convert once.
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
