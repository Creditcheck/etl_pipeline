package multitable

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"etl/internal/config"
	"etl/internal/storage"
	"etl/internal/transformer"
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
func (e *Engine2Pass) loadFactsStreaming(ctx context.Context, cfg Pipeline, columns []string, plan indexedPlan, lenient bool) error {
	logf := e.logger()

	batchSize := cfg.Runtime.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	// cache[dimTable][normalizedKey] = id
	cache := make(map[string]map[string]int64, len(plan.Dimensions))

	stream, err := StreamValidatedRows(ctx, cfg, columns)
	if err != nil {
		return err
	}

	// Batch of pooled rows.
	batch := make([]*transformer.Row, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		defer func() {
			for _, r := range batch {
				r.Free()
			}
			batch = batch[:0]
		}()

		if err := e.resolveBatchLookups(ctx, plan, batch, cache); err != nil {
			return err
		}

		for _, fact := range plan.Facts {
			inserted, dropped, err := e.insertFactBatch(ctx, fact, batch, cache, lenient)
			if err != nil {
				return err
			}
			if lenient && dropped > 0 {
				logf("stage=fact_batch table=%s inserted=%d dropped=%d", fact.Table.Name, inserted, dropped)
			}
		}
		return nil
	}

	seenRows := 0
	for r := range stream.Rows {
		seenRows++
		batch = append(batch, r)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if err := flush(); err != nil {
		return err
	}
	if err := stream.Wait(); err != nil {
		return err
	}

	logf("stage=pass2_rows seen_rows=%d", seenRows)
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
) (inserted int, dropped int, _ error) {
	cols := fact.TargetColumns

	outRows := make([][]any, 0, len(batch))
	for _, r := range batch {
		rowOut := make([]any, 0, len(fact.Columns))
		drop := false

		for _, c := range fact.Columns {
			if c.Lookup != nil {
				dimTable := c.Lookup.Table
				matchIdx := c.Lookup.MatchFieldIndex
				key := normalizeKey(r.V[matchIdx])

				var (
					id int64
					ok bool
				)
				if cm := cache[dimTable]; cm != nil {
					id, ok = cm[key]
				}
				if !ok {
					if lenient {
						drop = true
						break
					}
					return inserted, dropped, fmt.Errorf("fact %s: lookup miss table=%s key=%q", fact.Table.Name, dimTable, key)
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

	// Stable order for determinism.
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
		return strings.TrimSpace(t)
	case []byte:
		return strings.TrimSpace(string(t))
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func typedBindValue(v any) any {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case []byte:
		return strings.TrimSpace(string(t))
	default:
		return v
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (n int, err error) { return len(p), nil }
