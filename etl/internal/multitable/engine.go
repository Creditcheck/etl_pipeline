package multitable

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"etl/internal/config"
	"etl/internal/storage"
)

// Record is the canonical row representation used inside the multitable pipeline.
// Values are expected to already be coerced/validated by the transform layer.
type Record map[string]any

// Logger is the minimal logging interface used by Engine.
// *log.Logger satisfies this interface.
type Logger interface {
	Printf(format string, v ...any)
}

// LookupCache stores dimension key -> id mappings per table.
// It is safe for concurrent use.
type LookupCache struct {
	mu   sync.RWMutex
	data map[string]map[string]int64 // table -> normalizedKey -> id
}

// NewLookupCache returns an initialized lookup cache.
func NewLookupCache() *LookupCache {
	return &LookupCache{data: make(map[string]map[string]int64)}
}

// Get returns the cached ID for (table,key).
func (c *LookupCache) Get(table, key string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	m := c.data[table]
	if m == nil {
		return 0, false
	}
	v, ok := m[key]
	return v, ok
}

// Put stores (table,key)->id in the cache.
func (c *LookupCache) Put(table, key string, id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.data[table]
	if m == nil {
		m = make(map[string]int64)
		c.data[table] = m
	}
	m[key] = id
}

// Engine loads dimension and fact tables based on Pipeline configuration.
// It is intentionally small and testable: storage access and logging are injected.
type Engine struct {
	Repo   storage.MultiRepository
	Logger Logger
}

// Run executes a multi-table pipeline over already transformed records.
//
// Stages (timed):
//  1. Ensure tables exist (DDL)
//  2. Ensure + cache dimensions
//  3. Load facts
func (e *Engine) Run(ctx context.Context, cfg Pipeline, recs []Record) error {
	if e.Repo == nil {
		return fmt.Errorf("engine: Repo is required")
	}

	logf := e.logger()
	lenient := isLenient(cfg.Transform)

	plan, err := buildPlanFromPipeline(cfg)
	if err != nil {
		return err
	}

	// 1) DDL
	ddlStart := time.Now()
	if err := e.Repo.EnsureTables(ctx, plan.AllTables()); err != nil {
		return err
	}
	logf("stage=ddl ok duration=%s", time.Since(ddlStart).Truncate(time.Millisecond))

	cache := NewLookupCache()

	// 2) Dimensions
	dimStart := time.Now()
	for _, dim := range plan.DimensionsInOrder {
		s := time.Now()
		if err := e.ensureDimension(ctx, dim, recs, cache); err != nil {
			return err
		}
		logf("stage=dimension table=%s ok duration=%s", dim.Table.Name, time.Since(s).Truncate(time.Millisecond))
	}
	logf("stage=dimensions ok duration=%s", time.Since(dimStart).Truncate(time.Millisecond))

	// 3) Facts
	factStart := time.Now()
	for _, fact := range plan.FactsInOrder {
		s := time.Now()
		if err := e.loadFact(ctx, fact, recs, cache, lenient); err != nil {
			return err
		}
		logf("stage=fact table=%s ok duration=%s", fact.Table.Name, time.Since(s).Truncate(time.Millisecond))
	}
	logf("stage=facts ok duration=%s", time.Since(factStart).Truncate(time.Millisecond))

	return nil
}

func (e *Engine) logger() func(format string, v ...any) {
	// Avoid untestable global logging. If no logger is provided,
	// default to a logger that discards output (caller can inject one).
	if e.Logger == nil {
		l := log.New(discardWriter{}, "", 0)
		return l.Printf
	}
	return e.Logger.Printf
}

// ensureDimension ensures all dimension keys exist in the dimension table and are cached.
//
// Performance:
// If Cache.Prewarm is true, we do a *targeted prewarm* (only keys needed for this run),
// not a full-table scan. Full scans are too expensive for large dimensions and small batches.
func (e *Engine) ensureDimension(ctx context.Context, dim TablePlan, recs []Record, cache *LookupCache) error {
	logf := e.logger()

	if dim.Cache == nil {
		return fmt.Errorf("dimension %s: missing cache spec", dim.Table.Name)
	}

	keyCol := dim.Cache.KeyColumn
	valCol := dim.Cache.ValueColumn

	// Collect distinct needed keys (normalized for cache) and typed values for DB binding.
	keysNeeded, typedByKey, err := collectDistinctKeyValuesFromRecords(recs, dim.DimensionSourceField)
	if err != nil {
		return fmt.Errorf("dimension %s: collect keys: %w", dim.Table.Name, err)
	}
	if len(keysNeeded) == 0 {
		logf("stage=dimension table=%s needed_keys=0 (skipping)", dim.Table.Name)
		return nil
	}

	// Build the typed key list once (used for prewarm/select/insert).
	allKeysAny := make([]any, 0, len(keysNeeded))
	for _, k := range keysNeeded {
		allKeysAny = append(allKeysAny, typedByKey[k])
	}

	// Prewarm cache if configured: targeted prewarm for needed keys only.
	if dim.Cache.Prewarm {
		s := time.Now()
		kv, err := e.Repo.SelectKeyValueByKeys(ctx, dim.Table.Name, keyCol, valCol, allKeysAny)
		if err != nil {
			return err
		}
		for k, v := range kv {
			cache.Put(dim.Table.Name, k, v)
		}
		logf("stage=dimension_prewarm table=%s mode=targeted needed_keys=%d cached=%d duration=%s",
			dim.Table.Name, len(keysNeeded), len(kv), time.Since(s).Truncate(time.Millisecond))
	}

	// Conflict columns fall back to the cache key column.
	conflictCols := []string{keyCol}
	if dim.Table.Load.Conflict != nil && len(dim.Table.Load.Conflict.TargetColumns) > 0 {
		conflictCols = dim.Table.Load.Conflict.TargetColumns
	}

	// Insert missing keys.
	var missing []any
	for _, k := range keysNeeded {
		if _, ok := cache.Get(dim.Table.Name, k); ok {
			continue
		}
		missing = append(missing, typedByKey[k])
	}

	if len(missing) > 0 {
		s := time.Now()
		if err := e.Repo.EnsureDimensionKeys(ctx, dim.Table.Name, keyCol, missing, conflictCols); err != nil {
			return err
		}
		logf("stage=dimension_insert table=%s inserted_or_ensured=%d duration=%s",
			dim.Table.Name, len(missing), time.Since(s).Truncate(time.Millisecond))

		// Refresh only the missing keys (cheap).
		s2 := time.Now()
		kv, err := e.Repo.SelectKeyValueByKeys(ctx, dim.Table.Name, keyCol, valCol, missing)
		if err != nil {
			return err
		}
		for k, v := range kv {
			cache.Put(dim.Table.Name, k, v)
		}
		logf("stage=dimension_refresh table=%s refreshed=%d duration=%s",
			dim.Table.Name, len(kv), time.Since(s2).Truncate(time.Millisecond))
	}

	// Verify coverage after insert+select.
	for _, k := range keysNeeded {
		if _, ok := cache.Get(dim.Table.Name, k); !ok {
			return fmt.Errorf("failed to resolve %s for key=%q in %s", valCol, k, dim.Table.Name)
		}
	}

	logf("stage=dimension table=%s needed_keys=%d ok", dim.Table.Name, len(keysNeeded))
	return nil
}

// loadFact loads the fact table rows. In lenient mode, unresolved lookups drop the row.
// In strict mode, unresolved lookups fail the run.
//
// Performance note:
// For lookup columns with on_missing=insert, we batch missing keys:
//   - 1 insert (chunked in repo) + 1 select per lookup-table per fact load
//
// instead of per-row insert/select.
func (e *Engine) loadFact(ctx context.Context, fact TablePlan, recs []Record, cache *LookupCache, lenient bool) error {
	logf := e.logger()

	cols := make([]string, 0, len(fact.FactColumns))
	for _, c := range fact.FactColumns {
		cols = append(cols, c.TargetColumn)
	}

	// Phase A: batch ensure + refresh cache for lookup columns with on_missing=insert.
	type lookupKey struct {
		table        string
		matchColumn  string
		returnColumn string
	}

	// missing[lk][normalizedKey] = typedValue
	missing := make(map[lookupKey]map[string]any)

	for _, r := range recs {
		for _, o := range fact.FactColumns {
			if o.Lookup == nil || o.Lookup.OnMissing != "insert" {
				continue
			}

			v := r[o.Lookup.MatchField]
			nk := normalizeKey(v)
			if nk == "" {
				continue
			}
			if _, ok := cache.Get(o.Lookup.LookupTable, nk); ok {
				continue
			}

			lk := lookupKey{
				table:        o.Lookup.LookupTable,
				matchColumn:  o.Lookup.MatchColumn,
				returnColumn: o.Lookup.ReturnColumn,
			}
			m := missing[lk]
			if m == nil {
				m = make(map[string]any)
				missing[lk] = m
			}
			if _, exists := m[nk]; exists {
				continue
			}

			// Typed bind value (preserve strings like "000").
			m[nk] = typedBindValue(v)
		}
	}

	for lk, m := range missing {
		if len(m) == 0 {
			continue
		}
		keysAny := make([]any, 0, len(m))
		for _, v := range m {
			keysAny = append(keysAny, v)
		}

		if err := e.Repo.EnsureDimensionKeys(ctx, lk.table, lk.matchColumn, keysAny, []string{lk.matchColumn}); err != nil {
			return err
		}
		kv, err := e.Repo.SelectKeyValueByKeys(ctx, lk.table, lk.matchColumn, lk.returnColumn, keysAny)
		if err != nil {
			return err
		}
		for kk, vv := range kv {
			cache.Put(lk.table, kk, vv)
		}
	}

	// Phase B: build fact rows using cache.
	outRows := make([][]any, 0, len(recs))
	var dropped, misses int

	for _, r := range recs {
		outRow := make([]any, 0, len(fact.FactColumns))
		drop := false

		for _, o := range fact.FactColumns {
			if o.Lookup != nil {
				key := normalizeKey(r[o.Lookup.MatchField])
				id, ok := cache.Get(o.Lookup.LookupTable, key)
				if !ok {
					misses++
					if lenient {
						drop = true
						break
					}
					return fmt.Errorf("fact %s: lookup miss table=%s key=%q", fact.Table.Name, o.Lookup.LookupTable, key)
				}
				outRow = append(outRow, id)
				continue
			}

			if o.SourceField != "" {
				outRow = append(outRow, r[o.SourceField])
				continue
			}

			outRow = append(outRow, nil)
		}

		if drop {
			dropped++
			continue
		}
		outRows = append(outRows, outRow)
	}

	if len(outRows) == 0 {
		if lenient && (dropped > 0 || misses > 0) {
			logf("stage=fact table=%s dropped=%d misses=%d (no rows inserted)", fact.Table.Name, dropped, misses)
		}
		return nil
	}

	var dedupeCols []string
	if fact.Table.Load.Dedupe != nil && len(fact.Table.Load.Dedupe.ConflictColumns) > 0 {
		dedupeCols = append(dedupeCols, fact.Table.Load.Dedupe.ConflictColumns...)
	}

	_, err := e.Repo.InsertFactRows(ctx, fact.Table.Name, cols, outRows, dedupeCols)
	if err == nil && lenient && (dropped > 0 || misses > 0) {
		logf("stage=fact table=%s inserted=%d dropped=%d misses=%d", fact.Table.Name, len(outRows), dropped, misses)
	}
	return err
}

// ---- Plan types + building ----

// Plan is the minimal execution plan needed by Engine.
type Plan struct {
	DimensionsInOrder []TablePlan
	FactsInOrder      []TablePlan
}

// AllTables returns the union of dimension + fact table specs.
func (p Plan) AllTables() []storage.TableSpec {
	out := make([]storage.TableSpec, 0, len(p.DimensionsInOrder)+len(p.FactsInOrder))
	for _, t := range p.DimensionsInOrder {
		out = append(out, t.Table)
	}
	for _, t := range p.FactsInOrder {
		out = append(out, t.Table)
	}
	return out
}

// TablePlan describes either a dimension table or a fact table.
type TablePlan struct {
	Table storage.TableSpec

	// Dimension fields
	DimensionSourceField string
	Cache                *storage.CacheSpec

	// Fact fields
	FactColumns []FactColumnPlan
}

// FactColumnPlan describes how to populate one target column in the fact table.
type FactColumnPlan struct {
	TargetColumn string
	SourceField  string
	Lookup       *LookupPlan
}

// LookupPlan describes a dimension lookup for a fact column.
type LookupPlan struct {
	LookupTable  string
	MatchColumn  string
	MatchField   string
	ReturnColumn string
	OnMissing    string
}

// buildPlanFromPipeline builds Engine's plan from Pipeline config.
func buildPlanFromPipeline(cfg Pipeline) (Plan, error) {
	var plan Plan
	for _, t := range cfg.Storage.DB.Tables {
		switch t.Load.Kind {
		case "dimension":
			var src string
			if len(t.Load.FromRows) > 0 {
				src = t.Load.FromRows[0].SourceField
			}
			plan.DimensionsInOrder = append(plan.DimensionsInOrder, TablePlan{
				Table:                t,
				DimensionSourceField: src,
				Cache:                t.Load.Cache,
			})

		case "fact":
			tp := TablePlan{Table: t}
			for _, fr := range t.Load.FromRows {
				if fr.Lookup != nil {
					// Support a single match mapping.
					var matchCol, matchField string
					for k, v := range fr.Lookup.Match {
						matchCol = k
						matchField = v
						break
					}
					tp.FactColumns = append(tp.FactColumns, FactColumnPlan{
						TargetColumn: fr.TargetColumn,
						Lookup: &LookupPlan{
							LookupTable:  fr.Lookup.Table,
							MatchColumn:  matchCol,
							MatchField:   matchField,
							ReturnColumn: fr.Lookup.Return,
							OnMissing:    fr.Lookup.OnMissing,
						},
					})
				} else {
					tp.FactColumns = append(tp.FactColumns, FactColumnPlan{
						TargetColumn: fr.TargetColumn,
						SourceField:  fr.SourceField,
					})
				}
			}
			plan.FactsInOrder = append(plan.FactsInOrder, tp)

		default:
			return plan, fmt.Errorf("table %s: unknown load kind %q", t.Name, t.Load.Kind)
		}
	}
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

// collectDistinctKeyValuesFromRecords returns:
//  1. sorted normalized keys (for cache keying)
//  2. mapping normalized key -> typed value (for DB binding)
//
// This intentionally avoids guessing/parsing keys; it preserves transform output
// (e.g., "000" stays "000" when coerced as text).
func collectDistinctKeyValuesFromRecords(recs []Record, sourceField string) ([]string, map[string]any, error) {
	if sourceField == "" {
		return nil, nil, fmt.Errorf("sourceField is empty")
	}

	set := make(map[string]struct{})
	typed := make(map[string]any, len(recs))

	for _, r := range recs {
		v, ok := r[sourceField]
		if !ok || v == nil {
			continue
		}
		k := normalizeKey(v)
		if k == "" {
			continue
		}
		if _, exists := set[k]; !exists {
			set[k] = struct{}{}
			typed[k] = typedBindValue(v)
		}
	}

	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, typed, nil
}

// normalizeKey creates a stable cache key representation.
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

// typedBindValue returns a value suitable for DB binding that preserves strings.
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

// discardWriter is an io.Writer that discards all writes.
// Avoids depending on io.Discard to keep this file self-contained.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (n int, err error) { return len(p), nil }
