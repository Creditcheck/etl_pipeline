package multitable

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Engine is a config-driven multi-table loader.
// It is designed so planning and row-building can be unit-tested without Postgres.
type Engine struct {
	Pool *pgxpool.Pool
}

// Run executes a multi-table pipeline in a generic, config-driven way:
//  1. Create tables (DDL)
//  2. Build plan (dimensions, lookups, caches)
//  3. Ensure dimension tables contain required keys and build caches
//  4. COPY load fact tables using lookups resolved from caches
//
// IMPORTANT: The runner is responsible for producing typed, validated records
// using the existing transformer stack (Option 1).
func (e *Engine) Run(ctx context.Context, cfg Pipeline, recs []Record, mr MultiCopyer) error {
	if e.Pool == nil {
		return fmt.Errorf("engine requires Pool")
	}
	if mr == nil {
		return fmt.Errorf("engine requires MultiCopyer")
	}
	if err := validateMulti(cfg); err != nil {
		return err
	}

	// 1) DDL
	if err := ensureTables(ctx, e.Pool, cfg.Storage.DB.Tables); err != nil {
		return err
	}

	// 2) Build plan (dimensions needed, lookups, caches)
	plan, err := BuildPlan(cfg)
	if err != nil {
		return err
	}

	// 3) Execute dimension loads + build caches
	cache := NewLookupCache()
	for _, dim := range plan.DimensionsInOrder {
		if err := e.ensureDimension(ctx, dim, recs, cache); err != nil {
			return err
		}
	}

	// 4) Execute facts
	for _, fact := range plan.FactsInOrder {
		if err := e.loadFact(ctx, fact, recs, cache, mr); err != nil {
			return err
		}
	}

	return nil
}

// -------------------------
// Plan
// -------------------------

type Plan struct {
	DimensionsInOrder []TablePlan
	FactsInOrder      []TablePlan

	// quick access by table name
	ByName map[string]TablePlan
}

type TablePlan struct {
	Table TableSpec

	// CacheSpec (optional): key -> id
	Cache *CacheSpec

	// For dimensions: which source fields are needed to produce their key column.
	// For cache-based dims we currently assume one key column.
	DimensionKeyColumn   string
	DimensionSourceField string // canonical source field name

	// For facts: list of output columns in final row order and how to produce them
	Outputs []OutputExpr
}

type OutputExpr struct {
	TargetColumn string

	// Exactly one of:
	SourceField string      // take value from record field
	Lookup      *LookupExpr // resolve from cache
}

type LookupExpr struct {
	LookupTable string // e.g. "public.vehicles"
	// Match: db key column -> source field
	LookupKeyColumn string // e.g. "pcv" or "name"
	SourceField     string // e.g. "pcv" or "stat"
	ReturnColumn    string // e.g. "vehicle_id" or "country_id" (must be int64)
	OnMissing       string // "insert" (supported)
}

// BuildPlan inspects cfg.storage.db.tables and builds a generic execution plan.
// It is pure and unit-testable.
func BuildPlan(cfg Pipeline) (Plan, error) {
	p := Plan{ByName: map[string]TablePlan{}}

	// Index tables by name
	specByName := map[string]TableSpec{}
	for _, t := range cfg.Storage.DB.Tables {
		if t.Name == "" {
			return Plan{}, fmt.Errorf("table has empty name")
		}
		specByName[t.Name] = t
	}

	// First create plans for all tables
	for _, t := range cfg.Storage.DB.Tables {
		tp := TablePlan{Table: t}

		if t.Load.Cache != nil {
			tp.Cache = t.Load.Cache
		}

		switch t.Load.Kind {
		case "dimension":
			// Minimal requirement: cache.key/value so we can resolve lookups generically.
			if tp.Cache == nil {
				return Plan{}, fmt.Errorf("dimension table %s must define load.cache (minimal engine requirement)", t.Name)
			}
			tp.DimensionKeyColumn = tp.Cache.KeyColumn

			// Determine source field for the dimension key from from_rows mapping
			src, err := findSourceFieldForTarget(t.Load.FromRows, tp.DimensionKeyColumn)
			if err != nil {
				return Plan{}, fmt.Errorf("dimension %s: %w", t.Name, err)
			}
			tp.DimensionSourceField = src

			p.DimensionsInOrder = append(p.DimensionsInOrder, tp)

		case "fact":
			outs, err := buildFactOutputs(t.Load.FromRows)
			if err != nil {
				return Plan{}, fmt.Errorf("fact %s: %w", t.Name, err)
			}
			tp.Outputs = outs
			p.FactsInOrder = append(p.FactsInOrder, tp)

		default:
			return Plan{}, fmt.Errorf("table %s has unsupported load.kind: %s", t.Name, t.Load.Kind)
		}

		p.ByName[t.Name] = tp
	}

	// Ensure required lookup tables exist in config
	requiredDims := map[string]struct{}{}
	for _, fact := range p.FactsInOrder {
		for _, out := range fact.Outputs {
			if out.Lookup != nil {
				requiredDims[out.Lookup.LookupTable] = struct{}{}
			}
		}
	}
	for name := range requiredDims {
		if _, ok := p.ByName[name]; !ok {
			return Plan{}, fmt.Errorf("fact requires lookup table %s but it is not in storage.db.tables", name)
		}
	}

	return p, nil
}

func findSourceFieldForTarget(from []FromRowSpec, target string) (string, error) {
	for _, fr := range from {
		if fr.TargetColumn != target {
			continue
		}
		if fr.SourceField == "" {
			return "", fmt.Errorf("target_column %q has no source_field", target)
		}
		return fr.SourceField, nil
	}
	return "", fmt.Errorf("no from_rows mapping found for target_column %q", target)
}

func buildFactOutputs(from []FromRowSpec) ([]OutputExpr, error) {
	var outs []OutputExpr
	for _, fr := range from {
		if fr.TargetColumn == "" {
			return nil, fmt.Errorf("from_rows entry has empty target_column")
		}
		oe := OutputExpr{TargetColumn: fr.TargetColumn}
		if fr.Lookup != nil {
			lk, err := toLookupExpr(*fr.Lookup)
			if err != nil {
				return nil, err
			}
			oe.Lookup = &lk
		} else {
			if fr.SourceField == "" {
				return nil, fmt.Errorf("target_column %q requires source_field or lookup", fr.TargetColumn)
			}
			oe.SourceField = fr.SourceField
		}
		outs = append(outs, oe)
	}
	return outs, nil
}

func toLookupExpr(l LookupSpec) (LookupExpr, error) {
	if l.Table == "" || l.Return == "" || len(l.Match) != 1 {
		return LookupExpr{}, fmt.Errorf("lookup must specify table, return and exactly one match column (minimal engine requirement)")
	}
	var keyCol, srcField string
	for k, v := range l.Match {
		keyCol, srcField = k, v
	}
	if keyCol == "" || srcField == "" {
		return LookupExpr{}, fmt.Errorf("lookup.match must map db key column -> source field")
	}
	return LookupExpr{
		LookupTable:     l.Table,
		LookupKeyColumn: keyCol,
		SourceField:     srcField,
		ReturnColumn:    l.Return,
		OnMissing:       l.OnMissing,
	}, nil
}

// -------------------------
// Record model
// -------------------------

// Record is a canonical field map after header mapping.
// Values must already be typed+validated by the runner (transformer stack).
type Record map[string]any

// -------------------------
// Lookup cache (generic)
// -------------------------

// LookupCache stores single-column key -> int64 id for any lookup table.
type LookupCache struct {
	// tableName -> keyString -> id
	m map[string]map[string]int64
}

func NewLookupCache() *LookupCache {
	return &LookupCache{m: map[string]map[string]int64{}}
}

func (c *LookupCache) Get(table, key string) (int64, bool) {
	tm, ok := c.m[table]
	if !ok {
		return 0, false
	}
	v, ok := tm[key]
	return v, ok
}

func (c *LookupCache) Put(table, key string, id int64) {
	tm, ok := c.m[table]
	if !ok {
		tm = map[string]int64{}
		c.m[table] = tm
	}
	tm[key] = id
}

// -------------------------
// Dimension ensuring (generic)
// -------------------------

func (e *Engine) ensureDimension(ctx context.Context, dim TablePlan, recs []Record, cache *LookupCache) error {
	if dim.Cache == nil {
		return fmt.Errorf("dimension %s has no cache spec", dim.Table.Name)
	}
	keyCol := dim.Cache.KeyColumn
	valCol := dim.Cache.ValueColumn

	// 1) Prewarm cache if requested
	if dim.Cache.Prewarm {
		kv, err := e.selectAllKeyValue(ctx, dim.Table.Name, keyCol, valCol)
		if err != nil {
			return err
		}
		for k, v := range kv {
			cache.Put(dim.Table.Name, k, v)
		}
	}

	// 2) Collect required keys from records
	keysNeeded, err := collectDistinctKeysFromRecords(recs, dim.DimensionSourceField)
	if err != nil {
		return fmt.Errorf("collect keys for %s: %w", dim.Table.Name, err)
	}

	// 3) Insert missing keys
	var missing []any
	for _, k := range keysNeeded {
		if _, ok := cache.Get(dim.Table.Name, k); !ok {
			missing = append(missing, parseKeyAny(k))
		}
	}
	if len(missing) > 0 {
		if err := e.insertDimensionKeys(ctx, dim.Table, keyCol, missing); err != nil {
			return err
		}
	}

	// 4) Fetch ids for all keys and populate cache
	kv, err := e.selectKeyValueByKeys(ctx, dim.Table.Name, keyCol, valCol, keysNeeded)
	if err != nil {
		return err
	}
	for k, v := range kv {
		cache.Put(dim.Table.Name, k, v)
	}

	// 5) Validate completeness
	for _, k := range keysNeeded {
		if _, ok := cache.Get(dim.Table.Name, k); !ok {
			return fmt.Errorf("failed to resolve %s for key=%q in %s", valCol, k, dim.Table.Name)
		}
	}

	return nil
}

func collectDistinctKeysFromRecords(recs []Record, sourceField string) ([]string, error) {
	if sourceField == "" {
		return nil, fmt.Errorf("sourceField is empty")
	}
	set := map[string]struct{}{}
	for _, r := range recs {
		v, ok := r[sourceField]
		if !ok || v == nil {
			continue
		}
		k := normalizeKey(v)
		if k == "" {
			continue
		}
		set[k] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

func normalizeKey(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case int64:
		return fmt.Sprintf("%d", t)
	case int:
		return fmt.Sprintf("%d", t)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

// parseKeyAny keeps numeric keys numeric for insert if possible.
func parseKeyAny(key string) any {
	if key == "" {
		return ""
	}
	n, err := parseInt64(key)
	if err == nil {
		return n
	}
	return key
}

func (e *Engine) insertDimensionKeys(ctx context.Context, table TableSpec, keyCol string, keys []any) error {
	if len(keys) == 0 {
		return nil
	}

	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table.Name)
	b.WriteString(" (")
	b.WriteString(pgIdent(keyCol))
	b.WriteString(") VALUES ")

	args := make([]any, 0, len(keys))
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("($%d)", i+1))
		args = append(args, k)
	}

	conflictCols := []string{keyCol}
	if table.Load.Conflict != nil && len(table.Load.Conflict.TargetColumns) > 0 {
		conflictCols = table.Load.Conflict.TargetColumns
	}

	b.WriteString(" ON CONFLICT (")
	for i, c := range conflictCols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(pgIdent(c))
	}
	b.WriteString(") DO NOTHING;")

	if _, err := e.Pool.Exec(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("insert dimension keys into %s: %w", table.Name, err)
	}
	return nil
}

func (e *Engine) selectAllKeyValue(ctx context.Context, table, keyCol, valCol string) (map[string]int64, error) {
	q := fmt.Sprintf(`SELECT %s, %s FROM %s`, pgIdent(keyCol), pgIdent(valCol), table)
	rows, err := e.Pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("select prewarm %s: %w", table, err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var key any
		var id int64
		if err := rows.Scan(&key, &id); err != nil {
			return nil, fmt.Errorf("prewarm scan %s: %w", table, err)
		}
		out[normalizeKey(key)] = id
	}
	return out, rows.Err()
}

func (e *Engine) selectKeyValueByKeys(ctx context.Context, table, keyCol, valCol string, keys []string) (map[string]int64, error) {
	if len(keys) == 0 {
		return map[string]int64{}, nil
	}

	// If all keys parse as int64, query with []int64; else query with []string.
	allNumeric := true
	ints := make([]int64, 0, len(keys))
	for _, k := range keys {
		n, err := parseInt64(k)
		if err != nil {
			allNumeric = false
			break
		}
		ints = append(ints, n)
	}

	var (
		rows any
		err  error
	)

	q := fmt.Sprintf(`SELECT %s, %s FROM %s WHERE %s = ANY($1)`,
		pgIdent(keyCol), pgIdent(valCol), table, pgIdent(keyCol),
	)

	if allNumeric {
		r, e2 := e.Pool.Query(ctx, q, ints)
		rows, err = r, e2
	} else {
		r, e2 := e.Pool.Query(ctx, q, keys)
		rows, err = r, e2
	}
	if err != nil {
		return nil, fmt.Errorf("select %s by keys: %w", table, err)
	}

	pgxRows, ok := rows.(interface {
		Next() bool
		Scan(...any) error
		Close()
		Err() error
	})
	if !ok {
		return nil, errors.New("internal: unexpected rows type")
	}
	defer pgxRows.Close()

	out := map[string]int64{}
	for pgxRows.Next() {
		var key any
		var id int64
		if err := pgxRows.Scan(&key, &id); err != nil {
			return nil, fmt.Errorf("select scan %s: %w", table, err)
		}
		out[normalizeKey(key)] = id
	}
	return out, pgxRows.Err()
}

func parseInt64(s string) (int64, error) {
	var neg bool
	if strings.HasPrefix(s, "-") {
		neg = true
		s = strings.TrimPrefix(s, "-")
	}
	if s == "" {
		return 0, fmt.Errorf("empty int")
	}
	var n int64
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid digit %q", ch)
		}
		n = n*10 + int64(ch-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}

// -------------------------
// Fact load (generic)
// -------------------------

// MultiCopyer is the minimal interface the Engine needs to COPY into any table.
// It matches your earlier MultiRepository.CopyFromTable.
type MultiCopyer interface {
	CopyFromTable(ctx context.Context, table string, columns []string, rows [][]any) (int64, error)
}

func (e *Engine) loadFact(ctx context.Context, fact TablePlan, recs []Record, cache *LookupCache, mr MultiCopyer) error {
	if fact.Table.Load.Kind != "fact" {
		return fmt.Errorf("table %s is not fact", fact.Table.Name)
	}

	columns := make([]string, 0, len(fact.Outputs))
	for _, o := range fact.Outputs {
		columns = append(columns, o.TargetColumn)
	}

	rows := make([][]any, 0, len(recs))
	for _, r := range recs {
		outRow := make([]any, 0, len(fact.Outputs))
		for _, o := range fact.Outputs {
			if o.Lookup != nil {
				keyVal, ok := r[o.Lookup.SourceField]
				if !ok || keyVal == nil {
					return fmt.Errorf("fact %s: missing lookup source field %q", fact.Table.Name, o.Lookup.SourceField)
				}
				key := normalizeKey(keyVal)

				id, ok := cache.Get(o.Lookup.LookupTable, key)
				if !ok {
					return fmt.Errorf("fact %s: lookup miss table=%s key=%q (ensure dimension first)", fact.Table.Name, o.Lookup.LookupTable, key)
				}
				outRow = append(outRow, id)
			} else {
				outRow = append(outRow, r[o.SourceField])
			}
		}
		rows = append(rows, outRow)
	}

	// If no dedupe requested, keep current behavior (fast append).
	if fact.Table.Load.Dedupe == nil {
		_, err := mr.CopyFromTable(ctx, fact.Table.Name, columns, rows)
		if err != nil {
			return fmt.Errorf("copy fact %s: %w", fact.Table.Name, err)
		}
		return nil
	}

	// Dedupe requested: COPY into temp staging, then INSERT ... ON CONFLICT ...
	d := fact.Table.Load.Dedupe
	if len(d.ConflictColumns) == 0 {
		return fmt.Errorf("fact %s: dedupe.conflict_columns is empty", fact.Table.Name)
	}
	if d.Action != "do_nothing" {
		return fmt.Errorf("fact %s: unsupported dedupe.action=%q (supported: do_nothing)", fact.Table.Name, d.Action)
	}

	// IMPORTANT: ON CONFLICT requires a UNIQUE/PK constraint that matches conflict_columns.
	// Also note: UNIQUE constraints do NOT treat NULLs as equal in Postgres.
	//
	// Use a single transaction with a TEMP table so reruns are idempotent.
	conn, err := e.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("fact %s: acquire conn: %w", fact.Table.Name, err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("fact %s: begin: %w", fact.Table.Name, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tmp := tempTableNameFor(fact.Table.Name)

	// Create temp table with the same shape as target table.
	// We do NOT include constraints on the temp table.
	createTmp := fmt.Sprintf(`CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS)`, pgIdent(tmp), fact.Table.Name)
	if _, err := tx.Exec(ctx, createTmp); err != nil {
		return fmt.Errorf("fact %s: create temp table: %w", fact.Table.Name, err)
	}

	// COPY into temp table
	// Use pgx.Tx.CopyFrom to keep it in the same transaction/connection.
	ident, err := pgxIdentFromQualified(tmp)
	if err != nil {
		return fmt.Errorf("fact %s: temp ident: %w", fact.Table.Name, err)
	}
	pgxCols := make([]string, 0, len(columns))
	for _, c := range columns {
		pgxCols = append(pgxCols, c)
	}
	if _, err := tx.CopyFrom(ctx, ident, pgxCols, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("fact %s: copy into temp: %w", fact.Table.Name, err)
	}

	// INSERT into target with ON CONFLICT DO NOTHING
	ins := buildInsertOnConflictDoNothingSQL(fact.Table.Name, tmp, columns, d.ConflictColumns)
	if _, err := tx.Exec(ctx, ins); err != nil {
		return fmt.Errorf("fact %s: upsert insert: %w", fact.Table.Name, err)
	}

	// Drop temp table explicitly (TEMP tables are session-scoped; explicit drop avoids surprises).
	if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP TABLE %s`, pgIdent(tmp))); err != nil {
		return fmt.Errorf("fact %s: drop temp: %w", fact.Table.Name, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("fact %s: commit: %w", fact.Table.Name, err)
	}

	return nil
}

// -------------------------
// DDL (kept minimal)
// -------------------------

func ensureTables(ctx context.Context, pool *pgxpool.Pool, tables []TableSpec) error {
	for _, t := range tables {
		if !t.AutoCreateTable {
			continue
		}
		sql, err := buildCreateTableSQL(t)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("create table %s: %w", t.Name, err)
		}
	}
	return nil
}

func buildCreateTableSQL(t TableSpec) (string, error) {
	if t.Name == "" {
		return "", fmt.Errorf("table name is empty")
	}
	var parts []string

	if t.PrimaryKey != nil {
		pkType := strings.TrimSpace(t.PrimaryKey.Type)
		if pkType == "" {
			return "", fmt.Errorf("%s primary_key.type is empty", t.Name)
		}
		parts = append(parts, fmt.Sprintf("%s %s PRIMARY KEY", pgIdent(t.PrimaryKey.Name), pkType))
	}

	for _, c := range t.Columns {
		if c.Name == "" || c.Type == "" {
			return "", fmt.Errorf("%s has column with empty name/type", t.Name)
		}
		col := fmt.Sprintf("%s %s", pgIdent(c.Name), c.Type)

		nullable := true
		if c.Nullable != nil {
			nullable = *c.Nullable
		}
		if !nullable {
			col += " NOT NULL"
		}
		if c.References != "" {
			col += " REFERENCES " + c.References
		}
		parts = append(parts, col)
	}

	for _, con := range t.Constraints {
		switch con.Kind {
		case "unique":
			if len(con.Columns) == 0 {
				return "", fmt.Errorf("%s unique constraint has no columns", t.Name)
			}
			cols := make([]string, 0, len(con.Columns))
			for _, c := range con.Columns {
				cols = append(cols, pgIdent(c))
			}
			parts = append(parts, fmt.Sprintf("UNIQUE (%s)", strings.Join(cols, ", ")))
		default:
			return "", fmt.Errorf("%s unsupported constraint kind: %s", t.Name, con.Kind)
		}
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n);", t.Name, strings.Join(parts, ",\n  ")), nil
}

func pgIdent(id string) string {
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

// -------------------------
// Validation
// -------------------------

func validateMulti(cfg Pipeline) error {
	// Engine only needs storage/db tables at this point.
	if cfg.Storage.Kind == "" {
		return fmt.Errorf("storage.kind must not be empty")
	}
	if cfg.Storage.Kind != "postgres" {
		return fmt.Errorf("storage.kind must be postgres")
	}
	if cfg.Storage.DB.Mode != "multi_table" {
		return fmt.Errorf("storage.db.mode must be multi_table")
	}
	if len(cfg.Storage.DB.Tables) == 0 {
		return fmt.Errorf("storage.db.tables must not be empty")
	}
	return nil
}

// tempTableNameFor creates a safe unqualified temp table name.
func tempTableNameFor(qualifiedTarget string) string {
	// "public.imports" -> "tmp_imports"
	base := qualifiedTarget
	if i := strings.LastIndex(base, "."); i >= 0 {
		base = base[i+1:]
	}
	base = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, base)
	return "tmp_" + strings.ToLower(base)
}

func buildInsertOnConflictDoNothingSQL(targetTable, tmpTable string, cols []string, conflictCols []string) string {
	colList := make([]string, 0, len(cols))
	selList := make([]string, 0, len(cols))
	for _, c := range cols {
		colList = append(colList, pgIdent(c))
		selList = append(selList, pgIdent(c))
	}

	confList := make([]string, 0, len(conflictCols))
	for _, c := range conflictCols {
		confList = append(confList, pgIdent(c))
	}

	return fmt.Sprintf(
		`INSERT INTO %s (%s)
SELECT %s FROM %s
ON CONFLICT (%s) DO NOTHING`,
		targetTable,
		strings.Join(colList, ", "),
		strings.Join(selList, ", "),
		pgIdent(tmpTable),
		strings.Join(confList, ", "),
	)
}

// pgxIdentFromQualified parses "public.imports" or "tmp_imports" into pgx.Identifier.
func pgxIdentFromQualified(name string) (pgx.Identifier, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("empty table name")
	}
	if strings.Contains(name, `"`) {
		return nil, fmt.Errorf("quoted identifiers not supported here: %q", name)
	}
	parts := strings.Split(name, ".")
	if len(parts) == 1 {
		return pgx.Identifier{parts[0]}, nil
	}
	if len(parts) == 2 {
		return pgx.Identifier{parts[0], parts[1]}, nil
	}
	return nil, fmt.Errorf("invalid table name %q", name)
}
