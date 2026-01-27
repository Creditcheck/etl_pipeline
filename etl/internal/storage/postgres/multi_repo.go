package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"etl/internal/storage"
)

/*
MultiRepo implements storage.MultiRepository for Postgres.

It provides:
  - Fact inserts
  - Dimension helpers
  - Fully transactional SCD2 support using SELECT ... FOR UPDATE

History behavior matches MSSQL and SQLite implementations.
*/
type MultiRepo struct {
	pool *pgxpool.Pool
}

// NewMulti creates a new Postgres-backed MultiRepo.
func NewMulti(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		return nil, err
	}
	return &MultiRepo{pool: pool}, nil
}

// Close closes the connection pool.
func (r *MultiRepo) Close() {
	r.pool.Close()
}

// InsertFactRows inserts fact rows with optional SCD2 semantics.
func (r *MultiRepo) InsertFactRows(
	ctx context.Context,
	spec storage.TableSpec,
	table string,
	columns []string,
	rows [][]any,
	dedupeColumns []string,
) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	if spec.Load.Kind != "fact" ||
		spec.Load.History == nil ||
		!spec.Load.History.Enabled {

		// Non-SCD2 fact loads still require idempotency.
		//
		// The pipeline supports "dedupe.conflict_columns" for fact tables. That
		// must translate to INSERT ... ON CONFLICT (...) DO NOTHING in Postgres.
		// Without it, duplicate rows in the same input (or in reprocessing) cause
		// unique constraint violations and fail the run.
		return r.insertPlain(ctx, table, columns, rows, dedupeColumns)
	}

	return r.insertSCD2(ctx, spec, table, columns, rows)
}

// insertPlain performs a bulk INSERT.
//
// If dedupeColumns is non-empty, the INSERT is made idempotent using:
//
//	ON CONFLICT (<dedupeColumns...>) DO NOTHING
//
// This is intentionally conservative: the engine currently passes only the
// conflict columns, not a full "action" enum, so Postgres implements the only
// action supported by config today: "do_nothing".
func (r *MultiRepo) insertPlain(
	ctx context.Context,
	table string,
	columns []string,
	rows [][]any,
	dedupeColumns []string,

) (int64, error) {
	sql, args := buildInsertSQL(table, columns, rows, dedupeColumns)

	cmd, err := r.pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return cmd.RowsAffected(), nil
}

// buildInsertSQL constructs a single INSERT statement and its args for Postgres.
//
// Why this exists:
//   - It is pure and deterministic, so we can unit test correctness (especially
//     ON CONFLICT behavior and placeholder numbering) without a database.
//
// Constraints:
//   - rows must have the same length as columns for every row.
//   - columns must be non-empty.
func buildInsertSQL(table string, columns []string, rows [][]any, dedupeColumns []string) (string, []any) {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(pgIdent(c))
	}
	b.WriteString(") VALUES ")

	args := make([]any, 0, len(rows)*len(columns))
	p := 1
	for i, row := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j := range columns {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fmt.Sprintf("$%d", p))
			args = append(args, row[j])
			p++
		}
		b.WriteString(")")
	}

	// If the caller requested dedupe behavior, enforce it at the SQL layer.
	//
	// This makes the pipeline:
	//   - tolerant of duplicate rows within the same batch
	//   - idempotent across reprocessing the same file/batch
	if len(dedupeColumns) > 0 {
		b.WriteString(" ON CONFLICT (")
		for i, c := range dedupeColumns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(pgIdent(c))
		}
		b.WriteString(") DO NOTHING")
	}

	b.WriteString(";")
	return b.String(), args
}

// insertSCD2 performs SCD2 merging row-by-row inside a transaction.
func (r *MultiRepo) insertSCD2(
	ctx context.Context,
	spec storage.TableSpec,
	table string,
	columns []string,
	rows [][]any,
) (int64, error) {
	h := spec.Load.History
	now := time.Now().UTC()
	total := int64(0)

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	colIdx := indexColumns(columns)
	keyIdx := indicesFor(h.BusinessKey, colIdx)

	// row_hash is an optional but strongly recommended optimization for SCD2.
	// If present, it lets us detect changes using a single field compare
	// instead of comparing every column value.
	rowHashIdx, hasRowHash := indexOfColumn(columns, "row_hash")

	for _, row := range rows {
		curr, validFrom, err := r.fetchCurrentRowTx(
			ctx, tx, table, h, columns, keyIdx, row,
		)
		if err != nil {
			return total, err
		}

		if curr == nil {
			if err := r.insertCurrentRowTx(
				ctx, tx, table, columns, row, h, now,
			); err != nil {
				return total, err
			}
			total++
			continue
		}

		// Change detection:
		//
		// If a row_hash column exists, we treat it as the authoritative
		// "did anything change?" signal. This is the standard SCD2 pattern:
		//   - BusinessKey identifies the entity (one current row).
		//   - row_hash summarizes the full content of the entity row.
		//
		// Why we don't always use rowsEqual():
		//   - It's O(N) per row across all columns.
		//   - It is brittle across type round-trips (e.g. TEXT scanned as []byte).
		//
		// IMPORTANT:
		//   - When row_hash exists, it must be computed deterministically
		//     upstream (transformer "hash") and written into the fact row.
		changed := true
		if hasRowHash {
			changed = !equalScalar(curr[rowHashIdx], row[rowHashIdx])
		} else {
			changed = !rowsEqual(curr, row)
		}
		if !changed {
			continue
		}

		if err := r.insertHistoryRowTx(
			ctx, tx, table, columns, curr, validFrom, h, now,
		); err != nil {
			return total, err
		}

		if err := r.updateCurrentRowTx(
			ctx, tx, table, columns, row, h, keyIdx, now,
		); err != nil {
			return total, err
		}

		total++
	}

	if err := tx.Commit(ctx); err != nil {
		return total, err
	}
	return total, nil
}

// fetchCurrentRowTx fetches the current version for a business key.
func (r *MultiRepo) fetchCurrentRowTx(
	ctx context.Context,
	tx pgx.Tx,
	table string,
	h *storage.HistorySpec,
	columns []string,
	keyIdx []int,
	row []any,
) ([]any, time.Time, error) {
	var b strings.Builder
	b.WriteString("SELECT ")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(pgIdent(c))
	}
	b.WriteString(", ")
	b.WriteString(pgIdent(h.ValidFromColumn))
	b.WriteString(" FROM ")
	b.WriteString(table)
	b.WriteString(" WHERE ")
	b.WriteString(pgIdent(h.ValidToColumn))
	b.WriteString(" IS NULL AND ")

	args := make([]any, 0, len(keyIdx))
	p := 1
	for i, idx := range keyIdx {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(pgIdent(h.BusinessKey[i]))
		b.WriteString(fmt.Sprintf(" = $%d", p))
		args = append(args, row[idx])
		p++
	}
	b.WriteString(" FOR UPDATE")

	rows, err := tx.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, time.Time{}, nil
	}

	// IMPORTANT: pgx requires that Scan destinations are pointers.
	//
	// The previous implementation incorrectly passed a slice of `any` values
	// (all nil interfaces) directly to Scan. Since those are not pointers,
	// pgx cannot populate them reliably, and the returned row can contain nils.
	//
	// That bug is catastrophic for SCD2:
	//   1) We read the current row, but values (e.g. vehicle_id) remain nil.
	//   2) We then INSERT the "current" row into the history table.
	//   3) The history insert fails with NOT NULL violations (vehicle_id, etc.).
	//
	// Fix:
	//   - Allocate a values slice (out)
	//   - Build a parallel destinations slice containing &out[i]
	//   - Scan into those pointers
	//
	// This is the standard pgx pattern for scanning a dynamic column list.
	out := make([]any, len(columns))
	dests := make([]any, len(columns))
	for i := range out {
		dests[i] = &out[i]
	}

	var validFrom time.Time

	// Append the SCD2 valid_from value after the fact columns.
	if err := rows.Scan(append(dests, &validFrom)...); err != nil {
		return nil, time.Time{}, err
	}
	return out, validFrom, nil
}

// insertHistoryRowTx inserts a historical version.
func (r *MultiRepo) insertHistoryRowTx(
	ctx context.Context,
	tx pgx.Tx,
	table string,
	columns []string,
	curr []any,
	oldValidFrom time.Time,
	h *storage.HistorySpec,
	now time.Time,
) error {
	hist := table + "_history"

	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(hist)
	b.WriteString(" (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(pgIdent(c))
	}
	b.WriteString(", ")
	b.WriteString(pgIdent(h.ValidFromColumn))
	b.WriteString(", ")
	b.WriteString(pgIdent(h.ValidToColumn))
	b.WriteString(", ")
	b.WriteString(pgIdent(h.ChangedAtColumn))
	b.WriteString(") VALUES (")

	args := make([]any, 0, len(curr)+3)
	p := 1
	for _, v := range curr {
		b.WriteString(fmt.Sprintf("$%d, ", p))
		args = append(args, v)
		p++
	}
	b.WriteString(fmt.Sprintf("$%d, $%d, $%d)", p, p+1, p+2))
	args = append(args, oldValidFrom, now, now)

	_, err := tx.Exec(ctx, b.String(), args...)
	return err
}

// updateCurrentRowTx updates the active version in place.
func (r *MultiRepo) updateCurrentRowTx(
	ctx context.Context,
	tx pgx.Tx,
	table string,
	columns []string,
	row []any,
	h *storage.HistorySpec,
	keyIdx []int,
	now time.Time,
) error {
	var b strings.Builder
	b.WriteString("UPDATE ")
	b.WriteString(table)
	b.WriteString(" SET ")

	args := make([]any, 0, len(columns)+1)
	p := 1
	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(pgIdent(c))
		b.WriteString(fmt.Sprintf(" = $%d", p))
		args = append(args, row[i])
		p++
	}

	b.WriteString(", ")
	b.WriteString(pgIdent(h.ValidFromColumn))
	b.WriteString(fmt.Sprintf(" = $%d", p))
	args = append(args, now)
	p++

	b.WriteString(", ")
	b.WriteString(pgIdent(h.ValidToColumn))
	b.WriteString(" = NULL")

	b.WriteString(", ")
	b.WriteString(pgIdent(h.ChangedAtColumn))
	b.WriteString(fmt.Sprintf(" = $%d", p))
	args = append(args, now)
	p++

	b.WriteString(" WHERE ")
	b.WriteString(pgIdent(h.ValidToColumn))
	b.WriteString(" IS NULL AND ")

	for i, idx := range keyIdx {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(pgIdent(h.BusinessKey[i]))
		b.WriteString(fmt.Sprintf(" = $%d", p))
		args = append(args, row[idx])
		p++
	}

	_, err := tx.Exec(ctx, b.String(), args...)
	return err
}

/* ---------- helpers ---------- */

//type pgxTx interface {
//	Query(context.Context, string, ...any) (pgxRows, error)
//	Exec(context.Context, string, ...any) (pgxCmdTag, error)
//}

type pgxRows interface {
	Close()
	Next() bool
	Scan(...any) error
}

//type pgxCmdTag interface {
//	RowsAffected() int64
//}

func indexColumns(cols []string) map[string]int {
	m := make(map[string]int, len(cols))
	for i, c := range cols {
		m[c] = i
	}
	return m
}

func indicesFor(keys []string, idx map[string]int) []int {
	out := make([]int, len(keys))
	for i, k := range keys {
		out[i] = idx[k]
	}
	return out
}

func rowsEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if fmt.Sprint(a[i]) != fmt.Sprint(b[i]) {
			return false
		}
	}
	return true
}

// EnsureTables creates base and history tables when AutoCreateTable is enabled.
//
// For SCD2-enabled fact tables, this creates:
//   - base table with metadata columns (valid_from, valid_to, changed_at)
//   - history table <table>_history with same columns
//
// This method is idempotent.
func (r *MultiRepo) EnsureTables(ctx context.Context, tables []storage.TableSpec) error {
	for _, t := range tables {
		if !t.AutoCreateTable {
			continue
		}
		// NOTE: Table creation must *not* depend on SCD2 being enabled.
		//
		// Regression background:
		//   The original SCD2 implementation introduced buildSCD2CreateSQL which
		//   returned empty strings when history was disabled. EnsureTables then
		//   executed nothing for the common case (history disabled), causing the
		//   pipeline to proceed to inserts against non-existent tables.
		//
		// Fix:
		//   Always build and execute DDL for the base table. Optionally add a
		//   history table when History.Enabled.
		schemaSQL, baseSQL, histSQL, err := buildCreateSQL(t)
		if err != nil {
			return err
		}
		if schemaSQL != "" {
			if _, err := r.pool.Exec(ctx, schemaSQL); err != nil {
				return fmt.Errorf("create schema for %s: %w", t.Name, err)
			}
		}

		if baseSQL != "" {
			if _, err := r.pool.Exec(ctx, baseSQL); err != nil {
				return fmt.Errorf("create base table %s: %w", t.Name, err)
			}
		}
		if histSQL != "" {
			if _, err := r.pool.Exec(ctx, histSQL); err != nil {
				return fmt.Errorf("create history table %s_history: %w", t.Name, err)
			}
		}
	}
	return nil
}

// insertCurrentRowTx inserts a new *current* version of a fact row under SCD2.
//
// This is used when History is enabled for a fact table and there is no existing
// current row for the incoming business key.
//
// The inserted row becomes the current version by setting:
//   - valid_from = now
//   - valid_to   = NULL
//   - changed_at = now
//
// Parameters:
//   - tx must be an open pgx transaction that the caller controls.
//   - table is the base table name (not the "_history" table).
//   - columns are the non-metadata fact columns provided by the engine (must align with row).
//   - row contains values aligned with columns.
//   - h supplies the history metadata column names.
//   - now is the timestamp used for valid_from/changed_at.
//
// Requirements:
//   - The base table must contain the metadata columns named in h
//     (ValidFromColumn, ValidToColumn, ChangedAtColumn).
func (r *MultiRepo) insertCurrentRowTx(
	ctx context.Context,
	tx pgx.Tx,
	table string,
	columns []string,
	row []any,
	h *storage.HistorySpec,
	now time.Time,
) error {
	if table == "" {
		return fmt.Errorf("insertCurrentRowTx: table is empty")
	}
	if h == nil {
		return fmt.Errorf("insertCurrentRowTx: history spec is nil")
	}
	if len(columns) == 0 {
		return fmt.Errorf("insertCurrentRowTx: columns is empty")
	}
	if len(row) < len(columns) {
		return fmt.Errorf("insertCurrentRowTx: row has %d values, want at least %d", len(row), len(columns))
	}

	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (")

	// business/value columns
	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(pgIdent(c))
	}

	// metadata columns
	b.WriteString(", ")
	b.WriteString(pgIdent(h.ValidFromColumn))
	b.WriteString(", ")
	b.WriteString(pgIdent(h.ValidToColumn))
	b.WriteString(", ")
	b.WriteString(pgIdent(h.ChangedAtColumn))
	b.WriteString(") VALUES (")

	args := make([]any, 0, len(columns)+3)
	p := 1

	for i := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("$%d", p))
		args = append(args, row[i])
		p++
	}

	// valid_from
	b.WriteString(", ")
	b.WriteString(fmt.Sprintf("$%d", p))
	args = append(args, now)
	p++

	// valid_to (NULL)
	b.WriteString(", NULL")

	// changed_at
	b.WriteString(", ")
	b.WriteString(fmt.Sprintf("$%d", p))
	args = append(args, now)

	b.WriteString(")")

	_, err := tx.Exec(ctx, b.String(), args...)
	return err
}

// EnsureDimensionKeys inserts missing dimension keys into a dimension table.
//
// The function is idempotent: it uses Postgres ON CONFLICT DO NOTHING.
// Keys are inserted in chunks to stay well below Postgres's parameter limit.
//
// Parameters:
//   - table: destination table name (optionally schema-qualified if your code supports it).
//   - keyColumn: the logical key column (e.g. "customer_id").
//   - keys: list of keys to ensure exist.
//   - conflictColumns: columns forming the conflict target. If empty, defaults to [keyColumn].
func (r *MultiRepo) EnsureDimensionKeys(
	ctx context.Context,
	table string,
	keyColumn string,
	keys []any,
	conflictColumns []string,
) error {
	if len(keys) == 0 {
		return nil
	}
	if table == "" || keyColumn == "" {
		return fmt.Errorf("EnsureDimensionKeys: table and keyColumn are required")
	}
	if len(conflictColumns) == 0 {
		conflictColumns = []string{keyColumn}
	}

	// Conservative chunk size to keep SQL small and avoid extreme parameter counts.
	// 10k keys => 10k parameters; still safe in most cases, but we keep it smaller.
	const chunk = 2000

	for start := 0; start < len(keys); start += chunk {
		end := start + chunk
		if end > len(keys) {
			end = len(keys)
		}
		part := keys[start:end]
		if len(part) == 0 {
			continue
		}

		var b strings.Builder
		b.WriteString("INSERT INTO ")
		b.WriteString(table)
		b.WriteString(" (")
		b.WriteString(pgIdent(keyColumn))
		b.WriteString(") VALUES ")

		args := make([]any, 0, len(part))
		for i, k := range part {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fmt.Sprintf("($%d)", i+1))
			args = append(args, k)
		}

		b.WriteString(" ON CONFLICT (")
		for i, c := range conflictColumns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(pgIdent(c))
		}
		b.WriteString(") DO NOTHING")

		if _, err := r.pool.Exec(ctx, b.String(), args...); err != nil {
			return fmt.Errorf("EnsureDimensionKeys: insert into %s: %w", table, err)
		}
	}

	return nil
}

// SelectAllKeyValue returns a mapping from normalized key -> surrogate id for the whole dimension table.
//
// The returned map key is storage.NormalizeKey(original_key_value) so callers can
// reliably match string/int/etc key inputs.
func (r *MultiRepo) SelectAllKeyValue(
	ctx context.Context,
	table string,
	keyColumn string,
	valueColumn string,
) (map[string]int64, error) {
	if table == "" || keyColumn == "" || valueColumn == "" {
		return nil, fmt.Errorf("SelectAllKeyValue: table, keyColumn, valueColumn are required")
	}

	q := fmt.Sprintf(
		`SELECT %s, %s FROM %s`,
		pgIdent(keyColumn),
		pgIdent(valueColumn),
		table,
	)

	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("SelectAllKeyValue: query %s: %w", table, err)
	}
	defer rows.Close()

	out := make(map[string]int64)
	for rows.Next() {
		var k any
		var id int64
		if err := rows.Scan(&k, &id); err != nil {
			return nil, fmt.Errorf("SelectAllKeyValue: scan %s: %w", table, err)
		}
		out[storage.NormalizeKey(k)] = id
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("SelectAllKeyValue: rows %s: %w", table, err)
	}
	return out, nil
}

// SelectKeyValueByKeys returns a mapping from normalized key -> surrogate id for a set of keys.
//
// This uses a parameterized IN (...) list (chunked) instead of ANY($1) arrays to avoid
// driver array-typing edge cases and to avoid needing type-classification helpers.
func (r *MultiRepo) SelectKeyValueByKeys(
	ctx context.Context,
	table string,
	keyColumn string,
	valueColumn string,
	keys []any,
) (map[string]int64, error) {
	if len(keys) == 0 {
		return map[string]int64{}, nil
	}
	if table == "" || keyColumn == "" || valueColumn == "" {
		return nil, fmt.Errorf("SelectKeyValueByKeys: table, keyColumn, valueColumn are required")
	}

	const chunk = 2000
	out := make(map[string]int64, len(keys))

	for start := 0; start < len(keys); start += chunk {
		end := start + chunk
		if end > len(keys) {
			end = len(keys)
		}
		part := keys[start:end]
		if len(part) == 0 {
			continue
		}

		var b strings.Builder
		b.WriteString("SELECT ")
		b.WriteString(pgIdent(keyColumn))
		b.WriteString(", ")
		b.WriteString(pgIdent(valueColumn))
		b.WriteString(" FROM ")
		b.WriteString(table)
		b.WriteString(" WHERE ")
		b.WriteString(pgIdent(keyColumn))
		b.WriteString(" IN (")

		args := make([]any, 0, len(part))
		for i, k := range part {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fmt.Sprintf("$%d", i+1))
			args = append(args, k)
		}
		b.WriteString(")")

		rows, err := r.pool.Query(ctx, b.String(), args...)
		if err != nil {
			return nil, fmt.Errorf("SelectKeyValueByKeys: query %s: %w", table, err)
		}

		for rows.Next() {
			var k any
			var id int64
			if err := rows.Scan(&k, &id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("SelectKeyValueByKeys: scan %s: %w", table, err)
			}
			out[storage.NormalizeKey(k)] = id
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("SelectKeyValueByKeys: rows %s: %w", table, err)
		}
		rows.Close()
	}

	return out, nil
}

// buildCreateSQL generates DDL for the base table and (optionally) the SCD2
// history table.
//
// Outputs:
//   - schemaSQL: optional CREATE SCHEMA statement when t.Name is schema-qualified.
//   - baseSQL:   CREATE TABLE IF NOT EXISTS for the base table.
//   - histSQL:   CREATE TABLE IF NOT EXISTS for <table>_history when SCD2 is enabled.
//
// Design notes:
//   - The base table must be created regardless of SCD2 settings.
//   - For SCD2 tables, the base table includes metadata columns.
//   - The history table intentionally omits unique constraints so multiple
//     historical versions can coexist.
//   - The history table also omits the PrimaryKeySpec column because the SCD2
//     insert logic never writes that column into history rows.
//func buildCreateSQL(t storage.TableSpec) (schemaSQL, baseSQL, histSQL string, err error) {
//	if strings.TrimSpace(t.Name) == "" {
//		return "", "", "", fmt.Errorf("buildCreateSQL: table name is empty")
//	}
//
//	// If the table is schema-qualified (e.g. "public.countries"), ensure the
//	// schema exists. This makes auto_create_table more robust in environments
//	// where non-default schemas are used.
//	if schema, _ := splitQualifiedName(t.Name); schema != "" {
//		schemaSQL = fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, pgIdent(schema))
//	}
//
//	// Build base table column definitions.
//	baseCols, err := buildColumnDefsForBase(t)
//	if err != nil {
//		return "", "", "", err
//	}
//
//	// Append metadata columns for SCD2-enabled fact tables.
//	var history *storage.HistorySpec
//	if t.Load.History != nil && t.Load.History.Enabled {
//		history = t.Load.History
//
//		// NOTE: Users may choose to declare the SCD2 metadata columns explicitly in
//		// config (for clarity, or to control type/nullability), OR rely on the
//		// loader to add them automatically.
//		//
//		// We must support both without generating invalid DDL. If we blindly append
//		// metadata columns here, we can produce a "column specified more than once"
//		// error when the config already includes e.g. "valid_from".
//		//
//		// Approach:
//		//   - Compute the set of configured column names (case-insensitive).
//		//   - Only append metadata columns that aren't already present.
//		//
//		// This makes SCD2 DDL idempotent and robust across configuration styles.
//		configured := configuredColumnSet(t)
//
//		if !configured[strings.ToLower(strings.TrimSpace(history.ValidFromColumn))] {
//			baseCols = append(baseCols, fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, pgIdent(history.ValidFromColumn)))
//		}
//		if !configured[strings.ToLower(strings.TrimSpace(history.ValidToColumn))] {
//			baseCols = append(baseCols, fmt.Sprintf(`%s TIMESTAMPTZ`, pgIdent(history.ValidToColumn)))
//		}
//		if !configured[strings.ToLower(strings.TrimSpace(history.ChangedAtColumn))] {
//			baseCols = append(baseCols, fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, pgIdent(history.ChangedAtColumn)))
//		}
//	}
//
//	// Constraints are applied only to the base table.
//	constraints, err := buildBaseConstraints(t)
//	if err != nil {
//		return "", "", "", err
//	}
//	baseCols = append(baseCols, constraints...)
//
//	baseSQL = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s);`,
//		t.Name, strings.Join(baseCols, ", "))
//
//	// Optional history table.
//	if history != nil {
//		histCols, err := buildColumnDefsForHistory(t)
//		if err != nil {
//			return "", "", "", err
//		}
//		histCols = append(histCols,
//			fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, pgIdent(history.ValidFromColumn)),
//			fmt.Sprintf(`%s TIMESTAMPTZ`, pgIdent(history.ValidToColumn)),
//			fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, pgIdent(history.ChangedAtColumn)),
//		)
//		histSQL = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_history (%s);`,
//			t.Name, strings.Join(histCols, ", "))
//	}
//
//	return schemaSQL, baseSQL, histSQL, nil
//}

// buildColumnDefsForBase returns the list of "<col> <type> ..." definitions for
// the base table.
//
// Primary key handling:
//   - If PrimaryKeySpec is provided, we create it as the first column.
//   - The primary key column is not expected to be present in t.Columns.
func buildColumnDefsForBase(t storage.TableSpec) ([]string, error) {
	cols := make([]string, 0, len(t.Columns)+1)

	if t.PrimaryKey != nil {
		pk := strings.TrimSpace(t.PrimaryKey.Name)
		pkType := strings.TrimSpace(t.PrimaryKey.Type)
		if pk == "" || pkType == "" {
			return nil, fmt.Errorf("buildColumnDefsForBase: table %s: primary_key.name and primary_key.type are required", t.Name)
		}
		// Postgres supports inline PRIMARY KEY constraints.
		cols = append(cols, fmt.Sprintf(`%s %s PRIMARY KEY`, pgIdent(pk), pkType))
	}

	for _, c := range t.Columns {
		def, err := buildColumnDef(c)
		if err != nil {
			return nil, fmt.Errorf("buildColumnDefsForBase: table %s: %w", t.Name, err)
		}
		cols = append(cols, def)
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("buildColumnDefsForBase: table %s: no columns", t.Name)
	}
	return cols, nil
}

// buildColumnDefsForHistory returns the list of column definitions for the
// history table.
//
// Important:
//   - The history table intentionally omits PrimaryKeySpec, because history
//     inserts do not write that column.
func buildColumnDefsForHistory(t storage.TableSpec) ([]string, error) {
	cols := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		def, err := buildColumnDef(c)
		if err != nil {
			return nil, fmt.Errorf("buildColumnDefsForHistory: table %s: %w", t.Name, err)
		}
		cols = append(cols, def)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("buildColumnDefsForHistory: table %s: no columns", t.Name)
	}
	return cols, nil
}

// buildColumnDef renders a single column definition.
//
// Nullable semantics:
//   - nullable == nil  => NOT NULL (conservative default; matches existing engine expectations).
//   - nullable == true => NULL (no NOT NULL clause).
//   - nullable == false=> NOT NULL.
func buildColumnDef(c storage.ColumnSpec) (string, error) {
	name := strings.TrimSpace(c.Name)
	typ := strings.TrimSpace(c.Type)
	if name == "" || typ == "" {
		return "", fmt.Errorf("column name/type must be set")
	}

	var b strings.Builder
	b.WriteString(pgIdent(name))
	b.WriteString(" ")
	b.WriteString(typ)

	nullable := false
	if c.Nullable != nil {
		nullable = *c.Nullable
	}
	if !nullable {
		b.WriteString(" NOT NULL")
	}

	// Foreign key references are expressed inline in the column definition.
	// This keeps CreateTable DDL self-contained and matches typical Postgres style.
	if ref := strings.TrimSpace(c.References); ref != "" {
		b.WriteString(" REFERENCES ")
		b.WriteString(ref)
	}

	return b.String(), nil
}

// buildBaseConstraints generates table-level constraints for the base table.
//
// Today we only support UNIQUE constraints because that's the only constraint
// type exposed by storage.ConstraintSpec.
func buildBaseConstraints(t storage.TableSpec) ([]string, error) {
	if len(t.Constraints) == 0 {
		return nil, nil
	}

	out := make([]string, 0, len(t.Constraints))
	for _, c := range t.Constraints {
		kind := strings.ToLower(strings.TrimSpace(c.Kind))
		switch kind {
		case "unique":
			if len(c.Columns) == 0 {
				return nil, fmt.Errorf("table %s: unique constraint requires columns", t.Name)
			}
			var b strings.Builder
			b.WriteString("UNIQUE (")
			for i, col := range c.Columns {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(pgIdent(strings.TrimSpace(col)))
			}
			b.WriteString(")")
			out = append(out, b.String())
		default:
			return nil, fmt.Errorf("table %s: unsupported constraint kind %q", t.Name, c.Kind)
		}
	}
	return out, nil
}

// splitQualifiedName splits a schema-qualified name into (schema, table).
//
// Examples:
//   - "public.countries" => ("public", "countries")
//   - "countries"        => ("", "countries")
//
// This helper is intentionally conservative: it only handles a single dot.
// If callers pass a more complex expression, we treat it as unqualified.
func splitQualifiedName(name string) (schema string, table string) {
	name = strings.TrimSpace(name)
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return "", name
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}

// buildCreateSQL builds DDL for:
//   - Base table
//   - Optional SCD2 history table
//
// It avoids duplicating metadata columns if user already defined them.
func buildCreateSQL(t storage.TableSpec) (schemaSQL, baseSQL, histSQL string, err error) {
	if strings.TrimSpace(t.Name) == "" {
		return "", "", "", fmt.Errorf("table name is empty")
	}

	if schema, _ := splitQualifiedName(t.Name); schema != "" {
		schemaSQL = fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, pgIdent(schema))
	}

	baseCols, err := buildColumnDefsForBase(t)
	if err != nil {
		return "", "", "", err
	}

	var history *storage.HistorySpec
	if t.Load.History != nil && t.Load.History.Enabled {
		history = t.Load.History
		configured := configuredColumnSet(t)

		if !configured[strings.ToLower(history.ValidFromColumn)] {
			baseCols = append(baseCols, fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, pgIdent(history.ValidFromColumn)))
		}
		if !configured[strings.ToLower(history.ValidToColumn)] {
			baseCols = append(baseCols, fmt.Sprintf(`%s TIMESTAMPTZ`, pgIdent(history.ValidToColumn)))
		}
		if !configured[strings.ToLower(history.ChangedAtColumn)] {
			baseCols = append(baseCols, fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, pgIdent(history.ChangedAtColumn)))
		}
	}

	constraints, err := buildBaseConstraints(t)
	if err != nil {
		return "", "", "", err
	}
	baseCols = append(baseCols, constraints...)

	baseSQL = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s);`,
		t.Name, strings.Join(baseCols, ", "))

	if history != nil {
		histCols, err := buildColumnDefsForHistory(t)
		if err != nil {
			return "", "", "", err
		}

		configured := configuredColumnSet(t)
		if !configured[strings.ToLower(history.ValidFromColumn)] {
			histCols = append(histCols, fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, pgIdent(history.ValidFromColumn)))
		}
		if !configured[strings.ToLower(history.ValidToColumn)] {
			histCols = append(histCols, fmt.Sprintf(`%s TIMESTAMPTZ`, pgIdent(history.ValidToColumn)))
		}
		if !configured[strings.ToLower(history.ChangedAtColumn)] {
			histCols = append(histCols, fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, pgIdent(history.ChangedAtColumn)))
		}

		histSQL = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_history (%s);`,
			t.Name, strings.Join(histCols, ", "))
	}

	return
}

// configuredColumnSet returns lowercase column names defined in config.

func configuredColumnSet(t storage.TableSpec) map[string]bool {
	out := make(map[string]bool, len(t.Columns))
	for _, c := range t.Columns {
		out[strings.ToLower(strings.TrimSpace(c.Name))] = true
	}
	return out
}

// indexOfColumn returns the index of name in columns and whether it was found.
//
// This is intentionally case-sensitive because:
//   - columns are generated from config (which is case-sensitive in practice)
//   - pgIdent quoting depends on exact spelling
//
// If you later decide to normalize column names, do it consistently across:
//   - config validation
//   - SQL generation
//   - row materialization
func indexOfColumn(columns []string, name string) (int, bool) {
	for i, c := range columns {
		if c == name {
			return i, true
		}
	}
	return -1, false
}

// equalScalar compares a pair of values that logically represent a single
// scalar field (like row_hash).
//
// Why this exists:
//   - When scanning from Postgres via pgx, TEXT values can appear as:
//   - string
//   - []byte
//   - Direct interface comparisons can incorrectly report "not equal" even
//     when the textual content is identical.
//
// Behavior:
//   - nil equals nil
//   - []byte is compared by bytes, and can equal a string if UTF-8 content matches
//   - otherwise falls back to string formatting (stable enough for hashes)
func equalScalar(a any, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	// Normalize common Postgres text representations.
	switch av := a.(type) {
	case []byte:
		switch bv := b.(type) {
		case []byte:
			return string(av) == string(bv)
		case string:
			return string(av) == bv
		}
	case string:
		switch bv := b.(type) {
		case []byte:
			return av == string(bv)
		case string:
			return av == bv
		}
	}

	// For hashes, fmt.Sprint is acceptable as a final fallback since the
	// upstream hash transformer should produce stable, deterministic values.
	return fmt.Sprint(a) == fmt.Sprint(b)
}
