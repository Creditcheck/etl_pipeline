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
		return r.insertPlain(ctx, table, columns, rows)
	}

	return r.insertSCD2(ctx, spec, table, columns, rows)
}

// insertPlain performs a bulk INSERT.
func (r *MultiRepo) insertPlain(
	ctx context.Context,
	table string,
	columns []string,
	rows [][]any,
) (int64, error) {
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

	cmd, err := r.pool.Exec(ctx, b.String(), args...)
	if err != nil {
		return 0, err
	}
	return cmd.RowsAffected(), nil
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

		if rowsEqual(curr, row) {
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

	out := make([]any, len(columns))
	var validFrom time.Time
	if err := rows.Scan(append(out, &validFrom)...); err != nil {
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

		baseSQL, histSQL, err := buildSCD2CreateSQL(t)
		if err != nil {
			return err
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

func buildSCD2CreateSQL(t storage.TableSpec) (baseSQL, histSQL string, err error) {
	if t.Load.History == nil || !t.Load.History.Enabled {
		return "", "", nil
	}
	h := t.Load.History

	var cols []string
	for _, c := range t.Columns {
		cols = append(cols, fmt.Sprintf(`"%s" %s`, c.Name, c.Type))
	}
	cols = append(cols,
		fmt.Sprintf(`"%s" TIMESTAMPTZ NOT NULL`, h.ValidFromColumn),
		fmt.Sprintf(`"%s" TIMESTAMPTZ`, h.ValidToColumn),
		fmt.Sprintf(`"%s" TIMESTAMPTZ NOT NULL`, h.ChangedAtColumn),
	)

	baseSQL = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s);`,
		t.Name, strings.Join(cols, ", "))

	histSQL = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_history (%s);`,
		t.Name, strings.Join(cols, ", "))

	return
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
