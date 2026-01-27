package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"etl/internal/storage"
)

/*
MultiRepo implements storage.MultiRepository for Microsoft SQL Server.

This backend supports:
  - Dimension key management
  - Fact inserts
  - Fully transactional SCD2 (Slowly Changing Dimension Type 2) history for facts

SCD2 behavior:
  - Matching is done by History.BusinessKey
  - Current rows are those with valid_to IS NULL
  - On change:
  - The previous current row is copied into <table>_history
  - Its valid_to is set to now()
  - A new current row is written with valid_from = now()
  - If incoming values match the current row exactly, the operation is a no-op

Locking:
  - Uses UPDLOCK + ROWLOCK to serialize updates per business key
*/
type MultiRepo struct {
	db *sql.DB
}

// NewMulti creates a new MSSQL-backed MultiRepo.
func NewMulti(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
	db, err := sql.Open("sqlserver", cfg.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(64)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &MultiRepo{db: db}, nil
}

// Close closes the database connection pool.
func (r *MultiRepo) Close() {
	_ = r.db.Close()
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

// insertPlain performs a simple multi-row INSERT.
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
		b.WriteString("[" + c + "]")
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
			b.WriteString(fmt.Sprintf("@p%d", p))
			p++
			args = append(args, row[j])
		}
		b.WriteString(")")
	}

	res, err := r.db.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// insertSCD2 performs a full SCD2 merge for each row.
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

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	colIdx := make(map[string]int, len(columns))
	for i, c := range columns {
		colIdx[c] = i
	}

	keyIdx := make([]int, len(h.BusinessKey))
	for i, k := range h.BusinessKey {
		keyIdx[i] = colIdx[k]
	}

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

		if rowsEqual(curr, row, columns) {
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

	if err := tx.Commit(); err != nil {
		return total, err
	}
	return total, nil
}

// fetchCurrentRowTx fetches the current row for a business key.
func (r *MultiRepo) fetchCurrentRowTx(
	ctx context.Context,
	tx *sql.Tx,
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
		b.WriteString("[" + c + "]")
	}
	b.WriteString(", [" + h.ValidFromColumn + "] FROM ")
	b.WriteString(table)
	b.WriteString(" WITH (UPDLOCK, ROWLOCK) WHERE ")
	b.WriteString("[" + h.ValidToColumn + "] IS NULL AND ")

	args := make([]any, 0, len(keyIdx))
	for i, idx := range keyIdx {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString("[" + h.BusinessKey[i] + "] = @p" + strconv.Itoa(i+1))
		args = append(args, row[idx])
	}

	rowDB := tx.QueryRowContext(ctx, b.String(), args...)

	out := make([]any, len(columns))
	var validFrom time.Time
	err := rowDB.Scan(append(out, &validFrom)...)
	if err == sql.ErrNoRows {
		return nil, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	return out, validFrom, nil
}

// insertHistoryRowTx inserts a closed historical version.
func (r *MultiRepo) insertHistoryRowTx(
	ctx context.Context,
	tx *sql.Tx,
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
		b.WriteString("[" + c + "]")
	}
	b.WriteString(", [" + h.ValidFromColumn + "], [" + h.ValidToColumn + "], [" + h.ChangedAtColumn + "]) VALUES (")

	args := make([]any, 0, len(curr)+3)
	p := 1
	for _, v := range curr {
		b.WriteString("@p" + strconv.Itoa(p) + ", ")
		args = append(args, v)
		p++
	}
	b.WriteString("@p" + strconv.Itoa(p) + ", @p" + strconv.Itoa(p+1) + ", @p" + strconv.Itoa(p+2) + ")")
	args = append(args, oldValidFrom, now, now)

	_, err := tx.ExecContext(ctx, b.String(), args...)
	return err
}

// updateCurrentRowTx updates the current row in place.
func (r *MultiRepo) updateCurrentRowTx(
	ctx context.Context,
	tx *sql.Tx,
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
		b.WriteString("[" + c + "] = @p" + strconv.Itoa(p))
		args = append(args, row[i])
		p++
	}
	b.WriteString(", [" + h.ValidFromColumn + "] = @p" + strconv.Itoa(p))
	args = append(args, now)
	p++

	b.WriteString(", [" + h.ValidToColumn + "] = NULL")
	b.WriteString(", [" + h.ChangedAtColumn + "] = @p" + strconv.Itoa(p))
	args = append(args, now)
	p++

	b.WriteString(" WHERE [" + h.ValidToColumn + "] IS NULL AND ")

	for i, idx := range keyIdx {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString("[" + h.BusinessKey[i] + "] = @p" + strconv.Itoa(p))
		args = append(args, row[idx])
		p++
	}

	_, err := tx.ExecContext(ctx, b.String(), args...)
	return err
}

// EnsureTables creates base and history tables when AutoCreateTable is enabled.
//
// History tables are named <table>_history and include metadata columns.
// This method is idempotent.
func (r *MultiRepo) EnsureTables(ctx context.Context, tables []storage.TableSpec) error {
	for _, t := range tables {
		if !t.AutoCreateTable {
			continue
		}
		if t.Load.History == nil || !t.Load.History.Enabled {
			continue
		}
		h := t.Load.History

		var cols []string
		for _, c := range t.Columns {
			cols = append(cols, fmt.Sprintf("[%s] %s", c.Name, c.Type))
		}
		cols = append(cols,
			fmt.Sprintf("[%s] DATETIME2 NOT NULL", h.ValidFromColumn),
			fmt.Sprintf("[%s] DATETIME2 NULL", h.ValidToColumn),
			fmt.Sprintf("[%s] DATETIME2 NOT NULL", h.ChangedAtColumn),
		)

		baseSQL := fmt.Sprintf(`
IF OBJECT_ID(N'%s', N'U') IS NULL
CREATE TABLE %s (%s);`,
			t.Name, t.Name, strings.Join(cols, ", "))

		histSQL := fmt.Sprintf(`
IF OBJECT_ID(N'%s_history', N'U') IS NULL
CREATE TABLE %s_history (%s);`,
			t.Name, t.Name, strings.Join(cols, ", "))

		if _, err := r.db.ExecContext(ctx, baseSQL); err != nil {
			return err
		}
		if _, err := r.db.ExecContext(ctx, histSQL); err != nil {
			return err
		}
	}
	return nil
}

// rowsEqual compares rows using the "row_hash" column if present.
// Falls back to full comparison only if the column is missing.
func rowsEqual(a, b []any, columns []string) bool {
	hashIdx := -1
	for i, c := range columns {
		if c == "row_hash" {
			hashIdx = i
			break
		}
	}

	if hashIdx >= 0 {
		return fmt.Sprint(a[hashIdx]) == fmt.Sprint(b[hashIdx])
	}

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

// EnsureDimensionKeys inserts missing dimension keys into a dimension table.
//
// The operation is idempotent: existing keys are ignored.
//
// SQL Server implementation notes:
//   - MERGE is intentionally avoided due to correctness and performance issues.
//   - Keys are inserted using INSERT ... SELECT with a LEFT JOIN anti-semi-join.
//   - Keys are chunked to stay within SQL Server parameter limits.
//   - ConflictColumns must be empty or contain only keyColumn.
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
	if len(conflictColumns) > 0 &&
		(len(conflictColumns) != 1 || !strings.EqualFold(conflictColumns[0], keyColumn)) {
		return fmt.Errorf(
			"EnsureDimensionKeys: SQL Server requires conflict_columns == [%s] (got %v)",
			keyColumn, conflictColumns,
		)
	}

	// Deduplicate keys in-memory to avoid multiple insert attempts
	// for the same value in a single batch.
	uniq := make(map[string]any, len(keys))
	for _, k := range keys {
		nk := storage.NormalizeKey(k)
		if nk != "" {
			uniq[nk] = k
		}
	}
	if len(uniq) == 0 {
		return nil
	}

	// SQL Server has a hard limit of 2100 parameters.
	// We stay comfortably below that.
	const chunkSize = 1000

	// Use a single transaction for performance.
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("EnsureDimensionKeys: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	uniqKeys := make([]any, 0, len(uniq))
	for _, v := range uniq {
		uniqKeys = append(uniqKeys, v)
	}

	for start := 0; start < len(uniqKeys); start += chunkSize {
		end := start + chunkSize
		if end > len(uniqKeys) {
			end = len(uniqKeys)
		}
		part := uniqKeys[start:end]

		var b strings.Builder
		b.WriteString("INSERT INTO ")
		b.WriteString(table)
		b.WriteString(" ([")
		b.WriteString(keyColumn)
		b.WriteString("]) SELECT v.[key] FROM (VALUES ")

		args := make([]any, 0, len(part))
		for i, k := range part {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fmt.Sprintf("(@p%d)", i+1))
			args = append(args, k)
		}

		b.WriteString(") AS v([key]) LEFT JOIN ")
		b.WriteString(table)
		b.WriteString(" t ON t.[")
		b.WriteString(keyColumn)
		b.WriteString("] = v.[key] WHERE t.[")
		b.WriteString(keyColumn)
		b.WriteString("] IS NULL")

		if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
			return fmt.Errorf("EnsureDimensionKeys: insert %s: %w", table, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("EnsureDimensionKeys: commit: %w", err)
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
//   - tx must be an open transaction that the caller controls.
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
	tx *sql.Tx,
	table string,
	columns []string,
	row []any,
	h *storage.HistorySpec,
	now time.Time,
) error {
	if tx == nil {
		return fmt.Errorf("insertCurrentRowTx: tx is nil")
	}
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
		b.WriteString("[" + c + "]")
	}

	// metadata columns
	b.WriteString(", [" + h.ValidFromColumn + "]")
	b.WriteString(", [" + h.ValidToColumn + "]")
	b.WriteString(", [" + h.ChangedAtColumn + "]")
	b.WriteString(") VALUES (")

	args := make([]any, 0, len(columns)+3)
	p := 1

	for i := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("@p%d", p))
		args = append(args, row[i])
		p++
	}

	// Append metadata placeholders.
	// valid_from
	b.WriteString(", ")
	b.WriteString(fmt.Sprintf("@p%d", p))
	args = append(args, now)
	p++

	// valid_to (NULL)
	b.WriteString(", NULL")

	// changed_at
	b.WriteString(", ")
	b.WriteString(fmt.Sprintf("@p%d", p))
	args = append(args, now)

	b.WriteString(")")

	_, err := tx.ExecContext(ctx, b.String(), args...)
	return err
}

// SelectAllKeyValue returns a mapping from normalized key -> surrogate id
// for the entire dimension table.
func (r *MultiRepo) SelectAllKeyValue(
	ctx context.Context,
	table string,
	keyColumn string,
	valueColumn string,
) (map[string]int64, error) {
	if table == "" || keyColumn == "" || valueColumn == "" {
		return nil, fmt.Errorf("SelectAllKeyValue: table, keyColumn, valueColumn required")
	}

	q := fmt.Sprintf(
		"SELECT [%s], [%s] FROM %s",
		keyColumn, valueColumn, table,
	)

	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]int64)
	for rows.Next() {
		var k any
		var id int64
		if err := rows.Scan(&k, &id); err != nil {
			return nil, err
		}
		out[storage.NormalizeKey(k)] = id
	}
	return out, rows.Err()
}

// SelectKeyValueByKeys returns mapping from normalized key -> id for a set of keys.
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

	const chunk = 1000 // safe for SQL Server parameter limits
	out := make(map[string]int64, len(keys))

	for start := 0; start < len(keys); start += chunk {
		end := start + chunk
		if end > len(keys) {
			end = len(keys)
		}
		part := keys[start:end]

		var b strings.Builder
		b.WriteString("SELECT [")
		b.WriteString(keyColumn)
		b.WriteString("], [")
		b.WriteString(valueColumn)
		b.WriteString("] FROM ")
		b.WriteString(table)
		b.WriteString(" WHERE [")
		b.WriteString(keyColumn)
		b.WriteString("] IN (")

		args := make([]any, 0, len(part))
		for i, k := range part {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fmt.Sprintf("@p%d", i+1))
			args = append(args, k)
		}
		b.WriteString(")")

		rows, err := r.db.QueryContext(ctx, b.String(), args...)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var k any
			var id int64
			if err := rows.Scan(&k, &id); err != nil {
				rows.Close()
				return nil, err
			}
			out[storage.NormalizeKey(k)] = id
		}
		rows.Close()
	}

	return out, nil
}
