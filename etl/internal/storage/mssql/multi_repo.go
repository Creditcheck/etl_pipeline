package mssql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"etl/internal/storage"
)

// MultiRepo implements storage.MultiRepository for Microsoft SQL Server.
//
// This implementation supports:
//   - Dimension key management (idempotent inserts).
//   - Fact inserts:
//   - Plain bulk insert.
//   - Optional "dedupe insert" using NOT EXISTS (for idempotent reprocessing).
//   - SCD2 (Slowly Changing Dimension Type 2) for fact tables via a
//     current + history table model:
//   - <table>          contains exactly one current row per business key
//   - <table>_history  contains all previous versions
//
// SCD2 semantics:
//   - Business key is configured by spec.Load.History.BusinessKey.
//   - Current rows are those with ValidToColumn IS NULL.
//   - If incoming matches current (by row_hash if present, otherwise by full row),
//     the operation is a no-op (idempotent).
//   - If incoming differs:
//     1) Copy prior current row into <table>_history (preserving valid_from)
//     2) Close that history row by setting valid_to=now and changed_at=now
//     3) Update the base row in place (valid_from=now, valid_to=NULL, changed_at=now)
//
// Concurrency:
//   - fetchCurrentRowTx uses UPDLOCK + ROWLOCK so multiple writers for the same
//     business key serialize cleanly without table-wide locks.
//
// Note on driver registration (Behavior change):
//   - This package intentionally does NOT blank-import a SQL Server driver.
//     The application must register the "sqlserver" driver elsewhere.
//     See the "Behavior changes" section in the response.
type MultiRepo struct {
	db dbConn
}

// NewMulti constructs a MultiRepo using database/sql and the "sqlserver" driver.
//
// The caller must ensure a SQL Server driver is registered with database/sql under
// the name "sqlserver" before calling NewMulti. If not, sql.Open will fail.
//
// This method validates connectivity via PingContext.
func NewMulti(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
	raw, err := sql.Open("sqlserver", cfg.DSN)
	if err != nil {
		return nil, err
	}

	// Conservative defaults for ETL-style bursty loads.
	raw.SetMaxOpenConns(64)
	raw.SetMaxIdleConns(64)

	if err := raw.PingContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return &MultiRepo{db: &sqlDB{db: raw}}, nil
}

// Close releases database resources held by this repository.
func (r *MultiRepo) Close() {
	if r == nil || r.db == nil {
		return
	}
	_ = r.db.Close()
}

// EnsureTables creates base tables and (when SCD2 is enabled) their history tables.
//
// Behavior:
//   - If AutoCreateTable is false: no-op.
//   - Base tables are always created when AutoCreateTable is true.
//   - History tables are created only when Load.Kind == "fact" and History.Enabled == true.
//   - History tables do NOT inherit UNIQUE constraints from the base table.
//
// This method is idempotent and safe to run on every ETL invocation.
func (r *MultiRepo) EnsureTables(ctx context.Context, tables []storage.TableSpec) error {
	for _, t := range tables {
		if !t.AutoCreateTable {
			continue
		}

		baseSQL, histSQL, err := buildCreateSQL(t)
		if err != nil {
			return err
		}

		if _, err := r.db.ExecContext(ctx, baseSQL); err != nil {
			return fmt.Errorf("mssql: create base table %s: %w", t.Name, err)
		}
		if histSQL != "" {
			if _, err := r.db.ExecContext(ctx, histSQL); err != nil {
				return fmt.Errorf("mssql: create history table %s_history: %w", t.Name, err)
			}
		}
	}
	return nil
}

// InsertFactRows inserts fact rows into table using either plain inserts, dedupe inserts,
// or SCD2 merges depending on the TableSpec.
//
// Rules:
//   - If spec.Load.Kind != "fact": treated as plain insert.
//   - If spec.Load.History is nil or disabled: plain insert; if dedupeColumns is non-empty,
//     uses an "insert where not exists" pattern for idempotency.
//   - If spec.Load.History.Enabled: applies transactional SCD2 row-by-row logic.
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
	if table == "" {
		return 0, fmt.Errorf("InsertFactRows: table is empty")
	}
	if len(columns) == 0 {
		return 0, fmt.Errorf("InsertFactRows: columns is empty")
	}

	if spec.Load.Kind != "fact" || spec.Load.History == nil || !spec.Load.History.Enabled {
		return r.insertPlain(ctx, table, columns, rows, dedupeColumns)
	}

	return r.insertSCD2(ctx, spec, table, columns, rows)
}

// EnsureDimensionKeys idempotently inserts missing keys into a dimension table.
//
// Implementation details:
//   - Avoids MERGE.
//   - Uses INSERT ... SELECT over a VALUES table and LEFT JOIN anti-semi join.
//   - Chunks keys to stay well within SQL Server's 2100 parameter limit.
//   - Deduplicates keys deterministically (stable ordering) to avoid nondeterminism.
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

	// Deterministic dedupe:
	//   - keep the first encountered value for a normalized key
	//   - sort by normalized key string
	uniq := make(map[string]any, len(keys))
	order := make([]string, 0, len(keys))
	for _, k := range keys {
		nk := storage.NormalizeKey(k)
		if nk == "" {
			continue
		}
		if _, exists := uniq[nk]; exists {
			continue
		}
		uniq[nk] = k
		order = append(order, nk)
	}
	if len(order) == 0 {
		return nil
	}
	sort.Strings(order)

	// SQL Server has a hard limit of 2100 parameters. We stay comfortably below that.
	const chunkSize = 1000

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("EnsureDimensionKeys: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	uniqKeys := make([]any, 0, len(order))
	for _, nk := range order {
		uniqKeys = append(uniqKeys, uniq[nk])
	}

	for start := 0; start < len(uniqKeys); start += chunkSize {
		end := start + chunkSize
		if end > len(uniqKeys) {
			end = len(uniqKeys)
		}
		part := uniqKeys[start:end]

		q, args := buildEnsureDimensionKeysSQL(table, keyColumn, part)
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("EnsureDimensionKeys: insert %s: %w", table, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("EnsureDimensionKeys: commit: %w", err)
	}
	return nil
}

// SelectAllKeyValue returns a mapping from normalized key -> surrogate id for the entire table.
//
// It is used to prewarm in-memory caches for dimensions.
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
		"SELECT %s, %s FROM %s",
		mssqlIdent(keyColumn),
		mssqlIdent(valueColumn),
		mssqlTableIdent(table),
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// SelectKeyValueByKeys returns mapping from normalized key -> id for the provided keys.
//
// This method chunks the IN() list to avoid SQL Server parameter limits.
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
		return nil, fmt.Errorf("SelectKeyValueByKeys: table, keyColumn, valueColumn required")
	}

	const chunk = 1000
	out := make(map[string]int64, len(keys))

	for start := 0; start < len(keys); start += chunk {
		end := start + chunk
		if end > len(keys) {
			end = len(keys)
		}
		part := keys[start:end]

		q, args := buildSelectKeyValueByKeysSQL(table, keyColumn, valueColumn, part)

		rows, err := r.db.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var k any
			var id int64
			if err := rows.Scan(&k, &id); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out[storage.NormalizeKey(k)] = id
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}

	return out, nil
}

// insertPlain inserts rows into the table with optional dedupe behavior.
//
// If dedupeColumns is empty, it performs a standard bulk INSERT.
// If dedupeColumns is set, it inserts only rows that do not already exist, using NOT EXISTS.
// This makes reruns idempotent for the specified dedupe columns.
func (r *MultiRepo) insertPlain(
	ctx context.Context,
	table string,
	columns []string,
	rows [][]any,
	dedupeColumns []string,
) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	if len(dedupeColumns) == 0 {
		return r.insertPlainNoDedupe(ctx, table, columns, rows)
	}

	return r.insertPlainWithDedupe(ctx, table, columns, rows, dedupeColumns)
}

// insertPlainNoDedupe inserts all rows using a single INSERT ... VALUES statement.
func (r *MultiRepo) insertPlainNoDedupe(
	ctx context.Context,
	table string,
	columns []string,
	rows [][]any,
) (int64, error) {
	q, args := buildBulkInsertSQL(table, columns, rows)

	res, err := r.db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// insertPlainWithDedupe inserts rows only if they do not already exist per dedupeColumns.
//
// The statement is chunked to avoid SQL Server's parameter limit (2100).
func (r *MultiRepo) insertPlainWithDedupe(
	ctx context.Context,
	table string,
	columns []string,
	rows [][]any,
	dedupeColumns []string,
) (int64, error) {
	// Validate dedupe columns exist in the columns list.
	colPos := indexColumns(columns)
	for _, dc := range dedupeColumns {
		if _, ok := colPos[dc]; !ok {
			return 0, fmt.Errorf("insertPlainWithDedupe: dedupe column %q not present in columns", dc)
		}
	}

	// SQL Server parameter limit is 2100. Each row uses len(columns) parameters.
	// We compute a conservative maximum rows per statement.
	maxRows := 2000 / max(1, len(columns))
	if maxRows < 1 {
		maxRows = 1
	}

	var total int64
	for start := 0; start < len(rows); start += maxRows {
		end := start + maxRows
		if end > len(rows) {
			end = len(rows)
		}
		part := rows[start:end]

		q, args := buildInsertNotExistsSQL(table, columns, part, dedupeColumns)

		res, err := r.db.ExecContext(ctx, q, args...)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
	}

	return total, nil
}

// insertSCD2 applies SCD2 semantics row-by-row in a single transaction.
//
// This ensures atomicity for each row version transition.
func (r *MultiRepo) insertSCD2(
	ctx context.Context,
	spec storage.TableSpec,
	table string,
	columns []string,
	rows [][]any,
) (int64, error) {
	h := spec.Load.History
	if h == nil || !h.Enabled {
		return 0, fmt.Errorf("insertSCD2: history spec is nil/disabled")
	}
	if len(h.BusinessKey) == 0 {
		return 0, fmt.Errorf("insertSCD2: business_key must not be empty")
	}

	now := time.Now().UTC()
	var total int64

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	colIdx := indexColumns(columns)
	keyIdx, err := indicesFor(h.BusinessKey, colIdx)
	if err != nil {
		return 0, err
	}

	rowHashIdx, hasRowHash := indexOfColumn(columns, "row_hash")

	for _, row := range rows {
		curr, validFrom, err := fetchCurrentRowTx(ctx, tx, table, h, columns, keyIdx, row)
		if err != nil {
			return total, err
		}

		if curr == nil {
			if err := insertCurrentRowTx(ctx, tx, table, columns, row, h, now); err != nil {
				return total, err
			}
			total++
			continue
		}

		changed := true
		if hasRowHash {
			changed = !equalScalar(curr[rowHashIdx], row[rowHashIdx])
		} else {
			changed = !rowsEqualAll(curr, row)
		}
		if !changed {
			continue
		}

		if err := insertHistoryRowTx(ctx, tx, table, columns, curr, validFrom, h, now); err != nil {
			return total, err
		}
		if err := updateCurrentRowTx(ctx, tx, table, columns, row, h, keyIdx, now); err != nil {
			return total, err
		}

		total++
	}

	if err := tx.Commit(); err != nil {
		return total, err
	}
	return total, nil
}

// fetchCurrentRowTx loads and locks the current row for a given business key.
//
// It uses UPDLOCK + ROWLOCK to serialize writers for the same business key.
//
// Returns:
//   - (nil, zero, nil) if there is no current row.
//   - (rowValues, validFrom, nil) if found.
func fetchCurrentRowTx(
	ctx context.Context,
	tx txConn,
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
		b.WriteString(mssqlIdent(c))
	}
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ValidFromColumn))
	b.WriteString(" FROM ")
	b.WriteString(mssqlTableIdent(table))
	b.WriteString(" WITH (UPDLOCK, ROWLOCK) WHERE ")
	b.WriteString(mssqlIdent(h.ValidToColumn))
	b.WriteString(" IS NULL AND ")

	args := make([]any, 0, len(keyIdx))
	for i, idx := range keyIdx {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(mssqlIdent(h.BusinessKey[i]))
		b.WriteString(" = @p")
		b.WriteString(strconv.Itoa(i + 1))
		args = append(args, row[idx])
	}

	rowDB := tx.QueryRowContext(ctx, b.String(), args...)

	// IMPORTANT: Scan destinations must be pointers. We build a parallel slice of &out[i].
	out := make([]any, len(columns))
	dests := make([]any, len(columns))
	for i := range out {
		dests[i] = &out[i]
	}

	var validFrom time.Time
	if err := rowDB.Scan(append(dests, &validFrom)...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, time.Time{}, nil
		}
		return nil, time.Time{}, err
	}
	return out, validFrom, nil
}

// insertCurrentRowTx inserts a new current version of a fact row under SCD2.
//
// It sets:
//   - valid_from = now
//   - valid_to = NULL
//   - changed_at = now
func insertCurrentRowTx(
	ctx context.Context,
	tx txConn,
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
	b.WriteString(mssqlTableIdent(table))
	b.WriteString(" (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(mssqlIdent(c))
	}
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ValidFromColumn))
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ValidToColumn))
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ChangedAtColumn))
	b.WriteString(") VALUES (")

	args := make([]any, 0, len(columns)+2)
	p := 1

	for i := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("@p%d", p))
		args = append(args, row[i])
		p++
	}

	// valid_from
	b.WriteString(", ")
	b.WriteString(fmt.Sprintf("@p%d", p))
	args = append(args, now)
	p++

	// valid_to
	b.WriteString(", NULL")

	// changed_at
	b.WriteString(", ")
	b.WriteString(fmt.Sprintf("@p%d", p))
	args = append(args, now)

	b.WriteString(")")

	_, err := tx.ExecContext(ctx, b.String(), args...)
	return err
}

// insertHistoryRowTx inserts a closed version row into <table>_history.
//
// It preserves oldValidFrom, and sets valid_to and changed_at to now.
func insertHistoryRowTx(
	ctx context.Context,
	tx txConn,
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
	b.WriteString(mssqlTableIdent(hist))
	b.WriteString(" (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(mssqlIdent(c))
	}
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ValidFromColumn))
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ValidToColumn))
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ChangedAtColumn))
	b.WriteString(") VALUES (")

	args := make([]any, 0, len(curr)+3)
	p := 1
	for i := range curr {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("@p")
		b.WriteString(strconv.Itoa(p))
		args = append(args, curr[i])
		p++
	}

	b.WriteString(", @p")
	b.WriteString(strconv.Itoa(p))
	b.WriteString(", @p")
	b.WriteString(strconv.Itoa(p + 1))
	b.WriteString(", @p")
	b.WriteString(strconv.Itoa(p + 2))
	b.WriteString(")")

	args = append(args, oldValidFrom, now, now)

	_, err := tx.ExecContext(ctx, b.String(), args...)
	return err
}

// updateCurrentRowTx updates the current row in place, matching by business key and valid_to IS NULL.
//
// It sets:
//   - all fact columns
//   - valid_from = now
//   - valid_to = NULL
//   - changed_at = now
func updateCurrentRowTx(
	ctx context.Context,
	tx txConn,
	table string,
	columns []string,
	row []any,
	h *storage.HistorySpec,
	keyIdx []int,
	now time.Time,
) error {
	var b strings.Builder
	b.WriteString("UPDATE ")
	b.WriteString(mssqlTableIdent(table))
	b.WriteString(" SET ")

	args := make([]any, 0, len(columns)+2+len(keyIdx))
	p := 1

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(mssqlIdent(c))
		b.WriteString(" = @p")
		b.WriteString(strconv.Itoa(p))
		args = append(args, row[i])
		p++
	}

	// valid_from
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ValidFromColumn))
	b.WriteString(" = @p")
	b.WriteString(strconv.Itoa(p))
	args = append(args, now)
	p++

	// valid_to
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ValidToColumn))
	b.WriteString(" = NULL")

	// changed_at
	b.WriteString(", ")
	b.WriteString(mssqlIdent(h.ChangedAtColumn))
	b.WriteString(" = @p")
	b.WriteString(strconv.Itoa(p))
	args = append(args, now)
	p++

	b.WriteString(" WHERE ")
	b.WriteString(mssqlIdent(h.ValidToColumn))
	b.WriteString(" IS NULL AND ")

	for i, idx := range keyIdx {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(mssqlIdent(h.BusinessKey[i]))
		b.WriteString(" = @p")
		b.WriteString(strconv.Itoa(p))
		args = append(args, row[idx])
		p++
	}

	_, err := tx.ExecContext(ctx, b.String(), args...)
	return err
}

// buildCreateSQL builds idempotent CREATE TABLE SQL for base and history tables.
//
// The history table is only generated when:
//   - Load.Kind == "fact"
//   - History.Enabled == true
//
// The history table deliberately excludes UNIQUE constraints to allow multiple versions per business key.
func buildCreateSQL(t storage.TableSpec) (baseSQL, histSQL string, err error) {
	if strings.TrimSpace(t.Name) == "" {
		return "", "", fmt.Errorf("mssql: table name is empty")
	}

	baseDefs, err := buildCreateTableDefs(t, true, true)
	if err != nil {
		return "", "", err
	}
	baseSQL = wrapCreateIfMissing(t.Name, baseDefs)

	if t.Load.Kind == "fact" && t.Load.History != nil && t.Load.History.Enabled {
		histName := t.Name + "_history"

		// History must NOT carry unique constraints.
		histDefs, err := buildCreateTableDefs(t, false, false)
		if err != nil {
			return "", "", err
		}
		histSQL = wrapCreateIfMissing(histName, histDefs)
	}

	return baseSQL, histSQL, nil
}

// buildCreateTableDefs produces the "(...)" inner content for CREATE TABLE.
//
// includePrimaryKey controls whether to include TableSpec.PrimaryKey.
// includeUniqueConstraints controls whether to include TableSpec.Constraints of kind "unique".
//
// If History is enabled, this method appends the SCD2 metadata columns if they are not
// already present in TableSpec.Columns.
func buildCreateTableDefs(t storage.TableSpec, includePrimaryKey bool, includeUniqueConstraints bool) (string, error) {
	var parts []string

	if includePrimaryKey && t.PrimaryKey != nil {
		pkDef, err := mssqlPrimaryKeyDef(*t.PrimaryKey)
		if err != nil {
			return "", err
		}
		parts = append(parts, pkDef)
	}

	configured := configuredColumnSet(t)

	for _, c := range t.Columns {
		def, err := mssqlColumnDef(c)
		if err != nil {
			return "", err
		}
		parts = append(parts, def)
	}

	if t.Load.History != nil && t.Load.History.Enabled {
		h := t.Load.History
		if strings.TrimSpace(h.ValidFromColumn) == "" ||
			strings.TrimSpace(h.ValidToColumn) == "" ||
			strings.TrimSpace(h.ChangedAtColumn) == "" {
			return "", fmt.Errorf("mssql: history enabled but metadata column names are empty")
		}

		if !configured[strings.ToLower(strings.TrimSpace(h.ValidFromColumn))] {
			parts = append(parts, fmt.Sprintf("%s DATETIME2 NOT NULL", mssqlIdent(h.ValidFromColumn)))
		}
		if !configured[strings.ToLower(strings.TrimSpace(h.ValidToColumn))] {
			parts = append(parts, fmt.Sprintf("%s DATETIME2 NULL", mssqlIdent(h.ValidToColumn)))
		}
		if !configured[strings.ToLower(strings.TrimSpace(h.ChangedAtColumn))] {
			parts = append(parts, fmt.Sprintf("%s DATETIME2 NOT NULL", mssqlIdent(h.ChangedAtColumn)))
		}
	}

	if includeUniqueConstraints {
		for _, con := range t.Constraints {
			if !strings.EqualFold(con.Kind, "unique") {
				return "", fmt.Errorf("%s unsupported constraint kind: %s", t.Name, con.Kind)
			}
			if len(con.Columns) == 0 {
				return "", fmt.Errorf("%s unique constraint has no columns", t.Name)
			}
			var cols []string
			for _, c := range con.Columns {
				cols = append(cols, mssqlIdent(c))
			}
			parts = append(parts, fmt.Sprintf("UNIQUE (%s)", strings.Join(cols, ", ")))
		}
	}

	return strings.Join(parts, ", "), nil
}

// wrapCreateIfMissing wraps a CREATE TABLE statement in an OBJECT_ID guard.
//
// This keeps EnsureTables idempotent without requiring IF NOT EXISTS syntax.
func wrapCreateIfMissing(tableName string, innerDefs string) string {
	return fmt.Sprintf(
		"IF OBJECT_ID(N'%s', N'U') IS NULL BEGIN CREATE TABLE %s (%s); END;",
		tableName,
		mssqlTableIdent(tableName),
		innerDefs,
	)
}

// mssqlPrimaryKeyDef returns a column definition for an identity primary key.
//
// Supported types (case-insensitive):
//   - "serial", "identity" variants -> INT IDENTITY(1,1) PRIMARY KEY
//   - "bigserial" -> BIGINT IDENTITY(1,1) PRIMARY KEY
//   - otherwise uses pk.Type verbatim with PRIMARY KEY.
func mssqlPrimaryKeyDef(pk storage.PrimaryKeySpec) (string, error) {
	if strings.TrimSpace(pk.Name) == "" {
		return "", fmt.Errorf("mssql: primary key name is empty")
	}
	typ := strings.ToLower(strings.TrimSpace(pk.Type))
	switch typ {
	case "serial", "int identity", "integer identity", "identity":
		return fmt.Sprintf("%s INT IDENTITY(1,1) PRIMARY KEY", mssqlIdent(pk.Name)), nil
	case "bigserial":
		return fmt.Sprintf("%s BIGINT IDENTITY(1,1) PRIMARY KEY", mssqlIdent(pk.Name)), nil
	default:
		return fmt.Sprintf("%s %s PRIMARY KEY", mssqlIdent(pk.Name), pk.Type), nil
	}
}

// mssqlColumnDef builds a SQL Server column definition from storage.ColumnSpec.
//
// It respects nullability and attaches a raw REFERENCES clause if provided.
func mssqlColumnDef(c storage.ColumnSpec) (string, error) {
	if strings.TrimSpace(c.Name) == "" {
		return "", fmt.Errorf("mssql: column name is empty")
	}
	if strings.TrimSpace(c.Type) == "" {
		return "", fmt.Errorf("mssql: column %s type is empty", c.Name)
	}

	var b strings.Builder
	b.WriteString(mssqlIdent(c.Name))
	b.WriteString(" ")
	b.WriteString(c.Type)

	nullable := true
	if c.Nullable != nil {
		nullable = *c.Nullable
	}
	if !nullable {
		b.WriteString(" NOT NULL")
	}
	if strings.TrimSpace(c.References) != "" {
		b.WriteString(" REFERENCES ")
		b.WriteString(c.References)
	}

	return b.String(), nil
}

// configuredColumnSet returns a lowercase set of configured column names, used to prevent duplicates.
func configuredColumnSet(t storage.TableSpec) map[string]bool {
	out := make(map[string]bool, len(t.Columns))
	for _, c := range t.Columns {
		n := strings.ToLower(strings.TrimSpace(c.Name))
		if n == "" {
			continue
		}
		out[n] = true
	}
	return out
}

// buildEnsureDimensionKeysSQL returns the INSERT...SELECT SQL and args for a key chunk.
//
// This is split out purely for testability and clarity.
func buildEnsureDimensionKeysSQL(table, keyColumn string, keys []any) (string, []any) {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(mssqlTableIdent(table))
	b.WriteString(" (")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(") SELECT v.[key] FROM (VALUES ")

	args := make([]any, 0, len(keys))
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("(@p%d)", i+1))
		args = append(args, k)
	}

	b.WriteString(") AS v([key]) LEFT JOIN ")
	b.WriteString(mssqlTableIdent(table))
	b.WriteString(" t ON t.")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(" = v.[key] WHERE t.")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(" IS NULL")

	return b.String(), args
}

// buildSelectKeyValueByKeysSQL returns the SELECT ... IN (...) query and args.
func buildSelectKeyValueByKeysSQL(table, keyColumn, valueColumn string, keys []any) (string, []any) {
	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(", ")
	b.WriteString(mssqlIdent(valueColumn))
	b.WriteString(" FROM ")
	b.WriteString(mssqlTableIdent(table))
	b.WriteString(" WHERE ")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(" IN (")

	args := make([]any, 0, len(keys))
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("@p%d", i+1))
		args = append(args, k)
	}
	b.WriteString(")")

	return b.String(), args
}

// buildBulkInsertSQL builds a single INSERT ... VALUES statement for all rows.
func buildBulkInsertSQL(table string, columns []string, rows [][]any) (string, []any) {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(mssqlTableIdent(table))
	b.WriteString(" (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(mssqlIdent(c))
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
			args = append(args, row[j])
			p++
		}
		b.WriteString(")")
	}

	return b.String(), args
}

// buildInsertNotExistsSQL constructs a single INSERT...SELECT...WHERE NOT EXISTS for a chunk of rows.
//
// It materializes incoming rows as a derived table V via VALUES, then inserts only those
// rows that do not match existing rows per dedupeColumns.
//
// The returned SQL is deterministic for a given input.
func buildInsertNotExistsSQL(table string, columns []string, rows [][]any, dedupeColumns []string) (string, []any) {
	var b strings.Builder

	b.WriteString("INSERT INTO ")
	b.WriteString(mssqlTableIdent(table))
	b.WriteString(" (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(mssqlIdent(c))
	}

	b.WriteString(") SELECT ")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("v.")
		b.WriteString(mssqlIdent(c))
	}

	b.WriteString(" FROM (VALUES ")

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
			args = append(args, row[j])
			p++
		}
		b.WriteString(")")
	}

	b.WriteString(") AS v(")
	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(mssqlIdent(c))
	}
	b.WriteString(") WHERE NOT EXISTS (SELECT 1 FROM ")
	b.WriteString(mssqlTableIdent(table))
	b.WriteString(" t WHERE ")

	for i, dc := range dedupeColumns {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString("t.")
		b.WriteString(mssqlIdent(dc))
		b.WriteString(" = v.")
		b.WriteString(mssqlIdent(dc))
	}
	b.WriteString(")")

	return b.String(), args
}

// indexColumns returns a mapping of column name -> index.
//
// This helper is intentionally tiny and allocation-light; it is used on hot paths.
func indexColumns(columns []string) map[string]int {
	m := make(map[string]int, len(columns))
	for i, c := range columns {
		m[c] = i
	}
	return m
}

// indicesFor returns the indices for required columns based on colIdx.
//
// This helper returns a friendly error if a required column is missing.
func indicesFor(required []string, colIdx map[string]int) ([]int, error) {
	out := make([]int, len(required))
	for i, c := range required {
		idx, ok := colIdx[c]
		if !ok {
			return nil, fmt.Errorf("column %q not found in columns", c)
		}
		out[i] = idx
	}
	return out, nil
}

// indexOfColumn returns the index of a column and whether it exists.
func indexOfColumn(columns []string, name string) (int, bool) {
	for i, c := range columns {
		if c == name {
			return i, true
		}
	}
	return -1, false
}

// rowsEqualAll compares two slices by stringified scalar values.
//
// This is a pragmatic fallback for cases where row_hash is not available.
func rowsEqualAll(a, b []any) bool {
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

// equalScalar compares values in a driver-tolerant way, especially for row_hash.
//
// Drivers commonly return []byte for textual columns; this function ensures that
// string vs []byte compare correctly.
func equalScalar(a any, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
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
	return fmt.Sprint(a) == fmt.Sprint(b)
}

// mssqlIdent returns a bracket-quoted identifier, escaping ']' as ']]'.
func mssqlIdent(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

// mssqlTableIdent returns a bracket-quoted identifier for schema-qualified names.
//
// Example:
//
//	"dbo.imports" -> [dbo].[imports]
func mssqlTableIdent(name string) string {
	parts := strings.Split(name, ".")
	for i := range parts {
		parts[i] = mssqlIdent(strings.TrimSpace(parts[i]))
	}
	return strings.Join(parts, ".")
}

// max returns the maximum of a and b.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ---- database/sql seam types ----

// dbConn is a small interface over *sql.DB used to make this package testable.
//
// It intentionally includes only the methods this file needs.
type dbConn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (txConn, error)
	Close() error
}

// txConn is a small interface over *sql.Tx used for testability.
//
// It models the minimal transactional methods required by the SCD2 code path.
type txConn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) rowScanner
	Commit() error
	Rollback() error
}

// rowScanner is a narrow adapter over *sql.Row.Scan.
type rowScanner interface {
	Scan(dest ...any) error
}

// sqlDB wraps *sql.DB to implement dbConn.
type sqlDB struct {
	db *sql.DB
}

// ExecContext executes a non-query statement.
func (s *sqlDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}

// QueryContext executes a query statement and returns rows.
func (s *sqlDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

// BeginTx begins a transaction and returns a txConn wrapper.
func (s *sqlDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (txConn, error) {
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: tx}, nil
}

// Close closes the underlying database handle.
func (s *sqlDB) Close() error { return s.db.Close() }

// sqlTx wraps *sql.Tx to implement txConn.
type sqlTx struct {
	tx *sql.Tx
}

// ExecContext executes a statement within this transaction.
func (s *sqlTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.tx.ExecContext(ctx, query, args...)
}

// QueryRowContext executes a query expected to return at most one row.
func (s *sqlTx) QueryRowContext(ctx context.Context, query string, args ...any) rowScanner {
	return s.tx.QueryRowContext(ctx, query, args...)
}

// Commit commits the transaction.
func (s *sqlTx) Commit() error { return s.tx.Commit() }

// Rollback rolls back the transaction.
func (s *sqlTx) Rollback() error { return s.tx.Rollback() }

// compile-time sanity checks (no runtime cost).
var (
	_ dbConn = (*sqlDB)(nil)
	_ txConn = (*sqlTx)(nil)

	// Silence unused import risk if future edits remove errors usage.
	_ = errors.Is
)
