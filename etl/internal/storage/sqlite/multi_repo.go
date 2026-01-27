package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"etl/internal/storage"
)

// MultiRepo implements storage.MultiRepository for SQLite.
//
// Key design points vs Postgres:
//   - SQLite has no native TIMESTAMPTZ type. Even if config says "timestamptz",
//     modernc.org/sqlite stores it with TEXT affinity unless you intentionally
//     store INTEGER/REAL. Therefore, timestamps must be handled carefully.
//   - This repo stores SCD2 timestamps as RFC3339Nano strings for reliable
//     round-trip behavior and easy debugging.
type MultiRepo struct {
	db *sql.DB
}

func init() {
	storage.RegisterMulti("sqlite", NewMulti)
}

func NewMulti(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
	db, err := sql.Open("sqlite", cfg.DSN)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &MultiRepo{db: db}, nil
}

func (r *MultiRepo) Close() { _ = r.db.Close() }

// EnsureTables creates base tables and, when SCD2 history is enabled, also creates
// the corresponding "<table>_history" tables.
//
// This mirrors the Postgres backend behavior and keeps ETL startup idempotent.
//
// Important:
//   - The base table is always created when AutoCreateTable is true.
//   - The history table is created only when Load.Kind=="fact" and Load.History.Enabled.
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
			return fmt.Errorf("create table %s: %w", t.Name, err)
		}
		if histSQL != "" {
			if _, err := r.db.ExecContext(ctx, histSQL); err != nil {
				return fmt.Errorf("create history table %s_history: %w", t.Name, err)
			}
		}
	}
	return nil
}

// EnsureDimensionKeys inserts dimension keys that do not yet exist.
//
// SQLite does not support ON CONFLICT (...) in the same way as Postgres,
// but "INSERT OR IGNORE" works when the target has a UNIQUE/PK constraint.
func (r *MultiRepo) EnsureDimensionKeys(ctx context.Context, table string, keyColumn string, keys []any, conflictColumns []string) error {
	if len(keys) == 0 {
		return nil
	}
	// conflictColumns is ignored for SQLite: OR IGNORE relies on UNIQUE/PK constraints.
	_ = conflictColumns

	var b strings.Builder
	b.WriteString("INSERT OR IGNORE INTO ")
	b.WriteString(table)
	b.WriteString(" (")
	b.WriteString(sqlIdent(keyColumn))
	b.WriteString(") VALUES ")

	args := make([]any, 0, len(keys))
	for i := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(?)")
		args = append(args, keys[i])
	}

	_, err := r.db.ExecContext(ctx, b.String(), args...)
	return err
}

func (r *MultiRepo) SelectAllKeyValue(ctx context.Context, table, keyColumn, valueColumn string) (map[string]int64, error) {
	q := fmt.Sprintf(`SELECT %s, %s FROM %s`, sqlIdent(keyColumn), sqlIdent(valueColumn), table)
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var k any
		var id sql.NullInt64
		if err := rows.Scan(&k, &id); err != nil {
			return nil, err
		}
		if !id.Valid {
			return nil, fmt.Errorf(
				"sqlite: %s.%s is NULL; primary key not auto-generated (check primary_key.type mapping, e.g. use serial->INTEGER PRIMARY KEY)",
				table, valueColumn,
			)
		}
		out[storage.NormalizeKey(k)] = id.Int64
	}
	return out, rows.Err()
}

func (r *MultiRepo) SelectKeyValueByKeys(ctx context.Context, table, keyColumn, valueColumn string, keys []any) (map[string]int64, error) {
	if len(keys) == 0 {
		return map[string]int64{}, nil
	}
	ph := strings.TrimRight(strings.Repeat("?,", len(keys)), ",")
	q := fmt.Sprintf(
		`SELECT %s, %s FROM %s WHERE %s IN (%s)`,
		sqlIdent(keyColumn), sqlIdent(valueColumn), table, sqlIdent(keyColumn), ph,
	)

	rows, err := r.db.QueryContext(ctx, q, keys...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var k any
		var id sql.NullInt64
		if err := rows.Scan(&k, &id); err != nil {
			return nil, err
		}
		if !id.Valid {
			return nil, fmt.Errorf(
				"sqlite: %s.%s is NULL; primary key not auto-generated (check primary_key.type mapping, e.g. use serial->INTEGER PRIMARY KEY)",
				table, valueColumn,
			)
		}
		out[storage.NormalizeKey(k)] = id.Int64
	}
	return out, rows.Err()
}

// InsertFactRows inserts fact rows into the destination table.
//
// Behavior depends on the table spec:
//
//   - If spec.Load.Kind != "fact", behaves like a plain bulk insert.
//   - If spec.Load.History is nil or disabled, behaves like a plain bulk insert
//     (optionally using dedupeColumns via INSERT OR IGNORE).
//   - If spec.Load.Kind == "fact" and spec.Load.History.Enabled is true,
//     applies SCD2 semantics using base + history tables.
//
// For SCD2:
//   - Business key is History.BusinessKey.
//   - "Current" is defined as ValidToColumn IS NULL.
//   - If row_hash exists in columns, we compare row_hash to detect changes.
//     Otherwise we fall back to full-row comparison.
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

	if spec.Load.Kind != "fact" || spec.Load.History == nil || !spec.Load.History.Enabled {
		return r.insertPlain(ctx, table, columns, rows, dedupeColumns)
	}

	return r.insertSCD2(ctx, spec, table, columns, rows, time.Now().UTC())
}

func sqlIdent(id string) string {
	// SQLite supports "quoted identifiers"
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

// buildCreateSQL generates DDL for:
//   - base (current) table
//   - optional history table (<name>_history) when SCD2 is enabled
//
// It also ensures SCD2 metadata columns exist exactly once by appending them
// only if they are not already present in TableSpec.Columns.
func buildCreateSQL(t storage.TableSpec) (baseSQL, histSQL string, err error) {
	if strings.TrimSpace(t.Name) == "" {
		return "", "", fmt.Errorf("table name is empty")
	}

	baseSQL, err = buildCreateTableSQL(t /*includePrimaryKey=*/, true)
	if err != nil {
		return "", "", err
	}

	if t.Load.History != nil && t.Load.History.Enabled {
		// History table should not inherit PrimaryKeySpec; SCD2 writer never provides it.
		histSpec := t
		histSpec.Name = t.Name + "_history"
		histSpec.PrimaryKey = nil

		// IMPORTANT:
		// The history table must allow multiple versions per business key.
		// Therefore we must NOT carry over UNIQUE constraints from the base table,
		// especially the business-key UNIQUE that enforces "exactly one current row".
		histSpec.Constraints = nil

		histSQL, err = buildCreateTableSQL(histSpec /*includePrimaryKey=*/, false)
		if err != nil {
			return "", "", err
		}
	}

	return baseSQL, histSQL, nil
}

func buildCreateTableSQL(t storage.TableSpec, includePrimaryKey bool) (string, error) {
	var parts []string

	if includePrimaryKey && t.PrimaryKey != nil {
		pkType := strings.TrimSpace(strings.ToLower(t.PrimaryKey.Type))

		// Translate common postgres/mssql-ish pk types into sqlite semantics.
		// "INTEGER PRIMARY KEY" is special in sqlite: it becomes the rowid and auto-generates values.
		switch pkType {
		case "serial", "bigserial":
			parts = append(parts, fmt.Sprintf(`%s INTEGER PRIMARY KEY AUTOINCREMENT`, sqlIdent(t.PrimaryKey.Name)))
		case "int identity", "integer identity", "identity":
			parts = append(parts, fmt.Sprintf(`%s INTEGER PRIMARY KEY AUTOINCREMENT`, sqlIdent(t.PrimaryKey.Name)))
		default:
			parts = append(parts, fmt.Sprintf(`%s %s PRIMARY KEY`, sqlIdent(t.PrimaryKey.Name), t.PrimaryKey.Type))
		}
	}

	// Track explicitly configured columns to avoid duplicating SCD2 metadata columns.
	configured := configuredColumnSet(t)

	for _, c := range t.Columns {
		col := fmt.Sprintf("%s %s", sqlIdent(c.Name), c.Type)
		nullable := true
		if c.Nullable != nil {
			nullable = *c.Nullable
		}
		if !nullable {
			col += " NOT NULL"
		}
		// SQLite supports REFERENCES, but enforcement depends on PRAGMA foreign_keys=ON.
		if c.References != "" {
			col += " REFERENCES " + c.References
		}
		parts = append(parts, col)
	}

	if t.Load.History != nil && t.Load.History.Enabled {
		h := t.Load.History
		if !configured[strings.ToLower(strings.TrimSpace(h.ValidFromColumn))] {
			parts = append(parts, fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, sqlIdent(h.ValidFromColumn)))
		}
		if !configured[strings.ToLower(strings.TrimSpace(h.ValidToColumn))] {
			parts = append(parts, fmt.Sprintf(`%s TIMESTAMPTZ`, sqlIdent(h.ValidToColumn)))
		}
		if !configured[strings.ToLower(strings.TrimSpace(h.ChangedAtColumn))] {
			parts = append(parts, fmt.Sprintf(`%s TIMESTAMPTZ NOT NULL`, sqlIdent(h.ChangedAtColumn)))
		}
	}

	for _, con := range t.Constraints {
		if con.Kind != "unique" {
			return "", fmt.Errorf("%s unsupported constraint kind: %s", t.Name, con.Kind)
		}
		var cols []string
		for _, c := range con.Columns {
			cols = append(cols, sqlIdent(c))
		}
		parts = append(parts, fmt.Sprintf("UNIQUE (%s)", strings.Join(cols, ", ")))
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n);", t.Name, strings.Join(parts, ",\n  ")), nil
}

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

// insertPlain performs SQLite multi-row insert.
//
// If dedupeColumns is non-empty, uses "INSERT OR IGNORE" which requires a UNIQUE
// constraint matching those columns in the destination table.
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

	insertPrefix := "INSERT INTO "
	if len(dedupeColumns) > 0 {
		insertPrefix = "INSERT OR IGNORE INTO "
	}

	colList := make([]string, 0, len(columns))
	for _, c := range columns {
		colList = append(colList, sqlIdent(c))
	}
	placeholders := "(" + strings.TrimRight(strings.Repeat("?,", len(columns)), ",") + ")"

	var b strings.Builder
	b.WriteString(insertPrefix)
	b.WriteString(table)
	b.WriteString(" (")
	b.WriteString(strings.Join(colList, ", "))
	b.WriteString(") VALUES ")

	args := make([]any, 0, len(rows)*len(columns))
	for i, row := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(placeholders)
		args = append(args, row...)
	}

	res, err := r.db.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// insertSCD2 applies SCD2 semantics in a single transaction.
//
// The flow per incoming row:
//   - Fetch current row by business key (valid_to IS NULL).
//   - If no current row -> insert new current row.
//   - If row unchanged -> no-op.
//   - If row changed -> insert old current into history (closing it) and update current to new values.
func (r *MultiRepo) insertSCD2(
	ctx context.Context,
	spec storage.TableSpec,
	table string,
	columns []string,
	rows [][]any,
	now time.Time,
) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	h := spec.Load.History
	base := spec.Name
	hist := base + "_history"

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	// row_hash is optional but recommended. If present, we use it as the authoritative
	// "did anything change?" signal.
	rowHashIdx, hasRowHash := indexOfColumn(columns, "row_hash")

	var affected int64
	for _, row := range rows {
		keyVals, err := extractKeyValues(columns, row, h.BusinessKey)
		if err != nil {
			return affected, err
		}

		current, validFrom, err := fetchCurrentRow(ctx, tx, base, columns, h, keyVals)
		if err != nil {
			return affected, err
		}

		if current == nil {
			if err := insertCurrent(ctx, tx, base, columns, row, h, now); err != nil {
				return affected, err
			}
			affected++
			continue
		}

		// Idempotency/change detection:
		//   - Prefer row_hash when available.
		//   - Otherwise compare all columns (slower and more brittle).
		changed := true
		if hasRowHash {
			changed = !equalScalar(current[rowHashIdx], row[rowHashIdx])
		} else {
			changed = !rowsEqual(current, row)
		}
		if !changed {
			continue
		}

		if err := moveToHistory(ctx, tx, hist, columns, current, validFrom, h, now); err != nil {
			return affected, err
		}
		if err := updateCurrent(ctx, tx, base, columns, row, h, keyVals, now); err != nil {
			return affected, err
		}
		affected++
	}

	if err := tx.Commit(); err != nil {
		return affected, err
	}
	return affected, nil
}

func extractKeyValues(columns []string, row []any, keyCols []string) ([]any, error) {
	if len(keyCols) == 0 {
		return nil, fmt.Errorf("scd2: business_key must not be empty")
	}
	if len(columns) != len(row) {
		return nil, fmt.Errorf("scd2: row length %d != columns length %d", len(row), len(columns))
	}

	pos := make(map[string]int, len(columns))
	for i, c := range columns {
		pos[c] = i
	}

	out := make([]any, 0, len(keyCols))
	for _, k := range keyCols {
		i, ok := pos[k]
		if !ok {
			return nil, fmt.Errorf("scd2: business_key column %q not found in columns", k)
		}
		out = append(out, row[i])
	}
	return out, nil
}

// fetchCurrentRow selects the current row (valid_to IS NULL) for a given business key.
//
// Important SQLite behavior:
//   - valid_from is stored as TEXT, so we scan it as string and parse manually.
func fetchCurrentRow(
	ctx context.Context,
	tx *sql.Tx,
	baseTable string,
	columns []string,
	h *storage.HistorySpec,
	keyVals []any,
) ([]any, time.Time, error) {
	where, args := buildKeyWhere(h.BusinessKey, keyVals)
	q := fmt.Sprintf(
		`SELECT %s, %s FROM %s WHERE %s AND %s IS NULL LIMIT 1`,
		selectColumnList(columns),
		sqlIdent(h.ValidFromColumn),
		baseTable,
		where,
		sqlIdent(h.ValidToColumn),
	)

	row := tx.QueryRowContext(ctx, q, args...)

	out := make([]any, len(columns))
	scan := make([]any, len(columns))
	for i := range out {
		scan[i] = &out[i]
	}

	// Scan valid_from as string (SQLite driver typically returns TEXT).
	var validFromRaw sql.NullString
	if err := row.Scan(append(scan, &validFromRaw)...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, time.Time{}, nil
		}
		return nil, time.Time{}, err
	}
	if !validFromRaw.Valid || strings.TrimSpace(validFromRaw.String) == "" {
		return nil, time.Time{}, fmt.Errorf("sqlite scd2: %s.%s is NULL/empty for current row", baseTable, h.ValidFromColumn)
	}

	validFrom, err := parseSQLiteTime(validFromRaw.String)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("sqlite scd2: parse %s.%s=%q: %w", baseTable, h.ValidFromColumn, validFromRaw.String, err)
	}

	return out, validFrom, nil
}

func rowsEqual(a []any, b []any) bool {
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

// moveToHistory inserts the prior current row into the history table and closes it.
//
// We preserve valid_from from the *existing* row, and set:
//   - valid_to   = now
//   - changed_at = now
//
// Timestamps are stored as RFC3339Nano strings for SQLite stability.
func moveToHistory(
	ctx context.Context,
	tx *sql.Tx,
	historyTable string,
	columns []string,
	current []any,
	validFrom time.Time,
	h *storage.HistorySpec,
	now time.Time,
) error {
	hCols := append([]string{}, columns...)
	hCols = append(hCols, h.ValidFromColumn, h.ValidToColumn, h.ChangedAtColumn)

	placeholders := "(" + strings.TrimRight(strings.Repeat("?,", len(hCols)), ",") + ")"
	q := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES %s`,
		historyTable,
		joinIdentList(hCols),
		placeholders,
	)

	args := make([]any, 0, len(hCols))
	args = append(args, current...)
	args = append(args, formatSQLiteTime(validFrom), formatSQLiteTime(now), formatSQLiteTime(now))

	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

func updateCurrent(
	ctx context.Context,
	tx *sql.Tx,
	baseTable string,
	columns []string,
	incoming []any,
	h *storage.HistorySpec,
	keyVals []any,
	now time.Time,
) error {
	// Build SQL with placeholders that exactly match args.
	q, args := buildUpdateCurrentSQL(baseTable, columns, incoming, h, keyVals, now)
	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

func insertCurrent(
	ctx context.Context,
	tx *sql.Tx,
	baseTable string,
	columns []string,
	incoming []any,
	h *storage.HistorySpec,
	now time.Time,
) error {
	q, args := buildInsertCurrentSQL(baseTable, columns, incoming, h, now)
	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

// buildInsertCurrentSQL inserts a new current row and opens a new version.
//
// We set:
//   - valid_from = now
//   - valid_to   = NULL  (literal NULL in SQL)
//   - changed_at = now
//
// For SQLite, timestamps are stored as RFC3339Nano strings.
func buildInsertCurrentSQL(baseTable string, columns []string, incoming []any, h *storage.HistorySpec, now time.Time) (string, []any) {
	insCols := append([]string{}, columns...)
	insCols = append(insCols, h.ValidFromColumn, h.ValidToColumn, h.ChangedAtColumn)

	// One placeholder per fact column, plus valid_from and changed_at.
	// valid_to is literal NULL.
	valueParts := make([]string, 0, len(insCols))
	for range columns {
		valueParts = append(valueParts, "?")
	}
	valueParts = append(valueParts, "?", "NULL", "?")

	q := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		baseTable,
		joinIdentList(insCols),
		strings.Join(valueParts, ","),
	)

	args := make([]any, 0, len(columns)+2)
	args = append(args, incoming...)
	args = append(args, formatSQLiteTime(now), formatSQLiteTime(now))
	return q, args
}

// buildUpdateCurrentSQL overwrites the current row and opens a new version.
//
// We set:
//   - <fact columns> = incoming
//   - valid_from = now
//   - valid_to = NULL (literal NULL)
//   - changed_at = now
//
// We match only the current row:
//
//	WHERE <business key> AND valid_to IS NULL
func buildUpdateCurrentSQL(baseTable string, columns []string, incoming []any, h *storage.HistorySpec, keyVals []any, now time.Time) (string, []any) {
	setParts := make([]string, 0, len(columns)+3)
	for _, c := range columns {
		setParts = append(setParts, fmt.Sprintf("%s = ?", sqlIdent(c)))
	}
	setParts = append(setParts, fmt.Sprintf("%s = ?", sqlIdent(h.ValidFromColumn)))
	setParts = append(setParts, fmt.Sprintf("%s = NULL", sqlIdent(h.ValidToColumn)))
	setParts = append(setParts, fmt.Sprintf("%s = ?", sqlIdent(h.ChangedAtColumn)))

	where, whereArgs := buildKeyWhere(h.BusinessKey, keyVals)

	q := fmt.Sprintf(
		`UPDATE %s SET %s WHERE %s AND %s IS NULL`,
		baseTable,
		strings.Join(setParts, ", "),
		where,
		sqlIdent(h.ValidToColumn),
	)

	args := make([]any, 0, len(columns)+2+len(whereArgs))
	args = append(args, incoming...)
	args = append(args, formatSQLiteTime(now), formatSQLiteTime(now))
	args = append(args, whereArgs...)
	return q, args
}

// buildKeyWhere builds a "k1 = ? AND k2 = ? ..." clause and args.
func buildKeyWhere(keyCols []string, keyVals []any) (string, []any) {
	parts := make([]string, 0, len(keyCols))
	args := make([]any, 0, len(keyCols))
	for i, k := range keyCols {
		parts = append(parts, fmt.Sprintf("%s = ?", sqlIdent(k)))
		args = append(args, keyVals[i])
	}
	return strings.Join(parts, " AND "), args
}

func selectColumnList(columns []string) string {
	out := make([]string, 0, len(columns))
	for _, c := range columns {
		out = append(out, sqlIdent(c))
	}
	return strings.Join(out, ", ")
}

func joinIdentList(columns []string) string {
	out := make([]string, 0, len(columns))
	for _, c := range columns {
		out = append(out, sqlIdent(c))
	}
	return strings.Join(out, ", ")
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

// equalScalar compares two scalar values (used primarily for row_hash).
//
// It handles common database/sql differences where TEXT can scan as []byte or string.
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

// formatSQLiteTime formats a time as RFC3339Nano in UTC.
// We store timestamps as TEXT for reliable scanning/parsing with modernc.org/sqlite.
func formatSQLiteTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// parseSQLiteTime parses timestamps returned by SQLite into time.Time.
//
// Supported formats:
//   - RFC3339Nano (what we write)
//   - RFC3339
//   - Common "SQLite-like" formats used by other tools/libs:
//     "2006-01-02 15:04:05Z07:00"
//     "2006-01-02 15:04:05.999999999Z07:00"
//     "2006-01-02 15:04:05" (interpreted as UTC)
func parseSQLiteTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time string")
	}

	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if layout == "2006-01-02 15:04:05" {
			if ts, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
				return ts.UTC(), nil
			}
			continue
		}
		if ts, err := time.Parse(layout, s); err == nil {
			return ts.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format: %q", s)
}
