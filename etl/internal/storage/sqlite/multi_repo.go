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

func (r *MultiRepo) EnsureTables(ctx context.Context, tables []storage.TableSpec) error {
	for _, t := range tables {
		if !t.AutoCreateTable {
			continue
		}
		sql, err := buildCreateTableSQL(t)
		if err != nil {
			return err
		}
		if _, err := r.db.ExecContext(ctx, sql); err != nil {
			return fmt.Errorf("create table %s: %w", t.Name, err)
		}
	}
	return nil
}

func (r *MultiRepo) EnsureDimensionKeys(ctx context.Context, table string, keyColumn string, keys []any, conflictColumns []string) error {
	if len(keys) == 0 {
		return nil
	}
	// SQLite: INSERT OR IGNORE ignores UNIQUE/PK conflicts
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
			return nil, fmt.Errorf("sqlite: %s.%s is NULL; primary key not auto-generated (check primary_key.type mapping, e.g. use serial->INTEGER PRIMARY KEY)", table, valueColumn)
		}
		out[storage.NormalizeKey(k)] = id.Int64
	}
	//	for rows.Next() {
	//		var k any
	//		var id int64
	//		if err := rows.Scan(&k, &id); err != nil {
	//			return nil, err
	//		}
	//		out[storage.NormalizeKey(k)] = id
	//	}
	return out, rows.Err()
}

func (r *MultiRepo) SelectKeyValueByKeys(ctx context.Context, table, keyColumn, valueColumn string, keys []any) (map[string]int64, error) {
	if len(keys) == 0 {
		return map[string]int64{}, nil
	}
	ph := strings.Repeat("?,", len(keys))
	ph = strings.TrimRight(ph, ",")
	q := fmt.Sprintf(`SELECT %s, %s FROM %s WHERE %s IN (%s)`,
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
			return nil, fmt.Errorf("sqlite: %s.%s is NULL; primary key not auto-generated (check primary_key.type mapping, e.g. use serial->INTEGER PRIMARY KEY)", table, valueColumn)
		}
		out[storage.NormalizeKey(k)] = id.Int64

		//		var k any
		//		var id int64
		//		if err := rows.Scan(&k, &id); err != nil {
		//			return nil, err
		//		}
		//		out[storage.NormalizeKey(k)] = id
	}
	return out, rows.Err()
}

// InsertFactRows inserts fact rows into the destination table.
//
// Behavior depends on the table spec:
//
//   - If spec.Load.Kind != "fact", this function behaves like a plain bulk insert.
//
//   - If spec.Load.History is nil or spec.Load.History.Enabled is false, this function behaves
//     like a plain bulk insert (optionally using dedupeColumns for "ignore conflicts").
//
//   - If spec.Load.Kind == "fact" and spec.Load.History.Enabled is true, this function applies
//     SCD2 (Slowly Changing Dimension Type 2) semantics:
//
//   - Rows are matched by History.BusinessKey (must be a subset of `columns`).
//
//   - If an incoming row matches an existing current row by business key and values are identical,
//     the operation is a no-op (idempotent).
//
//   - If an incoming row matches by business key but values differ, the previous current row is
//     copied into the history table (spec.Name+"_history") and the current row is updated.
//
//   - If an incoming row does not match any current row by business key, it is inserted as a
//     new current row.
//
//   - If a record changes and later reverts to a previous value, that revert is treated as a new
//     version (another history entry is created).
//
// Metadata columns used by SCD2 are read from spec.Load.History:
//   - ValidFromColumn: when this version became current (set to now())
//   - ValidToColumn: when this version stopped being current (set to now() on close; NULL for current)
//   - ChangedAtColumn: system time when the change was written (set to now())
//
// Notes:
//   - History is only supported for facts by design.
//   - For SQLite, SCD2 is implemented via read/compare/update inside a transaction.
//   - This method assumes the base table and history table exist with the expected columns.
//     EnsureTables should create them when AutoCreateTable is enabled.
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

func buildCreateTableSQL(t storage.TableSpec) (string, error) {
	// Similar to postgres, but types differ; keep your explicit types in config if desired.
	// Minimal "create if not exists" builder.
	if t.Name == "" {
		return "", fmt.Errorf("table name is empty")
	}
	var parts []string
	//	if t.PrimaryKey != nil {
	//		// In SQLite, INTEGER PRIMARY KEY is special; config can use that explicitly.
	//		parts = append(parts, fmt.Sprintf(`%s %s PRIMARY KEY`, sqlIdent(t.PrimaryKey.Name), t.PrimaryKey.Type))
	//	}
	if t.PrimaryKey != nil {
		pkType := strings.TrimSpace(strings.ToLower(t.PrimaryKey.Type))

		// Translate common postgres/mssql-ish pk types into sqlite semantics.
		// "INTEGER PRIMARY KEY" is special in sqlite: it becomes the rowid and auto-generates values.
		switch pkType {
		case "serial", "bigserial":
			parts = append(parts, fmt.Sprintf(`%s INTEGER PRIMARY KEY AUTOINCREMENT`, sqlIdent(t.PrimaryKey.Name)))
		case "int identity", "integer identity", "identity":
			parts = append(parts, fmt.Sprintf(`%s INTEGER PRIMARY KEY AUTOINCREMENT`, sqlIdent(t.PrimaryKey.Name)))
		default:
			// Allow explicit sqlite types like "integer"
			parts = append(parts, fmt.Sprintf(`%s %s PRIMARY KEY`, sqlIdent(t.PrimaryKey.Name), t.PrimaryKey.Type))
		}
	}

	for _, c := range t.Columns {
		col := fmt.Sprintf("%s %s", sqlIdent(c.Name), c.Type)
		nullable := true
		if c.Nullable != nil {
			nullable = *c.Nullable
		}
		if !nullable {
			col += " NOT NULL"
		}
		// SQLite does support REFERENCES, but enforcement depends on PRAGMA foreign_keys=ON.
		if c.References != "" {
			col += " REFERENCES " + c.References
		}
		parts = append(parts, col)
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

// insertPlain performs the existing SQLite multi-row insert behavior.
//
// If dedupeColumns is non-empty, this uses "INSERT OR IGNORE" (requiring a UNIQUE
// constraint on the target table matching dedupeColumns) to avoid inserting duplicates.
// Otherwise it uses a plain "INSERT INTO".
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

// insertSCD2 applies SCD2 semantics for fact rows.
//
// It matches rows by the configured business key, compares the current row to the
// incoming row, and if different, moves the prior current row into a history table
// and updates the current row.
//
// The operation runs inside a single transaction to ensure that:
//   - closing the previous version
//   - inserting history
//   - updating current
//
// happen atomically.
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

	var affected int64
	for _, row := range rows {
		// Extract business key values from this incoming row.
		keyVals, err := extractKeyValues(columns, row, h.BusinessKey)
		if err != nil {
			return affected, err
		}

		// Fetch current row by business key (if any).
		current, err := fetchCurrentRow(ctx, tx, base, columns, h, keyVals)
		if err != nil {
			return affected, err
		}

		// If identical, no-op (idempotent).
		if current != nil && rowsEqual(current, row) {
			continue
		}

		if current == nil {
			// New key: insert as current.
			if err := insertCurrent(ctx, tx, base, columns, row, h, now); err != nil {
				return affected, err
			}
			affected++
			continue
		}

		// Existing key but values changed:
		// 1) copy prior current row to history table (closing it),
		// 2) update current row to new values (opening new version).
		if err := moveToHistory(ctx, tx, hist, columns, current, h, now); err != nil {
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

// extractKeyValues returns the values for the business key columns from an incoming row.
//
// It validates that every key column exists in `columns` and returns its value
// from the corresponding position in `row`.
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

// fetchCurrentRow selects the current row from the base table by business key.
//
// "Current" is defined as ValidToColumn IS NULL.
// Returns nil, nil when no current row exists.
func fetchCurrentRow(
	ctx context.Context,
	tx *sql.Tx,
	baseTable string,
	columns []string,
	h *storage.HistorySpec,
	keyVals []any,
) ([]any, error) {
	// SELECT <cols> FROM base WHERE k1=? AND k2=? ... AND valid_to IS NULL LIMIT 1;
	where, args := buildKeyWhere(columns, h.BusinessKey, keyVals)
	q := fmt.Sprintf(
		`SELECT %s FROM %s WHERE %s AND %s IS NULL LIMIT 1`,
		selectColumnList(columns),
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
	if err := row.Scan(scan...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return out, nil
}

// rowsEqual compares two row slices for equality, using fmt.Sprint normalization.
//
// This is a pragmatic equality check suitable for SQLite SCD2 where input rows
// often contain basic scalar types. If you require strict typing semantics,
// replace this with type-aware comparison.
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
// It writes the same business columns as the base table, plus history metadata:
//   - ValidToColumn is set to now()
//   - ChangedAtColumn is set to now()
//
// The history table is assumed to have at least the same columns as the base table
// plus the metadata columns.
func moveToHistory(
	ctx context.Context,
	tx *sql.Tx,
	historyTable string,
	columns []string,
	current []any,
	h *storage.HistorySpec,
	now time.Time,
) error {
	// Insert into history: (all base columns + valid_to + changed_at).
	// NOTE: This assumes base columns list does not already include those metadata columns.
	hCols := append([]string{}, columns...)
	hCols = append(hCols, h.ValidToColumn, h.ChangedAtColumn)

	placeholders := "(" + strings.TrimRight(strings.Repeat("?,", len(hCols)), ",") + ")"
	q := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES %s`,
		historyTable,
		joinIdentList(hCols),
		placeholders,
	)

	args := make([]any, 0, len(hCols))
	args = append(args, current...)
	args = append(args, now, now)

	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

// updateCurrent updates the current row in the base table to the incoming values
// and opens a new version.
//
// It sets:
//   - ValidFromColumn = now()
//   - ValidToColumn = NULL
//   - ChangedAtColumn = now()
//
// It matches the row by business key AND ValidToColumn IS NULL.
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
	setCols := append([]string{}, columns...)
	setCols = append(setCols, h.ValidFromColumn, h.ValidToColumn, h.ChangedAtColumn)

	setClause := buildSetClause(setCols, []string{h.ValidToColumn})
	where, whereArgs := buildKeyWhere(columns, h.BusinessKey, keyVals)

	q := fmt.Sprintf(
		`UPDATE %s SET %s WHERE %s AND %s IS NULL`,
		baseTable,
		setClause,
		where,
		sqlIdent(h.ValidToColumn),
	)

	// args are: <all column values> + valid_from + valid_to(NULL) + changed_at + whereArgs...
	args := make([]any, 0, len(setCols)+len(whereArgs))
	args = append(args, incoming...)
	args = append(args, now, nil, now)
	args = append(args, whereArgs...)

	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

// insertCurrent inserts a new current row into the base table and opens a new version.
//
// It sets:
//   - ValidFromColumn = now()
//   - ValidToColumn = NULL
//   - ChangedAtColumn = now()
func insertCurrent(
	ctx context.Context,
	tx *sql.Tx,
	baseTable string,
	columns []string,
	incoming []any,
	h *storage.HistorySpec,
	now time.Time,
) error {
	insCols := append([]string{}, columns...)
	insCols = append(insCols, h.ValidFromColumn, h.ValidToColumn, h.ChangedAtColumn)

	q := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		baseTable,
		joinIdentList(insCols),
		strings.TrimRight(strings.Repeat("?,", len(insCols)), ","),
	)

	args := make([]any, 0, len(insCols))
	args = append(args, incoming...)
	args = append(args, now, nil, now)

	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

// buildKeyWhere builds a "k1 = ? AND k2 = ? ..." clause and returns it with args.
//
// keyVals must be the same length as keyCols.
func buildKeyWhere(columns []string, keyCols []string, keyVals []any) (string, []any) {
	parts := make([]string, 0, len(keyCols))
	args := make([]any, 0, len(keyCols))
	for i, k := range keyCols {
		parts = append(parts, fmt.Sprintf("%s = ?", sqlIdent(k)))
		args = append(args, keyVals[i])
	}
	return strings.Join(parts, " AND "), args
}

// buildSetClause builds a "<col>=?,<col>=?,..." clause, optionally skipping columns.
//
// skipCols is a set of column names to skip in SET (e.g. to avoid setting valid_to twice).
func buildSetClause(cols []string, skipCols []string) string {
	skip := map[string]struct{}{}
	for _, s := range skipCols {
		skip[s] = struct{}{}
	}
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, ok := skip[c]; ok {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s = ?", sqlIdent(c)))
	}
	// valid_to is handled by appending nil in args; we include it here if desired by not skipping it.
	return strings.Join(parts, ", ")
}

// selectColumnList returns a comma-separated identifier list for SELECT clauses.
func selectColumnList(columns []string) string {
	out := make([]string, 0, len(columns))
	for _, c := range columns {
		out = append(out, sqlIdent(c))
	}
	return strings.Join(out, ", ")
}

// joinIdentList returns a comma-separated identifier list for INSERT column lists.
func joinIdentList(columns []string) string {
	out := make([]string, 0, len(columns))
	for _, c := range columns {
		out = append(out, sqlIdent(c))
	}
	return strings.Join(out, ", ")
}
