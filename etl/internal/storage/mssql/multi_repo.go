package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/microsoft/go-mssqldb"

	"etl/internal/storage"
)

// SQL Server supports a maximum of 2100 parameters per statement.
// We keep a small safety margin because some statements can add params later.
const (
	mssqlMaxParams       = 2100
	mssqlParamSafetyMarg = 50
)

type MultiRepo struct {
	db *sql.DB
}

func NewMulti(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
	db, err := sql.Open("sqlserver", cfg.DSN)
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
		sqlText, err := buildCreateTableSQL(t)
		if err != nil {
			return err
		}
		if _, err := r.db.ExecContext(ctx, sqlText); err != nil {
			return fmt.Errorf("create table %s: %w", t.Name, err)
		}
	}
	return nil
}

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

	// Minimal contract: the engine inserts only the key column for dimensions.
	// Require conflict target == keyColumn.
	if len(conflictColumns) == 0 {
		conflictColumns = []string{keyColumn}
	}
	if len(conflictColumns) != 1 || !strings.EqualFold(conflictColumns[0], keyColumn) {
		return fmt.Errorf("mssql: EnsureDimensionKeys requires conflict_columns == [%s] (got %v)", keyColumn, conflictColumns)
	}

	// Each key is 1 parameter, so batch by ~ (2100 - margin).
	maxKeys := mssqlMaxParams - mssqlParamSafetyMarg
	for start := 0; start < len(keys); start += maxKeys {
		end := start + maxKeys
		if end > len(keys) {
			end = len(keys)
		}
		if err := r.ensureDimensionKeysBatch(ctx, table, keyColumn, keys[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (r *MultiRepo) ensureDimensionKeysBatch(ctx context.Context, table string, keyColumn string, keys []any) error {
	// MERGE INTO T
	// USING (VALUES (@p1),(@p2),...) AS S([key])
	// ON T.[key]=S.[key]
	// WHEN NOT MATCHED THEN INSERT([key]) VALUES(S.[key]);
	var b strings.Builder
	b.WriteString("MERGE INTO ")
	b.WriteString(mssqlFQN(table))
	b.WriteString(" AS T USING (VALUES ")

	args := make([]any, 0, len(keys))
	for i := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		b.WriteString(mssqlParam(i + 1))
		b.WriteString(")")
		args = append(args, keys[i])
	}
	b.WriteString(") AS S(")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(") ON T.")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(" = S.")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(" WHEN NOT MATCHED THEN INSERT (")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(") VALUES (S.")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(");")

	if _, err := r.db.ExecContext(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("mssql: ensure dimension keys into %s: %w", table, err)
	}
	return nil
}

func (r *MultiRepo) SelectAllKeyValue(
	ctx context.Context,
	table string,
	keyColumn string,
	valueColumn string,
) (map[string]int64, error) {
	q := fmt.Sprintf(
		"SELECT %s, %s FROM %s",
		mssqlIdent(keyColumn),
		mssqlIdent(valueColumn),
		mssqlFQN(table),
	)

	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("mssql: select all %s: %w", table, err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var key any
		var id sql.NullInt64
		if err := rows.Scan(&key, &id); err != nil {
			return nil, fmt.Errorf("mssql: scan %s: %w", table, err)
		}
		if !id.Valid {
			return nil, fmt.Errorf("mssql: %s.%s is NULL; expected generated surrogate key", table, valueColumn)
		}
		out[storage.NormalizeKey(key)] = id.Int64
	}
	return out, rows.Err()
}

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

	// IN clause uses one param per key => chunk to stay <= 2100.
	maxKeys := mssqlMaxParams - mssqlParamSafetyMarg

	out := map[string]int64{}
	for start := 0; start < len(keys); start += maxKeys {
		end := start + maxKeys
		if end > len(keys) {
			end = len(keys)
		}
		part, err := r.selectKeyValueByKeysBatch(ctx, table, keyColumn, valueColumn, keys[start:end])
		if err != nil {
			return nil, err
		}
		for k, v := range part {
			out[k] = v
		}
	}
	return out, nil
}

func (r *MultiRepo) selectKeyValueByKeysBatch(
	ctx context.Context,
	table string,
	keyColumn string,
	valueColumn string,
	keys []any,
) (map[string]int64, error) {
	ph := make([]string, 0, len(keys))
	for i := range keys {
		ph = append(ph, mssqlParam(i+1))
	}

	q := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s IN (%s)",
		mssqlIdent(keyColumn),
		mssqlIdent(valueColumn),
		mssqlFQN(table),
		mssqlIdent(keyColumn),
		strings.Join(ph, ","),
	)

	rows, err := r.db.QueryContext(ctx, q, keys...)
	if err != nil {
		return nil, fmt.Errorf("mssql: select %s by keys: %w", table, err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var key any
		var id sql.NullInt64
		if err := rows.Scan(&key, &id); err != nil {
			return nil, fmt.Errorf("mssql: scan %s by keys: %w", table, err)
		}
		if !id.Valid {
			return nil, fmt.Errorf("mssql: %s.%s is NULL; expected generated surrogate key", table, valueColumn)
		}
		out[storage.NormalizeKey(key)] = id.Int64
	}
	return out, rows.Err()
}

func (r *MultiRepo) InsertFactRows(
	ctx context.Context,
	table string,
	columns []string,
	rows [][]any,
	dedupeColumns []string,
) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if len(columns) == 0 {
		return 0, fmt.Errorf("mssql: InsertFactRows columns empty for %s", table)
	}

	// Each row consumes len(columns) params.
	perRow := len(columns)
	maxParams := mssqlMaxParams - mssqlParamSafetyMarg
	maxRows := maxParams / perRow
	if maxRows < 1 {
		return 0, fmt.Errorf("mssql: too many columns (%d) to fit within param limit", perRow)
	}

	var total int64
	for start := 0; start < len(rows); start += maxRows {
		end := start + maxRows
		if end > len(rows) {
			end = len(rows)
		}
		var n int64
		var err error

		if len(dedupeColumns) == 0 {
			n, err = r.insertFactPlainBatch(ctx, table, columns, rows[start:end])
		} else {
			n, err = r.insertFactMergeDoNothingBatch(ctx, table, columns, rows[start:end], dedupeColumns)
		}
		if err != nil {
			return total, err
		}
		total += n
	}

	return total, nil
}

func (r *MultiRepo) insertFactPlainBatch(
	ctx context.Context,
	table string,
	columns []string,
	rows [][]any,
) (int64, error) {
	// INSERT INTO t (c1,c2) VALUES (@p1,@p2),(@p3,@p4)...
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(mssqlFQN(table))
	b.WriteString(" (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(mssqlIdent(c))
	}
	b.WriteString(") VALUES ")

	args := make([]any, 0, len(rows)*len(columns))
	paramN := 1
	for i, row := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j := range columns {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString(mssqlParam(paramN))
			paramN++
			args = append(args, row[j])
		}
		b.WriteString(")")
	}

	res, err := r.db.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("mssql: insert into %s: %w", table, err)
	}
	aff, _ := res.RowsAffected()
	return aff, nil
}

func (r *MultiRepo) insertFactMergeDoNothingBatch(
	ctx context.Context,
	table string,
	columns []string,
	rows [][]any,
	dedupeColumns []string,
) (int64, error) {
	// MERGE INTO T
	// USING (VALUES (...), (...)) AS S(c1,c2,...)
	// ON T.k1=S.k1 AND T.k2=S.k2
	// WHEN NOT MATCHED THEN INSERT (c1,c2,...) VALUES (S.c1,S.c2,...);
	var b strings.Builder
	b.WriteString("MERGE INTO ")
	b.WriteString(mssqlFQN(table))
	b.WriteString(" AS T USING (VALUES ")

	args := make([]any, 0, len(rows)*len(columns))
	paramN := 1
	for i, row := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j := range columns {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString(mssqlParam(paramN))
			paramN++
			args = append(args, row[j])
		}
		b.WriteString(")")
	}

	b.WriteString(") AS S(")
	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(mssqlIdent(c))
	}
	b.WriteString(") ON ")

	for i, kc := range dedupeColumns {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString("T.")
		b.WriteString(mssqlIdent(kc))
		b.WriteString(" = S.")
		b.WriteString(mssqlIdent(kc))
	}

	b.WriteString(" WHEN NOT MATCHED THEN INSERT (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(mssqlIdent(c))
	}

	b.WriteString(") VALUES (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("S.")
		b.WriteString(mssqlIdent(c))
	}

	b.WriteString(");")

	res, err := r.db.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("mssql: merge insert into %s: %w", table, err)
	}
	aff, _ := res.RowsAffected()
	return aff, nil
}

// -------------------------
// DDL helpers (MSSQL-specific)
// -------------------------

func buildCreateTableSQL(t storage.TableSpec) (string, error) {
	if t.Name == "" {
		return "", fmt.Errorf("table name is empty")
	}

	var parts []string

	if t.PrimaryKey != nil {
		pkType := strings.TrimSpace(strings.ToLower(t.PrimaryKey.Type))
		switch pkType {
		case "serial", "bigserial":
			ty := "INT"
			if pkType == "bigserial" {
				ty = "BIGINT"
			}
			parts = append(parts, fmt.Sprintf("%s %s IDENTITY(1,1) PRIMARY KEY", mssqlIdent(t.PrimaryKey.Name), ty))
		default:
			parts = append(parts, fmt.Sprintf("%s %s PRIMARY KEY", mssqlIdent(t.PrimaryKey.Name), t.PrimaryKey.Type))
		}
	}

	for _, c := range t.Columns {
		if c.Name == "" || c.Type == "" {
			return "", fmt.Errorf("%s has column with empty name/type", t.Name)
		}
		col := fmt.Sprintf("%s %s", mssqlIdent(c.Name), c.Type)

		nullable := true
		if c.Nullable != nil {
			nullable = *c.Nullable
		}
		if !nullable {
			col += " NOT NULL"
		} else {
			col += " NULL"
		}

		if c.References != "" {
			col += " REFERENCES " + mssqlRef(c.References)
		}

		parts = append(parts, col)
	}

	for _, con := range t.Constraints {
		switch strings.ToLower(strings.TrimSpace(con.Kind)) {
		case "unique":
			if len(con.Columns) == 0 {
				return "", fmt.Errorf("%s unique constraint has no columns", t.Name)
			}
			var cols []string
			for _, c := range con.Columns {
				cols = append(cols, mssqlIdent(c))
			}
			parts = append(parts, fmt.Sprintf("UNIQUE (%s)", strings.Join(cols, ", ")))
		default:
			return "", fmt.Errorf("%s unsupported constraint kind: %s", t.Name, con.Kind)
		}
	}

	fullName := mssqlObjectNameLiteral(t.Name)

	return fmt.Sprintf(
		`IF OBJECT_ID(N'%s', N'U') IS NULL
BEGIN
  CREATE TABLE %s (
    %s
  );
END;`,
		fullName,
		mssqlFQN(t.Name),
		strings.Join(parts, ",\n    "),
	), nil
}

func mssqlParam(n int) string { return fmt.Sprintf("@p%d", n) }

func mssqlIdent(id string) string {
	id = strings.TrimSpace(id)
	id = strings.ReplaceAll(id, "]", "]]")
	return "[" + id + "]"
}

func mssqlFQN(fqn string) string {
	fqn = strings.TrimSpace(fqn)
	parts := strings.Split(fqn, ".")
	if len(parts) == 1 {
		return mssqlIdent(parts[0])
	}
	if len(parts) == 2 {
		return mssqlIdent(parts[0]) + "." + mssqlIdent(parts[1])
	}
	return mssqlIdent(fqn)
}

func mssqlObjectNameLiteral(fqn string) string { return strings.TrimSpace(fqn) }

func mssqlRef(ref string) string {
	ref = strings.TrimSpace(ref)
	open := strings.Index(ref, "(")
	close := strings.LastIndex(ref, ")")
	if open < 0 || close < 0 || close < open {
		return ref
	}
	tablePart := strings.TrimSpace(ref[:open])
	colPart := strings.TrimSpace(ref[open+1 : close])
	return fmt.Sprintf("%s(%s)", mssqlFQN(tablePart), mssqlIdent(colPart))
}
