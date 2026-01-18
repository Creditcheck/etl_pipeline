package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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

func (r *MultiRepo) InsertFactRows(ctx context.Context, table string, columns []string, rows [][]any, dedupeColumns []string) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// SQLite: to "do nothing" on conflicts, use INSERT OR IGNORE + UNIQUE constraint.
	// If dedupeColumns is empty, use plain INSERT.
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
