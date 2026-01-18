package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"etl/internal/storage"
)

// SQL Server supports a maximum of 2100 parameters per statement.
// We keep a small safety margin because some statements can add params later.
//
// References:
//   - SQL Server parameter limit: 2100 total parameters per statement.
const (
	mssqlMaxParams       = 2100
	mssqlParamSafetyMarg = 50

	// Debug and performance knobs are intentionally env-based to avoid widening the
	// backend-agnostic config surface.
	//
	// ETL_MSSQL_DEBUG=1 enables detailed per-call and per-chunk timings inside the repo.
	// This is useful when the engine already logs per InsertFactRows call, but the repo
	// may split that call into multiple SQL statements due to the 2100 parameter limit.
	//
	// ETL_MSSQL_TABLOCK=1 adds "WITH (TABLOCK)" to INSERT targets. For ETL-style loads,
	// this can reduce lock overhead and improve throughput in SQL Server. It is off by
	// default because it changes locking behavior (table-level locks).
	envDebug   = "ETL_MSSQL_DEBUG"
	envTablock = "ETL_MSSQL_TABLOCK"
)

type MultiRepo struct {
	db *sql.DB

	// dbg enables verbose timing logs to help diagnose performance bottlenecks.
	dbg bool

	// tablock enables INSERT ... WITH (TABLOCK).
	// This can speed up ETL inserts but may increase contention with other workloads.
	tablock bool

	// lg is used only when dbg is enabled.
	lg *log.Logger
}

func NewMulti(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
	db, err := sql.Open("sqlserver", cfg.DSN)
	if err != nil {
		return nil, err
	}

	// Important for throughput: allow multiple loader workers to hold multiple physical connections.
	// If these are too low, the client becomes the bottleneck even if loader_workers > 1.
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(64)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &MultiRepo{
		db:      db,
		dbg:     envBool(envDebug),
		tablock: envBool(envTablock),
		lg:      log.New(os.Stdout, "", log.LstdFlags),
	}, nil
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

// EnsureDimensionKeys inserts missing dimension keys idempotently.
//
// Postgres implements this via INSERT ... ON CONFLICT DO NOTHING.
//
// For SQL Server, avoid MERGE:
//   - MERGE is notoriously tricky and can be slow/buggy at scale.
//   - MERGE can fail when the source contains duplicate keys or has concurrency edge cases.
//
// Safer pattern used here:
//
//	INSERT INTO T(key)
//	SELECT S.key
//	FROM (VALUES (...), (...)) AS S(key)
//	LEFT JOIN T ON T.key = S.key
//	WHERE T.key IS NULL;
//
// We also dedupe keys in-memory before issuing a statement to avoid unique violations
// when the same key appears multiple times inside a single batch.
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

	// Critical: SQL Server statements do not automatically dedupe the source set.
	// If the same key appears twice and the target doesn't have it yet, both rows can
	// attempt insertion and hit a UNIQUE constraint.
	keys = dedupeAnyByNormalizeKey(keys)
	if len(keys) == 0 {
		return nil
	}

	maxKeys := mssqlMaxParams - mssqlParamSafetyMarg

	// Performance: execute all chunks in a single transaction.
	// Autocommit per batch is extremely expensive on SQL Server at scale.
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mssql: begin tx EnsureDimensionKeys: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	startAll := time.Now()
	chunks := 0

	for start := 0; start < len(keys); start += maxKeys {
		end := start + maxKeys
		if end > len(keys) {
			end = len(keys)
		}

		chunks++
		startChunk := time.Now()

		if err := r.ensureDimensionKeysBatchTx(ctx, tx, table, keyColumn, keys[start:end]); err != nil {
			return err
		}

		if r.dbg {
			r.lg.Printf("stage=mssql_dim_ensure table=%s keys=%d chunk=%d duration=%s",
				table, end-start, chunks, time.Since(startChunk).Truncate(time.Millisecond))
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mssql: commit EnsureDimensionKeys: %w", err)
	}

	if r.dbg {
		r.lg.Printf("stage=mssql_dim_ensure_done table=%s total_keys=%d chunks=%d duration=%s",
			table, len(keys), chunks, time.Since(startAll).Truncate(time.Millisecond))
	}

	return nil
}

func (r *MultiRepo) ensureDimensionKeysBatchTx(ctx context.Context, tx *sql.Tx, table string, keyColumn string, keys []any) error {
	keys = dedupeAnyByNormalizeKey(keys)
	if len(keys) == 0 {
		return nil
	}

	// INSERT INTO T (key) [WITH (TABLOCK)]
	// SELECT S.key
	// FROM (VALUES (@p1),(@p2),...) AS S([key])
	// LEFT JOIN T ON T.key = S.key
	// WHERE T.key IS NULL;
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(mssqlFQN(table))
	if r.tablock {
		b.WriteString(" WITH (TABLOCK)")
	}
	b.WriteString(" (")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(") SELECT S.")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(" FROM (VALUES ")

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
	b.WriteString(") LEFT JOIN ")
	b.WriteString(mssqlFQN(table))
	b.WriteString(" AS T ON T.")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(" = S.")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(" WHERE T.")
	b.WriteString(mssqlIdent(keyColumn))
	b.WriteString(" IS NULL;")

	if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
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

// InsertFactRows inserts fact rows. If dedupeColumns is provided, inserts must be idempotent.
//
// Postgres does this with INSERT ... ON CONFLICT (dedupeColumns...) DO NOTHING.
//
// For SQL Server we avoid MERGE and instead use the safer anti-join pattern:
//
//	INSERT INTO T(cols...) [WITH (TABLOCK)]
//	SELECT S.cols...
//	FROM (VALUES (...), (...)) AS S(cols...)
//	LEFT JOIN T ON T.k1=S.k1 AND ...
//	WHERE T.k1 IS NULL;
//
// Important: SQL Server does NOT remove duplicates inside S. If the batch contains multiple rows with
// the same dedupe key and the target does not have that key yet, SQL Server will attempt multiple
// inserts and hit a UNIQUE constraint.
//
// To match Postgres behavior (duplicates in the same statement are ignored), we dedupe within each
// chunk in Go (stable: keeps the first occurrence).
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

	perRow := len(columns)
	maxParams := mssqlMaxParams - mssqlParamSafetyMarg
	maxRows := maxParams / perRow
	if maxRows < 1 {
		return 0, fmt.Errorf("mssql: too many columns (%d) to fit within param limit", perRow)
	}

	// Performance: execute all chunks in one transaction.
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("mssql: begin tx InsertFactRows: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	startAll := time.Now()
	chunks := 0

	var total int64
	for start := 0; start < len(rows); start += maxRows {
		end := start + maxRows
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]
		chunks++

		startChunk := time.Now()

		var aff int64
		if len(dedupeColumns) == 0 {
			aff, err = r.insertFactPlainBatchTx(ctx, tx, table, columns, chunk)
		} else {
			// Match Postgres behavior: duplicates inside the same "batch statement" should not error.
			chunk, err = dedupeRowsByColumns(chunk, columns, dedupeColumns)
			if err != nil {
				return total, fmt.Errorf("mssql: dedupe fact rows for %s: %w", table, err)
			}
			if len(chunk) == 0 {
				continue
			}
			aff, err = r.insertFactDoNothingLeftJoinTx(ctx, tx, table, columns, chunk, dedupeColumns)
		}
		if err != nil {
			return total, err
		}
		total += aff

		if r.dbg {
			r.lg.Printf("stage=mssql_fact_insert table=%s chunk=%d rows_in=%d rows_aff=%d duration=%s",
				table, chunks, len(chunk), aff, time.Since(startChunk).Truncate(time.Millisecond))
		}
	}

	if err := tx.Commit(); err != nil {
		return total, fmt.Errorf("mssql: commit InsertFactRows: %w", err)
	}

	if r.dbg {
		r.lg.Printf("stage=mssql_fact_insert_done table=%s chunks=%d total_rows_in=%d total_aff=%d duration=%s",
			table, chunks, len(rows), total, time.Since(startAll).Truncate(time.Millisecond))
	}

	return total, nil
}

func (r *MultiRepo) insertFactPlainBatchTx(
	ctx context.Context,
	tx *sql.Tx,
	table string,
	columns []string,
	rows [][]any,
) (int64, error) {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(mssqlFQN(table))
	if r.tablock {
		b.WriteString(" WITH (TABLOCK)")
	}
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

	res, err := tx.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("mssql: insert into %s: %w", table, err)
	}
	aff, _ := res.RowsAffected()
	return aff, nil
}

// insertFactDoNothingLeftJoinTx performs an idempotent insert using a LEFT JOIN anti-join:
//
//	INSERT INTO T(cols...) [WITH (TABLOCK)]
//	SELECT S.cols...
//	FROM (VALUES (...), (...)) AS S(cols...)
//	LEFT JOIN T ON T.k1=S.k1 AND ...
//	WHERE T.k1 IS NULL;
//
// This is typically faster than a correlated NOT EXISTS predicate on SQL Server for this pattern,
// especially with a UNIQUE index on the dedupeColumns.
func (r *MultiRepo) insertFactDoNothingLeftJoinTx(
	ctx context.Context,
	tx *sql.Tx,
	table string,
	columns []string,
	rows [][]any,
	dedupeColumns []string,
) (int64, error) {
	var b strings.Builder

	b.WriteString("INSERT INTO ")
	b.WriteString(mssqlFQN(table))
	if r.tablock {
		b.WriteString(" WITH (TABLOCK)")
	}
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
		b.WriteString("S.")
		b.WriteString(mssqlIdent(c))
	}

	b.WriteString(" FROM (VALUES ")

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
	b.WriteString(") LEFT JOIN ")
	b.WriteString(mssqlFQN(table))
	b.WriteString(" AS T ON ")

	for i, kc := range dedupeColumns {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString("T.")
		b.WriteString(mssqlIdent(kc))
		b.WriteString(" = S.")
		b.WriteString(mssqlIdent(kc))
	}

	// For anti-join, we check NULL on the first dedupe key.
	// This assumes dedupeColumns are NOT NULL or at least that the unique constraint
	// semantics match the data model. If dedupe keys can be NULL, we should instead
	// add a computed marker or use NOT EXISTS (but most fact unique keys should be NOT NULL).
	b.WriteString(" WHERE T.")
	b.WriteString(mssqlIdent(dedupeColumns[0]))
	b.WriteString(" IS NULL;")

	res, err := tx.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("mssql: insert-do-nothing into %s: %w", table, err)
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

// dedupeAnyByNormalizeKey deduplicates values by storage.NormalizeKey, preserving the first occurrence.
//
// This is required to preserve Postgres-like idempotence when the input stream contains duplicates
// within the same batch and the SQL Server statement does not dedupe them automatically.
func dedupeAnyByNormalizeKey(keys []any) []any {
	seen := make(map[string]struct{}, len(keys))
	out := make([]any, 0, len(keys))
	for _, k := range keys {
		nk := storage.NormalizeKey(k)
		if nk == "" {
			continue
		}
		if _, ok := seen[nk]; ok {
			continue
		}
		seen[nk] = struct{}{}
		out = append(out, k)
	}
	return out
}

// dedupeRowsByColumns deduplicates fact rows on the provided dedupeColumns.
// It preserves the first occurrence of each dedupe key (stable), which mirrors
// Postgres INSERT ... ON CONFLICT DO NOTHING behavior within a single statement.
//
// Key idea:
//   - Postgres tolerates duplicates inside one INSERT statement and effectively inserts one.
//   - SQL Server "INSERT ... WHERE NOT EXISTS" does NOT remove duplicates inside the source S,
//     so duplicates in S can still cause unique constraint violations when the target lacks the key.
//
// This function is intentionally backend-local because it compensates for SQL Server statement semantics.
func dedupeRowsByColumns(rows [][]any, columns []string, dedupeColumns []string) ([][]any, error) {
	if len(rows) == 0 || len(dedupeColumns) == 0 {
		return rows, nil
	}

	indexByName := make(map[string]int, len(columns))
	for i, c := range columns {
		indexByName[c] = i
	}

	keyIdx := make([]int, 0, len(dedupeColumns))
	for _, kc := range dedupeColumns {
		i, ok := indexByName[kc]
		if !ok {
			return nil, fmt.Errorf("dedupe column %q not present in insert columns", kc)
		}
		keyIdx = append(keyIdx, i)
	}

	seen := make(map[string]struct{}, len(rows))
	out := make([][]any, 0, len(rows))

	// Unit separator is very unlikely to appear in normalized keys.
	const sep = "\x1f"
	var b strings.Builder

	for _, row := range rows {
		b.Reset()

		for i, idx := range keyIdx {
			if i > 0 {
				b.WriteString(sep)
			}
			var v any
			if idx >= 0 && idx < len(row) {
				v = row[idx]
			}
			b.WriteString(storage.NormalizeKey(v))
		}

		k := b.String()
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, row)
	}

	return out, nil
}

func envBool(name string) bool {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
	return b
}
