package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"etl/internal/storage"
)

const pgMaxParams = 65535

type MultiRepo struct {
	pool *pgxpool.Pool
}

func NewMulti(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		return nil, err
	}
	return &MultiRepo{pool: pool}, nil
}

func (r *MultiRepo) Close() { r.pool.Close() }

func (r *MultiRepo) EnsureTables(ctx context.Context, tables []storage.TableSpec) error {
	for _, t := range tables {
		if !t.AutoCreateTable {
			continue
		}
		sql, err := buildCreateTableSQL(t)
		if err != nil {
			return err
		}
		if _, err := r.pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("create table %s: %w", t.Name, err)
		}
	}
	return nil
}

// EnsureDimensionKeys inserts missing dimension keys with ON CONFLICT DO NOTHING.
// Signature MUST match storage.MultiRepository: keys is []any.
//
// Important detail: for text-like keys, we bind args as []string and for lookups we use []string for ANY($1)
// so pgx encodes a proper text[] array. For numeric-like keys we bind as []int64.
func (r *MultiRepo) EnsureDimensionKeys(ctx context.Context, table, keyColumn string, keys []any, conflictColumns []string) error {
	//	if len(keys) > 0 {
	//		fmt.Printf("DEBUG EnsureDimensionKeys table=%s col=%s key0=%v type0=%T\n", table, keyColumn, keys[0], keys[0])
	//	}

	if len(keys) == 0 {
		return nil
	}
	if len(conflictColumns) == 0 {
		conflictColumns = []string{keyColumn}
	}

	kind := inferKeyKind(keys)

	// normalize + dedupe
	var strKeys []string
	var intKeys []int64

	switch kind {
	case keyText:
		strKeys = dedupeStrings(coerceKeysToStrings(keys))
		if len(strKeys) == 0 {
			return nil
		}
	case keyInt:
		intKeys = dedupeInt64(coerceKeysToInt64(keys))
		if len(intKeys) == 0 {
			return nil
		}
	default:
		// fallback: insert as-is (still dedupe by NormalizeKey)
		keys = dedupeAnyByNormalizeKey(keys)
		if len(keys) == 0 {
			return nil
		}
	}

	chunkSize := 1000 // batch size (safe + predictable)

	for start := 0; ; {
		var (
			n    int
			args []any
		)

		switch kind {
		case keyText:
			if start >= len(strKeys) {
				return nil
			}
			n = chunkSize
			if start+n > len(strKeys) {
				n = len(strKeys) - start
			}
			args = make([]any, 0, n)
			for i := 0; i < n; i++ {
				args = append(args, strKeys[start+i])
			}

		case keyInt:
			if start >= len(intKeys) {
				return nil
			}
			n = chunkSize
			if start+n > len(intKeys) {
				n = len(intKeys) - start
			}
			args = make([]any, 0, n)
			for i := 0; i < n; i++ {
				args = append(args, intKeys[start+i])
			}

		default:
			if start >= len(keys) {
				return nil
			}
			n = chunkSize
			if start+n > len(keys) {
				n = len(keys) - start
			}
			args = make([]any, 0, n)
			for i := 0; i < n; i++ {
				args = append(args, keys[start+i])
			}
		}

		var b strings.Builder
		b.WriteString("INSERT INTO ")
		b.WriteString(table)
		b.WriteString(" (")
		b.WriteString(pgIdent(keyColumn))
		b.WriteString(") VALUES ")

		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fmt.Sprintf("($%d)", i+1))
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
			return fmt.Errorf("insert dimension keys into %s: %w", table, err)
		}

		start += n
	}
}

func (r *MultiRepo) SelectAllKeyValue(ctx context.Context, table string, keyColumn string, valueColumn string) (map[string]int64, error) {
	q := fmt.Sprintf(`SELECT %s, %s FROM %s`, pgIdent(keyColumn), pgIdent(valueColumn), table)
	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int64{}
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

// SelectKeyValueByKeys returns id map for the given keys.
// Signature MUST match storage.MultiRepository: keys is []any.
//
// Critical: for text-like keys, pass []string to ANY($1) so pgx sends text[].
// Passing []any often breaks encoding and/or yields no matches.
func (r *MultiRepo) SelectKeyValueByKeys(ctx context.Context, table, keyColumn, valueColumn string, keys []any) (map[string]int64, error) {
	if len(keys) == 0 {
		return map[string]int64{}, nil
	}

	kind := inferKeyKind(keys)

	q := fmt.Sprintf(`SELECT %s, %s FROM %s WHERE %s = ANY($1)`,
		pgIdent(keyColumn), pgIdent(valueColumn), table, pgIdent(keyColumn),
	)

	var rows pgxRows
	var err error

	switch kind {
	case keyText:
		strKeys := dedupeStrings(coerceKeysToStrings(keys))
		if len(strKeys) == 0 {
			return map[string]int64{}, nil
		}
		rows, err = r.pool.Query(ctx, q, strKeys) // []string => text[]
	case keyInt:
		intKeys := dedupeInt64(coerceKeysToInt64(keys))
		if len(intKeys) == 0 {
			return map[string]int64{}, nil
		}
		rows, err = r.pool.Query(ctx, q, intKeys) // []int64 => int8[]
	default:
		// fallback: try as-is (may work for some types), but this is last resort
		keys = dedupeAnyByNormalizeKey(keys)
		if len(keys) == 0 {
			return map[string]int64{}, nil
		}
		rows, err = r.pool.Query(ctx, q, keys)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int64{}
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

// InsertFactRows: bulk insert with optional ON CONFLICT DO NOTHING, chunked to pgMaxParams.
func (r *MultiRepo) InsertFactRows(ctx context.Context, table string, columns []string, rows [][]any, dedupeColumns []string) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if len(columns) == 0 {
		return 0, fmt.Errorf("no columns provided")
	}

	colsPerRow := len(columns)
	maxRowsPerStmt := pgMaxParams / colsPerRow
	if maxRowsPerStmt < 1 {
		return 0, fmt.Errorf("too many columns (%d): exceeds postgres parameter limit", colsPerRow)
	}

	var total int64

	for start := 0; start < len(rows); start += maxRowsPerStmt {
		end := start + maxRowsPerStmt
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]

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

		args := make([]any, 0, len(chunk)*len(columns))
		argN := 1

		for i, row := range chunk {
			if len(row) < len(columns) {
				return total, fmt.Errorf("row %d has %d values, expected %d", start+i, len(row), len(columns))
			}
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("(")
			for j := range columns {
				if j > 0 {
					b.WriteString(", ")
				}
				b.WriteString(fmt.Sprintf("$%d", argN))
				argN++
				args = append(args, row[j])
			}
			b.WriteString(")")
		}

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

		cmd, err := r.pool.Exec(ctx, b.String(), args...)
		if err != nil {
			return total, err
		}
		total += cmd.RowsAffected()
	}

	return total, nil
}

// ---- helpers ----

func buildCreateTableSQL(t storage.TableSpec) (string, error) {
	if t.Name == "" {
		return "", fmt.Errorf("table name is empty")
	}
	var parts []string

	if t.PrimaryKey != nil {
		parts = append(parts, fmt.Sprintf("%s %s PRIMARY KEY", pgIdent(t.PrimaryKey.Name), t.PrimaryKey.Type))
	}

	for _, c := range t.Columns {
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
		if con.Kind != "unique" {
			return "", fmt.Errorf("%s unsupported constraint kind: %s", t.Name, con.Kind)
		}
		var cols []string
		for _, c := range con.Columns {
			cols = append(cols, pgIdent(c))
		}
		parts = append(parts, fmt.Sprintf("UNIQUE (%s)", strings.Join(cols, ", ")))
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n);", t.Name, strings.Join(parts, ",\n  ")), nil
}

// ---- key typing helpers ----

type keyKind int

const (
	keyUnknown keyKind = iota
	keyText
	keyInt
)

func inferKeyKind(keys []any) keyKind {
	// If we see any string/[]byte, treat as text keys.
	for _, k := range keys {
		switch k.(type) {
		case string, []byte:
			return keyText
		}
	}
	// Otherwise, if we see any integer-ish types, treat as int keys.
	for _, k := range keys {
		switch k.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return keyInt
		}
	}
	return keyUnknown
}

func coerceKeysToStrings(keys []any) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k == nil {
			continue
		}
		// NormalizeKey preserves leading zeros for strings; for non-strings it stringifies.
		s := storage.NormalizeKey(k)
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func coerceKeysToInt64(keys []any) []int64 {
	out := make([]int64, 0, len(keys))
	for _, k := range keys {
		switch v := k.(type) {
		case int64:
			out = append(out, v)
		case int:
			out = append(out, int64(v))
		case int32:
			out = append(out, int64(v))
		case int16:
			out = append(out, int64(v))
		case int8:
			out = append(out, int64(v))
		case uint64:
			if v <= uint64(^uint64(0)>>1) {
				out = append(out, int64(v))
			}
		case uint:
			out = append(out, int64(v))
		case uint32:
			out = append(out, int64(v))
		case uint16:
			out = append(out, int64(v))
		case uint8:
			out = append(out, int64(v))
		default:
			// ignore non-integer types
		}
	}
	return out
}

func dedupeStrings(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
}

func dedupeInt64(keys []int64) []int64 {
	seen := make(map[int64]struct{}, len(keys))
	out := make([]int64, 0, len(keys))
	for _, k := range keys {
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
}

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

// pgxpool.Query returns pgx.Rows which has these methods.
// This tiny interface lets us avoid importing pgx directly.
type pgxRows interface {
	Close()
	Err() error
	Next() bool
	Scan(dest ...any) error
}
