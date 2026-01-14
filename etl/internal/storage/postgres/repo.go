// Package postgres implements a Postgres repository using pgx v5. It performs
// a COPY into a temporary table followed by an upsert into the target table.
package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds Postgres repository configuration.
type Config struct {
	DSN        string   // connection string for pgxpool
	Table      string   // fully qualified target table name, e.g., "public.hr_events"
	Columns    []string // ordered columns for COPY and INSERT
	KeyColumns []string // conflict target columns
}

// Repository is a Postgres-backed implementation of storage.Repository.
// With CopyFromTable, it can also act as a multi-table repository.
type Repository struct {
	pool *pgxpool.Pool
	cfg  Config
}

// NewRepository constructs a Repository and returns a Close function for cleanup.
func NewRepository(ctx context.Context, cfg Config) (*Repository, func(), error) {
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("pgxpool: %w", err)
	}
	close := func() { pool.Close() }
	return &Repository{pool: pool, cfg: cfg}, close, nil
}

// Close releases held resources. This allows *Repository to satisfy a MultiRepository-style interface.
func (r *Repository) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

// updateColumns generates a list of column updates in the format: "col = EXCLUDED.col"
func updateColumns(cols []string) []string {
	var updates []string
	for _, col := range cols {
		updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", pgIdent(col), pgIdent(col)))
	}
	return updates
}

// buildDeleteCondition builds the condition for deleting matching rows based on key_columns and date_column.
func buildDeleteCondition(keyColumns []string) string {
	conds := make([]string, 0, len(keyColumns)+1)
	for _, col := range keyColumns {
		conds = append(conds, fmt.Sprintf("T.%s = S.%s", pgIdent(col), pgIdent(col)))
	}
	return strings.Join(conds, " AND ")
}

// filterConflictKeys ensures that the columns in the conflict condition are not included in the update statement.
func filterConflictKeys(setParts []string, keys []string) []string {
	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}

	var result []string
	for _, part := range setParts {
		// Skip parts that are part of the conflict key (e.g., pcv, date_from)
		col := strings.Split(part, " = ")[0]
		if _, isKey := keySet[col]; !isKey {
			result = append(result, part)
		}
	}
	return result
}

// toCopyVal passes values to pgx as-is so it can encode them properly.
// nil stays nil; everything else returns unchanged.
func toCopyVal(v any) any {
	if v == nil {
		return nil
	}
	return v
}

// pgIdent safely quotes a single identifier segment for Postgres.
func pgIdent(id string) string { return `"` + strings.ReplaceAll(id, `"`, `""`) + `"` }

// pgFQN quotes a possibly schema-qualified name like "public.hr_events" to
// "public"."hr_events". If no dot is present, returns a single quoted ident.
func pgFQN(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = pgIdent(p)
	}
	return strings.Join(parts, ".")
}

// mapIdent maps a list of column names to their quoted forms.
func mapIdent(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = pgIdent(c)
	}
	return out
}

// toString converts values to their string representation suitable for TEXT.
func toString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// CopyFrom preserves the existing single-table behavior by copying into cfg.Table.
// This keeps the current ETL pipeline unchanged. :contentReference[oaicite:1]{index=1}
func (r *Repository) CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	return r.CopyFromTable(ctx, r.cfg.Table, columns, rows)
}

// CopyFromTable is the new multi-table primitive. It copies the given rows into the provided table.
func (r *Repository) CopyFromTable(ctx context.Context, table string, columns []string, rows [][]any) (int64, error) {
	if table == "" {
		return 0, fmt.Errorf("table must not be empty")
	}
	return r.pool.CopyFrom(ctx, splitFQN(table), columns, pgx.CopyFromRows(rows))
}

// splitFQN converts "schema.table" into a pgx.Identifier {"schema","table"}.
// If no dot is present, returns {"table"}.
func splitFQN(fqn string) pgx.Identifier {
	parts := strings.Split(fqn, ".")
	id := make(pgx.Identifier, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			id = append(id, p)
		}
	}
	return id
}

// Exec implements storage.Repository.Exec for Postgres.
func (r *Repository) Exec(ctx context.Context, sql string) error {
	_, err := r.pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("error executing SQL: %w", err)
	}
	return err
}
