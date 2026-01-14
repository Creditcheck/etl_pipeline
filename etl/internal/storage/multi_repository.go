package storage

import (
	"context"
	"fmt"
)

// MultiRepository is like Repository, but it can write to multiple destination tables.
// It’s intentionally tiny: enough to support dimension/fact loading while allowing
// reuse of the existing batched loader (LoadBatches) via BindTable.
type MultiRepository interface {
	// CopyFromTable inserts rows into the given fully-qualified table name.
	// The provided columns define the row order.
	CopyFromTable(ctx context.Context, table string, columns []string, rows [][]any) (int64, error)

	// Exec runs an arbitrary statement (DDL, etc.).
	Exec(ctx context.Context, sql string) error

	// Close releases held resources.
	Close()
}

// BindTable adapts a MultiRepository to the existing single-table Repository
// interface by binding a fixed table name. This is the “minimal refactor” bridge:
// existing code can keep using Repository + LoadBatches, while new code can
// route to different tables by creating multiple bound repos.
func BindTable(mr MultiRepository, table string) (Repository, error) {
	if mr == nil {
		return nil, fmt.Errorf("multi repository must not be nil")
	}
	if table == "" {
		return nil, fmt.Errorf("table must not be empty")
	}
	return &boundRepo{mr: mr, table: table}, nil
}

type boundRepo struct {
	mr    MultiRepository
	table string
}

var _ Repository = (*boundRepo)(nil)

func (b *boundRepo) CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	return b.mr.CopyFromTable(ctx, b.table, columns, rows)
}

func (b *boundRepo) Exec(ctx context.Context, sql string) error {
	return b.mr.Exec(ctx, sql)
}

func (b *boundRepo) Close() {
	b.mr.Close()
}
