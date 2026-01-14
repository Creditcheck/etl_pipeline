package multitable

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"etl/internal/storage"
)

// TableCopyer implements MultiCopyer using the storage-agnostic factory.
// It does NOT import any backend packages.
type TableCopyer struct {
	newRepo func(ctx context.Context, cfg storage.Config) (storage.Repository, error)
	baseCfg storage.Config

	mu    sync.Mutex
	repos map[string]storage.Repository
}

func NewTableCopyer(
	newRepo func(ctx context.Context, cfg storage.Config) (storage.Repository, error),
	base storage.Config,
) *TableCopyer {
	return &TableCopyer{
		newRepo: newRepo,
		baseCfg: base,
		repos:   map[string]storage.Repository{},
	}
}

func (t *TableCopyer) CopyFromTable(ctx context.Context, table string, columns []string, rows [][]any) (int64, error) {
	if table == "" {
		return 0, fmt.Errorf("copy: table is empty")
	}
	if len(columns) == 0 {
		return 0, fmt.Errorf("copy: columns empty for table %s", table)
	}

	repo, err := t.getRepo(ctx, table, columns)
	if err != nil {
		return 0, err
	}
	return repo.CopyFrom(ctx, columns, rows)
}

func (t *TableCopyer) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, r := range t.repos {
		r.Close()
	}
	t.repos = map[string]storage.Repository{}
}

func (t *TableCopyer) getRepo(ctx context.Context, table string, columns []string) (storage.Repository, error) {
	key := repoKey(table, columns)

	t.mu.Lock()
	if r, ok := t.repos[key]; ok {
		t.mu.Unlock()
		return r, nil
	}
	t.mu.Unlock()

	cfg := t.baseCfg
	cfg.Table = table
	cfg.Columns = append([]string(nil), columns...)

	r, err := t.newRepo(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("new repo (kind=%s table=%s): %w", cfg.Kind, cfg.Table, err)
	}

	t.mu.Lock()
	// double-check to avoid leaking if raced
	if existing, ok := t.repos[key]; ok {
		t.mu.Unlock()
		r.Close()
		return existing, nil
	}
	t.repos[key] = r
	t.mu.Unlock()

	return r, nil
}

func repoKey(table string, columns []string) string {
	return table + "|" + strings.Join(columns, ",")
}
