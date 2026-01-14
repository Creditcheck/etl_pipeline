package storage

import (
	"context"
	"fmt"
	"sync"
)

// MultiConfig is the minimal config needed to create a multi-table repository.
type MultiConfig struct {
	Kind string
	DSN  string
}

// MultiRepository is a backend-agnostic interface for multi-table ETL.
//
// IMPORTANT: This interface is intentionally minimal and focused on the
// operations the multi-table engine needs. Each backend implements these
// semantics in its own idiomatic way (Postgres ON CONFLICT, SQLite OR IGNORE,
// etc).
type MultiRepository interface {
	Close()

	// EnsureTables creates tables and constraints as needed.
	// (Backends may implement "create-if-not-exists" semantics, mirroring single-table.)
	EnsureTables(ctx context.Context, tables []TableSpec) error

	// Dimension APIs: idempotent key ensure + lookup.
	EnsureDimensionKeys(ctx context.Context, table string, keyColumn string, keys []any, conflictColumns []string) error
	SelectKeyValueByKeys(ctx context.Context, table string, keyColumn string, valueColumn string, keys []any) (map[string]int64, error)
	SelectAllKeyValue(ctx context.Context, table string, keyColumn string, valueColumn string) (map[string]int64, error)

	// Fact insert. Must be able to behave idempotently if dedupeColumns is provided.
	InsertFactRows(ctx context.Context, table string, columns []string, rows [][]any, dedupeColumns []string) (int64, error)
}

// ---- multi factories (mirrors storage.New for single-table) ----

type multiFactory func(ctx context.Context, cfg MultiConfig) (MultiRepository, error)

var (
	multiMu        sync.RWMutex
	multiFactories = map[string]multiFactory{}
)

// RegisterMulti registers a multi-table backend under a kind (e.g. "postgres", "sqlite").
func RegisterMulti(kind string, f multiFactory) {
	multiMu.Lock()
	defer multiMu.Unlock()
	if kind == "" {
		panic("storage: RegisterMulti called with empty kind")
	}
	if f == nil {
		panic("storage: RegisterMulti called with nil factory")
	}
	if _, exists := multiFactories[kind]; exists {
		panic(fmt.Sprintf("storage: multi factory already registered for kind=%q", kind))
	}
	multiFactories[kind] = f
}

// NewMulti constructs a MultiRepository using the registered backend factory.
func NewMulti(ctx context.Context, cfg MultiConfig) (MultiRepository, error) {
	if cfg.Kind == "" {
		return nil, fmt.Errorf("storage: missing multi.Kind")
	}
	multiMu.RLock()
	f := multiFactories[cfg.Kind]
	multiMu.RUnlock()
	if f == nil {
		return nil, fmt.Errorf("unsupported multi storage.kind=%s", cfg.Kind)
	}
	return f(ctx, cfg)
}
