package storage

import (
	"context"
	"fmt"
	"sync"
)

// MultiConfig is the minimal configuration needed to create a multi-table repository.
//
// When to use:
//   - Use MultiConfig when constructing a MultiRepository via NewMulti.
//
// Edge cases:
//   - Kind must be non-empty and must match a registered backend kind.
//   - DSN is passed through to the backend factory; validation is backend-specific.
//
// Errors:
//   - NewMulti returns an error if Kind is empty or unsupported.
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
	// Close releases any backend resources (connections, prepared statements, etc).
	//
	// When to use:
	//   - Always call Close when you are done with the repository to avoid leaks.
	//
	// Edge cases:
	//   - Implementations should be safe to call once at process shutdown.
	//   - Repeated calls may be a no-op or may panic, depending on backend; callers
	//     should treat Close as "call once".
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
//
// When to use:
//   - Call RegisterMulti from an init() function in a backend package.
//   - The `kind` string becomes the lookup key used by NewMulti.
//
// Edge cases:
//   - kind must be non-empty.
//   - f must be non-nil.
//   - Registering the same kind more than once panics. This is intentional to
//     fail fast and avoid ambiguous backend selection.
//
// Panics:
//   - If kind is empty.
//   - If f is nil.
//   - If kind is already registered.
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
//
// When to use:
//   - Call NewMulti when running a multi-table pipeline and you need a repository
//     for the configured backend kind.
//
// Edge cases:
//   - If cfg.Kind is empty, NewMulti returns an error.
//   - If cfg.Kind is not registered, NewMulti returns an error.
//
// Concurrency:
//   - Safe for concurrent use with RegisterMulti. NewMulti takes a read lock while
//     selecting the factory.
//
// Errors:
//   - Returns an error if cfg.Kind is empty or unsupported.
//   - Returns whatever error the registered factory returns.
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
