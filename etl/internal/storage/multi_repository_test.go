package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// stubMultiRepo is a minimal MultiRepository implementation for registry tests.
//
// When to use:
//   - Use stubMultiRepo when you only care that NewMulti returns *some* repository,
//     not about backend semantics.
//
// Edge cases:
//   - Methods intentionally do nothing and return zero values.
//   - Close is a no-op; callers should not rely on side effects.
type stubMultiRepo struct{}

func (stubMultiRepo) Close() {}

func (stubMultiRepo) EnsureTables(ctx context.Context, tables []TableSpec) error { return nil }

func (stubMultiRepo) EnsureDimensionKeys(ctx context.Context, table string, keyColumn string, keys []any, conflictColumns []string) error {
	return nil
}
func (stubMultiRepo) SelectKeyValueByKeys(ctx context.Context, table string, keyColumn string, valueColumn string, keys []any) (map[string]int64, error) {
	return nil, nil
}
func (stubMultiRepo) SelectAllKeyValue(ctx context.Context, table string, keyColumn string, valueColumn string) (map[string]int64, error) {
	return nil, nil
}
func (stubMultiRepo) InsertFactRows(ctx context.Context, table string, columns []string, rows [][]any, dedupeColumns []string) (int64, error) {
	return 0, nil
}

// withMultiRegistrySnapshot snapshots the global multiFactories map, resets it to empty,
// runs fn, and restores the snapshot when fn returns.
//
// Why this helper exists:
//   - multiFactories is package-global mutable state.
//   - RegisterMulti and NewMulti operate on that global map.
//   - Tests must not leak registrations across test cases, otherwise ordering and
//     parallelism can cause flaky failures.
//
// Concurrency:
//   - Snapshot/reset/restore are performed under the same mutex used in production,
//     making this helper safe under `go test -race`.
func withMultiRegistrySnapshot(t testing.TB, fn func()) {
	t.Helper()

	multiMu.Lock()
	orig := make(map[string]multiFactory, len(multiFactories))
	for k, v := range multiFactories {
		orig[k] = v
	}
	// Reset to an empty registry for the duration of the test/benchmark.
	multiFactories = map[string]multiFactory{}
	multiMu.Unlock()

	defer func() {
		multiMu.Lock()
		multiFactories = orig
		multiMu.Unlock()
	}()

	fn()
}

// TestRegisterMulti_PanicsOnInvalidInput verifies that RegisterMulti fails fast on
// programmer errors (invalid kind or nil factory).
//
// What this protects:
//   - The registry is intended to be initialized in init() functions.
//   - Invalid registrations indicate a programming bug and should be detected
//     immediately (panic) rather than silently accepted.
//
// Edge cases:
//   - Empty kind must panic.
//   - Nil factory must panic.
func TestRegisterMulti_PanicsOnInvalidInput(t *testing.T) {
	withMultiRegistrySnapshot(t, func() {
		tests := []struct {
			name      string
			kind      string
			factory   multiFactory
			wantPanic string
		}{
			{
				name: "empty_kind_panics",
				kind: "",
				factory: func(context.Context, MultiConfig) (MultiRepository, error) {
					return stubMultiRepo{}, nil
				},
				wantPanic: "storage: RegisterMulti called with empty kind",
			},
			{
				name:      "nil_factory_panics",
				kind:      "postgres",
				factory:   nil,
				wantPanic: "storage: RegisterMulti called with nil factory",
			},
		}

		for _, tc := range tests {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				defer func() {
					r := recover()
					if r == nil {
						t.Fatalf("expected panic, got nil")
					}
					msg := fmt.Sprint(r)
					if msg != tc.wantPanic {
						t.Fatalf("panic=%q, want %q", msg, tc.wantPanic)
					}
				}()

				RegisterMulti(tc.kind, tc.factory)
			})
		}
	})
}

// TestRegisterMulti_PanicsOnDuplicateKind verifies that attempting to register the
// same kind twice panics.
//
// What this protects:
//   - Duplicate registration indicates a configuration/packaging bug (e.g. importing
//     two backends that claim the same kind).
//   - Panicking is preferable to nondeterministically selecting whichever init()
//     ran first.
//
// Edge cases:
//   - The panic message includes the kind, which helps debugging when many backends exist.
func TestRegisterMulti_PanicsOnDuplicateKind(t *testing.T) {
	withMultiRegistrySnapshot(t, func() {
		RegisterMulti("sqlite", func(context.Context, MultiConfig) (MultiRepository, error) {
			return stubMultiRepo{}, nil
		})

		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("expected panic, got nil")
			}
			msg := fmt.Sprint(r)
			// Use substring matching because the message formatting is not part of the
			// API contract, but the presence of the kind is important for debugging.
			if !strings.Contains(msg, `already registered for kind="sqlite"`) {
				t.Fatalf("panic=%q, want substring %q", msg, `already registered for kind="sqlite"`)
			}
		}()

		RegisterMulti("sqlite", func(context.Context, MultiConfig) (MultiRepository, error) {
			return stubMultiRepo{}, nil
		})
	})
}

// TestNewMulti_ValidationAndLookupErrors verifies the two primary error paths in NewMulti:
// missing kind and unsupported kind.
//
// What this protects:
//   - Callers should get a clear error when the config is missing.
//   - Callers should get a clear error when no backend was registered for the kind.
//
// Notes:
//   - We match substrings rather than exact strings to avoid brittle coupling to
//     wording while still enforcing meaning.
func TestNewMulti_ValidationAndLookupErrors(t *testing.T) {
	withMultiRegistrySnapshot(t, func() {
		t.Run("missing_kind_returns_error", func(t *testing.T) {
			repo, err := NewMulti(context.Background(), MultiConfig{})
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if repo != nil {
				t.Fatalf("repo=%v, want nil", repo)
			}
			if !strings.Contains(err.Error(), "missing multi.Kind") {
				t.Fatalf("err=%q, want substring %q", err.Error(), "missing multi.Kind")
			}
		})

		t.Run("unsupported_kind_returns_error", func(t *testing.T) {
			repo, err := NewMulti(context.Background(), MultiConfig{Kind: "unknown"})
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if repo != nil {
				t.Fatalf("repo=%v, want nil", repo)
			}
			if !strings.Contains(err.Error(), "unsupported multi storage.kind=") {
				t.Fatalf("err=%q, want substring %q", err.Error(), "unsupported multi storage.kind=")
			}
		})
	})
}

// TestNewMulti_CallsFactoryAndReturnsRepo verifies the happy path:
// when a kind is registered, NewMulti calls the factory with the provided context
// and config and returns the repository it constructs.
//
// What this protects:
//   - Context propagation: important for cancellation, tracing, and timeouts.
//   - Config propagation: ensures DSN and other future fields reach the backend.
//   - Return path: NewMulti should return exactly what the factory returns.
func TestNewMulti_CallsFactoryAndReturnsRepo(t *testing.T) {
	withMultiRegistrySnapshot(t, func() {
		type ctxKey struct{}
		ctx := context.WithValue(context.Background(), ctxKey{}, "marker")

		var gotCtx context.Context
		var gotCfg MultiConfig

		wantCfg := MultiConfig{Kind: "postgres", DSN: "dsn://x"}
		wantRepo := stubMultiRepo{}

		RegisterMulti("postgres", func(c context.Context, cfg MultiConfig) (MultiRepository, error) {
			gotCtx = c
			gotCfg = cfg
			return wantRepo, nil
		})

		repo, err := NewMulti(ctx, wantCfg)
		if err != nil {
			t.Fatalf("NewMulti() err=%v, want nil", err)
		}
		if repo == nil {
			t.Fatalf("NewMulti() repo=nil, want non-nil")
		}
		if gotCtx != ctx {
			t.Fatalf("factory ctx mismatch: got=%v want=%v", gotCtx, ctx)
		}
		if gotCfg != wantCfg {
			t.Fatalf("factory cfg mismatch: got=%#v want=%#v", gotCfg, wantCfg)
		}
	})
}

// TestNewMulti_PropagatesFactoryError verifies that if a backend factory returns an
// error, NewMulti returns that same error to the caller.
//
// What this protects:
//   - Error precedence: caller should see the underlying reason (e.g. bad DSN,
//     auth failure, driver error).
//   - Wrap semantics: NewMulti does not wrap factory errors, so errors.Is should
//     work as expected.
func TestNewMulti_PropagatesFactoryError(t *testing.T) {
	withMultiRegistrySnapshot(t, func() {
		wantErr := errors.New("dial failed")

		RegisterMulti("postgres", func(context.Context, MultiConfig) (MultiRepository, error) {
			return nil, wantErr
		})

		repo, err := NewMulti(context.Background(), MultiConfig{Kind: "postgres", DSN: "dsn"})
		if repo != nil {
			t.Fatalf("repo=%v, want nil", repo)
		}
		if !errors.Is(err, wantErr) {
			t.Fatalf("err=%v, want errors.Is(err, %v)=true", err, wantErr)
		}
	})
}

// TestNewMulti_ConcurrentRegisterAndNewMulti_IsRaceSafe verifies that concurrent use
// of RegisterMulti and NewMulti does not cause data races or panics.
//
// Why this test matters:
//   - The registry is a shared map guarded by an RWMutex.
//   - It is common for backends to register in init(), and for application code to
//     call NewMulti early during startup.
//   - This test ensures the locking strategy holds under concurrency and `-race`.
//
// What this test does (intentionally):
//   - Registers only unique kinds to avoid intentional duplicate-kind panics.
//   - Calls NewMulti for a mix of existing and potentially-not-yet-registered kinds.
//   - Does not assert deterministic success/failure for "future" kinds; we only
//     care that the code remains race-free and stable.
func TestNewMulti_ConcurrentRegisterAndNewMulti_IsRaceSafe(t *testing.T) {
	withMultiRegistrySnapshot(t, func() {
		ctx := context.Background()

		// Register one initial kind so NewMulti can resolve a known kind immediately.
		RegisterMulti("k0", func(context.Context, MultiConfig) (MultiRepository, error) {
			return stubMultiRepo{}, nil
		})

		const (
			registerers = 8
			callers     = 16
			iterations  = 500
		)

		var wg sync.WaitGroup
		wg.Add(registerers + callers)

		// Concurrently register unique kinds (no duplicates).
		for i := 0; i < registerers; i++ {
			i := i
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					kind := fmt.Sprintf("k%d_%d", i, j)
					RegisterMulti(kind, func(context.Context, MultiConfig) (MultiRepository, error) {
						return stubMultiRepo{}, nil
					})
				}
			}()
		}

		// Concurrently call NewMulti. "Unsupported kind" is acceptable for kinds
		// that may never be registered; we are only checking race safety.
		for i := 0; i < callers; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					kind := "k0"
					if j%2 == 1 {
						kind = fmt.Sprintf("kX_%d", j)
					}
					_, _ = NewMulti(ctx, MultiConfig{Kind: kind, DSN: "dsn"})
				}
			}()
		}

		wg.Wait()
	})
}

// TestNormalizeKey verifies that NormalizeKey converts dimension keys to a canonical
// string form suitable for cache keys.
//
// Why this matters:
//   - Multi-table ETL uses in-memory caches to map dimension "business keys"
//     (e.g. "Germany", "8429529") to database IDs.
//   - Different parsers/backends can yield keys as different Go types (string,
//     []byte, int, int64, or custom Stringer types).
//   - NormalizeKey provides a consistent, backend-agnostic cache key.
//
// Edge cases covered:
//   - nil -> ""
//   - strings and []byte are trimmed
//   - numeric types use base-10 formatting
//   - default case uses fmt.Sprint then trims
func TestNormalizeKey(t *testing.T) {
	// Custom type to exercise the default (fmt.Sprint) path.
	type custom struct{}

	tests := []struct {
		name string
		in   any
		want string
	}{
		{name: "nil", in: nil, want: ""},
		{name: "string_trimmed", in: "  Germany \n", want: "Germany"},
		{name: "int64_formats", in: int64(8429529), want: "8429529"},
		{name: "int_formats", in: int(42), want: "42"},
		{name: "bytes_trimmed", in: []byte("  abc\t"), want: "abc"},
		// fmt.Sprint(custom{}) -> "{}"
		{name: "default_fmt_sprint_trimmed", in: custom{}, want: "{}"},
		// fmt.Sprint("  x  ") produces "  x  " then TrimSpace -> "x"
		{name: "default_trims_fmt_output", in: fmt.Sprint("  x  "), want: "x"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeKey(tc.in); got != tc.want {
				t.Fatalf("NormalizeKey(%T=%v)=%q, want %q", tc.in, tc.in, got, tc.want)
			}
		})
	}
}

// BenchmarkNewMulti_RegisteredKind measures the steady-state cost of NewMulti when
// the requested kind is present.
//
// Benchmark design:
//   - No I/O, no database.
//   - Uses a stub factory.
//   - Isolates the global registry to avoid interference with other tests.
func BenchmarkNewMulti_RegisteredKind(b *testing.B) {
	withMultiRegistrySnapshot(b, func() {
		RegisterMulti("postgres", func(context.Context, MultiConfig) (MultiRepository, error) {
			return stubMultiRepo{}, nil
		})

		cfg := MultiConfig{Kind: "postgres", DSN: "dsn://x"}
		ctx := context.Background()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := NewMulti(ctx, cfg)
			if err != nil {
				b.Fatalf("NewMulti() err=%v", err)
			}
		}
	})
}

// BenchmarkRegisterMulti_UniqueKinds measures RegisterMulti overhead when registering
// many unique kinds.
//
// Benchmark design:
//   - Avoids duplicate kinds (which would intentionally panic).
//   - Uses a minimal stub factory.
//   - Isolates the global registry to avoid interference with other tests.
func BenchmarkRegisterMulti_UniqueKinds(b *testing.B) {
	withMultiRegistrySnapshot(b, func() {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			kind := fmt.Sprintf("k_%d", i)
			RegisterMulti(kind, func(context.Context, MultiConfig) (MultiRepository, error) {
				return stubMultiRepo{}, nil
			})
		}
	})
}

// BenchmarkNormalizeKey measures NormalizeKey cost for common input types.
//
// Benchmark design:
//   - Uses representative dimension-key types seen in ETL pipelines.
//   - Avoids allocations in setup; focuses on the conversion path.
func BenchmarkNormalizeKey(b *testing.B) {
	cases := []struct {
		name string
		in   any
	}{
		{name: "string", in: "  Germany "},
		{name: "bytes", in: []byte("  abc\t")},
		{name: "int64", in: int64(8429529)},
		{name: "int", in: int(42)},
		{name: "default", in: struct{ X int }{X: 1}},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = NormalizeKey(tc.in)
			}
		})
	}
}
