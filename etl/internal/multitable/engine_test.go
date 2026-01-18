package multitable

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"etl/internal/config"
	"etl/internal/storage"
	"etl/internal/transformer"
)

// recordingMultiRepo is a deterministic in-memory MultiRepository used by engine tests.
// It records method calls and can be configured to return errors.
//
// When to use:
//   - Unit testing Engine2Pass behavior without a real database.
//   - Verifying batching, dedupe behavior, conflict-column selection, and error propagation.
//
// Concurrency:
//   - Methods are safe for concurrent use by loader workers.
type recordingMultiRepo struct {
	mu sync.Mutex

	ensureTablesCalls int
	ensureTablesArgs  [][]storage.TableSpec

	ensureDimCalls []ensureDimCall

	selectCalls []selectCall

	insertCalls []insertCall

	// lookupData maps: table -> normalizedKey -> id
	lookupData map[string]map[string]int64

	// error injection
	ensureTablesErr error
	ensureDimErr    error
	selectErr       error
	insertErr       error

	closed atomic.Int64
}

type ensureDimCall struct {
	table           string
	keyColumn       string
	keys            []any
	conflictColumns []string
}

type selectCall struct {
	table       string
	keyColumn   string
	valueColumn string
	keys        []any
}

type insertCall struct {
	table         string
	columns       []string
	rows          [][]any
	dedupeColumns []string
}

func (r *recordingMultiRepo) Close() { r.closed.Add(1) }

func (r *recordingMultiRepo) EnsureTables(ctx context.Context, tables []storage.TableSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureTablesCalls++
	cp := append([]storage.TableSpec(nil), tables...)
	r.ensureTablesArgs = append(r.ensureTablesArgs, cp)

	return r.ensureTablesErr
}

func (r *recordingMultiRepo) EnsureDimensionKeys(ctx context.Context, table string, keyColumn string, keys []any, conflictColumns []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	c := ensureDimCall{
		table:           table,
		keyColumn:       keyColumn,
		keys:            append([]any(nil), keys...),
		conflictColumns: append([]string(nil), conflictColumns...),
	}
	r.ensureDimCalls = append(r.ensureDimCalls, c)

	return r.ensureDimErr
}

func (r *recordingMultiRepo) SelectKeyValueByKeys(ctx context.Context, table string, keyColumn string, valueColumn string, keys []any) (map[string]int64, error) {
	r.mu.Lock()
	r.selectCalls = append(r.selectCalls, selectCall{
		table:       table,
		keyColumn:   keyColumn,
		valueColumn: valueColumn,
		keys:        append([]any(nil), keys...),
	})
	data := r.lookupData
	err := r.selectErr
	r.mu.Unlock()

	if err != nil {
		return nil, err
	}

	// Return only the requested keys.
	out := map[string]int64{}
	td := data[table]
	for _, k := range keys {
		nk := normalizeKey(k)
		if nk == "" {
			continue
		}
		if id, ok := td[nk]; ok {
			out[nk] = id
		}
	}
	return out, nil
}

func (r *recordingMultiRepo) SelectAllKeyValue(ctx context.Context, table string, keyColumn string, valueColumn string) (map[string]int64, error) {
	// Not used by Engine2Pass; keep for interface completeness.
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.selectErr != nil {
		return nil, r.selectErr
	}
	cp := map[string]int64{}
	for k, v := range r.lookupData[table] {
		cp[k] = v
	}
	return cp, nil
}

func (r *recordingMultiRepo) InsertFactRows(ctx context.Context, table string, columns []string, rows [][]any, dedupeColumns []string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.insertCalls = append(r.insertCalls, insertCall{
		table:         table,
		columns:       append([]string(nil), columns...),
		rows:          deepCopyRows(rows),
		dedupeColumns: append([]string(nil), dedupeColumns...),
	})

	if r.insertErr != nil {
		return 0, r.insertErr
	}
	return int64(len(rows)), nil
}

func deepCopyRows(in [][]any) [][]any {
	out := make([][]any, len(in))
	for i := range in {
		out[i] = append([]any(nil), in[i]...)
	}
	return out
}

// makeRow returns a pooled *transformer.Row with the provided positional values.
// The returned row is owned by the caller.
//
// When to use:
//   - Building deterministic streams for Engine2Pass tests.
func makeRow(values ...any) *transformer.Row {
	r := transformer.GetRow(len(values))
	for i := range values {
		r.V[i] = values[i]
	}
	return r
}

// streamFromRows returns an Engine2Pass StreamFn that produces a fresh ValidatedStream
// each time it is called (important because Engine2Pass consumes two passes).
//
// The returned stream closes Rows after emitting all rows. Wait() returns waitErr.
//
// Ownership:
//   - Each emitted row is owned by the engine and must be freed by it.
func streamFromRows(rows [][]any, waitErr error, calls *atomic.Int64) StreamFn {
	return func(ctx context.Context, cfg Pipeline, columns []string) (*ValidatedStream, error) {
		calls.Add(1)
		ch := make(chan *transformer.Row, len(rows))

		go func() {
			defer close(ch)
			for _, v := range rows {
				select {
				case <-ctx.Done():
					// Stop producing on cancellation; engine will drain what it already has.
					return
				default:
				}
				ch <- makeRow(v...)
			}
		}()

		return &ValidatedStream{
			Rows: ch,
			Wait: func() error { return waitErr },
			ParseErrorCount: func() uint64 {
				return 0
			},
		}, nil
	}
}

// TestEngine2Pass_Run_RepoRequired verifies Run fails fast when Repo is nil.
//
// When to use:
//   - Prevent regressions that would panic inside Repo method calls.
func TestEngine2Pass_Run_RepoRequired(t *testing.T) {
	t.Parallel()

	e := &Engine2Pass{}
	err := e.Run(context.Background(), Pipeline{}, nil)
	if err == nil {
		t.Fatalf("Run() err=nil, want error")
	}
	if !strings.Contains(err.Error(), "Repo is required") {
		t.Fatalf("Run() err=%q, want contains %q", err.Error(), "Repo is required")
	}
}

// TestEngine2Pass_Run_HappyPath verifies that Run performs:
//   - EnsureTables for all tables,
//   - Pass 1 EnsureDimensionKeys calls,
//   - Pass 2 lookup resolution and InsertFactRows,
//
// using the injected Stream seam.
//
// Edge cases:
//   - Confirms Stream is invoked twice (two-pass contract).
func TestEngine2Pass_Run_HappyPath(t *testing.T) {
	t.Parallel()

	// Schema: one dimension + one fact that looks up the dimension.
	dim := storage.TableSpec{
		Name: "dim_user",
		Load: storage.LoadSpec{
			Kind: "dimension",
			FromRows: []storage.FromRowSpec{
				{TargetColumn: "user_key", SourceField: "user"},
			},
			// Conflict override for dimension ensures we cover conflictColumns logic.
			Conflict: &storage.ConflictSpec{TargetColumns: []string{"user_key"}},
			Cache:    &storage.CacheSpec{KeyColumn: "user_key", ValueColumn: "id"},
		},
	}
	fact := storage.TableSpec{
		Name: "fact_events",
		Columns: []storage.ColumnSpec{
			{Name: "user_id"},
			{Name: "event"},
		},
		Load: storage.LoadSpec{
			Kind: "fact",
			FromRows: []storage.FromRowSpec{
				{
					TargetColumn: "user_id",
					Lookup: &storage.LookupSpec{
						Table: "dim_user",
						Match: map[string]string{"user_key": "user"},
					},
				},
				{TargetColumn: "event", SourceField: "event"},
			},
		},
	}

	cfg := Pipeline{
		Source:  Source{Kind: "file", File: &FileSource{Path: "ignored"}},
		Parser:  Parser{Kind: "csv"},
		Storage: Storage{Kind: "sqlite", DB: MultiDB{Mode: "multi_table", Tables: []storage.TableSpec{fact, dim}}},
		Runtime: RuntimeConfig{
			BatchSize:     2,
			LoaderWorkers: 2,
		},
	}

	repo := &recordingMultiRepo{
		lookupData: map[string]map[string]int64{
			"dim_user": {"alice": 101, "bob": 202},
		},
	}

	var streamCalls atomic.Int64
	e := &Engine2Pass{
		Repo:   repo,
		Logger: &fakeLogger{}, // reuse from runner_test.go
		Stream: streamFromRows([][]any{
			{"alice", "login"},
			{"bob", "click"},
			{"alice", "logout"},
		}, nil, &streamCalls),
	}

	err := e.Run(context.Background(), cfg, []string{"user", "event"})
	if err != nil {
		t.Fatalf("Run() err=%v, want nil", err)
	}

	// Two passes.
	if got := streamCalls.Load(); got != 2 {
		t.Fatalf("Stream calls=%d, want 2", got)
	}

	// EnsureTables called once with all tables.
	if repo.ensureTablesCalls != 1 {
		t.Fatalf("EnsureTables calls=%d, want 1", repo.ensureTablesCalls)
	}

	// Pass 1 should have ensured dimension keys; with BatchSize=2 and 3 rows,
	// expected at least 2 calls (flush at 2, then final flush).
	if len(repo.ensureDimCalls) < 2 {
		t.Fatalf("EnsureDimensionKeys calls=%d, want >=2", len(repo.ensureDimCalls))
	}
	for _, c := range repo.ensureDimCalls {
		if c.table != "dim_user" {
			t.Fatalf("EnsureDimensionKeys table=%q, want dim_user", c.table)
		}
		if !reflect.DeepEqual(c.conflictColumns, []string{"user_key"}) {
			t.Fatalf("EnsureDimensionKeys conflictColumns=%v, want %v", c.conflictColumns, []string{"user_key"})
		}
	}

	// Pass 2 should insert fact rows.
	if len(repo.insertCalls) == 0 {
		t.Fatalf("InsertFactRows calls=0, want >=1")
	}
	for _, ic := range repo.insertCalls {
		if ic.table != "fact_events" {
			t.Fatalf("InsertFactRows table=%q, want fact_events", ic.table)
		}
		if !reflect.DeepEqual(ic.columns, []string{"user_id", "event"}) {
			t.Fatalf("InsertFactRows columns=%v, want %v", ic.columns, []string{"user_id", "event"})
		}
	}
}

// TestEnsureDimensionsStreaming_DedupeWithinBatch verifies that the bounded
// within-batch dedupe option removes duplicate keys before calling the repo.
//
// When to use:
//   - Any time key normalization or typedBindValue rules change.
//   - Any time batching/dedupe behavior changes.
func TestEnsureDimensionsStreaming_DedupeWithinBatch(t *testing.T) {
	t.Parallel()

	dim := storage.TableSpec{
		Name: "dim_k",
		Load: storage.LoadSpec{
			Kind: "dimension",
			FromRows: []storage.FromRowSpec{
				{TargetColumn: "k", SourceField: "k"},
			},
		},
	}

	cfg := Pipeline{
		Storage: Storage{Kind: "sqlite", DB: MultiDB{Mode: "multi_table", Tables: []storage.TableSpec{dim}}},
		Runtime: RuntimeConfig{
			BatchSize:                      4,
			DedupeDimensionKeysWithinBatch: true,
			DedupeDimensionKeys:            false, // global dedupe off; this test only covers within-batch
			LoaderWorkers:                  1,
			TransformWorkers:               1,
			ReaderWorkers:                  1,
			ChannelBuffer:                  8,
		},
	}

	plan, err := buildIndexedPlan(cfg, []string{"k"})
	if err != nil {
		t.Fatalf("buildIndexedPlan() err=%v, want nil", err)
	}

	repo := &recordingMultiRepo{
		lookupData: map[string]map[string]int64{},
	}

	var streamCalls atomic.Int64
	e := &Engine2Pass{
		Repo: repo,
		Stream: streamFromRows([][]any{
			{" a "},       // -> "a"
			{"a"},         // dup
			{[]byte("a")}, // dup, typed as []byte
			{"b"},
		}, nil, &streamCalls),
	}

	if err := e.ensureDimensionsStreaming(context.Background(), cfg, []string{"k"}, plan); err != nil {
		t.Fatalf("ensureDimensionsStreaming() err=%v, want nil", err)
	}

	if got := streamCalls.Load(); got != 1 {
		t.Fatalf("Stream calls=%d, want 1", got)
	}
	if len(repo.ensureDimCalls) != 1 {
		t.Fatalf("EnsureDimensionKeys calls=%d, want 1", len(repo.ensureDimCalls))
	}

	gotKeys := repo.ensureDimCalls[0].keys
	// Dedupe should reduce to "a" and "b" (2 keys).
	if len(gotKeys) != 2 {
		t.Fatalf("EnsureDimensionKeys keys len=%d, want 2 keys=%v", len(gotKeys), gotKeys)
	}
	if normalizeKey(gotKeys[0]) != "a" || normalizeKey(gotKeys[1]) != "b" {
		t.Fatalf("EnsureDimensionKeys keys=%v, want normalized [a b]", gotKeys)
	}
}

// TestInsertFactBatch_StrictVsLenient verifies insertFactBatch behavior:
//
//   - Strict mode returns errors on missing/empty lookup keys.
//   - Lenient mode drops offending rows instead.
//
// Edge cases:
//   - Empty lookup keys with nullable columns become NULL instead of errors/drops.
func TestInsertFactBatch_StrictVsLenient(t *testing.T) {
	t.Parallel()

	nullable := func(b bool) *bool { return &b }

	fact := indexedFact{
		Table: storage.TableSpec{Name: "facts"},
		TargetColumns: []string{
			"user_id",
			"event",
			"maybe_user_id",
		},
		Columns: []indexedFactColumn{
			{
				TargetColumn: "user_id",
				Lookup: &indexedLookup{
					Table:           "dim_user",
					MatchFieldIndex: 0,
				},
				Nullable: false,
			},
			{
				TargetColumn:     "event",
				SourceFieldIndex: 1,
			},
			{
				TargetColumn: "maybe_user_id",
				Lookup: &indexedLookup{
					Table:           "dim_user",
					MatchFieldIndex: 2,
				},
				Nullable: true,
			},
		},
	}

	repo := &recordingMultiRepo{
		lookupData: map[string]map[string]int64{},
	}
	e := &Engine2Pass{Repo: repo}

	makeBatch := func() []*transformer.Row {
		// [user,event,maybe_user]
		return []*transformer.Row{
			makeRow("alice", "login", ""), // maybe_user empty -> NULL (nullable)
			makeRow("missing", "click", "alice"),
		}
	}

	cache := map[string]map[string]int64{
		"dim_user": {"alice": 1}, // "missing" not present
	}

	tests := []struct {
		name        string
		lenient     bool
		wantErrLike string
		wantDropped int
	}{
		{
			name:        "strict errors on lookup miss",
			lenient:     false,
			wantErrLike: "lookup miss",
			wantDropped: 0,
		},
		{
			name:        "lenient drops lookup miss",
			lenient:     true,
			wantErrLike: "",
			wantDropped: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			repo.mu.Lock()
			repo.insertCalls = nil
			repo.mu.Unlock()

			batch := makeBatch()
			defer func() {
				for _, r := range batch {
					r.Free()
				}
			}()

			inserted, dropped, err := e.insertFactBatch(context.Background(), fact, batch, cache, tt.lenient)

			if tt.wantErrLike != "" {
				if err == nil {
					t.Fatalf("insertFactBatch() err=nil, want contains %q", tt.wantErrLike)
				}
				if !strings.Contains(err.Error(), tt.wantErrLike) {
					t.Fatalf("insertFactBatch() err=%q, want contains %q", err.Error(), tt.wantErrLike)
				}
				return
			}

			if err != nil {
				t.Fatalf("insertFactBatch() err=%v, want nil", err)
			}
			if dropped != tt.wantDropped {
				t.Fatalf("dropped=%d, want %d", dropped, tt.wantDropped)
			}
			// Only the "alice" row should be inserted in lenient mode.
			if inserted != 1 {
				t.Fatalf("inserted=%d, want 1", inserted)
			}
			if len(repo.insertCalls) != 1 {
				t.Fatalf("InsertFactRows calls=%d, want 1", len(repo.insertCalls))
			}
			// Verify nullable lookup produced NULL in output.
			got := repo.insertCalls[0].rows[0][2]
			if got != nil {
				t.Fatalf("maybe_user_id=%v, want nil", got)
			}

			_ = nullable // avoid unused helper in case of future expansion
		})
	}
}

// TestBuildIndexedPlan_LookupMatchDeterministic verifies that plan compilation
// chooses a stable match field from a lookup match map.
//
// When to use:
//   - Any time storage.LookupSpec.Match representation changes.
//   - Prevent flaky tests due to randomized map iteration.
func TestBuildIndexedPlan_LookupMatchDeterministic(t *testing.T) {
	t.Parallel()

	// Fact lookup.Match is a map[dbColumn]sourceField. We expect pickLookupMatchField
	// to select the source field of the lexicographically smallest dbColumn key.
	fact := storage.TableSpec{
		Name: "facts",
		Load: storage.LoadSpec{
			Kind: "fact",
			FromRows: []storage.FromRowSpec{
				{
					TargetColumn: "dim_id",
					Lookup: &storage.LookupSpec{
						Table: "dim",
						Match: map[string]string{
							"z_key": "srcZ",
							"a_key": "srcA",
						},
					},
				},
			},
		},
	}
	cfg := Pipeline{
		Storage: Storage{Kind: "sqlite", DB: MultiDB{Mode: "multi_table", Tables: []storage.TableSpec{fact}}},
	}

	plan, err := buildIndexedPlan(cfg, []string{"srcA", "srcZ"})
	if err != nil {
		t.Fatalf("buildIndexedPlan() err=%v, want nil", err)
	}
	if len(plan.Facts) != 1 || len(plan.Facts[0].Columns) != 1 || plan.Facts[0].Columns[0].Lookup == nil {
		t.Fatalf("plan facts missing lookup: %+v", plan)
	}

	// If deterministic, it should pick match field "srcA" -> index 0.
	got := plan.Facts[0].Columns[0].Lookup.MatchFieldIndex
	if got != 0 {
		t.Fatalf("MatchFieldIndex=%d, want 0 (srcA)", got)
	}
}

// TestLoadFactsStreaming_WorkerErrorCancels verifies that a worker error is
// returned and that the engine stops promptly.
//
// This test is intentionally light: it validates the error propagation contract
// and avoids relying on goroutine timing details.
func TestLoadFactsStreaming_WorkerErrorCancels(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("insert failed")

	dim := storage.TableSpec{
		Name: "dim_user",
		Load: storage.LoadSpec{
			Kind: "dimension",
			FromRows: []storage.FromRowSpec{
				{TargetColumn: "user_key", SourceField: "user"},
			},
			Cache: &storage.CacheSpec{KeyColumn: "user_key", ValueColumn: "id"},
		},
	}
	fact := storage.TableSpec{
		Name: "facts",
		Load: storage.LoadSpec{
			Kind: "fact",
			FromRows: []storage.FromRowSpec{
				{
					TargetColumn: "user_id",
					Lookup: &storage.LookupSpec{
						Table: "dim_user",
						Match: map[string]string{"user_key": "user"},
					},
				},
			},
		},
	}

	cfg := Pipeline{
		Storage: Storage{Kind: "sqlite", DB: MultiDB{Mode: "multi_table", Tables: []storage.TableSpec{dim, fact}}},
		Runtime: RuntimeConfig{
			BatchSize:     2,
			LoaderWorkers: 2,
		},
	}

	plan, err := buildIndexedPlan(cfg, []string{"user"})
	if err != nil {
		t.Fatalf("buildIndexedPlan() err=%v, want nil", err)
	}

	repo := &recordingMultiRepo{
		lookupData: map[string]map[string]int64{
			"dim_user": {"a": 1, "b": 2},
		},
		insertErr: wantErr,
	}

	var streamCalls atomic.Int64
	e := &Engine2Pass{
		Repo: repo,
		Stream: streamFromRows([][]any{
			{"a"},
			{"b"},
			{"a"},
			{"b"},
		}, nil, &streamCalls),
	}

	_, err = e.loadFactsStreaming(context.Background(), cfg, []string{"user"}, plan, false)
	if !errors.Is(err, wantErr) {
		t.Fatalf("loadFactsStreaming() err=%v, want %v", err, wantErr)
	}
}

// --- Benchmarks ---------------------------------------------------------------

// BenchmarkNormalizeKey measures normalizeKey on common primitive types.
//
// When to use:
//   - Detect allocation/perf regressions in hot-path normalization.
func BenchmarkNormalizeKey(b *testing.B) {
	cases := []struct {
		name string
		v    any
	}{
		{"string_no_trim", "abc"},
		{"string_trim", "  abc  "},
		{"bytes_no_trim", []byte("abc")},
		{"bytes_trim", []byte("  abc  ")},
		{"int64", int64(123456789)},
		{"float64", 3.1415926535},
		{"bool_true", true},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			var sink string
			for i := 0; i < b.N; i++ {
				sink = normalizeKey(tc.v)
			}
			_ = sink
		})
	}
}

// BenchmarkTypedBindValue measures typedBindValue for typical parser outputs.
func BenchmarkTypedBindValue(b *testing.B) {
	cases := []struct {
		name string
		v    any
	}{
		{"string_no_trim", "abc"},
		{"string_trim", "  abc  "},
		{"bytes_trim", []byte("  abc  ")},
		{"int64", int64(42)},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			var sink any
			for i := 0; i < b.N; i++ {
				sink = typedBindValue(tc.v)
			}
			_ = sink
		})
	}
}

// BenchmarkDedupeTypedKeys measures bounded within-batch dedupe overhead.
func BenchmarkDedupeTypedKeys(b *testing.B) {
	in := make([]any, 0, 4096)
	for i := 0; i < 2048; i++ {
		in = append(in, "x")
		in = append(in, fmt.Sprintf("k%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dedupeTypedKeys(in)
	}
}

// BenchmarkBuildIndexedPlan measures plan compilation cost on a modest schema.
//
// When to use:
//   - Detect regressions from additional sorting or map work in plan compilation.
func BenchmarkBuildIndexedPlan(b *testing.B) {
	dim := storage.TableSpec{
		Name: "dim_user",
		Load: storage.LoadSpec{
			Kind: "dimension",
			FromRows: []storage.FromRowSpec{
				{TargetColumn: "user_key", SourceField: "user"},
			},
			Cache: &storage.CacheSpec{KeyColumn: "user_key", ValueColumn: "id"},
		},
	}
	fact := storage.TableSpec{
		Name: "facts",
		Load: storage.LoadSpec{
			Kind: "fact",
			FromRows: []storage.FromRowSpec{
				{
					TargetColumn: "user_id",
					Lookup: &storage.LookupSpec{
						Table: "dim_user",
						Match: map[string]string{"user_key": "user"},
					},
				},
				{TargetColumn: "event", SourceField: "event"},
			},
		},
	}

	cfg := Pipeline{
		Storage: Storage{Kind: "sqlite", DB: MultiDB{Mode: "multi_table", Tables: []storage.TableSpec{dim, fact}}},
	}
	cols := []string{"user", "event"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := buildIndexedPlan(cfg, cols); err != nil {
			b.Fatalf("buildIndexedPlan() err=%v", err)
		}
	}
}

// BenchmarkInsertFactBatch measures the per-row overhead of building [][]any
// for InsertFactRows.
//
// Note: this benchmark uses a no-op repo and focuses on row construction cost.
func BenchmarkInsertFactBatch(b *testing.B) {
	fact := indexedFact{
		Table: storage.TableSpec{Name: "facts"},
		TargetColumns: []string{
			"user_id",
			"event",
		},
		Columns: []indexedFactColumn{
			{
				TargetColumn: "user_id",
				Lookup: &indexedLookup{
					Table:           "dim_user",
					MatchFieldIndex: 0,
				},
			},
			{
				TargetColumn:     "event",
				SourceFieldIndex: 1,
			},
		},
	}

	repo := &recordingMultiRepo{
		lookupData: map[string]map[string]int64{},
	}
	e := &Engine2Pass{Repo: repo}

	// Build a reusable batch of rows.
	batch := make([]*transformer.Row, 0, 2048)
	for i := 0; i < cap(batch); i++ {
		batch = append(batch, makeRow("alice", "evt"))
	}
	defer func() {
		for _, r := range batch {
			r.Free()
		}
	}()

	cache := map[string]map[string]int64{"dim_user": {"alice": 1}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := e.insertFactBatch(context.Background(), fact, batch, cache, false)
		if err != nil {
			b.Fatalf("insertFactBatch() err=%v", err)
		}
	}
}

// mustOptions adapts plain maps into config.Options.
// It is intentionally local to avoid collisions with other *_test helpers.
func mustOptions(m map[string]any) config.Options {
	if m == nil {
		return config.Options{}
	}
	return config.Options(m)
}
