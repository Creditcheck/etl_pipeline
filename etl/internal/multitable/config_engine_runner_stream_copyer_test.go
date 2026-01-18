package multitable

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"etl/internal/config"
	"etl/internal/schema"
	"etl/internal/storage"
)

func boolPtr(b bool) *bool { return &b }

// TestValidateMultiConfig verifies that validateMultiConfig enforces the minimum
// required shape for a multi-table pipeline.
//
// When to use:
//   - Use this test as a safety net when evolving the JSON schema.
//   - It protects runtime from failing deep inside the engine due to misconfig.
//
// Edge cases:
//   - Missing file source.
//   - Unsupported parser kind.
//   - Missing storage kind.
//   - Wrong DB mode.
//   - No tables.
//
// Errors:
//   - Returns a descriptive error on the first failing invariant.
func TestValidateMultiConfig(t *testing.T) {
	t.Parallel()

	base := Pipeline{
		Source: Source{Kind: "file", File: &FileSource{Path: "in.csv"}},
		Parser: Parser{Kind: "csv"},
		Storage: Storage{
			Kind: "sqlite",
			DB: MultiDB{
				Mode: "multi_table",
				Tables: []storage.TableSpec{
					{Name: "t1"},
				},
			},
		},
	}

	tests := []struct {
		name    string
		mutate  func(Pipeline) Pipeline
		wantErr string
	}{
		{
			name: "ok",
			mutate: func(p Pipeline) Pipeline {
				return p
			},
			wantErr: "",
		},
		{
			name: "missing source file path",
			mutate: func(p Pipeline) Pipeline {
				p.Source.File.Path = ""
				return p
			},
			wantErr: "source.kind=file",
		},
		{
			name: "unsupported parser kind",
			mutate: func(p Pipeline) Pipeline {
				p.Parser.Kind = "xml"
				return p
			},
			wantErr: "parser.kind must be csv or json",
		},
		{
			name: "missing storage kind",
			mutate: func(p Pipeline) Pipeline {
				p.Storage.Kind = ""
				return p
			},
			wantErr: "storage.kind must be set",
		},
		{
			name: "wrong db mode",
			mutate: func(p Pipeline) Pipeline {
				p.Storage.DB.Mode = "single_table"
				return p
			},
			wantErr: "storage.db.mode must be multi_table",
		},
		{
			name: "empty tables",
			mutate: func(p Pipeline) Pipeline {
				p.Storage.DB.Tables = nil
				return p
			},
			wantErr: "storage.db.tables must not be empty",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.mutate(base)
			err := validateMultiConfig(cfg)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("validateMultiConfig() err=%v, want nil", err)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("validateMultiConfig() err=nil, want contains %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("validateMultiConfig() err=%q, want contains %q", err.Error(), tt.wantErr)
				}
			}
		})
	}
}

// TestContractFromTransforms verifies that contractFromTransforms locates a validate
// transform and decodes its embedded contract blob into a schema.Contract.
//
// When to use:
//   - Any time you evolve contract JSON structure or the validate transform config.
//
// Edge cases:
//   - No validate transform.
//   - Validate transform without contract.
//   - Contract values are decoded from generic map[string]any.
//
// Errors:
//   - Returns marshal/unmarshal errors for malformed contract shapes.
func TestContractFromTransforms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ts      []config.Transform
		want    *schema.Contract
		wantErr bool
	}{
		{
			name: "no validate transform",
			ts:   nil,
			want: nil,
		},
		{
			name: "validate without contract",
			ts: []config.Transform{
				{Kind: "validate", Options: mustOptions(map[string]any{})},
			},
			want: nil,
		},
		{
			name: "contract decodes",
			ts: []config.Transform{
				{Kind: "validate", Options: mustOptions(map[string]any{
					"contract": map[string]any{
						"fields": []any{
							map[string]any{"name": "a", "type": "text", "required": true},
							map[string]any{"name": "b", "type": "bigint", "required": false},
						},
					},
				})},
			},
			want: &schema.Contract{
				Fields: []schema.Field{
					{Name: "a", Type: "text", Required: true},
					{Name: "b", Type: "bigint", Required: false},
				},
			},
		},
		{
			name: "malformed contract fails",
			ts: []config.Transform{
				{Kind: "validate", Options: mustOptions(map[string]any{
					"contract": func() {}, // json.Marshal should fail
				})},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := contractFromTransforms(tt.ts)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("contractFromTransforms() err=nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("contractFromTransforms() err=%v, want nil", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("contractFromTransforms()=%#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestValidateInputsFromPipeline verifies type normalization and required-field extraction
// for the validate stage.
//
// When to use:
//   - Whenever contract parsing or type normalization rules change.
//
// Edge cases:
//   - Empty type defaults to text.
//   - "string" maps to text.
//   - Common typo "bitint" maps to bigint.
//   - Empty field names are ignored.
func TestValidateInputsFromPipeline(t *testing.T) {
	t.Parallel()

	ts := []config.Transform{
		{Kind: "validate", Options: mustOptions(map[string]any{
			"contract": map[string]any{
				"fields": []any{
					map[string]any{"name": "a", "type": "", "required": true},
					map[string]any{"name": "b", "type": "string", "required": false},
					map[string]any{"name": "c", "type": "bitint", "required": true},
					map[string]any{"name": "", "type": "text", "required": true},
				},
			},
		})},
	}

	required, types, err := validateInputsFromPipeline(ts)
	if err != nil {
		t.Fatalf("validateInputsFromPipeline() err=%v, want nil", err)
	}
	if want := []string{"a", "c"}; !reflect.DeepEqual(required, want) {
		t.Fatalf("required=%v, want %v", required, want)
	}
	wantTypes := map[string]string{
		"a": "text",
		"b": "text",
		"c": "bigint",
	}
	if !reflect.DeepEqual(types, wantTypes) {
		t.Fatalf("types=%v, want %v", types, wantTypes)
	}
}

// TestIsLenient verifies the policy detection logic for validation transforms.
//
// When to use:
//   - Anytime validate policy handling changes.
//
// Edge cases:
//   - policy is any scalar, converted through fmt.Sprint.
//   - policy is whitespace and case-insensitive.
func TestIsLenient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ts   []config.Transform
		want bool
	}{
		{"no transforms", nil, false},
		{"no validate transforms", []config.Transform{{Kind: "coerce", Options: mustOptions(nil)}}, false},
		{"validate strict", []config.Transform{{Kind: "validate", Options: mustOptions(map[string]any{"policy": "strict"})}}, false},
		{"validate lenient", []config.Transform{{Kind: "validate", Options: mustOptions(map[string]any{"policy": "LeNiEnT"})}}, true},
		{"validate lenient with spaces", []config.Transform{{Kind: "validate", Options: mustOptions(map[string]any{"policy": "  lenient  "})}}, true},
		{"first validate strict second lenient", []config.Transform{
			{Kind: "validate", Options: mustOptions(map[string]any{"policy": "strict"})},
			{Kind: "validate", Options: mustOptions(map[string]any{"policy": "lenient"})},
		}, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isLenient(tt.ts); got != tt.want {
				t.Fatalf("isLenient()=%v, want %v", got, tt.want)
			}
		})
	}
}

// TestNormalizeKey verifies normalizeKey behavior for common hot-path types.
//
// When to use:
//   - Anytime you change key normalization or add new hot-path cases.
//
// Edge cases:
//   - nil and empty normalize to "".
//   - strings/[]byte trim only when edge space is present.
//   - numeric formatting uses strconv, avoiding fmt allocation.
func TestNormalizeKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, ""},
		{"string no trim", "abc", "abc"},
		{"string trim", " abc ", "abc"},
		{"bytes no trim", []byte("abc"), "abc"},
		{"bytes trim", []byte(" abc "), "abc"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"int", int(42), "42"},
		{"int64", int64(-7), "-7"},
		{"uint64", uint64(9), "9"},
		{"float64", float64(1.5), "1.5"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := normalizeKey(tt.in); got != tt.want {
				t.Fatalf("normalizeKey(%v)=%q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestTypedBindValue verifies typedBindValue preserves types while applying trim rules
// compatible with DB bind semantics.
//
// When to use:
//   - Anytime parsing/trim rules change.
//
// Edge cases:
//   - []byte is trimmed via bytes.TrimSpace and may share backing array.
//   - string trim only if edge space is present.
//   - non-string values pass through unchanged.
func TestTypedBindValue(t *testing.T) {
	t.Parallel()

	inBytes := []byte(" abc ")
	gotBytes := typedBindValue(inBytes).([]byte)
	if string(gotBytes) != "abc" {
		t.Fatalf("typedBindValue([]byte)=%q, want %q", string(gotBytes), "abc")
	}

	if got := typedBindValue(" abc ").(string); got != "abc" {
		t.Fatalf("typedBindValue(string)=%q, want %q", got, "abc")
	}

	if got := typedBindValue(123).(int); got != 123 {
		t.Fatalf("typedBindValue(int)=%v, want %v", got, 123)
	}
}

// TestDedupeTypedKeys verifies dedupeTypedKeys performs bounded, stable-first dedupe.
//
// When to use:
//   - Any time you change dedupe rules.
//
// Edge cases:
//   - Empty/blank keys are dropped.
//   - The first typed value for a normalized key is preserved.
func TestDedupeTypedKeys(t *testing.T) {
	t.Parallel()

	in := []any{" a ", "a", []byte("b"), []byte(" b "), "", nil, "c"}
	got := dedupeTypedKeys(in)

	// "a" keeps first typed value (" a ").
	// "b" keeps first typed value ([]byte("b")) because normalizeKey("b")==normalizeKey(" b ").
	want := []any{" a ", []byte("b"), "c"}

	if len(got) != len(want) {
		t.Fatalf("dedupeTypedKeys() len=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		gb, gok := got[i].([]byte)
		wb, wok := want[i].([]byte)
		if gok && wok {
			if string(gb) != string(wb) {
				t.Fatalf("dedupeTypedKeys()[%d]=%q, want %q", i, string(gb), string(wb))
			}
			continue
		}
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("dedupeTypedKeys()[%d]=%#v, want %#v", i, got[i], want[i])
		}
	}
}

// TestPickLookupMatchField verifies deterministic match selection.
func TestPickLookupMatchField(t *testing.T) {
	t.Parallel()

	match := map[string]string{
		"z": "src_z",
		"a": "src_a",
		"m": "src_m",
	}
	if got := pickLookupMatchField(match); got != "src_a" {
		t.Fatalf("pickLookupMatchField()=%q, want %q", got, "src_a")
	}
	if got := pickLookupMatchField(nil); got != "" {
		t.Fatalf("pickLookupMatchField(nil)=%q, want empty", got)
	}
}

// TestBuildIndexedPlan_SortsAndIndexes verifies buildIndexedPlan produces stable ordering
// and correct column indexes for dimension and fact specs.
//
// When to use:
//   - When changing storage table specs or plan compilation.
//
// Note:
//   - Lookup match is a map in the storage schema; buildIndexedPlan must produce
//     deterministic results regardless of map iteration order.
func TestBuildIndexedPlan_SortsAndIndexes(t *testing.T) {
	t.Parallel()

	cols := []string{"id", "user_email", "event_ts"}

	cfg := Pipeline{
		Storage: Storage{
			DB: MultiDB{
				Tables: []storage.TableSpec{
					{
						Name: "events",
						Columns: []storage.ColumnSpec{
							{Name: "user_id", Nullable: boolPtr(false)},
							{Name: "ts", Nullable: boolPtr(false)},
						},
						Load: storage.LoadSpec{
							Kind: "fact",
							FromRows: []storage.FromRowSpec{
								{TargetColumn: "ts", SourceField: "event_ts"},
								{
									TargetColumn: "user_id",
									Lookup: &storage.LookupSpec{
										Table: "users_dim",
										Match: map[string]string{
											// db key col -> source field
											"email": "user_email",
										},
									},
								},
							},
						},
					},
					{
						Name: "users_dim",
						Load: storage.LoadSpec{
							Kind: "dimension",
							FromRows: []storage.FromRowSpec{
								{TargetColumn: "email", SourceField: "user_email"},
							},
							Cache: &storage.CacheSpec{
								KeyColumn:   "email",
								ValueColumn: "id",
							},
						},
					},
				},
			},
		},
	}

	plan, err := buildIndexedPlan(cfg, cols)
	if err != nil {
		t.Fatalf("buildIndexedPlan() err=%v, want nil", err)
	}

	if len(plan.Dimensions) != 1 || plan.Dimensions[0].Table.Name != "users_dim" {
		t.Fatalf("dimensions=%v, want [users_dim]", plan.Dimensions)
	}
	if plan.Dimensions[0].SourceIndex != 1 {
		t.Fatalf("users_dim SourceIndex=%d, want 1", plan.Dimensions[0].SourceIndex)
	}
	if plan.Dimensions[0].KeyColumn != "email" || plan.Dimensions[0].ValueColumn != "id" {
		t.Fatalf("dim key/value=%s/%s, want email/id", plan.Dimensions[0].KeyColumn, plan.Dimensions[0].ValueColumn)
	}

	if len(plan.Facts) != 1 || plan.Facts[0].Table.Name != "events" {
		t.Fatalf("facts=%v, want [events]", plan.Facts)
	}

	var gotIdx []int
	for _, c := range plan.Facts[0].Columns {
		if c.Lookup != nil {
			gotIdx = append(gotIdx, c.Lookup.MatchFieldIndex)
		} else {
			gotIdx = append(gotIdx, c.SourceFieldIndex)
		}
	}
	sort.Ints(gotIdx)
	if !reflect.DeepEqual(gotIdx, []int{1, 2}) {
		t.Fatalf("compiled column indexes=%v, want [1 2]", gotIdx)
	}
}

// TestRepoKey verifies repoKey produces stable join keys.
func TestRepoKey(t *testing.T) {
	t.Parallel()

	if got, want := repoKey("t", []string{"a", "b"}), "t|a,b"; got != want {
		t.Fatalf("repoKey()=%q, want %q", got, want)
	}
}

// TestTableCopyer_CopyFromTable validates argument checks and repository reuse.
//
// Edge cases:
//   - Empty table and empty columns return errors.
//   - Identical (table, columns) reuses the same repo instance.
func TestTableCopyer_CopyFromTable(t *testing.T) {
	t.Parallel()

	var created atomic.Int64
	newRepo := func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
		created.Add(1)
		return &fakeRepo{copyN: 123}, nil
	}

	tc := NewTableCopyer(newRepo, storage.Config{Kind: "sqlite"})
	t.Cleanup(tc.Close)

	_, err := tc.CopyFromTable(context.Background(), "", []string{"a"}, [][]any{{1}})
	if err == nil || !strings.Contains(err.Error(), "table is empty") {
		t.Fatalf("CopyFromTable(empty table) err=%v, want contains %q", err, "table is empty")
	}

	_, err = tc.CopyFromTable(context.Background(), "t", nil, [][]any{{1}})
	if err == nil || !strings.Contains(err.Error(), "columns empty") {
		t.Fatalf("CopyFromTable(empty columns) err=%v, want contains %q", err, "columns empty")
	}

	n1, err := tc.CopyFromTable(context.Background(), "t", []string{"a", "b"}, [][]any{{1, 2}})
	if err != nil {
		t.Fatalf("CopyFromTable() err=%v, want nil", err)
	}
	n2, err := tc.CopyFromTable(context.Background(), "t", []string{"a", "b"}, [][]any{{3, 4}})
	if err != nil {
		t.Fatalf("CopyFromTable() err=%v, want nil", err)
	}
	if n1 != 123 || n2 != 123 {
		t.Fatalf("CopyFromTable() n1=%d n2=%d, want 123", n1, n2)
	}
	if got := created.Load(); got != 1 {
		t.Fatalf("repos created=%d, want 1", got)
	}
}

// TestTableCopyer_GetRepo_Concurrent verifies getRepo does not leak repositories
// when multiple goroutines race to create the same (table, columns) repo.
//
// Notes:
//   - Run with: go test -race ./...
func TestTableCopyer_GetRepo_Concurrent(t *testing.T) {
	t.Parallel()

	var (
		created atomic.Int64
		closed  atomic.Int64
	)

	newRepo := func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
		created.Add(1)
		return &fakeRepo{
			copyN: 1,
			onClose: func() {
				closed.Add(1)
			},
		}, nil
	}

	tc := NewTableCopyer(newRepo, storage.Config{Kind: "sqlite"})
	t.Cleanup(tc.Close)

	ctx := context.Background()
	const goroutines = 32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, _ = tc.CopyFromTable(ctx, "t", []string{"a", "b"}, [][]any{{1, 2}})
		}()
	}
	wg.Wait()

	// Some races may create extra repos, but losers must be closed promptly.
	if created.Load() < 1 {
		t.Fatalf("created=%d, want >=1", created.Load())
	}

	tc.Close()
	if closed.Load() < created.Load()-1 {
		t.Fatalf("closed=%d created=%d, want closed >= created-1", closed.Load(), created.Load())
	}
}

// mustOptions builds a config.Options from a plain map.
//
// This helper exists to keep tests readable; config.Options is map-backed.
//func mustOptions(m map[string]any) config.Options {
//	if m == nil {
//		return config.Options{}
//	}
//	return config.Options(m)
//}

type fakeRepo struct {
	copyN   int64
	onClose func()
}

func (f *fakeRepo) CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	return f.copyN, nil
}

func (f *fakeRepo) Exec(ctx context.Context, sql string) error { return nil }

func (f *fakeRepo) Close() {
	if f.onClose != nil {
		f.onClose()
	}
}

// TestTableCopyer_FactoryError verifies getRepo wraps factory errors with context.
func TestTableCopyer_FactoryError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	newRepo := func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
		return nil, wantErr
	}

	tc := NewTableCopyer(newRepo, storage.Config{Kind: "sqlite"})
	t.Cleanup(tc.Close)

	_, err := tc.CopyFromTable(context.Background(), "t", []string{"a"}, [][]any{{1}})
	if err == nil {
		t.Fatalf("CopyFromTable() err=nil, want error")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("CopyFromTable() err=%v, want wrapped %v", err, wantErr)
	}
	if !strings.Contains(err.Error(), "new repo") {
		t.Fatalf("CopyFromTable() err=%q, want contains %q", err.Error(), "new repo")
	}
}
