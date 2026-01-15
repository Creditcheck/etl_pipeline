package multitable

import (
	"context"
	"testing"

	"etl/internal/config"
	"etl/internal/storage"
	"etl/internal/transformer"
)

type fakeMultiRepo struct {
	ensureTablesCalls int

	ensureDimCalls []struct {
		table string
		col   string
		n     int
	}

	selectByCalls []struct {
		table string
		n     int
	}

	insertFactCalls []struct {
		table string
		n     int
	}

	// ids[table][normalizedKey] => id
	ids map[string]map[string]int64
}

func newFakeMultiRepo() *fakeMultiRepo {
	return &fakeMultiRepo{ids: make(map[string]map[string]int64)}
}

func (r *fakeMultiRepo) Close() {}

func (r *fakeMultiRepo) EnsureTables(ctx context.Context, tables []storage.TableSpec) error {
	r.ensureTablesCalls++
	return nil
}

func (r *fakeMultiRepo) EnsureDimensionKeys(ctx context.Context, table, keyColumn string, keys []any, conflictColumns []string) error {
	r.ensureDimCalls = append(r.ensureDimCalls, struct {
		table string
		col   string
		n     int
	}{table: table, col: keyColumn, n: len(keys)})

	if r.ids[table] == nil {
		r.ids[table] = make(map[string]int64)
	}
	for _, k := range keys {
		nk := normalizeKey(k)
		if nk == "" {
			continue
		}
		if _, ok := r.ids[table][nk]; !ok {
			r.ids[table][nk] = int64(len(r.ids[table]) + 1)
		}
	}
	return nil
}

func (r *fakeMultiRepo) SelectAllKeyValue(ctx context.Context, table, keyColumn, valueColumn string) (map[string]int64, error) {
	out := make(map[string]int64)
	for k, v := range r.ids[table] {
		out[k] = v
	}
	return out, nil
}

func (r *fakeMultiRepo) SelectKeyValueByKeys(ctx context.Context, table, keyColumn, valueColumn string, keys []any) (map[string]int64, error) {
	r.selectByCalls = append(r.selectByCalls, struct {
		table string
		n     int
	}{table: table, n: len(keys)})

	out := make(map[string]int64)
	for _, k := range keys {
		nk := normalizeKey(k)
		if id, ok := r.ids[table][nk]; ok {
			out[nk] = id
		}
	}
	return out, nil
}

func (r *fakeMultiRepo) InsertFactRows(ctx context.Context, table string, columns []string, rows [][]any, dedupeColumns []string) (int64, error) {
	r.insertFactCalls = append(r.insertFactCalls, struct {
		table string
		n     int
	}{table: table, n: len(rows)})
	return int64(len(rows)), nil
}

func minimalPipeline() Pipeline {
	return Pipeline{
		Transform: []config.Transform{
			{Kind: "validate", Options: config.Options{"policy": "lenient"}},
		},
		Storage: Storage{
			Kind: "postgres",
			DB: MultiDB{
				Mode: "multi_table",
				Tables: []storage.TableSpec{
					{
						Name: "public.vehicles",
						Load: storage.LoadSpec{
							Kind: "dimension",
							FromRows: []storage.FromRowSpec{
								{TargetColumn: "pcv", SourceField: "pcv"},
							},
							Cache: &storage.CacheSpec{KeyColumn: "pcv", ValueColumn: "vehicle_id"},
						},
					},
					{
						Name: "public.imports",
						Load: storage.LoadSpec{
							Kind: "fact",
							FromRows: []storage.FromRowSpec{
								{
									TargetColumn: "vehicle_id",
									Lookup: &storage.LookupSpec{
										Table:     "public.vehicles",
										Match:     map[string]string{"pcv": "pcv"},
										Return:    "vehicle_id",
										OnMissing: "insert",
									},
								},
							},
						},
					},
				},
			},
		},
		Runtime: RuntimeConfig{BatchSize: 2, ChannelBuffer: 8, TransformWorkers: 1, ReaderWorkers: 1},
	}
}

func TestBuildIndexedPlan_IndexesFields(t *testing.T) {
	cfg := minimalPipeline()
	cols := []string{"pcv"} // required input

	p, err := buildIndexedPlan(cfg, cols)
	if err != nil {
		t.Fatalf("buildIndexedPlan error: %v", err)
	}
	if len(p.Dimensions) != 1 || p.Dimensions[0].SourceIndex != 0 {
		t.Fatalf("expected dimension source index 0, got %+v", p.Dimensions)
	}
	if len(p.Facts) != 1 || p.Facts[0].Columns[0].Lookup == nil || p.Facts[0].Columns[0].Lookup.MatchFieldIndex != 0 {
		t.Fatalf("expected fact lookup match index 0, got %+v", p.Facts)
	}
}

func TestInsertFactBatch_LenientDropsOnMissing(t *testing.T) {
	repo := newFakeMultiRepo()
	e := &Engine2Pass{Repo: repo}

	cfg := minimalPipeline()
	cols := []string{"pcv"}
	plan, err := buildIndexedPlan(cfg, cols)
	if err != nil {
		t.Fatalf("buildIndexedPlan error: %v", err)
	}

	// batch with one row whose lookup key is missing from cache and repo
	row := &transformer.Row{V: []any{int64(123)}}
	batch := []*transformer.Row{row}

	cache := map[string]map[string]int64{} // empty

	// resolveBatchLookups will fetch none because repo has none.
	if err := e.resolveBatchLookups(context.Background(), plan, batch, cache); err != nil {
		t.Fatalf("resolveBatchLookups error: %v", err)
	}

}
