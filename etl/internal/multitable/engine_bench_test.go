package multitable

import (
	"context"
	"testing"

	"etl/internal/config"
	"etl/internal/storage"
	"etl/internal/transformer"
)

func BenchmarkResolveBatchLookups_AndInsertFactBatch(b *testing.B) {
	repo := newFakeMultiRepo()
	e := &Engine2Pass{Repo: repo}

	cfg := Pipeline{
		Transform: []config.Transform{{Kind: "validate", Options: config.Options{"policy": "lenient"}}},
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
		Runtime: RuntimeConfig{BatchSize: 1024, ChannelBuffer: 256, TransformWorkers: 1, ReaderWorkers: 1},
	}

	cols := []string{"pcv"}
	plan, err := buildIndexedPlan(cfg, cols)
	if err != nil {
		b.Fatalf("buildIndexedPlan error: %v", err)
	}

	// Seed repo with ids so lookups resolve.
	repo.ids["public.vehicles"] = make(map[string]int64)
	for i := 0; i < 1024; i++ {
		repo.ids["public.vehicles"][itoa(i)] = int64(i + 1)
	}

	// Build a batch with 1024 rows.
	batch := make([]*transformer.Row, 1024)
	for i := 0; i < 1024; i++ {
		batch[i] = &transformer.Row{V: []any{itoa(i)}}
	}

	cache := map[string]map[string]int64{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.resolveBatchLookups(context.Background(), plan, batch, cache); err != nil {
			b.Fatalf("resolveBatchLookups error: %v", err)
		}
		if _, _, err := e.insertFactBatch(context.Background(), plan.Facts[0], batch, cache, false); err != nil {
			b.Fatalf("insertFactBatch error: %v", err)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [32]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + (i % 10))
		i /= 10
	}
	return string(buf[pos:])
}
