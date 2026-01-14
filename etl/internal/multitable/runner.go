package multitable

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"

	"etl/internal/storage"
)

type Runner struct {
	NewPool func(ctx context.Context, dsn string) (*pgxpool.Pool, error)

	// storage-agnostic factory seam
	NewRepository func(ctx context.Context, cfg storage.Config) (storage.Repository, error)
}

func NewDefaultRunner() *Runner {
	return &Runner{
		NewPool: func(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
			return pgxpool.New(ctx, dsn)
		},
		NewRepository: func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
			return storage.New(ctx, cfg)
		},
	}
}

func (r *Runner) Run(ctx context.Context, cfg Pipeline) error {
	if err := validateMultiConfig(cfg); err != nil {
		return err
	}

	dsn := os.ExpandEnv(cfg.Storage.DB.DSN)

	pool, err := r.NewPool(ctx, dsn)
	if err != nil {
		return fmt.Errorf("db pool: %w", err)
	}
	defer pool.Close()

	// Derive canonical input fields needed from config (no hardcoding).
	columns := requiredInputColumns(cfg)

	// Option 1: reuse existing parsing + transformer stack to produce typed, validated records.
	// Implement this in internal/multitable/stream_stack.go (see below).
	recs, err := StreamAndCollectRecords(ctx, cfg, columns)
	if err != nil {
		return err
	}

	// COPY router uses storage.New(), no backend imports here.
	copyer := NewTableCopyer(r.NewRepository, storage.Config{
		Kind: cfg.Storage.Kind,
		DSN:  dsn,
	})
	defer copyer.Close()

	engine := &Engine{Pool: pool}
	return engine.Run(ctx, cfg, recs, copyer)
}

func validateMultiConfig(cfg Pipeline) error {
	if cfg.Source.Kind != "file" || cfg.Source.File == nil || cfg.Source.File.Path == "" {
		return fmt.Errorf("source.kind=file and source.file.path are required")
	}
	if cfg.Parser.Kind != "csv" {
		return fmt.Errorf("parser.kind must be csv")
	}
	if cfg.Storage.Kind == "" {
		return fmt.Errorf("storage.kind must be set")
	}
	if cfg.Storage.DB.Mode != "multi_table" {
		return fmt.Errorf("storage.db.mode must be multi_table")
	}
	if len(cfg.Storage.DB.Tables) == 0 {
		return fmt.Errorf("storage.db.tables must not be empty")
	}
	return nil
}

// requiredInputColumns derives the minimal set of canonical fields required by storage.db.tables[].load rules.
func requiredInputColumns(cfg Pipeline) []string {
	set := map[string]struct{}{}

	for _, t := range cfg.Storage.DB.Tables {
		for _, fr := range t.Load.FromRows {
			if fr.SourceField != "" {
				set[fr.SourceField] = struct{}{}
			}
			if fr.Lookup != nil {
				for _, srcField := range fr.Lookup.Match {
					set[srcField] = struct{}{}
				}
			}
		}
	}

	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
