package multitable

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"

	"etl/internal/config"
	"etl/internal/schema"
	"etl/internal/storage"
)

type Runner struct {
	// Factory seam so runner is unit-testable.
	NewMultiRepo func(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error)
}

func NewDefaultRunner() *Runner {
	return &Runner{
		NewMultiRepo: func(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
			return storage.NewMulti(ctx, cfg)
		},
	}
}

func (r *Runner) Run(ctx context.Context, cfg Pipeline) error {
	if err := validateMultiConfig(cfg); err != nil {
		return err
	}

	dsn := os.ExpandEnv(cfg.Storage.DB.DSN)

	mr, err := r.NewMultiRepo(ctx, storage.MultiConfig{
		Kind: cfg.Storage.Kind,
		DSN:  dsn,
	})
	if err != nil {
		return fmt.Errorf("multi repo: %w", err)
	}
	defer mr.Close()

	columns := requiredInputColumns(cfg)

	recs, err := StreamAndCollectRecords(ctx, cfg, columns)
	if err != nil {
		return err
	}

	// Timestamped, testable logger (injectable).
	logger := log.New(os.Stdout, "", log.LstdFlags)

	engine := &Engine{
		Repo:   mr,
		Logger: logger,
	}
	return engine.Run(ctx, cfg, recs)
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

// requiredInputColumns derives the minimal set of canonical fields that must be present
// in the transformed record stream.
// It includes fields referenced by:
// - storage.db.tables[].load.from_rows (source_field + lookup.match source fields)
// - transform.coerce.options.types keys
// - validate.contract.fields names
func requiredInputColumns(cfg Pipeline) []string {
	set := map[string]struct{}{}

	// 1) Storage-driven fields
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

	// 2) Transform-driven fields (coerce types keys)
	for _, field := range coerceFields(cfg.Transform) {
		set[field] = struct{}{}
	}

	// 3) Validation-driven fields (contract fields)
	for _, field := range contractFields(cfg.Transform) {
		set[field] = struct{}{}
	}

	cols := make([]string, 0, len(set))
	for c := range set {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

func coerceFields(ts []config.Transform) []string {
	fields := map[string]struct{}{}
	for _, t := range ts {
		if t.Kind != "coerce" {
			continue
		}
		m := t.Options.StringMap("types")
		for k := range m {
			if k != "" {
				fields[k] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(fields))
	for k := range fields {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func contractFields(ts []config.Transform) []string {
	c, err := contractFromTransforms(ts)
	if err != nil || c == nil {
		return nil
	}
	fields := make([]string, 0, len(c.Fields))
	for _, f := range c.Fields {
		if f.Name != "" {
			fields = append(fields, f.Name)
		}
	}
	sort.Strings(fields)
	return fields
}

func contractFromTransforms(ts []config.Transform) (*schema.Contract, error) {
	for _, t := range ts {
		if t.Kind != "validate" {
			continue
		}
		raw := t.Options.Any("contract")
		if raw == nil {
			return nil, nil
		}
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		var c schema.Contract
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, err
		}
		return &c, nil
	}
	return nil, nil
}
