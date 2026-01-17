package multitable

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"etl/internal/config"
	"etl/internal/schema"
	"etl/internal/storage"
)

// Engine is the minimal contract Runner needs to execute a pipeline.
//
// When to use:
//   - Production: Engine2Pass satisfies this contract.
//   - Tests: provide a fake implementation to observe inputs and force errors.
type Engine interface {
	Run(ctx context.Context, cfg Pipeline, columns []string) error
}

// Runner coordinates multi-table pipeline execution.
//
// Runner is intentionally thin: it validates config, expands environment variables,
// computes the streaming column set, constructs dependencies, and runs the engine.
//
// Testability:
// Runner supports dependency injection via the public fields below. In production,
// NewDefaultRunner populates these fields with default implementations.
type Runner struct {
	// NewMultiRepo constructs a storage.MultiRepository.
	// This seam makes Runner unit-testable without importing backend packages.
	NewMultiRepo func(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error)

	// NewEngine constructs an Engine (default: *Engine2Pass).
	// This seam allows unit tests to validate the computed columns without running
	// the full streaming system.
	NewEngine func(repo storage.MultiRepository, logger Logger) Engine

	// NewLogger constructs a Logger used by Runner and the engine.
	// Default logs to LogWriter (typically os.Stdout).
	NewLogger func(w io.Writer) Logger

	// ExpandEnv expands environment variables in DSNs (default: os.ExpandEnv).
	ExpandEnv func(s string) string

	// LogWriter is the destination for the default logger.
	// If nil, os.Stdout is used.
	LogWriter io.Writer
}

// NewDefaultRunner returns a Runner wired to production implementations.
func NewDefaultRunner() *Runner {
	return &Runner{
		NewMultiRepo: func(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
			return storage.NewMulti(ctx, cfg)
		},
		NewEngine: func(repo storage.MultiRepository, logger Logger) Engine {
			return &Engine2Pass{
				Repo:   repo,
				Logger: logger,
			}
		},
		NewLogger: func(w io.Writer) Logger {
			return log.New(w, "", log.LstdFlags)
		},
		ExpandEnv: os.ExpandEnv,
		LogWriter: os.Stdout,
	}
}

// Run validates config, constructs dependencies, and executes the pipeline.
//
// Errors:
//   - Returns validation errors for malformed configs.
//   - Wraps repository construction failures.
//   - Returns any error returned by the engine.
func (r *Runner) Run(ctx context.Context, cfg Pipeline) error {
	if err := validateMultiConfig(cfg); err != nil {
		return err
	}

	deps, err := r.deps()
	if err != nil {
		return err
	}

	dsn := deps.expandEnv(cfg.Storage.DB.DSN)

	repo, err := deps.newMultiRepo(ctx, storage.MultiConfig{
		Kind: cfg.Storage.Kind,
		DSN:  dsn,
	})
	if err != nil {
		return fmt.Errorf("multi repo: %w", err)
	}
	defer repo.Close()

	columns, added := requiredInputColumns(cfg)
	if len(added) > 0 {
		deps.logger.Printf("stage=columns added=%v total=%d", added, len(columns))
	}

	engine := deps.newEngine(repo, deps.logger)
	return engine.Run(ctx, cfg, columns)
}

type runnerDeps struct {
	newMultiRepo func(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error)
	newEngine    func(repo storage.MultiRepository, logger Logger) Engine
	logger       Logger
	expandEnv    func(string) string
}

func (r *Runner) deps() (*runnerDeps, error) {
	newMultiRepo := r.NewMultiRepo
	if newMultiRepo == nil {
		return nil, fmt.Errorf("runner: NewMultiRepo is required")
	}
	newEngine := r.NewEngine
	if newEngine == nil {
		// Default to Engine2Pass to keep behavior stable if a caller only injects NewMultiRepo.
		newEngine = func(repo storage.MultiRepository, logger Logger) Engine {
			return &Engine2Pass{Repo: repo, Logger: logger}
		}
	}

	expandEnv := r.ExpandEnv
	if expandEnv == nil {
		expandEnv = os.ExpandEnv
	}

	w := r.LogWriter
	if w == nil {
		w = os.Stdout
	}

	newLogger := r.NewLogger
	if newLogger == nil {
		newLogger = func(w io.Writer) Logger { return log.New(w, "", log.LstdFlags) }
	}
	logger := newLogger(w)

	return &runnerDeps{
		newMultiRepo: newMultiRepo,
		newEngine:    newEngine,
		logger:       logger,
		expandEnv:    expandEnv,
	}, nil
}

func validateMultiConfig(cfg Pipeline) error {
	if cfg.Storage.DB.Mode != "multi_table" {
		return fmt.Errorf("storage.db.mode must be multi_table")
	}
	if len(cfg.Storage.DB.Tables) == 0 {
		return fmt.Errorf("storage.db.tables must not be empty")
	}
	if cfg.Storage.Kind == "" {
		return fmt.Errorf("storage.kind must be set")
	}
	if cfg.Source.Kind != "file" || cfg.Source.File == nil || cfg.Source.File.Path == "" {
		return fmt.Errorf("source.kind=file and source.file.path are required")
	}
	if cfg.Parser.Kind != "csv" && cfg.Parser.Kind != "json" {
		return fmt.Errorf("parser.kind must be csv or json")
	}
	return nil
}

// requiredInputColumns returns:
//
//   - columns: the full ordered column set used by the streaming pipeline.
//     This includes source fields referenced by storage/lookup/contract/coerce,
//     PLUS any fields produced by transforms (e.g. hash target_field).
//
//   - added: which fields were added because they are generated by transforms.
//
// Rationale:
// Streaming rows are represented as fixed-width slices keyed by this column list.
// If a transform wants to write a new field (like row_hash) it MUST exist in the
// column set, otherwise the value is silently absent and later inserts may fail
// (e.g. NOT NULL violations).
func requiredInputColumns(cfg Pipeline) (columns []string, added []string) {
	set := make(map[string]struct{})

	add := func(field string) {
		field = strings.TrimSpace(field)
		if field == "" {
			return
		}
		set[field] = struct{}{}
	}

	// 1) Storage-driven fields.
	for _, t := range cfg.Storage.DB.Tables {
		for _, fr := range t.Load.FromRows {
			add(fr.SourceField)
			if fr.Lookup != nil {
				// Lookup match is map[dbKeyCol]sourceField. :contentReference[oaicite:3]{index=3}
				for _, srcField := range fr.Lookup.Match {
					add(srcField)
				}
			}
		}
	}

	// 2) Transform-driven input fields (e.g. coerce.types keys).
	for _, field := range coerceFields(cfg.Transform) {
		add(field)
	}

	// 3) Validation-driven fields (contract fields).
	for _, field := range contractFields(cfg.Transform) {
		add(field)
	}

	// 4) Transform-generated fields (hash target_field, etc.).
	for _, f := range transformGeneratedFields(cfg.Transform) {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if _, exists := set[f]; exists {
			continue
		}
		set[f] = struct{}{}
		added = append(added, f)
	}
	sort.Strings(added)

	cols := make([]string, 0, len(set))
	for c := range set {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols, added
}

// coerceFields extracts fields referenced by coerce transforms.
//
// Behavior:
//   - Aggregates all keys from options.types across all coerce transforms.
//   - Returns a sorted list with duplicates removed.
//
// Edge cases:
//   - Non-existent or non-map "types" returns an empty list (handled by Options).
func coerceFields(ts []config.Transform) []string {
	fields := make(map[string]struct{})
	for _, t := range ts {
		if t.Kind != "coerce" {
			continue
		}
		m := t.Options.StringMap("types")
		for k := range m {
			k = strings.TrimSpace(k)
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

// contractFields extracts field names from the first validate.contract block.
//
// Behavior:
//   - Locates the first transform with kind "validate".
//   - If it has a "contract" option, decodes it into schema.Contract.
//   - Returns the sorted list of contract field names.
//
// Errors:
//   - Returns any JSON marshal/unmarshal errors when contract has invalid shape.
func contractFields(ts []config.Transform) []string {
	c, err := contractFromTransforms(ts)
	if err != nil || c == nil {
		return nil
	}
	fields := make([]string, 0, len(c.Fields))
	for _, f := range c.Fields {
		name := strings.TrimSpace(f.Name)
		if name != "" {
			fields = append(fields, name)
		}
	}
	sort.Strings(fields)
	return fields
}

// transformGeneratedFields returns transform output fields that must be present
// in the streaming column set.
//
// Current builtins that generate new fields:
//   - hash: options.target_field (string)
//
// We also support a generic pattern for future transforms:
//   - options.target_field (string)
//   - options.target_fields ([]string or []any)
//
// This keeps the runner robust as you add more builtins.
func transformGeneratedFields(ts []config.Transform) []string {
	outSet := make(map[string]struct{})

	add := func(s string) {
		s = strings.TrimSpace(s)
		if s != "" {
			outSet[s] = struct{}{}
		}
	}

	for _, t := range ts {
		// Common convention: "target_field".
		add(t.Options.String("target_field", ""))

		// Optional convention: "target_fields": ["a","b"].
		raw := t.Options.Any("target_fields")
		if raw == nil {
			continue
		}

		switch v := raw.(type) {
		case []any:
			for _, it := range v {
				add(fmt.Sprint(it))
			}
		case []string:
			for _, s := range v {
				add(s)
			}
		default:
			// Ignore unknown types; config validation can enforce shape later.
		}
	}

	out := make([]string, 0, len(outSet))
	for k := range outSet {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// contractFromTransforms extracts the first validate.contract and decodes it.
//
// Behavior:
//   - Finds the first transform with kind "validate".
//   - If options.contract is missing, returns (nil, nil).
//   - Otherwise marshals the raw contract option back into JSON and unmarshals
//     into schema.Contract.
//
// Errors:
//   - Returns marshal/unmarshal errors if the contract option contains unsupported
//     types or has an incompatible structure.
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
