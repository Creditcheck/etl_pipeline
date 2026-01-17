package multitable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"etl/internal/config"
	"etl/internal/storage"
)

type fakeLogger struct {
	msgs []string
}

func (l *fakeLogger) Printf(format string, v ...any) {
	l.msgs = append(l.msgs, fmt.Sprintf(format, v...))
}

type fakeMultiRepo struct {
	closed atomic.Int64
}

func (r *fakeMultiRepo) Close() { r.closed.Add(1) }

func (r *fakeMultiRepo) EnsureTables(ctx context.Context, tables []storage.TableSpec) error {
	return nil
}
func (r *fakeMultiRepo) EnsureDimensionKeys(ctx context.Context, table string, keyColumn string, keys []any, conflictColumns []string) error {
	return nil
}
func (r *fakeMultiRepo) SelectKeyValueByKeys(ctx context.Context, table string, keyColumn string, valueColumn string, keys []any) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (r *fakeMultiRepo) SelectAllKeyValue(ctx context.Context, table string, keyColumn string, valueColumn string) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (r *fakeMultiRepo) InsertFactRows(ctx context.Context, table string, columns []string, rows [][]any, dedupeColumns []string) (int64, error) {
	return int64(len(rows)), nil
}

type fakeEngine struct {
	gotColumns []string
	gotCfg     Pipeline
	runErr     error
	calls      atomic.Int64
}

func (e *fakeEngine) Run(ctx context.Context, cfg Pipeline, columns []string) error {
	e.calls.Add(1)
	e.gotCfg = cfg
	e.gotColumns = append([]string(nil), columns...)
	return e.runErr
}

// TestRunner_Run_ValidationFailure verifies Runner stops early on invalid config
// and does not attempt to construct a repository or engine.
func TestRunner_Run_ValidationFailure(t *testing.T) {
	t.Parallel()

	var newRepoCalls atomic.Int64
	r := &Runner{
		NewMultiRepo: func(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
			newRepoCalls.Add(1)
			return &fakeMultiRepo{}, nil
		},
		NewEngine: func(repo storage.MultiRepository, logger Logger) Engine {
			return &fakeEngine{}
		},
		NewLogger: func(w io.Writer) Logger { return &fakeLogger{} },
	}

	cfg := Pipeline{} // invalid
	err := r.Run(context.Background(), cfg)
	if err == nil {
		t.Fatalf("Run() err=nil, want error")
	}
	if newRepoCalls.Load() != 0 {
		t.Fatalf("NewMultiRepo calls=%d, want 0", newRepoCalls.Load())
	}
}

// TestRunner_Run_ConstructsRepoAndEngine verifies that Run expands DSN,
// computes required columns, logs "added" fields, and calls engine.Run.
func TestRunner_Run_ConstructsRepoAndEngine(t *testing.T) {
	t.Parallel()

	repo := &fakeMultiRepo{}
	var gotMultiCfg storage.MultiConfig

	log := &fakeLogger{}
	engine := &fakeEngine{}

	r := &Runner{
		NewMultiRepo: func(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
			gotMultiCfg = cfg
			return repo, nil
		},
		NewEngine: func(repo storage.MultiRepository, logger Logger) Engine {
			if logger != log {
				t.Fatalf("NewEngine() logger mismatch")
			}
			return engine
		},
		NewLogger: func(w io.Writer) Logger {
			return log
		},
		ExpandEnv: func(s string) string {
			return strings.ReplaceAll(s, "${DSN}", "expanded")
		},
	}

	cfg := Pipeline{
		Source: Source{Kind: "file", File: &FileSource{Path: "in.csv"}},
		Parser: Parser{Kind: "csv"},
		Transform: []config.Transform{
			{
				Kind: "coerce",
				Options: mustOptions(map[string]any{
					"types": map[string]any{
						"coerce_in": "text",
					},
				}),
			},
			{
				Kind: "hash",
				Options: mustOptions(map[string]any{
					"target_field": "row_hash",
					"fields":       []any{"a"},
				}),
			},
			{
				Kind: "validate",
				Options: mustOptions(map[string]any{
					"contract": map[string]any{
						"fields": []any{
							map[string]any{"name": "contract_in", "type": "text", "required": true},
						},
					},
				}),
			},
		},
		Storage: Storage{
			Kind: "sqlite",
			DB: MultiDB{
				DSN:  "${DSN}",
				Mode: "multi_table",
				Tables: []storage.TableSpec{
					{
						Name: "facts",
						Load: storage.LoadSpec{
							Kind: "fact",
							FromRows: []storage.FromRowSpec{
								{TargetColumn: "c1", SourceField: "src1"},
								{
									TargetColumn: "c2",
									Lookup: &storage.LookupSpec{
										Table: "dim",
										Match: map[string]string{
											"id": "src_lookup",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := r.Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run() err=%v, want nil", err)
	}

	if gotMultiCfg.Kind != "sqlite" || gotMultiCfg.DSN != "expanded" {
		t.Fatalf("NewMultiRepo cfg=%+v, want Kind=sqlite DSN=expanded", gotMultiCfg)
	}

	if engine.calls.Load() != 1 {
		t.Fatalf("engine.Run calls=%d, want 1", engine.calls.Load())
	}

	wantCols := []string{"coerce_in", "contract_in", "row_hash", "src1", "src_lookup"}
	if !reflect.DeepEqual(engine.gotColumns, wantCols) {
		t.Fatalf("engine columns=%v, want %v", engine.gotColumns, wantCols)
	}

	found := false
	for _, m := range log.msgs {
		if strings.Contains(m, "stage=columns") && strings.Contains(m, "row_hash") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("logger msgs=%v, want stage=columns log containing row_hash", log.msgs)
	}

	if repo.closed.Load() != 1 {
		t.Fatalf("repo closed=%d, want 1", repo.closed.Load())
	}
}

// TestRunner_Run_RepoFactoryError verifies that NewMultiRepo errors are wrapped
// and that engine is not constructed.
func TestRunner_Run_RepoFactoryError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("nope")
	var engineCalls atomic.Int64

	r := &Runner{
		NewMultiRepo: func(ctx context.Context, cfg storage.MultiConfig) (storage.MultiRepository, error) {
			return nil, wantErr
		},
		NewEngine: func(repo storage.MultiRepository, logger Logger) Engine {
			engineCalls.Add(1)
			return &fakeEngine{}
		},
		NewLogger: func(w io.Writer) Logger { return &fakeLogger{} },
		ExpandEnv: func(s string) string { return s },
	}

	cfg := Pipeline{
		Source:  Source{Kind: "file", File: &FileSource{Path: "in.csv"}},
		Parser:  Parser{Kind: "csv"},
		Storage: Storage{Kind: "sqlite", DB: MultiDB{DSN: "x", Mode: "multi_table", Tables: []storage.TableSpec{{Name: "t"}}}},
	}

	err := r.Run(context.Background(), cfg)
	if err == nil {
		t.Fatalf("Run() err=nil, want error")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run() err=%v, want wrapped %v", err, wantErr)
	}
	if !strings.Contains(err.Error(), "multi repo") {
		t.Fatalf("Run() err=%q, want contains %q", err.Error(), "multi repo")
	}
	if engineCalls.Load() != 0 {
		t.Fatalf("NewEngine calls=%d, want 0", engineCalls.Load())
	}
}

// TestRequiredInputColumns verifies the union logic and deterministic sorting.
//
// When to use:
//   - Keep this test whenever modifying requiredInputColumns or related helpers.
func TestRequiredInputColumns(t *testing.T) {
	t.Parallel()

	cfg := Pipeline{
		Transform: []config.Transform{
			{
				Kind: "coerce",
				Options: mustOptions(map[string]any{
					"types": map[string]any{
						"b": "text",
						"a": "text",
					},
				}),
			},
			{
				Kind: "hash",
				Options: mustOptions(map[string]any{
					"target_field":  "gen",
					"target_fields": []any{"gen2", "gen"},
				}),
			},
			{
				Kind: "validate",
				Options: mustOptions(map[string]any{
					"contract": map[string]any{
						"fields": []any{
							map[string]any{"name": "c"},
							map[string]any{"name": "  d  "},
						},
					},
				}),
			},
		},
		Storage: Storage{
			DB: MultiDB{
				Tables: []storage.TableSpec{
					{
						Name: "t",
						Load: storage.LoadSpec{
							Kind: "fact",
							FromRows: []storage.FromRowSpec{
								{TargetColumn: "x", SourceField: "src"},
								{
									TargetColumn: "y",
									Lookup: &storage.LookupSpec{
										Table: "dim",
										Match: map[string]string{
											"k": "src_lookup",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cols, added := requiredInputColumns(cfg)
	if wantAdded := []string{"gen", "gen2"}; !reflect.DeepEqual(added, wantAdded) {
		t.Fatalf("added=%v, want %v", added, wantAdded)
	}
	wantCols := []string{"a", "b", "c", "d", "gen", "gen2", "src", "src_lookup"}
	if !reflect.DeepEqual(cols, wantCols) {
		t.Fatalf("cols=%v, want %v", cols, wantCols)
	}
}
