package multitable

import (
	"context"
	"errors"
	"testing"
	"time"

	"etl/internal/storage"
	"etl/internal/transformer"
)

// drainRepo is a minimal MultiRepository fake for testing loadFactsStreaming drain behavior.
//
// When to use:
//   - Tests that need InsertFactRows to fail deterministically (to trigger cancellation).
//
// Edge cases:
//   - Only InsertFactRows returns an injected error; other methods are no-ops.
//   - Close is a no-op.
type drainRepo struct {
	insertErr error
}

func (d *drainRepo) Close() {}

func (d *drainRepo) EnsureTables(context.Context, []storage.TableSpec) error { return nil }

func (d *drainRepo) EnsureDimensionKeys(context.Context, string, string, []any, []string) error {
	return nil
}

func (d *drainRepo) SelectKeyValueByKeys(context.Context, string, string, string, []any) (map[string]int64, error) {
	return map[string]int64{}, nil
}

func (d *drainRepo) SelectAllKeyValue(context.Context, string, string, string) (map[string]int64, error) {
	return map[string]int64{}, nil
}

func (d *drainRepo) InsertFactRows(context.Context, storage.TableSpec, string, []string, [][]any, []string) (int64, error) {
	if d.insertErr != nil {
		return 0, d.insertErr
	}
	return 0, nil
}

func makeTestRow(v ...any) *transformer.Row {
	r := &transformer.Row{V: make([]any, len(v))}
	copy(r.V, v)
	return r
}

// TestLoadFactsStreaming_WorkerErrorStillDrainsStream verifies the critical drain-safety contract.
//
// Scenario:
//   - The loader hits a hard error (InsertFactRows fails).
//   - The engine cancels its internal context.
//   - Even after cancellation, it must keep draining stream.Rows until closed,
//     otherwise stream.Wait can deadlock and the whole run "hangs".
//
// This test creates an unbuffered stream where the producer goroutine cannot finish
// unless the engine continues reading from stream.Rows after cancellation.
func TestLoadFactsStreaming_WorkerErrorStillDrainsStream(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")

	// Build a tiny plan: a single fact table, no lookups.
	factSpec := storage.TableSpec{
		Name: "facts",
		Load: storage.LoadSpec{
			Kind: "fact",
			FromRows: []storage.FromRowSpec{
				{TargetColumn: "a", SourceField: "a"},
			},
		},
	}
	cfg := Pipeline{
		Job: "job",
		Storage: Storage{
			Kind: "postgres",
			DB:   MultiDB{Mode: "multi_table", Tables: []storage.TableSpec{factSpec}},
		},
		Runtime: RuntimeConfig{
			BatchSize:     2,
			LoaderWorkers: 1,
			ChannelBuffer: 0,
		},
	}
	plan, err := buildIndexedPlan(cfg, []string{"a"})
	if err != nil {
		t.Fatalf("buildIndexedPlan() err=%v, want nil", err)
	}

	// Unbuffered stream: if the engine stops reading early, the sender goroutine
	// blocks and Wait() never completes.
	rowsCh := make(chan *transformer.Row)
	sentAll := make(chan struct{})

	streamFn := func(ctx context.Context, _ Pipeline, _ []string) (*ValidatedStream, error) {
		go func() {
			defer close(sentAll)
			defer close(rowsCh)

			// Enough rows to require multiple sends after cancellation.
			for i := 0; i < 10; i++ {
				rowsCh <- makeTestRow(i)
			}
		}()

		return &ValidatedStream{
			Rows: rowsCh,
			Wait: func() error {
				<-sentAll
				return nil
			},
			ParseErrorCount: func() uint64 { return 0 },
			ProcessedCount:  func() uint64 { return 10 },
			TransformRejectedCount: func() uint64 {
				return 0
			},
			ValidateDroppedCount: func() uint64 {
				return 0
			},
		}, nil
	}

	e := &Engine2Pass{
		Repo:   &drainRepo{insertErr: wantErr},
		Stream: streamFn,
	}

	// Guard against regressions: if the engine deadlocks, the test would hang.
	// A timeout here is acceptable because it's not "timing based"; it's a liveness guard.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, gotErr := e.loadFactsStreaming(ctx, cfg, []string{"a"}, plan, false)
	if gotErr == nil {
		t.Fatalf("loadFactsStreaming() err=nil, want %v", wantErr)
	}
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("loadFactsStreaming() err=%v, want errors.Is(err, %v)=true", gotErr, wantErr)
	}

	// If draining failed, stream.Wait would not complete and the call above would
	// not return before the context timeout.
}
