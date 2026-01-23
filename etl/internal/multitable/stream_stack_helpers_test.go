package multitable

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"etl/internal/config"
	"etl/internal/transformer"
)

// TestValidateStreamConfig verifies streaming config validation is deterministic
// and does not start goroutines or touch I/O.
//
// Concurrency:
//   - Subtests run in parallel. Pipeline contains pointer fields (Source.File),
//     so each subtest must start from a fresh baseline config to avoid sharing
//     mutable pointers.
//
// Errors:
//   - Validates required source.kind/file.path.
//   - Validates parser.kind is "csv" or "json".
func TestValidateStreamConfig(t *testing.T) {
	//t.Parallel()

	newBase := func() Pipeline {
		return Pipeline{
			Source: Source{Kind: "file", File: &FileSource{Path: "x"}},
			Parser: Parser{Kind: "csv"},
		}
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
			name: "missing file path",
			mutate: func(p Pipeline) Pipeline {
				p.Source.File.Path = ""
				return p
			},
			wantErr: "source.kind=file",
		},
		{
			name: "missing file struct",
			mutate: func(p Pipeline) Pipeline {
				p.Source.File = nil
				return p
			},
			wantErr: "source.kind=file",
		},
		{
			name: "bad parser kind",
			mutate: func(p Pipeline) Pipeline {
				p.Parser.Kind = "xml"
				return p
			},
			wantErr: "parser.kind must be csv or json",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			//t.Parallel()

			err := validateStreamConfig(tt.mutate(newBase()))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateStreamConfig() err=%v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateStreamConfig() err=nil, want contains %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateStreamConfig() err=%q, want contains %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestNormalizeRuntime verifies runtime defaulting is applied consistently.
//
// Edge cases:
//   - Non-positive values are replaced by defaults.
func TestNormalizeRuntime(t *testing.T) {
	//t.Parallel()

	tests := []struct {
		name string
		in   RuntimeConfig
		want RuntimeConfig
	}{
		{
			name: "defaults applied",
			in:   RuntimeConfig{},
			want: RuntimeConfig{
				ChannelBuffer:    256,
				ReaderWorkers:    1,
				TransformWorkers: 1,
			},
		},
		{
			name: "preserves positive values",
			in: RuntimeConfig{
				ChannelBuffer:    10,
				ReaderWorkers:    2,
				TransformWorkers: 3,
			},
			want: RuntimeConfig{
				ChannelBuffer:    10,
				ReaderWorkers:    2,
				TransformWorkers: 3,
			},
		},
		{
			name: "negative values default",
			in: RuntimeConfig{
				ChannelBuffer:    -1,
				ReaderWorkers:    -2,
				TransformWorkers: -3,
			},
			want: RuntimeConfig{
				ChannelBuffer:    256,
				ReaderWorkers:    1,
				TransformWorkers: 1,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			//t.Parallel()

			got := normalizeRuntime(tt.in)
			if got.ChannelBuffer != tt.want.ChannelBuffer ||
				got.ReaderWorkers != tt.want.ReaderWorkers ||
				got.TransformWorkers != tt.want.TransformWorkers {
				t.Fatalf("normalizeRuntime()=%+v, want %+v", got, tt.want)
			}
		})
	}
}

type nopReadCloser struct{ io.Reader }

func (nopReadCloser) Close() error { return nil }

// TestStreamValidatedRows_TerminalParserError verifies a fatal parser error is
// surfaced by Wait() after Rows closes.
//
// This test patches StreamValidatedRows' package-level dependency seams directly
// (openSource / streamCSVRows / downstream loops) to avoid coupling to helper
// patch functions and to ensure the parser error returned by the parser adapter
// becomes the stream's terminal error.
//
// Concurrency note:
//   - This test intentionally does NOT use t.Parallel() because it mutates
//     package-level variables that are shared across tests.
func TestStreamValidatedRows_TerminalParserError(t *testing.T) {
	wantErr := errors.New("fatal parse")

	// Save originals.
	oldOpen := openSource
	oldCSV := streamCSVRows
	oldTransform := transformLoop
	oldHash := hashLoop
	oldValidate := validateLoop

	// Restore on exit.
	t.Cleanup(func() {
		openSource = oldOpen
		streamCSVRows = oldCSV
		transformLoop = oldTransform
		hashLoop = oldHash
		validateLoop = oldValidate
	})

	// Patch open to provide an empty source.
	openSource = func(path string) (io.ReadCloser, error) {
		return nopReadCloser{Reader: strings.NewReader("")}, nil
	}

	// Patch CSV parser to return the fatal error immediately.
	streamCSVRows = func(
		ctx context.Context,
		src io.ReadCloser,
		columns []string,
		opt config.Options,
		out chan<- *transformer.Row,
		onErr func(int, error),
	) error {
		return wantErr
	}

	// Downstream stages must not block / must be deterministic.
	transformLoop = passthroughTransformLoop
	hashLoop = passthroughHashLoop
	validateLoop = passthroughValidateLoop

	cfg := Pipeline{
		Job:    "test",
		Source: Source{Kind: "file", File: &FileSource{Path: "ignored"}},
		Parser: Parser{Kind: "csv"},
		Runtime: RuntimeConfig{
			TransformWorkers: 1,
			ChannelBuffer:    8,
		},
	}

	stream, err := StreamValidatedRows(context.Background(), cfg, []string{"a"})
	if err != nil {
		t.Fatalf("StreamValidatedRows() err=%v, want nil", err)
	}

	// Drain rows; should be none, but drain defensively.
	for r := range stream.Rows {
		r.Free()
	}

	err = stream.Wait()
	if !errors.Is(err, wantErr) {
		t.Fatalf("Wait() err=%v, want %v", err, wantErr)
	}
}

// ---- local dep patching (kept minimal to avoid duplicating runner_test fakes) ----

type streamDepsForHelpers struct {
	open func(path string) (io.ReadCloser, error)

	csv  func(ctx context.Context, src io.ReadCloser, columns []string, opt config.Options, out chan<- *transformer.Row, onErr func(line int, err error)) error
	json func(ctx context.Context, r io.Reader, columns []string, opt config.Options, out chan<- *transformer.Row, onErr func(line int, err error)) error

	transform func(ctx context.Context, columns []string, in <-chan *transformer.Row, out chan<- *transformer.Row, spec transformer.CoerceSpec, onReject func(int, string))
	hash      func(ctx context.Context, columns []string, in <-chan *transformer.Row, out chan<- *transformer.Row, spec transformer.HashSpec, onReject func(int, string))
	validate  func(ctx context.Context, columns []string, required []string, types map[string]string, in <-chan *transformer.Row, out chan<- *transformer.Row, onReject func(int, string))
}

func patchStreamDepsForHelpers(t *testing.T, d streamDepsForHelpers) func() {
	t.Helper()

	origOpen := openSource
	origCSV := streamCSVRows
	origJSON := streamJSONRows
	origTransform := transformLoop
	origHash := hashLoop
	origValidate := validateLoop

	if d.open != nil {
		openSource = d.open
	}
	if d.csv != nil {
		streamCSVRows = d.csv
	}
	if d.json != nil {
		streamJSONRows = d.json
	}
	if d.transform != nil {
		transformLoop = d.transform
	}
	if d.hash != nil {
		hashLoop = d.hash
	}
	if d.validate != nil {
		validateLoop = d.validate
	}

	return func() {
		openSource = origOpen
		streamCSVRows = origCSV
		streamJSONRows = origJSON
		transformLoop = origTransform
		hashLoop = origHash
		validateLoop = origValidate
	}
}

func passthroughTransformLoop(ctx context.Context, columns []string, in <-chan *transformer.Row, out chan<- *transformer.Row, spec transformer.CoerceSpec, onReject func(int, string)) {
	for r := range in {
		out <- r
	}
}

func passthroughHashLoop(ctx context.Context, columns []string, in <-chan *transformer.Row, out chan<- *transformer.Row, spec transformer.HashSpec, onReject func(int, string)) {
	for r := range in {
		out <- r
	}
}

func passthroughValidateLoop(ctx context.Context, columns []string, required []string, types map[string]string, in <-chan *transformer.Row, out chan<- *transformer.Row, onReject func(int, string)) {
	for r := range in {
		out <- r
	}
}
