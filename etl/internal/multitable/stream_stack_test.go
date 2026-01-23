package multitable

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"etl/internal/config"
	"etl/internal/transformer"
)

// TestStreamValidatedRows_ParseErrorCount verifies non-fatal parse errors are counted
// and do not force Wait() to fail.
//
// This test patches dependencies to avoid real I/O and parsing.
func TestStreamValidatedRows_ConfigValidation(t *testing.T) {

	restore := patchStreamDeps(t, streamDeps{
		open: func(path string) (io.ReadCloser, error) {
			return nopReadCloser{Reader: strings.NewReader("")}, nil
		},
		transform: passthroughTransform,
		hash:      passthroughHash,
		validate:  passthroughValidate,
		csv: func(ctx context.Context, src io.ReadCloser, columns []string, opt config.Options, out chan<- *transformer.Row, onErr func(int, error)) error {
			return nil
		},
		json: func(ctx context.Context, r io.Reader, columns []string, opt config.Options, out chan<- *transformer.Row, onErr func(int, error)) error {
			return nil
		},
	})
	t.Cleanup(restore)

	base := Pipeline{
		Source: Source{Kind: "file", File: &FileSource{Path: "ignored"}},
		Parser: Parser{Kind: "csv"},
		Runtime: RuntimeConfig{
			TransformWorkers: 1,
			ChannelBuffer:    8,
		},
	}

	tests := []struct {
		name    string
		mutate  func(Pipeline) Pipeline
		wantErr string
	}{
		{
			name: "bad parser kind",
			mutate: func(p Pipeline) Pipeline {
				p.Parser.Kind = "xml"
				return p
			},
			wantErr: "parser.kind must be csv or json",
		},
		{
			name: "hash transform missing fields",
			mutate: func(p Pipeline) Pipeline {
				p.Transform = []config.Transform{
					{Kind: "hash", Options: mustOptions(map[string]any{"target_field": "h"})},
				}
				return p
			},
			wantErr: "hash transform requires options.target_field",
		},
		{
			name: "hash transform unsupported algorithm",
			mutate: func(p Pipeline) Pipeline {
				p.Transform = []config.Transform{
					{Kind: "hash", Options: mustOptions(map[string]any{
						"target_field": "h",
						"fields":       []any{"a"},
						"algorithm":    "md5",
					})},
				}
				return p
			},
			wantErr: "hash algorithm",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			//t.Parallel()

			_, err := StreamValidatedRows(context.Background(), tt.mutate(base), []string{"a"})
			if err == nil {
				t.Fatalf("StreamValidatedRows() err=nil, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("StreamValidatedRows() err=%q, want contains %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// ---- dependency patching ----

type streamDeps struct {
	open func(path string) (io.ReadCloser, error)

	csv  func(ctx context.Context, src io.ReadCloser, columns []string, opt config.Options, out chan<- *transformer.Row, onErr func(line int, err error)) error
	json func(ctx context.Context, r io.Reader, columns []string, opt config.Options, out chan<- *transformer.Row, onErr func(line int, err error)) error

	transform func(ctx context.Context, columns []string, in <-chan *transformer.Row, out chan<- *transformer.Row, spec transformer.CoerceSpec, onReject func(int, string))
	hash      func(ctx context.Context, columns []string, in <-chan *transformer.Row, out chan<- *transformer.Row, spec transformer.HashSpec, onReject func(int, string))
	validate  func(ctx context.Context, columns []string, required []string, types map[string]string, in <-chan *transformer.Row, out chan<- *transformer.Row, onReject func(int, string))
}

func patchStreamDeps(t *testing.T, d streamDeps) func() {
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

func passthroughTransform(ctx context.Context, columns []string, in <-chan *transformer.Row, out chan<- *transformer.Row, spec transformer.CoerceSpec, onReject func(int, string)) {
	for r := range in {
		out <- r
	}
}

func passthroughHash(ctx context.Context, columns []string, in <-chan *transformer.Row, out chan<- *transformer.Row, spec transformer.HashSpec, onReject func(int, string)) {
	for r := range in {
		out <- r
	}
}

func passthroughValidate(ctx context.Context, columns []string, required []string, types map[string]string, in <-chan *transformer.Row, out chan<- *transformer.Row, onReject func(int, string)) {
	for r := range in {
		out <- r
	}
}

// Keep the compiler honest about imports in case we tweak tests later.
var _ = os.DevNull
