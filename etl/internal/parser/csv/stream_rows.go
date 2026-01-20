package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"etl/internal/config"
	"etl/internal/transformer"
	"etl/internal/transformer/builtin"
)

var (
	scrubFromLikvidaci = []byte(` "v likvidaci""`)
	scrubToLikvidaci   = []byte(` (v likvidaci)"`)
)

func wrapWithLikvidaciScrub(r io.ReadCloser, enable bool) io.ReadCloser {
	if !enable {
		return r
	}

	type rc struct {
		io.Reader
		io.Closer
	}

	return &rc{
		Reader: newStreamingRewriter(r, scrubFromLikvidaci, scrubToLikvidaci),
		Closer: r,
	}
}

// StreamCSVRows streams CSV into pooled *transformer.Row objects aligned to the
// target 'columns' order.
//
// NOTE on cancellation:
// On ctx cancellation we must NOT return in-flight rows to the pool (Drop instead),
// otherwise the parser can reuse them immediately while downstream drain-safe
// stages still read them.
func StreamCSVRows(
	ctx context.Context,
	src io.ReadCloser,
	columns []string,
	opt config.Options,
	out chan<- *transformer.Row,
	onErr func(line int, err error),
) error {
	defer src.Close()

	var line int

	hasHeader := opt.Bool("has_header", true)
	comma := opt.Rune("comma", ',')
	trim := opt.Bool("trim_space", true)
	hm := opt.StringMap("header_map")
	lazy := opt.Bool("lazy_quotes", false)
	fieldsPer := opt.Int("fields_per_record", 0)

	r := wrapWithLikvidaciScrub(src, opt.Bool("stream_scrub_likvidaci", false))

	cr := csv.NewReader(r)
	cr.Comma = comma
	cr.ReuseRecord = true
	cr.LazyQuotes = lazy
	if fieldsPer != 0 {
		cr.FieldsPerRecord = fieldsPer
	} else {
		cr.FieldsPerRecord = -1
	}

	colIx := make([]int, len(columns))
	for i := range colIx {
		colIx[i] = -1
	}

	readRec := func() ([]string, error) {
		line++
		return cr.Read()
	}

	if hasHeader {
		hdr, err := readRec()
		if err != nil {
			if onErr != nil {
				onErr(line, fmt.Errorf("read header: %w", err))
			}
			return err
		}
		srcToIdx := make(map[string]int, len(hdr))
		for i, h := range hdr {
			if builtin.HasEdgeSpace(h) {
				h = strings.TrimSpace(h)
			}
			if i == 0 {
				h = strings.TrimPrefix(h, "\uFEFF")
			}
			if mapped, ok := hm[h]; ok {
				h = mapped
			} else {
				h = strings.ReplaceAll(strings.ToLower(h), " ", "_")
			}
			srcToIdx[h] = i
		}
		for t, target := range columns {
			if si, ok := srcToIdx[target]; ok {
				colIx[t] = si
			}
		}
	} else {
		for i := range columns {
			colIx[i] = i
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rec, err := readRec()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if onErr != nil {
				onErr(line, fmt.Errorf("csv read: %w", err))
			}
			continue
		}

		row := transformer.GetRow(len(columns))
		row.Line = line

		for t := range columns {
			si := colIx[t]
			if si < 0 || si >= len(rec) {
				row.V[t] = nil
				continue
			}
			v := rec[si]
			if trim && builtin.HasEdgeSpace(v) {
				v = strings.TrimSpace(v)
			}
			if v == "" {
				row.V[t] = nil
			} else {
				row.V[t] = v
			}
		}

		select {
		case out <- row:
		case <-ctx.Done():
			// IMPORTANT: do not re-pool on cancellation
			row.Drop()
			return ctx.Err()
		}
	}
}
