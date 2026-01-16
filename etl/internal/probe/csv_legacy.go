package probe

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"etl/internal/schema"
)

// readCSVSample parses CSV bytes into a header row and a slice of data rows.
//
// The implementation is intentionally best-effort and is designed for probing:
//   - records with the wrong field count are skipped
//   - the sample is expected to already be cut to a newline boundary
func readCSVSample(data []byte, delimiter rune) ([]string, [][]string, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, nil, nil
	}

	r := csv.NewReader(bytes.NewReader(data))
	r.Comma = delimiter
	r.FieldsPerRecord = -1 // we validate manually
	r.LazyQuotes = true

	headers, err := r.Read()
	if err != nil {
		return nil, nil, err
	}
	for i := range headers {
		headers[i] = strings.TrimSpace(headers[i])
	}

	rows := make([][]string, 0, 1024)
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return headers, rows, err
		}
		if len(rec) != len(headers) {
			continue
		}
		for i := range rec {
			rec[i] = strings.TrimSpace(rec[i])
		}
		rows = append(rows, rec)
	}

	return headers, rows, nil
}

// inferTypes infers a coarse type per column. Returned values are internal
// probe labels (not schema.Contract types). Expected set:
//   - "integer", "float", "boolean", "date", "timestamp", "text"
func inferTypes(headers []string, rows [][]string) []string {
	if len(headers) == 0 {
		return nil
	}

	out := make([]string, len(headers))
	for i := range out {
		out[i] = "text"
	}

	for col := range headers {
		var seen bool
		allInt := true
		allFloat := true
		allBool := true
		allDate := true
		allTS := true

		for _, r := range rows {
			if col >= len(r) {
				continue
			}
			v := strings.TrimSpace(r[col])
			if v == "" {
				continue
			}
			seen = true

			if allInt {
				if _, err := strconv.ParseInt(v, 10, 64); err != nil {
					allInt = false
				}
			}
			if allFloat {
				if _, err := strconv.ParseFloat(v, 64); err != nil {
					allFloat = false
				}
			}
			if allBool {
				if _, ok := parseBoolLoose(v); !ok {
					allBool = false
				}
			}
			if allDate {
				if _, _, ok := parseDateLoose(v); !ok {
					allDate = false
				}
			}
			if allTS {
				if _, _, ok := parseTimestampLoose(v); !ok {
					allTS = false
				}
			}
		}

		if !seen {
			out[col] = "text"
			continue
		}
		// Prefer more specific types.
		switch {
		case allInt:
			out[col] = "integer"
		case allBool:
			out[col] = "boolean"
		case allDate:
			out[col] = "date"
		case allTS:
			out[col] = "timestamp"
		case allFloat:
			out[col] = "float"
		default:
			out[col] = "text"
		}
	}

	return out
}

// detectColumnLayouts attempts to pick a layout string per column for columns
// inferred as date or timestamp. Non-date columns return "".
func detectColumnLayouts(rows [][]string, inferred []string) []string {
	out := make([]string, len(inferred))
	for i := range inferred {
		if inferred[i] != "date" && inferred[i] != "timestamp" {
			continue
		}
		counts := map[string]int{}
		for _, r := range rows {
			if i >= len(r) {
				continue
			}
			v := strings.TrimSpace(r[i])
			if v == "" {
				continue
			}
			var layout string
			var ok bool
			if inferred[i] == "date" {
				_, layout, ok = parseDateLoose(v)
			} else {
				_, layout, ok = parseTimestampLoose(v)
			}
			if ok && layout != "" {
				counts[layout]++
			}
		}
		best := ""
		bestN := 0
		for lay, n := range counts {
			if n > bestN {
				best = lay
				bestN = n
			}
		}
		out[i] = best
	}
	return out
}

// chooseMajorityLayout picks the most common date layout across date/timestamp
// columns. If none are present, returns "".
func chooseMajorityLayout(colLayouts []string, inferred []string) string {
	counts := map[string]int{}
	for i, lay := range colLayouts {
		if lay == "" {
			continue
		}
		if i < len(inferred) && (inferred[i] == "date" || inferred[i] == "timestamp") {
			counts[lay]++
		}
	}
	best := ""
	bestN := 0
	for lay, n := range counts {
		if n > bestN {
			best = lay
			bestN = n
		}
	}
	return best
}

// renderCSVSummary renders a small human-readable summary used by the legacy probe.
func renderCSVSummary(headers, normalized, inferred, colLayouts []string, coerceLayout string, rows [][]string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "sample_rows=%d\n", len(rows))
	fmt.Fprintf(&b, "coerce_layout=%s\n", coerceLayout)
	fmt.Fprintf(&b, "header,normalized,type,layout\n")
	for i := range headers {
		lay := ""
		if i < len(colLayouts) {
			lay = colLayouts[i]
		}
		fmt.Fprintf(&b, "%s,%s,%s,%s\n", headers[i], normalized[i], inferred[i], lay)
	}
	return []byte(b.String())
}

// jsonConfig is the legacy config shape produced by the historical probe tool.
// It is retained for backwards compatibility.
//
// New code should prefer config.Pipeline from ProbeURL / ProbeURLAny.
type jsonConfig struct {
	Table      string   `json:"table"`
	Columns    []string `json:"columns"`
	KeyColumns []string `json:"key_columns"`
	DateColumn string   `json:"date_column"`

	AutoCreateTable bool `json:"auto_create_table"`

	Coerce struct {
		Layout string            `json:"layout"`
		Types  map[string]string `json:"types"`
	} `json:"coerce"`

	Contract schema.Contract `json:"contract"`
}

func newJSONConfig(name string) jsonConfig {
	var cfg jsonConfig
	cfg.Table = normalizeFieldName(name)
	cfg.AutoCreateTable = true
	cfg.Coerce.Types = map[string]string{}
	return cfg
}

func printJSONConfig(cfg jsonConfig) ([]byte, error) {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, err
	}
	b = append(b, '\n')
	return b, nil
}

// inferKeyColumns tries to pick a stable key column set.
// For probing, we pick the first non-empty column with high distinctness.
func inferKeyColumns(headers, normalized, inferred []string, rows [][]string) []string {
	if len(headers) == 0 {
		return nil
	}
	const capDistinct = 10000
	bestIdx := -1
	bestDistinct := 0
	for i := range headers {
		set := make(map[string]struct{})
		for _, r := range rows {
			if i >= len(r) {
				continue
			}
			v := strings.TrimSpace(r[i])
			if v == "" {
				continue
			}
			set[v] = struct{}{}
			if len(set) >= capDistinct {
				break
			}
		}
		if len(set) > bestDistinct {
			bestDistinct = len(set)
			bestIdx = i
		}
	}
	if bestIdx >= 0 {
		return []string{normalized[bestIdx]}
	}
	return nil
}

// contractTypeFromInference maps probe inference labels to contract/coerce types.
func contractTypeFromInference(inferred string) string {
	switch inferred {
	case "integer":
		return "bigint"
	case "float":
		return "float"
	case "boolean":
		return "bool"
	case "date":
		return "date"
	case "timestamp":
		return "timestamp"
	default:
		return "text"
	}
}

func parseBoolLoose(s string) (bool, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "1", "t", "true", "yes", "y":
		return true, true
	case "0", "f", "false", "no", "n":
		return false, true
	default:
		return false, false
	}
}

var dateLayouts = []string{
	"2006-01-02",
	"02.01.2006",
	"02/01/2006",
	"01/02/2006",
}

var tsLayouts = []string{
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05.000Z07:00",
	"02.01.2006 15:04:05",
}

func parseDateLoose(s string) (time.Time, string, bool) {
	s = strings.TrimSpace(s)
	for _, lay := range dateLayouts {
		if t, err := time.Parse(lay, s); err == nil {
			return t, lay, true
		}
	}
	return time.Time{}, "", false
}

func parseTimestampLoose(s string) (time.Time, string, bool) {
	s = strings.TrimSpace(s)
	for _, lay := range tsLayouts {
		if t, err := time.Parse(lay, s); err == nil {
			return t, lay, true
		}
	}
	return time.Time{}, "", false
}

func allNonEmptySample(rows [][]string, col int) bool {
	for _, r := range rows {
		if col >= len(r) {
			continue
		}
		if strings.TrimSpace(r[col]) == "" {
			return false
		}
	}
	return len(rows) > 0
}
