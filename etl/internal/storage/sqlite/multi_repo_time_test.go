package sqlite

import (
	"testing"
	"time"
)

func TestParseSQLiteTime_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantUTC string
		wantErr bool
	}{
		{
			name:    "rfc3339nano",
			in:      "2026-01-27T12:17:08.123456789Z",
			wantUTC: "2026-01-27T12:17:08.123456789Z",
		},
		{
			name:    "rfc3339",
			in:      "2026-01-27T12:17:08Z",
			wantUTC: "2026-01-27T12:17:08Z",
		},
		{
			name:    "sqlite_space_tz",
			in:      "2026-01-27 12:17:08+00:00",
			wantUTC: "2026-01-27T12:17:08Z",
		},
		{
			name:    "sqlite_space_tz_nanos",
			in:      "2026-01-27 12:17:08.000000000+00:00",
			wantUTC: "2026-01-27T12:17:08Z",
		},
		{
			name:    "sqlite_no_tz_assume_utc",
			in:      "2026-01-27 12:17:08",
			wantUTC: "2026-01-27T12:17:08Z",
		},
		{
			name:    "invalid",
			in:      "not-a-time",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSQLiteTime(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseSQLiteTime(%q) err=%v wantErr=%v", tt.in, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			// Normalize to RFC3339Nano for comparison stability.
			gotStr := got.UTC().Format(time.RFC3339Nano)
			// wantUTC values above are RFC3339; accept either exact or RFC3339Nano equivalent.
			want, _ := time.Parse(time.RFC3339Nano, tt.wantUTC)
			if want.IsZero() {
				want, _ = time.Parse(time.RFC3339, tt.wantUTC)
			}
			if got.UTC() != want.UTC() {
				t.Fatalf("got=%s want=%s", gotStr, want.UTC().Format(time.RFC3339Nano))
			}
		})
	}
}

func TestFormatSQLiteTime_RoundTrip(t *testing.T) {
	t.Parallel()
	in := time.Date(2026, 1, 27, 12, 17, 8, 123, time.FixedZone("X", 3600))
	s := formatSQLiteTime(in)
	got, err := parseSQLiteTime(s)
	if err != nil {
		t.Fatalf("parseSQLiteTime(formatSQLiteTime()) err=%v", err)
	}
	if got.UTC() != in.UTC() {
		t.Fatalf("round trip mismatch: got=%s want=%s", got.UTC(), in.UTC())
	}
}
