package multitable

// This file defines just enough config structure to parse the user-provided
// multi-table JSON config. It intentionally mirrors the config shown in chat.
// It does not attempt to replace internal/config/config.go (single-table).

import (
	"etl/internal/config"
	"etl/internal/storage"
)

type Pipeline struct {
	Job       string             `json:"job"`
	Source    Source             `json:"source"`
	Parser    Parser             `json:"parser"`
	Transform []config.Transform `json:"transform"`
	Storage   Storage            `json:"storage"`
	Runtime   RuntimeConfig      `json:"runtime"`
}

type Source struct {
	Kind string      `json:"kind"`
	File *FileSource `json:"file,omitempty"`
}

type FileSource struct {
	Path string `json:"path"`
}

type Parser struct {
	Kind    string         `json:"kind"`
	Options config.Options `json:"options"`
}

type Storage struct {
	// Backend kind: "postgres" | "mssql" | "sqlite"
	Kind string  `json:"kind"`
	DB   MultiDB `json:"db"`
}

type MultiDB struct {
	DSN  string `json:"dsn"`
	Mode string `json:"mode"` // must be "multi_table"

	// Tables + load rules used by Engine and by storage backends.
	// These types live in internal/storage so backends can consume them.
	Tables []storage.TableSpec `json:"tables"`
}

// RuntimeConfig controls pipeline execution behavior.
type RuntimeConfig struct {
	ReaderWorkers    int `json:"reader_workers"`
	TransformWorkers int `json:"transform_workers"`
	LoaderWorkers    int `json:"loader_workers"`
	BatchSize        int `json:"batch_size"`
	ChannelBuffer    int `json:"channel_buffer"`

	// DebugTimings enables timing logs for expensive operations (notably pass2 inserts).
	// When enabled, the engine logs duration per InsertFactRows call and batch stats.
	DebugTimings bool `json:"debug_timings"`

	// DedupeDimensionKeys controls whether the engine deduplicates dimension keys.
	// When false (recommended), the engine streams keys to the DB and relies on the
	// backend's idempotent semantics (ON CONFLICT DO NOTHING / OR IGNORE / etc).
	DedupeDimensionKeys bool `json:"dedupe_dimension_keys"`

	// DedupeDimensionKeysWithinBatch controls whether the engine performs a small
	// in-memory dedupe inside each batch before calling EnsureDimensionKeys.
	DedupeDimensionKeysWithinBatch bool `json:"dedupe_dimension_keys_within_batch"`
}
