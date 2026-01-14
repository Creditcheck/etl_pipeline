package multitable

// This file defines just enough config structure to parse the user-provided
// multi-table JSON config. It intentionally mirrors the config shown in chat.
// It does not attempt to replace internal/config/config.go (single-table).

import "etl/internal/config"

type Pipeline struct {
	Job       string             `json:"job"`
	Source    Source             `json:"source"`
	Parser    Parser             `json:"parser"`
	Transform []config.Transform `json:"transform"` // <-- ADD THIS
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
	Options config.Options `json:"options"` // <-- CHANGE THIS (was CSVOptions)
}

type Storage struct {
	Kind string          `json:"kind"` // "postgres"
	DB   PostgresMultiDB `json:"db"`
}

type PostgresMultiDB struct {
	DSN    string      `json:"dsn"`
	Mode   string      `json:"mode"` // "multi_table"
	Tables []TableSpec `json:"tables"`
}

type TableSpec struct {
	Name            string           `json:"name"` // "public.countries"
	AutoCreateTable bool             `json:"auto_create_table"`
	PrimaryKey      *PrimaryKeySpec  `json:"primary_key,omitempty"`
	Columns         []ColumnSpec     `json:"columns"`
	Constraints     []ConstraintSpec `json:"constraints,omitempty"`
	Load            LoadSpec         `json:"load"`
}

type PrimaryKeySpec struct {
	Name string `json:"name"` // country_id
	Type string `json:"type"` // serial
}

type ColumnSpec struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   *bool  `json:"nullable,omitempty"`
	References string `json:"references,omitempty"` // "public.countries(country_id)"
}

type ConstraintSpec struct {
	Kind    string   `json:"kind"` // "unique"
	Columns []string `json:"columns"`
}

type LoadSpec struct {
	Kind     string        `json:"kind"` // "dimension" | "fact"
	FromRows []FromRowSpec `json:"from_rows"`

	// for dimensions
	Conflict  *ConflictSpec `json:"conflict,omitempty"`
	Returning []string      `json:"returning,omitempty"`
	Cache     *CacheSpec    `json:"cache,omitempty"`

	// for facts
	Dedupe *DedupeSpec `json:"dedupe,omitempty"`
}

type FromRowSpec struct {
	TargetColumn string          `json:"target_column"`
	SourceField  string          `json:"source_field,omitempty"`
	Transform    []TransformSpec `json:"transform,omitempty"`
	Lookup       *LookupSpec     `json:"lookup,omitempty"`
}

type TransformSpec struct {
	Kind string `json:"kind"` // "trim"
}

type ConflictSpec struct {
	TargetColumns []string `json:"target_columns"`
	Action        string   `json:"action"` // "do_nothing"
}

type CacheSpec struct {
	KeyColumn   string `json:"key_column"`   // "name"
	ValueColumn string `json:"value_column"` // "country_id"
	Prewarm     bool   `json:"prewarm"`
}

type LookupSpec struct {
	Table     string            `json:"table"`      // "public.countries"
	Match     map[string]string `json:"match"`      // {"name": "stat"}
	Return    string            `json:"return"`     // "country_id"
	OnMissing string            `json:"on_missing"` // "insert"
}

type RuntimeConfig struct {
	BatchSize        int `json:"batch_size"`
	ChannelBuffer    int `json:"channel_buffer"`
	LoaderWorkers    int `json:"loader_workers"`
	ReaderWorkers    int `json:"reader_workers"`
	TransformWorkers int `json:"transform_workers"`
}

type DedupeSpec struct {
	ConflictColumns []string `json:"conflict_columns"`
	Action          string   `json:"action"` // "do_nothing" (supported)
}
