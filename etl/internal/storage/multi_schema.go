// To keep the engine generic, the TableSpec types need to live in a place both multitable and backend packages can import without circular deps.
package storage

type TableSpec struct {
	Name            string           `json:"name"`
	AutoCreateTable bool             `json:"auto_create_table"`
	PrimaryKey      *PrimaryKeySpec  `json:"primary_key,omitempty"`
	Columns         []ColumnSpec     `json:"columns"`
	Constraints     []ConstraintSpec `json:"constraints,omitempty"`
	Load            LoadSpec         `json:"load"`
}

type PrimaryKeySpec struct {
	Name string `json:"name"`
	Type string `json:"type"` // e.g. serial / int identity, etc
}

type ColumnSpec struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	References string `json:"references,omitempty"`
	Nullable   *bool  `json:"nullable,omitempty"`
}

type ConstraintSpec struct {
	Kind    string   `json:"kind"` // "unique"
	Columns []string `json:"columns"`
}

type LoadSpec struct {
	Kind     string        `json:"kind"` // "dimension" | "fact"
	FromRows []FromRowSpec `json:"from_rows"`

	// dimension
	Conflict *ConflictSpec `json:"conflict,omitempty"`
	Cache    *CacheSpec    `json:"cache,omitempty"`

	// fact
	Dedupe *DedupeSpec `json:"dedupe,omitempty"`
}

type DedupeSpec struct {
	ConflictColumns []string `json:"conflict_columns"`
	Action          string   `json:"action"` // "do_nothing"
}

type ConflictSpec struct {
	TargetColumns []string `json:"target_columns"`
	Action        string   `json:"action"` // "do_nothing"
}

type CacheSpec struct {
	KeyColumn   string `json:"key_column"`
	ValueColumn string `json:"value_column"`
	Prewarm     bool   `json:"prewarm"`
}

type FromRowSpec struct {
	TargetColumn string      `json:"target_column"`
	SourceField  string      `json:"source_field,omitempty"`
	Lookup       *LookupSpec `json:"lookup,omitempty"`
}

type LookupSpec struct {
	Table     string            `json:"table"`
	Match     map[string]string `json:"match"` // db key col -> source field
	Return    string            `json:"return"`
	OnMissing string            `json:"on_missing"`
}
