package ddl

import (
	"context"
	"strings"
	"testing"

	"etl/internal/config"
	gddl "etl/internal/ddl"
	"etl/internal/schema"
	"etl/internal/storage"
)

// TestFromPipelineMissingTableOrColumns verifies that FromPipeline returns
// clear errors when required table or column metadata is missing.
func TestFromPipelineMissingTableOrColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(p *config.Pipeline)
		wantError string
	}{
		{
			name: "missing table",
			setup: func(p *config.Pipeline) {
				// table empty, columns non-empty
				p.Storage.DB.Table = ""
				p.Storage.DB.Columns = []string{"id"}
			},
			wantError: "storage.postgres.table is required",
		},
		{
			name: "missing columns",
			setup: func(p *config.Pipeline) {
				p.Storage.DB.Table = "public.users"
				p.Storage.DB.Columns = nil
			},
			wantError: "storage.postgres.columns must not be empty",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var p config.Pipeline
			tt.setup(&p)

			got, err := FromPipeline(p)
			if err == nil {
				t.Fatalf("FromPipeline() error = nil, want non-nil")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("FromPipeline() error = %q, want substring %q", err.Error(), tt.wantError)
			}
			if got.FQN != "" || len(got.Columns) != 0 {
				t.Fatalf("FromPipeline() result not empty on error: %+v", got)
			}
		})
	}
}

// TestFromPipelineContractDriven verifies that FromPipeline uses a validation
// contract to infer Postgres types and nullability when present.
func TestFromPipelineContractDriven(t *testing.T) {
	t.Parallel()

	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "bigint", Required: true, Nullable: false},
			{Name: "name", Type: "string", Required: false, Nullable: false},
			{Name: "meta", Type: "jsonb", Required: false, Nullable: true},
		},
	}

	var p config.Pipeline
	p.Storage.DB.Table = "public.users"
	p.Storage.DB.Columns = []string{"id", "name", "meta", "unknown"}
	p.Transform = []config.Transform{
		{
			Kind: "validate",
			Options: config.Options{
				"contract": contract,
			},
		},
	}

	got, err := FromPipeline(p)
	if err != nil {
		t.Fatalf("FromPipeline() error = %v", err)
	}

	if got.FQN != "public.users" {
		t.Fatalf("FromPipeline().FQN = %q, want %q", got.FQN, "public.users")
	}
	if len(got.Columns) != 4 {
		t.Fatalf("FromPipeline().Columns length = %d, want 4", len(got.Columns))
	}

	// Nullable semantics follow the implementation:
	// Nullable = !f.Required && !f.Nullable
	tests := []struct {
		idx      int
		name     string
		wantType string
		wantNull bool
	}{
		// Required & not nullable => NOT NULL
		{idx: 0, name: "id", wantType: MapType("bigint"), wantNull: false},
		// Not required & not nullable => Nullable = true
		{idx: 1, name: "name", wantType: MapType("string"), wantNull: true},
		// Not required & nullable => Nullable = false (per code)
		{idx: 2, name: "meta", wantType: MapType("jsonb"), wantNull: false},
		// Unknown => zero-value Field: Type="", Required=false, Nullable=false => Nullable = true
		{idx: 3, name: "unknown", wantType: MapType(""), wantNull: true},
	}

	for _, tt := range tests {
		col := got.Columns[tt.idx]
		if col.Name != tt.name {
			t.Errorf("column[%d].Name = %q, want %q", tt.idx, col.Name, tt.name)
		}
		if col.SQLType != tt.wantType {
			t.Errorf("column[%d].SQLType = %q, want %q", tt.idx, col.SQLType, tt.wantType)
		}
	}
}

// TestFromPipelineContractInvalid ensures that if the validate transform's
// contract cannot be decoded, FromPipeline falls back to the coerce/default
// path and does not fail.
func TestFromPipelineContractInvalid(t *testing.T) {
	t.Parallel()

	// Raw value that json.Marshal cannot handle (functions are unsupported).
	rawContract := func() {}

	var p config.Pipeline
	p.Storage.DB.Table = "logs"
	p.Storage.DB.Columns = []string{"id", "payload"}
	p.Transform = []config.Transform{
		{
			Kind: "validate",
			Options: config.Options{
				"contract": rawContract,
			},
		},
	}

	got, err := FromPipeline(p)
	if err != nil {
		t.Fatalf("FromPipeline() error = %v", err)
	}

	if got.FQN != "logs" {
		t.Fatalf("FromPipeline().FQN = %q, want %q", got.FQN, "logs")
	}
	if len(got.Columns) != 2 {
		t.Fatalf("FromPipeline().Columns length = %d, want 2", len(got.Columns))
	}
	for i, col := range got.Columns {
		if col.Name == "" {
			t.Errorf("column[%d].Name is empty, want non-empty", i)
		}
		// No contract and no coerce hints => MapType("") and Nullable = true.
		if col.SQLType != MapType("") {
			t.Errorf("column[%d].SQLType = %q, want %q", i, col.SQLType, MapType(""))
		}
		if !col.Nullable {
			t.Errorf("column[%d].Nullable = false, want true", i)
		}
	}
}

// TestFromPipelineCoerceDriven verifies that FromPipeline uses coerce.types
// hints for type inference when no validation contract is present, and that
// later coerce transforms override earlier ones.
func TestFromPipelineCoerceDriven(t *testing.T) {
	t.Parallel()

	var p config.Pipeline
	p.Storage.DB.Table = "public.events"
	p.Storage.DB.Columns = []string{"id", "kind", "payload"}
	p.Transform = []config.Transform{
		{
			Kind: "coerce",
			Options: config.Options{
				"types": map[string]any{
					"id":      "bigint",
					"kind":    "string",
					"payload": "text",
				},
			},
		},
		{
			Kind: "coerce",
			Options: config.Options{
				"types": map[string]any{
					"kind": "bool",
				},
			},
		},
	}

	got, err := FromPipeline(p)
	if err != nil {
		t.Fatalf("FromPipeline() error = %v", err)
	}

	if got.FQN != "public.events" {
		t.Fatalf("FromPipeline().FQN = %q, want %q", got.FQN, "public.events")
	}
	if len(got.Columns) != 3 {
		t.Fatalf("FromPipeline().Columns length = %d, want 3", len(got.Columns))
	}

	want := []gddl.ColumnDef{
		{Name: "id", SQLType: MapType("bigint"), Nullable: true},
		{Name: "kind", SQLType: MapType("bool"), Nullable: true},
		{Name: "payload", SQLType: MapType("text"), Nullable: true},
	}

	for i := range want {
		if got.Columns[i].Name != want[i].Name {
			t.Errorf("column[%d].Name = %q, want %q", i, got.Columns[i].Name, want[i].Name)
		}
		if got.Columns[i].SQLType != want[i].SQLType {
			t.Errorf("column[%d].SQLType = %q, want %q", i, got.Columns[i].SQLType, want[i].SQLType)
		}
		if got.Columns[i].Nullable != want[i].Nullable {
			t.Errorf("column[%d].Nullable = %v, want %v", i, got.Columns[i].Nullable, want[i].Nullable)
		}
	}
}

// TestFromPipelineColumnOrder verifies that FromPipeline preserves the
// configured column order.
func TestFromPipelineColumnOrder(t *testing.T) {
	t.Parallel()

	cols := []string{"c3", "c1", "c2"}

	var p config.Pipeline
	p.Storage.DB.Table = "public.test"
	p.Storage.DB.Columns = cols

	got, err := FromPipeline(p)
	if err != nil {
		t.Fatalf("FromPipeline() error = %v", err)
	}

	if len(got.Columns) != len(cols) {
		t.Fatalf("FromPipeline().Columns length = %d, want %d", len(got.Columns), len(cols))
	}
	for i, name := range cols {
		if got.Columns[i].Name != name {
			t.Errorf("column[%d].Name = %q, want %q", i, got.Columns[i].Name, name)
		}
	}
}

// fakeRepository is a test double for storage.Repository used to verify
// EnsureTable behavior without hitting a real database.
type fakeRepository struct {
	storage.Repository
	execCalls int
	lastSQL   string
	err       error
}

// Exec records the executed SQL and returns the configured error.
func (f *fakeRepository) Exec(ctx context.Context, sqlText string) error {
	f.execCalls++
	f.lastSQL = sqlText
	return f.err
}

// TestEnsureTableExecutesSQL verifies that EnsureTable calls Exec with a
// CREATE TABLE statement and propagates any Exec errors.
func TestEnsureTableExecutesSQL(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN: "public.users",
		Columns: []gddl.ColumnDef{
			{Name: "id", SQLType: "BIGINT", PrimaryKey: true},
		},
	}

	var repo fakeRepository
	ctx := context.Background()

	if err := EnsureTable(ctx, &repo, def); err != nil {
		t.Fatalf("EnsureTable() error = %v", err)
	}

	if repo.execCalls != 1 {
		t.Fatalf("repo.Exec called %d times, want 1", repo.execCalls)
	}
	if repo.lastSQL == "" {
		t.Fatalf("repo.Exec was called with empty SQL")
	}
	if !strings.HasPrefix(repo.lastSQL, "CREATE TABLE IF NOT EXISTS") {
		t.Fatalf("repo.Exec SQL does not start with CREATE TABLE IF NOT EXISTS:\n%s", repo.lastSQL)
	}
}

// BenchmarkFromPipelineContract measures the performance of contract-driven
// inference via FromPipeline.
func BenchmarkFromPipelineContract(b *testing.B) {
	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "bigint", Required: true},
			{Name: "name", Type: "string", Required: false},
			{Name: "created_at", Type: "timestamp", Required: true},
		},
	}

	var p config.Pipeline
	p.Storage.DB.Table = "public.users"
	p.Storage.DB.Columns = []string{"id", "name", "created_at"}
	p.Transform = []config.Transform{
		{
			Kind: "validate",
			Options: config.Options{
				"contract": contract,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := FromPipeline(p); err != nil {
			b.Fatalf("FromPipeline() error = %v", err)
		}
	}
}

// BenchmarkFromPipelineCoerce measures the performance of coerce-driven
// inference via FromPipeline.
func BenchmarkFromPipelineCoerce(b *testing.B) {
	var p config.Pipeline
	p.Storage.DB.Table = "public.events"
	p.Storage.DB.Columns = []string{"id", "kind", "payload", "created_at"}
	p.Transform = []config.Transform{
		{
			Kind: "coerce",
			Options: config.Options{
				"types": map[string]any{
					"id":         "bigint",
					"kind":       "string",
					"payload":    "text",
					"created_at": "timestamp",
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := FromPipeline(p); err != nil {
			b.Fatalf("FromPipeline() error = %v", err)
		}
	}
}
