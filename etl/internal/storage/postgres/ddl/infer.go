// Package ddl provides Postgres-specific inference helpers that turn a
// config.Pipeline plus (optionally) a schema.Contract into a generic
// ddl.TableDef, using the Postgres storage settings.
package ddl

import (
	"encoding/json"
	"fmt"

	"etl/internal/config"
	gddl "etl/internal/ddl"
	"etl/internal/schema" // Contract / Field only
)

// FromPipeline infers a Postgres table definition from the pipeline's storage
// + validation/transform configuration.
//
// It is Postgres-specific because it looks at p.Storage.DB.* and uses
// Postgres MapType.
func FromPipeline(p config.Pipeline) (gddl.TableDef, error) {
	table := p.Storage.DB.Table
	cols := p.Storage.DB.Columns

	if table == "" {
		return gddl.TableDef{}, fmt.Errorf("postgres ddl: storage.postgres.table is required")
	}
	if len(cols) == 0 {
		return gddl.TableDef{}, fmt.Errorf("postgres ddl: storage.postgres.columns must not be empty")
	}

	// Try to find a validate/contract transform.
	var contract *schema.Contract
	for _, t := range p.Transform {
		if t.Kind != "validate" {
			continue
		}
		raw := t.Options.Any("contract")
		if raw == nil {
			continue
		}
		b, err := json.Marshal(raw)
		if err != nil {
			continue
		}
		var c schema.Contract
		if err := json.Unmarshal(b, &c); err == nil && len(c.Fields) > 0 {
			contract = &c
			break
		}
	}

	defs := make([]gddl.ColumnDef, 0, len(cols))

	if contract != nil {
		// Use contract to drive types + nullability.
		idx := make(map[string]schema.Field, len(contract.Fields))
		for _, f := range contract.Fields {
			idx[f.Name] = f
		}
		for _, name := range cols {
			f := idx[name] // zero value if missing; MapType("") → TEXT
			defs = append(defs, gddl.ColumnDef{
				Name:    name,
				SQLType: MapType(f.Type),
				// Nullable: f.Required && f.Nullable, // adjust to your semantics
				Nullable: !f.Required, // adjust to your semantics
				// PrimaryKey / Default can be filled here if you encode them in config later.
			})
		}
	} else {
		// Fallback: use coerce.types hints if present, otherwise TEXT.
		types := map[string]string{}
		for _, t := range p.Transform {
			if t.Kind != "coerce" {
				continue
			}
			for k, v := range t.Options.StringMap("types") {
				types[k] = v
			}
		}
		for _, name := range cols {
			defs = append(defs, gddl.ColumnDef{
				Name:    name,
				SQLType: MapType(types[name]),
				// Nullable left as default (true) when no contract is present.
				Nullable: true,
			})
		}
	}

	return gddl.TableDef{
		FQN:     table,
		Columns: defs,
	}, nil
}
