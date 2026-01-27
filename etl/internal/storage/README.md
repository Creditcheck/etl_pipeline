# Slowly Changing Facts (SCD2) with Current + History Tables

This codebase supports **Slowly Changing Dimension Type 2 (SCD2)** semantics for **fact tables** using a **current table + history table** model.

This allows the pipeline to:

- Preserve all historical versions of a fact row  
- Maintain **exactly one current version per business key**  
- Detect changes efficiently using a **row hash**  
- Be **idempotent** when reprocessing the same batch  

---

## Mental Model

For a fact table named:

```
public.imports
```

SCD2 creates and manages:

| Table | Purpose |
|------|---------|
| `public.imports` | Holds **only the current version** of each business entity |
| `public.imports_history` | Holds **all previous versions** |

Both tables share the same business columns plus metadata:

| Column | Meaning |
|--------|---------|
| `valid_from` | When this version became current |
| `valid_to` | When this version stopped being current (NULL = still current) |
| `changed_at` | When the pipeline wrote this version |

---

## What Is a “Business Key”?

The **business key** uniquely identifies the *real-world entity* represented by the row.

Example for imports:

```json
"business_key": ["vehicle_id", "country_id"]
```

This means:

> There can be only **one current row** per `(vehicle_id, country_id)`.

If a new row arrives with the same business key:

- If nothing changed → do nothing  
- If something changed → version the row  

---

## How Versioning Works

When a row arrives:

### Case 1 — No existing current row
Insert as a **new current row**:

```
valid_from = now
valid_to   = NULL
changed_at = now
```

---

### Case 2 — Current row exists and NOTHING changed
Do nothing (idempotent).

---

### Case 3 — Current row exists and SOMETHING changed

1. Move current row → history table  
   ```
   valid_to = now
   ```

2. Insert new row as current  
   ```
   valid_from = now
   valid_to   = NULL
   changed_at = now
   ```

---

## Why `row_hash` Exists

Comparing every column to detect changes is:

- Slower  
- Fragile (type conversions, NULL handling, formatting differences)

Instead, we compute a **deterministic fingerprint** of the row’s business attributes:

```
row_hash = sha256("field=value␟field=value␟...")
```

Then change detection becomes:

```
IF current.row_hash == incoming.row_hash
    → no change
ELSE
    → version row
```

This makes SCD2 **O(1)** per row instead of comparing all fields.

---

## How to Configure `row_hash`

### 1️⃣ Add a hash transform

```json
{
  "kind": "hash",
  "options": {
    "fields": ["pcv", "stat", "datum_dovozu"],
    "target_field": "row_hash",
    "include_field_names": true,
    "trim_space": true,
    "separator": "\u001f",
    "algorithm": "sha256",
    "encoding": "hex",
    "overwrite": true
  }
}
```

**Rules:**

✔ Hash only **business attributes**, not surrogate IDs  
✔ Field order must be stable  
✔ Normalization (trim/lowercase) should match business rules  

---

### 2️⃣ Add `row_hash` column to fact table

```json
{ "name": "row_hash", "type": "text", "nullable": false }
```

---

### 3️⃣ Map it during fact load

```json
{ "target_column": "row_hash", "source_field": "row_hash" }
```

---

### 4️⃣ Do **NOT** include it in the business key

❌ Wrong:

```json
"business_key": ["vehicle_id", "country_id", "row_hash"]
```

That would create **multiple current rows**, defeating SCD2.

✔ Correct:

```json
"business_key": ["vehicle_id", "country_id"]
```

---

## Example Fact Table Config (SCD2 Enabled)

```json
{
  "name": "public.imports",
  "auto_create_table": true,
  "primary_key": { "name": "import_id", "type": "serial" },
  "columns": [
    { "name": "vehicle_id", "type": "int", "nullable": false, "references": "public.vehicles(vehicle_id)" },
    { "name": "country_id", "type": "int", "nullable": false, "references": "public.countries(country_id)" },
    { "name": "import_date", "type": "date", "nullable": true },
    { "name": "row_hash", "type": "text", "nullable": false },
    { "name": "valid_from", "type": "timestamptz", "nullable": false },
    { "name": "valid_to", "type": "timestamptz", "nullable": true },
    { "name": "changed_at", "type": "timestamptz", "nullable": false }
  ],
  "constraints": [
    { "kind": "unique", "columns": ["vehicle_id", "country_id"] }
  ],
  "load": {
    "kind": "fact",
    "history": {
      "enabled": true,
      "business_key": ["vehicle_id", "country_id"],
      "valid_from_column": "valid_from",
      "valid_to_column": "valid_to",
      "changed_at_column": "changed_at"
    },
    "from_rows": [
      { "target_column": "vehicle_id", "lookup": { "table": "public.vehicles", "match": { "pcv": "pcv" }, "return": "vehicle_id", "on_missing": "insert" } },
      { "target_column": "country_id", "lookup": { "table": "public.countries", "match": { "name": "stat" }, "return": "country_id", "on_missing": "insert" } },
      { "target_column": "import_date", "source_field": "datum_dovozu" },
      { "target_column": "row_hash", "source_field": "row_hash" }
    ]
  }
}
```

---

# Behavioral Tests (Table-Driven)

These tests demonstrate SCD2 correctness at the logic level.

### `scd2_rowhash_test.go`

```go
package postgres

import "testing"

func TestSCD2RowHashChangeDetection(t *testing.T) {
	tests := []struct {
		name         string
		currentHash  any
		newHash      any
		expectChange bool
	}{
		{"same string", "abc", "abc", false},
		{"string vs bytes same", "abc", []byte("abc"), false},
		{"different hash", "abc", "def", true},
		{"nil both", nil, nil, false},
		{"nil vs value", nil, "abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := !equalScalar(tt.currentHash, tt.newHash)
			if changed != tt.expectChange {
				t.Fatalf("expected change=%v got %v", tt.expectChange, changed)
			}
		})
	}
}
```

---

### `scd2_business_key_test.go`

```go
package postgres

import "testing"

func TestBusinessKeyIndexing(t *testing.T) {
	cols := []string{"vehicle_id", "country_id", "row_hash"}
	idx := indexColumns(cols)
	keyIdx := indicesFor([]string{"vehicle_id", "country_id"}, idx)

	if keyIdx[0] != 0 || keyIdx[1] != 1 {
		t.Fatalf("business key indices incorrect: %v", keyIdx)
	}
}
```

---

### `scd2_insert_decision_test.go`

```go
package postgres

import "testing"

func TestInsertDecisionUsingRowHash(t *testing.T) {
	columns := []string{"vehicle_id", "country_id", "row_hash"}
	rowHashIdx, ok := indexOfColumn(columns, "row_hash")
	if !ok {
		t.Fatal("row_hash not found")
	}

	current := []any{1, 2, "aaa"}
	incomingSame := []any{1, 2, "aaa"}
	incomingChanged := []any{1, 2, "bbb"}

	if equalScalar(current[rowHashIdx], incomingSame[rowHashIdx]) != true {
		t.Fatal("expected no change")
	}
	if equalScalar(current[rowHashIdx], incomingChanged[rowHashIdx]) != false {
		t.Fatal("expected change")
	}
}
```

---

## Summary

| Feature | Provided By |
|--------|-------------|
| One current row per entity | Business key + unique constraint |
| Full history preserved | `_history` table |
| Efficient change detection | `row_hash` |
| Idempotent loads | row_hash + business key logic |
| Accurate timestamps | `valid_from`, `valid_to`, `changed_at` |

