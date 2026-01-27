# Fact History (SCD2) Behavior

This project implements **Slowly Changing Dimension Type 2 (SCD2)** semantics
for **fact tables**, using a **current table + history table** model.

The goal is to:
- Preserve *all* historical versions of fact rows
- Maintain exactly one *current* version per business key
- Allow idempotent reprocessing of the same batch
- Detect changes efficiently using a `row_hash`

---

## Table Model

For a fact table named `facts`, two physical tables exist:

- `facts` — contains **only the current version**
- `facts_history` — contains **all previous versions**

Both tables share the same business columns plus metadata columns:

| Column        | Meaning |
|---------------|---------|
| `valid_from`  | When this version became current |
| `valid_to`    | When this version stopped being current (`NULL` for active rows) |
| `changed_at`  | When the system wrote this version |

---

## Matching Rules

Rows are matched using the configured **business key**:

```yaml
History:
  enabled: true
  business_key: ["customer_id"]
```

Only rows with `valid_to IS NULL` are considered current.

---

## Change Detection

Rows are compared using a **checksum column**:

```
row_hash
```

If the incoming `row_hash` matches the current row’s `row_hash`,
the operation is a **no-op**.

This makes repeated batch runs idempotent and fast.

---

## Timeline Examples

### Example 1 — Same batch repeated

| Time | Incoming | Result |
|-----:|----------|--------|
| T1 | A | Insert A |
| T2 | A | No-op |
| T3 | A | No-op |

**facts table**

| value | valid_from | valid_to |
|-------|------------|----------|
| A     | T1         | NULL     |

**facts_history table**

(empty)

---

### Example 2 — Value changes

| Time | Incoming |
|-----:|----------|
| T1 | A |
| T2 | A₂ |

**facts table**

| value | valid_from | valid_to |
|-------|------------|----------|
| A₂    | T2         | NULL     |

**facts_history table**

| value | valid_from | valid_to |
|-------|------------|----------|
| A     | T1         | T2       |

---

### Example 3 — Change then revert

| Time | Incoming |
|-----:|----------|
| T1 | A |
| T2 | A₂ |
| T3 | A |

**facts table**

| value | valid_from | valid_to |
|-------|------------|----------|
| A     | T3         | NULL     |

**facts_history table**

| value | valid_from | valid_to |
|-------|------------|----------|
| A     | T1         | T2       |
| A₂    | T2         | T3       |

Reverting to a previous value **creates a new version**.

---

### Example 4 — Business key changes

| Time | Incoming |
|-----:|----------|
| T1 | id=1, A |
| T2 | id=2, A |

**facts table**

| id | value | valid_to |
|----|-------|----------|
| 1  | A     | NULL     |
| 2  | A     | NULL     |

**facts_history table**

(empty)

Each business key has an independent lineage.

---

## Is this Strict SCD2?

**Semantically: yes.**  
**Physically: slightly different.**

This is an **SCD2 implementation with a separate history table**, sometimes called:

- SCD2 with audit shadow table
- Current + History split model

The behavior matches classic SCD2 rules, but avoids storing multiple versions in the base table.

---

## Guarantees

✔ Exactly one current row per business key  
✔ All changes preserved forever  
✔ Idempotent batch reprocessing  
✔ Deterministic history  
✔ Efficient hash-based comparison  

---

## Summary

This design is optimized for ETL workloads:

- Fast current reads
- Cheap reprocessing
- Complete historical traceability

It is safe, predictable, and well-suited for fact tables.
