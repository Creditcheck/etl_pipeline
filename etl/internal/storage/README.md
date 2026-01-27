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
|---------------|--------|
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

