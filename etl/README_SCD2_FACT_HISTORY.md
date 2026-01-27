# Fact History (SCD2) for Multi-Table Loads

This project supports optional SCD2 (Slowly Changing Dimension Type 2) history tracking
for **fact tables** in the multi-table loader.

When enabled, the loader keeps a current version in the base fact table and appends
prior versions to a companion history table.

## Configuration

History is configured per-table in the multi-table table spec:

```yaml
load:
  kind: fact
  history:
    enabled: true
    business_key: ["customer_id"]
    valid_from_column: "valid_from"
    valid_to_column: "valid_to"
    changed_at_column: "changed_at"

Notes:

 - History is only supported for facts.

 - business_key determines identity across batches.

 - valid_to being NULL indicates the current version.

Tables

For a fact table named customers, history is stored in:

customers (current rows)

customers_history (previous versions)

The history table contains the same business columns plus the configured metadata columns.

Behavioral Guarantees (answers to common questions)
What happens when I run the same batch/data several times in a row?

No new history is created.
If an incoming row matches an existing current row by business key and all values are identical,
the loader performs a no-op for that row (idempotent).

What happens when batch 1 has record A, and batch 2 modifies it to A_2?

If business_key matches and values differ:

The previous current row (A) is copied into *_history and closed (valid_to = now()).

The base table’s current row is updated to A_2 and opened (valid_from = now(), valid_to = NULL).

So you end up with:

 - base: current = A_2

 - history: one row = A

What happens when the key/hash stays the same, but other values change?

That is the standard SCD2 update case described above:

the old version goes to history

the base table becomes the new current version

What happens when the key also slightly changes?

A business key change is treated as a new entity:

 - the loader inserts a new current row under the new business key

 - it does not automatically close or modify the previous key’s row

(If you need “key change implies closing previous key”, that is a different business rule and
should be explicitly modeled.)

What happens when in the 3rd batch, record A_2 goes back to original A?

Reverts are treated as new versions (SCD2 is append-only history):

 - A is inserted/updated as the current row again

 - A_2 is moved into history and closed

After A → A_2 → A:

 - base: current = A

 - history: two rows = (A, then A_2) in the order they were replaced

# Backend Notes
## SQLite

SQLite implements SCD2 in the repository layer:

 - It runs read/compare/update inside a transaction.

 - It requires the base table and history table to exist with the expected columns.

Other backends (Postgres, MSSQL) should implement equivalent semantics using their preferred
upsert/merge strategies or triggers.
