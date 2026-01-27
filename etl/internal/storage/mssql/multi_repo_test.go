package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"etl/internal/storage"
)

func TestEnsureDimensionKeys_DeterministicOrdering(t *testing.T) {
	t.Parallel()

	db := &fakeDB{}
	repo := &MultiRepo{db: db}

	keysA := []any{"b", "a", "a", "c"}
	keysB := []any{"c", "b", "a"}

	run := func(keys []any) []any {
		db.reset()
		if err := repo.EnsureDimensionKeys(context.Background(), "dbo.dim", "name", keys, []string{"name"}); err != nil {
			t.Fatalf("EnsureDimensionKeys err: %v", err)
		}
		if len(db.execCalls) == 0 {
			t.Fatalf("expected exec calls")
		}
		return db.execCalls[0].args
	}

	argsA := run(keysA)
	argsB := run(keysB)

	if !reflect.DeepEqual(argsA, argsB) {
		t.Fatalf("args differ:\nA=%v\nB=%v", argsA, argsB)
	}
	if want := []any{"a", "b", "c"}; !reflect.DeepEqual(argsA, want) {
		t.Fatalf("args=%v want=%v", argsA, want)
	}
}

func TestInsertSCD2_HappyPath_NoCurrentRow_InsertsCurrent(t *testing.T) {
	t.Parallel()

	db := &fakeDB{
		beginTx: func() *fakeTx {
			return &fakeTx{
				row: &fakeRow{
					onScan: func(dest []any) error {
						return sql.ErrNoRows
					},
				},
			}
		},
	}
	repo := &MultiRepo{db: db}

	spec := storage.TableSpec{
		Name: "dbo.facts",
		Load: storage.LoadSpec{
			Kind: "fact",
			History: &storage.HistorySpec{
				Enabled:         true,
				BusinessKey:     []string{"k"},
				ValidFromColumn: "valid_from",
				ValidToColumn:   "valid_to",
				ChangedAtColumn: "changed_at",
			},
		},
	}

	cols := []string{"k", "row_hash"}
	rows := [][]any{{"x", "h1"}}

	n, err := repo.InsertFactRows(context.Background(), spec, "dbo.facts", cols, rows, nil)
	if err != nil {
		t.Fatalf("InsertFactRows err: %v", err)
	}
	if n != 1 {
		t.Fatalf("inserted=%d want=1", n)
	}
	// Insert happens via tx; fakeTx forwards into fakeDB.execCalls.
	if len(db.execCalls) == 0 {
		t.Fatalf("expected exec calls")
	}
}

func TestInsertSCD2_Change_WritesHistoryAndUpdatesCurrent(t *testing.T) {
	t.Parallel()

	oldValidFrom := time.Date(2026, 1, 27, 10, 0, 0, 0, time.UTC)

	db := &fakeDB{
		beginTx: func() *fakeTx {
			return &fakeTx{
				row: &fakeRow{
					onScan: func(dest []any) error {
						// dest: &out[0], &out[1], &validFrom
						*(dest[0].(*any)) = "x"
						*(dest[1].(*any)) = "h1"
						*(dest[2].(*time.Time)) = oldValidFrom
						return nil
					},
				},
			}
		},
	}
	repo := &MultiRepo{db: db}

	spec := storage.TableSpec{
		Name: "dbo.facts",
		Load: storage.LoadSpec{
			Kind: "fact",
			History: &storage.HistorySpec{
				Enabled:         true,
				BusinessKey:     []string{"k"},
				ValidFromColumn: "valid_from",
				ValidToColumn:   "valid_to",
				ChangedAtColumn: "changed_at",
			},
		},
	}

	cols := []string{"k", "row_hash"}
	rows := [][]any{{"x", "h2"}} // changed row_hash

	n, err := repo.InsertFactRows(context.Background(), spec, "dbo.facts", cols, rows, nil)
	if err != nil {
		t.Fatalf("InsertFactRows err: %v", err)
	}
	if n != 1 {
		t.Fatalf("inserted=%d want=1", n)
	}

	// Expect:
	//  - INSERT history
	//  - UPDATE current
	if len(db.execCalls) != 2 {
		t.Fatalf("execCalls=%d want=2", len(db.execCalls))
	}
	if !strings.Contains(db.execCalls[0].query, "_history") {
		t.Fatalf("first exec should target history insert; got query=%s", db.execCalls[0].query)
	}
	if !strings.HasPrefix(db.execCalls[1].query, "UPDATE") {
		t.Fatalf("second exec should update current; got query=%s", db.execCalls[1].query)
	}
}

/* -------------------- fakes -------------------- */

type execCall struct {
	query string
	args  []any
}

type fakeDB struct {
	execCalls []execCall

	// beginTx optionally supplies a tx instance (defaults to a basic tx).
	beginTx func() *fakeTx
}

func (f *fakeDB) reset() { f.execCalls = nil }

func (f *fakeDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	f.execCalls = append(f.execCalls, execCall{query: query, args: append([]any(nil), args...)})
	return fakeResult(1), nil
}

func (f *fakeDB) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("fakeDB.QueryContext not implemented in these tests")
}

// BeginTx matches the dbConn seam in multi_repo.go (returns txConn).
func (f *fakeDB) BeginTx(_ context.Context, _ *sql.TxOptions) (txConn, error) {
	tx := &fakeTx{parent: f}
	if f.beginTx != nil {
		tx = f.beginTx()
		tx.parent = f
	}
	return tx, nil
}

func (f *fakeDB) Close() error { return nil }

type fakeTx struct {
	parent    *fakeDB
	execCalls []execCall
	row       *fakeRow
}

func (f *fakeTx) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	call := execCall{query: query, args: append([]any(nil), args...)}
	f.execCalls = append(f.execCalls, call)
	if f.parent != nil {
		f.parent.execCalls = append(f.parent.execCalls, call)
	}
	return fakeResult(1), nil
}

func (f *fakeTx) QueryRowContext(_ context.Context, _ string, _ ...any) rowScanner {
	if f.row == nil {
		return &fakeRow{onScan: func([]any) error { return sql.ErrNoRows }}
	}
	return f.row
}

func (f *fakeTx) Commit() error   { return nil }
func (f *fakeTx) Rollback() error { return nil }

type fakeRow struct {
	onScan func(dest []any) error
}

func (f *fakeRow) Scan(dest ...any) error {
	if f.onScan == nil {
		return sql.ErrNoRows
	}
	return f.onScan(dest)
}

type fakeResult int64

func (r fakeResult) LastInsertId() (int64, error) { return 0, fmt.Errorf("not supported") }
func (r fakeResult) RowsAffected() (int64, error) { return int64(r), nil }
