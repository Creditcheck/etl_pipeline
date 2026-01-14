package storage

import (
	"context"
	"testing"
)

type fakeMultiRepo struct {
	lastTable   string
	lastColumns []string
	lastRows    [][]any
	copyCalls   int
	execCalls   int
	closeCalls  int

	copyN   int64
	copyErr error
	execErr error
}

func (f *fakeMultiRepo) CopyFromTable(ctx context.Context, table string, columns []string, rows [][]any) (int64, error) {
	f.copyCalls++
	f.lastTable = table
	f.lastColumns = append([]string(nil), columns...)
	f.lastRows = rows
	return f.copyN, f.copyErr
}

func (f *fakeMultiRepo) Exec(ctx context.Context, sql string) error {
	f.execCalls++
	return f.execErr
}

func (f *fakeMultiRepo) Close() { f.closeCalls++ }

func TestBindTable_BindsTableAndDelegatesCopy(t *testing.T) {
	mr := &fakeMultiRepo{copyN: 7}
	repo, err := BindTable(mr, "public.imports")
	if err != nil {
		t.Fatalf("BindTable: %v", err)
	}

	n, err := repo.CopyFrom(context.Background(), []string{"a", "b"}, [][]any{{1, 2}})
	if err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	if n != 7 {
		t.Fatalf("expected n=7, got %d", n)
	}

	if mr.copyCalls != 1 {
		t.Fatalf("expected 1 copy call, got %d", mr.copyCalls)
	}
	if mr.lastTable != "public.imports" {
		t.Fatalf("expected table public.imports, got %q", mr.lastTable)
	}
	if len(mr.lastColumns) != 2 || mr.lastColumns[0] != "a" || mr.lastColumns[1] != "b" {
		t.Fatalf("unexpected columns: %#v", mr.lastColumns)
	}
}

func TestBindTable_DelegatesExecAndClose(t *testing.T) {
	mr := &fakeMultiRepo{}
	repo, err := BindTable(mr, "public.countries")
	if err != nil {
		t.Fatalf("BindTable: %v", err)
	}

	if err := repo.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	repo.Close()

	if mr.execCalls != 1 {
		t.Fatalf("expected 1 exec call, got %d", mr.execCalls)
	}
	if mr.closeCalls != 1 {
		t.Fatalf("expected 1 close call, got %d", mr.closeCalls)
	}
}

func TestBindTable_RejectsEmptyInputs(t *testing.T) {
	if _, err := BindTable(nil, "public.t"); err == nil {
		t.Fatalf("expected error for nil repo")
	}
	mr := &fakeMultiRepo{}
	if _, err := BindTable(mr, ""); err == nil {
		t.Fatalf("expected error for empty table")
	}
}
