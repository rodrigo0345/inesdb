package main

import (
	"path/filepath"
	"testing"

	"github.com/rodrigo0345/omag/internal/database"
)

func newTestNodeWithDB(t *testing.T) *Node {
	t.Helper()
	tmp := t.TempDir()
	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Close()
	})

	n := NewNode()
	n.db = engine
	n.txnManager = engine.IsolationManager()
	return n
}

func TestExecuteTxn_UsesDatabaseValueForReadAfterWrite(t *testing.T) {
	n := newTestNodeWithDB(t)

	ops := []any{
		[]any{"w", 1, 42},
		[]any{"r", 1, nil},
	}
	results := n.executeTxn(ops)
	if len(results) != 2 {
		t.Fatalf("executeTxn() returned %d ops, want 2", len(results))
	}

	writeRes, ok := results[0].([]any)
	if !ok || len(writeRes) != 3 {
		t.Fatalf("unexpected write result shape: %#v", results[0])
	}
	if writeRes[0] != "w" || writeRes[1] != 1 || writeRes[2] != 42 {
		t.Fatalf("write result = %#v, want [w 1 42]", writeRes)
	}

	readRes, ok := results[1].([]any)
	if !ok || len(readRes) != 3 {
		t.Fatalf("unexpected read result shape: %#v", results[1])
	}
	if readRes[0] != "r" || readRes[1] != 1 {
		t.Fatalf("read result head = %#v, want [r 1 ...]", readRes)
	}
	if got, ok := readRes[2].(float64); !ok || got != 42 {
		t.Fatalf("read result value = %#v, want float64(42)", readRes[2])
	}
}

func TestExecuteTxn_ReadMissingKeyReturnsNil(t *testing.T) {
	n := newTestNodeWithDB(t)

	results := n.executeTxn([]any{[]any{"r", "missing", nil}})
	if len(results) != 1 {
		t.Fatalf("executeTxn() returned %d ops, want 1", len(results))
	}

	readRes, ok := results[0].([]any)
	if !ok || len(readRes) != 3 {
		t.Fatalf("unexpected read result shape: %#v", results[0])
	}
	if readRes[0] != "r" || readRes[1] != "missing" {
		t.Fatalf("read result head = %#v, want [r missing ...]", readRes)
	}
	if readRes[2] != nil {
		t.Fatalf("read missing value = %#v, want nil", readRes[2])
	}
}

func TestExecuteTxn_PersistsAcrossTransactions(t *testing.T) {
	n := newTestNodeWithDB(t)

	_ = n.executeTxn([]any{[]any{"w", "k", map[string]any{"x": 7}}})
	results := n.executeTxn([]any{[]any{"r", "k", nil}})
	if len(results) != 1 {
		t.Fatalf("executeTxn() returned %d ops, want 1", len(results))
	}

	readRes, ok := results[0].([]any)
	if !ok || len(readRes) != 3 {
		t.Fatalf("unexpected read result shape: %#v", results[0])
	}
	obj, ok := readRes[2].(map[string]any)
	if !ok {
		t.Fatalf("read result value type = %T, want map[string]any", readRes[2])
	}
	if got, ok := obj["x"].(float64); !ok || got != 7 {
		t.Fatalf("read result value x = %#v, want float64(7)", obj["x"])
	}
}

