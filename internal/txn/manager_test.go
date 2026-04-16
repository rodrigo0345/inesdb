package txn

import (
	"os"
	"path/filepath"
	"testing"

	wallog "github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/testutil"
	"github.com/rodrigo0345/omag/internal/txn/write_handler"
)

func TestNewTransactionManagerWithWAL(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()

	tm := NewTransactionManager(nil, wm, bpm, storage)

	if tm == nil {
		t.Fatal("expected non-nil transaction manager")
	}
}

func TestNewTransactionManagerWithoutWAL(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	storage := testutil.NewInMemoryStorageEngine()

	tm := NewTransactionManager(nil, nil, bpm, storage)

	if tm == nil {
		t.Fatal("expected non-nil transaction manager")
	}
}

func TestGetRollbackManager(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()

	tm := NewTransactionManager(nil, wm, bpm, storage)
	rm := tm.GetRollbackManager()

	if rm == nil {
		t.Fatal("expected non-nil rollback manager")
	}
}

func TestGetWriteHandlerWithWAL(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()

	tm := NewTransactionManager(nil, wm, bpm, storage)
	wh := tm.GetWriteHandler()

	if wh == nil {
		t.Fatal("expected non-nil write handler")
	}

	_, ok := wh.(*write_handler.DefaultWriteHandler)
	if !ok {
		t.Error("expected DefaultWriteHandler when WAL is provided")
	}
}

func TestGetWriteHandlerWithoutWAL(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	storage := testutil.NewInMemoryStorageEngine()

	tm := NewTransactionManager(nil, nil, bpm, storage)
	wh := tm.GetWriteHandler()

	if wh == nil {
		t.Fatal("expected non-nil write handler")
	}

	_, ok := wh.(*write_handler.MVCCWriteHandler)
	if !ok {
		t.Error("expected MVCCWriteHandler when WAL is not provided")
	}
}

func TestTransactionManagerIntegration(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()

	tm := NewTransactionManager(nil, wm, bpm, storage)

	if tm.GetRollbackManager() == nil {
		t.Error("rollback manager should not be nil")
	}

	if tm.GetWriteHandler() == nil {
		t.Error("write handler should not be nil")
	}
}

func TestTransactionManagerMultiple(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()

	tm1 := NewTransactionManager(nil, wm, bpm, storage)
	tm2 := NewTransactionManager(nil, wm, bpm, storage)

	if tm1 == tm2 {
		t.Error("multiple transaction managers should be different instances")
	}
}
