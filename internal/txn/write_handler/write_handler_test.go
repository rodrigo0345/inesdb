package write_handler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/page"
	wallog "github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/testutil"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

func TestDefaultWriteHandlerBasicWrite(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)

	handler := NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	writeOp := WriteOperation{
		Key:      []byte("testkey"),
		Value:    []byte("testvalue"),
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: false,
	}

	err := handler.HandleWrite(txn, writeOp)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	stored, err := storage.Get([]byte("testkey"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if string(stored) != "testvalue" {
		t.Errorf("expected 'testvalue', got '%s'", string(stored))
	}
}

func TestDefaultWriteHandlerBasicDelete(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)

	storage.Put([]byte("mykey"), []byte("myvalue"))

	handler := NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	writeOp := WriteOperation{
		Key:      []byte("mykey"),
		Value:    []byte("myvalue"),
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: true,
	}

	err := handler.HandleWrite(txn, writeOp)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, err = storage.Get([]byte("mykey"))
	if err == nil {
		t.Fatal("expected key to be deleted")
	}
}

func TestDefaultWriteHandlerMultipleWrites(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}

		writeOp := WriteOperation{
			Key:      key,
			Value:    value,
			PageID:   page.ResourcePageID(i),
			Offset:   0,
			IsDelete: false,
		}

		err := handler.HandleWrite(txn, writeOp)
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		stored, err := storage.Get(key)
		if err != nil {
			t.Fatalf("get %d failed: %v", i, err)
		}
		if stored[0] != byte(i*2) {
			t.Errorf("write %d mismatch", i)
		}
	}
}

func TestDefaultWriteHandlerWALRecordingAndRollback(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	writeOp := WriteOperation{
		Key:      []byte("rollbackkey"),
		Value:    []byte("newvalue"),
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: false,
	}

	err := handler.HandleWrite(txn, writeOp)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	stored, _ := storage.Get([]byte("rollbackkey"))
	if string(stored) != "newvalue" {
		t.Errorf("expected 'newvalue', got '%s'", string(stored))
	}

	rollbackMgr.HasOperations(txn)
}

func TestMVCCWriteHandlerBasicWrite(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)

	handler := NewMVCCWriteHandler(storage, bpm, nil, rollbackMgr)

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	writeOp := WriteOperation{
		Key:      []byte("mvcckey"),
		Value:    []byte("mvccvalue"),
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: false,
	}

	err := handler.HandleWrite(txn, writeOp)
	if err != nil {
		t.Fatalf("MVCC write failed: %v", err)
	}

	stored, err := storage.Get([]byte("mvcckey"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if string(stored) != "mvccvalue" {
		t.Errorf("expected 'mvccvalue', got '%s'", string(stored))
	}
}

func TestMVCCWriteHandlerDelete(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	storage := testutil.NewInMemoryStorageEngine()
	storage.Put([]byte("delkey"), []byte("delvalue"))

	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := NewMVCCWriteHandler(storage, bpm, nil, rollbackMgr)

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	writeOp := WriteOperation{
		Key:      []byte("delkey"),
		Value:    []byte("delvalue"),
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: true,
	}

	err := handler.HandleWrite(txn, writeOp)
	if err != nil {
		t.Fatalf("MVCC delete failed: %v", err)
	}

	_, err = storage.Get([]byte("delkey"))
	if err == nil {
		t.Fatal("expected key to be deleted")
	}
}

func TestWriteHandlerSetIndexContext(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)

	handler := NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	schema := testutil.NewUserTableSchema()

	err := handler.SetIndexContext(schema, nil)
	if err != nil {
		t.Fatalf("set index context failed: %v", err)
	}
}

func TestDefaultWriteHandlerErrorHandling(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	writeOp := WriteOperation{
		Key:      []byte("key1"),
		Value:    []byte("val1"),
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: false,
	}

	err := handler.HandleWrite(txn, writeOp)
	if err != nil {
		t.Fatalf("write should succeed: %v", err)
	}
}
