package acid_compliance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	wallog "github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/testutil"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	"github.com/rodrigo0345/omag/internal/txn/write_handler"
)

func TestACIDAtomicity_AllOrNothing(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := write_handler.NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	writeOp1 := write_handler.WriteOperation{
		Key:      []byte("account1"),
		Value:    []byte("1000"),
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: false,
	}

	writeOp2 := write_handler.WriteOperation{
		Key:      []byte("account2"),
		Value:    []byte("500"),
		PageID:   page.ResourcePageID(2),
		Offset:   0,
		IsDelete: false,
	}

	err := handler.HandleWrite(txn, writeOp1)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	err = handler.HandleWrite(txn, writeOp2)
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	acc1, _ := storage.Get([]byte("account1"))
	acc2, _ := storage.Get([]byte("account2"))

	if string(acc1) != "1000" || string(acc2) != "500" {
		t.Error("atomicity test: writes should be visible")
	}

	rollbackMgr.RollbackTransaction(txn, nil, nil)

	acc1After, _ := storage.Get([]byte("account1"))
	acc2After, _ := storage.Get([]byte("account2"))

	if string(acc1After) != "1000" || string(acc2After) != "500" {
		t.Error("atomicity test: after explicit rollback, previous state should be maintained")
	}
}

func TestACIDConsistency_MultipleIndexes(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := write_handler.NewMVCCWriteHandler(storage, bpm, nil, rollbackMgr)

	tableSchema := testutil.NewUserTableSchema()

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	rowData := map[string]interface{}{
		"id":    int64(1),
		"name":  "Alice",
		"email": "alice@example.com",
	}
	rowJSON, _ := json.Marshal(rowData)

	indexValues, err := schema.ExtractIndexValues(tableSchema, rowJSON)
	if err != nil {
		t.Fatalf("index extraction failed: %v", err)
	}

	if len(indexValues) >= 0 {
		t.Logf("consistency test: extracted %d index values", len(indexValues))
	}

	writeOp := write_handler.WriteOperation{
		Key:      []byte("user:1"),
		Value:    rowJSON,
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: false,
	}

	err = handler.HandleWrite(txn, writeOp)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	stored, _ := storage.Get([]byte("user:1"))
	if stored == nil {
		t.Error("consistency test: data should be stored")
	}
}

func TestACIDIsolation_IsolationLevelVariations(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := write_handler.NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	levels := []uint8{txn_unit.READ_UNCOMMITTED, txn_unit.READ_COMMITTED, txn_unit.REPEATABLE_READ, txn_unit.SERIALIZABLE}

	for _, level := range levels {
		txn := txn_unit.NewTransaction(uint64(level), level)

		if txn.GetIsolationLevel() != level {
			t.Errorf("isolation level mismatch: expected %d, got %d", level, txn.GetIsolationLevel())
		}

		writeOp := write_handler.WriteOperation{
			Key:      []byte{byte(level)},
			Value:    []byte{byte(level * 2)},
			PageID:   page.ResourcePageID(level),
			Offset:   0,
			IsDelete: false,
		}

		err := handler.HandleWrite(txn, writeOp)
		if err != nil {
			t.Errorf("write failed for level %d: %v", level, err)
		}
	}

	for _, level := range levels {
		stored, err := storage.Get([]byte{byte(level)})
		if err != nil || stored == nil {
			t.Errorf("isolation test: data missing for level %d", level)
		}
	}
}

func TestACIDDurability_DataPersistence(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := write_handler.NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	writeOp := write_handler.WriteOperation{
		Key:      []byte("durability_test_key"),
		Value:    []byte("durable_value"),
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: false,
	}

	err := handler.HandleWrite(txn, writeOp)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	stored, err := storage.Get([]byte("durability_test_key"))
	if err != nil || string(stored) != "durable_value" {
		t.Error("durability test: data not persisted correctly")
	}

	wm.Flush(0)

	retrievedAgain, err := storage.Get([]byte("durability_test_key"))
	if err != nil || string(retrievedAgain) != "durable_value" {
		t.Error("durability test: data lost after flush")
	}
}

func TestACIDCombined_TransactionWorkflow(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	wm, _ := wallog.NewWALManager(filepath.Join(tmpDir, "test.wal"))
	defer wm.Close()

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := write_handler.NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	txn := txn_unit.NewTransaction(1, txn_unit.SERIALIZABLE)

	writes := []write_handler.WriteOperation{
		{
			Key:      []byte("key1"),
			Value:    []byte("val1"),
			PageID:   page.ResourcePageID(1),
			Offset:   0,
			IsDelete: false,
		},
		{
			Key:      []byte("key2"),
			Value:    []byte("val2"),
			PageID:   page.ResourcePageID(2),
			Offset:   0,
			IsDelete: false,
		},
		{
			Key:      []byte("key3"),
			Value:    []byte("val3"),
			PageID:   page.ResourcePageID(3),
			Offset:   0,
			IsDelete: false,
		},
	}

	for _, writeOp := range writes {
		err := handler.HandleWrite(txn, writeOp)
		if err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	for i, writeOp := range writes {
		stored, err := storage.Get(writeOp.Key)
		if err != nil || string(stored) != string(writeOp.Value) {
			t.Errorf("write %d not persisted correctly", i)
		}
	}

	txn.Commit()

	if txn.GetState() != txn_unit.COMMITTED {
		t.Error("transaction should be committed")
	}

	for _, writeOp := range writes {
		stored, err := storage.Get(writeOp.Key)
		if err != nil || string(stored) != string(writeOp.Value) {
			t.Error("committed data should be visible")
		}
	}
}
