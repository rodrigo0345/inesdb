package rollback

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

func newTestBPM(t *testing.T, tmpDir string) buffer.IBufferPoolManager {
	t.Helper()
	dm, err := buffer.NewDiskManager(filepath.Join(tmpDir, "test.db"))
	if err != nil {
		t.Fatalf("NewDiskManager: %v", err)
	}
	t.Cleanup(func() { _ = dm.Close() })
	bpm := buffer.NewBufferPoolManager(64, dm)
	t.Cleanup(func() { _ = bpm.Close() })
	return bpm
}

func TestNewRollbackManager(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	if rm == nil {
		t.Fatal("expected non-nil rollback manager")
	}
}

func TestRecordPageWrite(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	opID, err := rm.RecordPageWrite(txn, page.ResourcePageID(0), 0, []byte{1, 2, 3})
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if opID == 0 {
		t.Error("expected non-zero operation ID")
	}
}

func TestRecordPageWriteMultiple(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	opID1, _ := rm.RecordPageWrite(txn, 0, 0, []byte{1})
	opID2, _ := rm.RecordPageWrite(txn, 1, 0, []byte{2})
	opID3, _ := rm.RecordPageWrite(txn, 2, 0, []byte{3})
	if opID1 >= opID2 || opID2 >= opID3 {
		t.Error("operation IDs should be unique and increasing")
	}
}

func TestRollbackTransactionNil(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	if err := rm.RollbackTransaction(nil, nil, nil); err == nil {
		t.Error("expected error when rolling back nil transaction")
	}
}

func TestRollbackTransactionCommitted(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	txn.Commit()
	if err := rm.RollbackTransaction(txn, nil, nil); err == nil {
		t.Error("expected error when rolling back committed transaction")
	}
}

func TestHasOperations(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	if rm.HasOperations(txn) {
		t.Error("expected no operations for new transaction")
	}
	rm.RecordPageWrite(txn, 0, 0, []byte{1})
	if !rm.HasOperations(txn) {
		t.Error("expected operations after recording write")
	}
}

func TestRollbackToSavePointNil(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	if err := rm.RollbackToSavePoint(nil, 0); err == nil {
		t.Error("expected error for nil transaction")
	}
}

func TestRollbackToSavePointInvalid(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	if err := rm.RollbackToSavePoint(txn, -1); err == nil {
		t.Error("expected error for negative savepoint")
	}
}

func TestGetOperationCount(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	rm.RecordPageWrite(txn, 0, 0, []byte{1})
	rm.RecordPageWrite(txn, 1, 0, []byte{2})
	if !rm.HasOperations(txn) {
		t.Error("expected operations to be recorded")
	}
}

func TestRollbackManagerTransactionID(t *testing.T) {
	txn := txn_unit.NewTransaction(42, txn_unit.SERIALIZABLE)
	if txn.GetID() != 42 {
		t.Errorf("expected transaction ID 42, got %d", txn.GetID())
	}
}

func TestMultipleTransactionsRecording(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)
	rm := NewRollbackManager(newTestBPM(t, tmpDir))
	txn1 := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	txn2 := txn_unit.NewTransaction(2, txn_unit.READ_COMMITTED)
	_, err1 := rm.RecordPageWrite(txn1, 0, 0, []byte{1})
	_, err2 := rm.RecordPageWrite(txn2, 1, 0, []byte{2})
	if err1 != nil || err2 != nil {
		t.Error("expected both records to succeed")
	}
	if !rm.HasOperations(txn1) || !rm.HasOperations(txn2) {
		t.Error("expected operations for both transactions")
	}
}
