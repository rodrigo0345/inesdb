package txn

import (
	"os"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/txn/testutil"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

func TestNewRollbackManager(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)

	if rm == nil {
		t.Fatal("expected non-nil rollback manager")
	}
}

func TestRecordPageWrite(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	pageID := page.ResourcePageID(0)
	beforeData := []byte{1, 2, 3, 4, 5}

	opID, err := rm.RecordPageWrite(txn, pageID, 0, beforeData)

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

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)
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

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)

	err := rm.RollbackTransaction(nil, nil, nil)
	if err == nil {
		t.Error("expected error when rolling back nil transaction")
	}
}

func TestRollbackTransactionCommitted(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	txn.Commit()

	err := rm.RollbackTransaction(txn, nil, nil)
	if err == nil {
		t.Error("expected error when rolling back committed transaction")
	}
}

func TestHasOperations(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	hasOps := rm.HasOperations(txn)
	if hasOps {
		t.Error("expected no operations for new transaction")
	}

	rm.RecordPageWrite(txn, 0, 0, []byte{1})

	hasOps = rm.HasOperations(txn)
	if !hasOps {
		t.Error("expected operations after recording write")
	}
}

func TestRollbackToSavePointNil(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)

	err := rm.RollbackToSavePoint(nil, 0)

	if err == nil {
		t.Error("expected error for nil transaction")
	}
}

func TestRollbackToSavePointInvalid(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)
	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	err := rm.RollbackToSavePoint(txn, -1)

	if err == nil {
		t.Error("expected error for negative savepoint")
	}
}

func TestGetOperationCount(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)
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

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	rm := NewRollbackManager(bpm)

	txn1 := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)
	txn2 := txn_unit.NewTransaction(2, txn_unit.READ_COMMITTED)

	_, err1 := rm.RecordPageWrite(txn1, 0, 0, []byte{1})
	_, err2 := rm.RecordPageWrite(txn2, 1, 0, []byte{2})

	if err1 != nil || err2 != nil {
		t.Error("expected both records to succeed")
	}

	if !rm.HasOperations(txn1) || !rm.HasOperations(txn2) {
		t.Error("expected operations to be recorded for both transactions")
	}
}
