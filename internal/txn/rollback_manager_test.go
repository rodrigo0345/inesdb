package txn

import (
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/page"
)

// mockBufferPoolManager implements IBufferPoolManager for testing
type mockBufferPoolManager struct{}

func (m *mockBufferPoolManager) NewPage() (*page.IResourcePage, error) {
	return nil, nil
}

func (m *mockBufferPoolManager) PinPage(pageID page.ResourcePageID) (page.IResourcePage, error) {
	return nil, nil
}

func (m *mockBufferPoolManager) UnpinPage(pageID page.ResourcePageID, isDirty bool) error {
	return nil
}

// TestNewRollbackManager tests rollback manager creation
func TestNewRollbackManager(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)

	if rm == nil {
		t.Fatal("expected non-nil rollback manager")
	}
}

// TestRecordPageWrite tests recording page write operations
func TestRecordPageWrite(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

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

// TestRecordPageWriteMultiple tests recording multiple page writes
func TestRecordPageWriteMultiple(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

	// Record multiple writes
	opID1, _ := rm.RecordPageWrite(txn, 0, 0, []byte{1})
	opID2, _ := rm.RecordPageWrite(txn, 1, 0, []byte{2})
	opID3, _ := rm.RecordPageWrite(txn, 2, 0, []byte{3})

	// IDs should be unique and increasing
	if opID1 >= opID2 || opID2 >= opID3 {
		t.Error("operation IDs should be unique and increasing")
	}
}

// TestRollbackTransactionNil tests rollback with nil transaction
func TestRollbackTransactionNil(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)

	// Should error with nil transaction - test the error condition only
	err := rm.RollbackTransaction(nil, nil, nil)
	if err == nil {
		t.Error("expected error when rolling back nil transaction")
	}
}

// TestRollbackTransactionCommitted tests rolling back committed transaction
func TestRollbackTransactionCommitted(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

	// Mark as committed
	txn.Commit()

	// Should error when trying to rollback committed transaction
	err := rm.RollbackTransaction(txn, nil, nil)
	if err == nil {
		t.Error("expected error when rolling back committed transaction")
	}
}

// TestHasOperations tests checking for recorded operations
func TestHasOperations(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

	// Transaction with no operations
	hasOps := rm.HasOperations(txn)
	if hasOps {
		t.Error("expected no operations for new transaction")
	}

	// Add an operation
	rm.RecordPageWrite(txn, 0, 0, []byte{1})

	hasOps = rm.HasOperations(txn)
	if !hasOps {
		t.Error("expected operations after recording write")
	}
}

// TestRollbackToSavePointNil tests rollback to savepoint with nil transaction
func TestRollbackToSavePointNil(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)

	err := rm.RollbackToSavePoint(nil, 0)

	if err == nil {
		t.Error("expected error for nil transaction")
	}
}

// TestRollbackToSavePointInvalid tests rollback with invalid savepoint
func TestRollbackToSavePointInvalid(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

	err := rm.RollbackToSavePoint(txn, -1)

	if err == nil {
		t.Error("expected error for negative savepoint")
	}
}

// TestGetOperationCount tests getting operation count
func TestGetOperationCount(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

	// Add some operations indirectly through Record
	rm.RecordPageWrite(txn, 0, 0, []byte{1})
	rm.RecordPageWrite(txn, 1, 0, []byte{2})

	// Check using HasOperations
	if !rm.HasOperations(txn) {
		t.Error("expected operations to be recorded")
	}
}

// TestRollbackManagerTransactionID tests verifying transaction ID in rollback
func TestRollbackManagerTransactionID(t *testing.T) {
	txn := NewTransaction(42, SERIALIZABLE)

	if txn.GetID() != 42 {
		t.Errorf("expected transaction ID 42, got %d", txn.GetID())
	}
}

// TestMultipleTransactionsRecording tests recording operations in multiple transactions
func TestMultipleTransactionsRecording(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)

	// Create multiple transactions
	txn1 := NewTransaction(1, READ_COMMITTED)
	txn2 := NewTransaction(2, READ_COMMITTED)

	_, err1 := rm.RecordPageWrite(txn1, 0, 0, []byte{1})
	_, err2 := rm.RecordPageWrite(txn2, 1, 0, []byte{2})

	if err1 != nil || err2 != nil {
		t.Error("expected both records to succeed")
	}

	// Verify operations are recorded
	if !rm.HasOperations(txn1) || !rm.HasOperations(txn2) {
		t.Error("expected operations to be recorded for both transactions")
	}
}
