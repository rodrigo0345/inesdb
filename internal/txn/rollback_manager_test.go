package txn

import (
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/page"
)

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

func (m *mockBufferPoolManager) FlushAll() error {
	return nil
}

func (m *mockBufferPoolManager) Close() error {
	return nil
}

func TestNewRollbackManager(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)

	if rm == nil {
		t.Fatal("expected non-nil rollback manager")
	}
}

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

func TestRecordPageWriteMultiple(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

	opID1, _ := rm.RecordPageWrite(txn, 0, 0, []byte{1})
	opID2, _ := rm.RecordPageWrite(txn, 1, 0, []byte{2})
	opID3, _ := rm.RecordPageWrite(txn, 2, 0, []byte{3})

	if opID1 >= opID2 || opID2 >= opID3 {
		t.Error("operation IDs should be unique and increasing")
	}
}

func TestRollbackTransactionNil(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)

	err := rm.RollbackTransaction(nil, nil, nil)
	if err == nil {
		t.Error("expected error when rolling back nil transaction")
	}
}

func TestRollbackTransactionCommitted(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

	txn.Commit()

	err := rm.RollbackTransaction(txn, nil, nil)
	if err == nil {
		t.Error("expected error when rolling back committed transaction")
	}
}

func TestHasOperations(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

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
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)

	err := rm.RollbackToSavePoint(nil, 0)

	if err == nil {
		t.Error("expected error for nil transaction")
	}
}

func TestRollbackToSavePointInvalid(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

	err := rm.RollbackToSavePoint(txn, -1)

	if err == nil {
		t.Error("expected error for negative savepoint")
	}
}

func TestGetOperationCount(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)
	txn := NewTransaction(1, READ_COMMITTED)

	rm.RecordPageWrite(txn, 0, 0, []byte{1})
	rm.RecordPageWrite(txn, 1, 0, []byte{2})

	if !rm.HasOperations(txn) {
		t.Error("expected operations to be recorded")
	}
}

func TestRollbackManagerTransactionID(t *testing.T) {
	txn := NewTransaction(42, SERIALIZABLE)

	if txn.GetID() != 42 {
		t.Errorf("expected transaction ID 42, got %d", txn.GetID())
	}
}

func TestMultipleTransactionsRecording(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	rm := NewRollbackManager(mockBufMgr)

	txn1 := NewTransaction(1, READ_COMMITTED)
	txn2 := NewTransaction(2, READ_COMMITTED)

	_, err1 := rm.RecordPageWrite(txn1, 0, 0, []byte{1})
	_, err2 := rm.RecordPageWrite(txn2, 1, 0, []byte{2})

	if err1 != nil || err2 != nil {
		t.Error("expected both records to succeed")
	}

	if !rm.HasOperations(txn1) || !rm.HasOperations(txn2) {
		t.Error("expected operations to be recorded for both transactions")
	}
}
