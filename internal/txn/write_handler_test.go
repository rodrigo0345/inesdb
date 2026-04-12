package txn

import (
	"testing"
)

type mockWriteHandler struct{}

func (m *mockWriteHandler) Write(txn *Transaction, key []byte, value []byte) error {
	return nil
}

func (m *mockWriteHandler) Delete(txn *Transaction, key []byte) error {
	return nil
}

func TestWriteHandlerInterface(t *testing.T) {
	handler := &mockWriteHandler{}

	key := []byte("test_key")
	value := []byte("test_value")
	txn := NewTransaction(1, READ_COMMITTED)

	err := handler.Write(txn, key, value)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	err = handler.Delete(txn, key)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestDefaultWriteHandlerCreation(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	mockRollbackMgr := NewRollbackManager(mockBufMgr)
	mockLogMgr := &mockLogManager{}
	mockStorage := &mockStorageEngine{}

	handler := NewDefaultWriteHandler(mockStorage, mockRollbackMgr, mockBufMgr, mockLogMgr)

	if handler == nil {
		t.Fatal("expected non-nil default write handler")
	}
}

func TestMVCCWriteHandlerCreation(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	mockStorage := &mockStorageEngine{}
	mockRollbackMgr := NewRollbackManager(mockBufMgr)

	handler := NewMVCCWriteHandler(mockStorage, mockBufMgr, nil, mockRollbackMgr)

	if handler == nil {
		t.Fatal("expected non-nil MVCC write handler")
	}
}

func TestWriteHandlerWithDifferentIsolationLevels(t *testing.T) {
	isolationLevels := []uint8{
		READ_UNCOMMITTED,
		READ_COMMITTED,
		REPEATABLE_READ,
		SERIALIZABLE,
	}

	mockBufMgr := &mockBufferPoolManager{}
	mockRollbackMgr := NewRollbackManager(mockBufMgr)
	mockLogMgr := &mockLogManager{}
	mockStorage := &mockStorageEngine{}

	for _, level := range isolationLevels {
		t.Run("isolation level", func(t *testing.T) {
			txn := NewTransaction(1, level)
			handler := NewDefaultWriteHandler(mockStorage, mockRollbackMgr, mockBufMgr, mockLogMgr)

			if txn.GetIsolationLevel() != level {
				t.Errorf("expected isolation level %d, got %d", level, txn.GetIsolationLevel())
			}

			if handler == nil {
				t.Error("expected non-nil handler")
			}
		})
	}
}

func TestWriteHandlerMultiple(t *testing.T) {
	mockBufMgr := &mockBufferPoolManager{}
	mockRollbackMgr := NewRollbackManager(mockBufMgr)
	mockLogMgr := &mockLogManager{}
	mockStorage := &mockStorageEngine{}

	handler1 := NewDefaultWriteHandler(mockStorage, mockRollbackMgr, mockBufMgr, mockLogMgr)
	handler2 := NewDefaultWriteHandler(mockStorage, mockRollbackMgr, mockBufMgr, mockLogMgr)

	if handler1 == handler2 {
		t.Error("multiple handlers should be different instances")
	}
}

func TestWriteHandlerBasicOperation(t *testing.T) {
	handler := &mockWriteHandler{}
	txn := NewTransaction(1, READ_COMMITTED)
	key := []byte("key1")
	value := []byte("value1")

	err := handler.Write(txn, key, value)
	if err != nil {
		t.Errorf("expected nil error on write, got %v", err)
	}

	err = handler.Delete(txn, key)
	if err != nil {
		t.Errorf("expected nil error on delete, got %v", err)
	}
}

func TestWriteHandlerWithNilKey(t *testing.T) {
	handler := &mockWriteHandler{}
	txn := NewTransaction(1, READ_COMMITTED)

	err := handler.Write(txn, nil, []byte("value"))
	if err != nil {
	}
}

func TestWriteHandlerWithEmptyValue(t *testing.T) {
	handler := &mockWriteHandler{}
	txn := NewTransaction(1, READ_COMMITTED)
	key := []byte("key")

	err := handler.Write(txn, key, []byte{})
	if err != nil {
	}
}

func TestWriteHandlerMultipleWrites(t *testing.T) {
	handler := &mockWriteHandler{}
	txn := NewTransaction(1, READ_COMMITTED)

	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		err := handler.Write(txn, key, value)
		if err != nil {
			t.Errorf("expected nil error on write %d, got %v", i, err)
		}
	}
}

func TestWriteHandlerInterleavedOperations(t *testing.T) {
	handler := &mockWriteHandler{}
	txn := NewTransaction(1, SERIALIZABLE)

	err := handler.Write(txn, []byte("key1"), []byte("val1"))
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	err = handler.Delete(txn, []byte("key1"))
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	err = handler.Write(txn, []byte("key1"), []byte("val2"))
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}
