package txn

import (
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/txn/undo"
)

func TestNewTransaction(t *testing.T) {
	tests := []struct {
		name           string
		txnID          uint64
		isolationLevel uint8
	}{
		{"read uncommitted", 1, READ_UNCOMMITTED},
		{"read committed", 2, READ_COMMITTED},
		{"repeatable read", 3, REPEATABLE_READ},
		{"serializable", 4, SERIALIZABLE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn := NewTransaction(tt.txnID, tt.isolationLevel)

			if txn == nil {
				t.Fatal("expected transaction, got nil")
			}

			if txn.GetID() != tt.txnID {
				t.Errorf("expected txn ID %d, got %d", tt.txnID, txn.GetID())
			}

			if txn.GetState() != ACTIVE {
				t.Errorf("expected ACTIVE state, got %v", txn.GetState())
			}

			if txn.GetIsolationLevel() != tt.isolationLevel {
				t.Errorf("expected isolation level %d, got %d", tt.isolationLevel, txn.GetIsolationLevel())
			}
		})
	}
}

func TestTransactionState(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	if txn.GetState() != ACTIVE {
		t.Errorf("initial state should be ACTIVE, got %v", txn.GetState())
	}

	txn.Commit()
	if txn.GetState() != COMMITTED {
		t.Errorf("expected COMMITTED state, got %v", txn.GetState())
	}

	txn2 := NewTransaction(2, READ_UNCOMMITTED)
	txn2.Abort()
	if txn2.GetState() != ABORTED {
		t.Errorf("expected ABORTED state, got %v", txn2.GetState())
	}
}

func TestTransactionSharedLocks(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)
	key1 := []byte("key1")
	key2 := []byte("key2")

	txn.AddSharedLock(key1)
	txn.AddSharedLock(key2)

	txn.RemoveSharedLock(key1)

	txn.RemoveSharedLock(key2)
}

func TestTransactionExclusiveLocks(t *testing.T) {
	txn := NewTransaction(1, SERIALIZABLE)
	key1 := []byte("key1")
	key2 := []byte("key2")

	txn.AddExclusiveLock(key1)
	txn.AddExclusiveLock(key2)

	txn.RemoveExclusiveLock(key1)

	txn.RemoveExclusiveLock(key2)
}

func TestRemoveSharedLockNonExistent(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)
	key := []byte("nonexistent")

	txn.RemoveSharedLock(key)
}

func TestRemoveExclusiveLockNonExistent(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)
	key := []byte("nonexistent")

	txn.RemoveExclusiveLock(key)
}

func TestTransactionMixedLocks(t *testing.T) {
	txn := NewTransaction(1, SERIALIZABLE)
	keyA := []byte("keyA")
	keyB := []byte("keyB")

	txn.AddSharedLock(keyA)
	txn.AddExclusiveLock(keyB)

	txn.RemoveSharedLock(keyA)
	txn.RemoveExclusiveLock(keyB)
}

func TestTransactionGetUndoLog(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)
	undoLog := txn.GetUndoLog()

	if undoLog == nil {
		t.Fatal("expected non-nil undo log")
	}
}

func TestTransactionGetIsolationLevel(t *testing.T) {
	tests := []struct {
		level uint8
	}{
		{READ_UNCOMMITTED},
		{READ_COMMITTED},
		{REPEATABLE_READ},
		{SERIALIZABLE},
	}

	for _, tt := range tests {
		txn := NewTransaction(1, tt.level)
		if txn.GetIsolationLevel() != tt.level {
			t.Errorf("expected isolation level %d, got %d", tt.level, txn.GetIsolationLevel())
		}
	}
}

func TestBytesEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        []byte
		b        []byte
		expected bool
	}{
		{"equal empty", []byte{}, []byte{}, true},
		{"equal single", []byte{1}, []byte{1}, true},
		{"equal multiple", []byte{1, 2, 3}, []byte{1, 2, 3}, true},
		{"not equal different values", []byte{1}, []byte{2}, false},
		{"not equal different length", []byte{1, 2}, []byte{1, 2, 3}, false},
		{"not equal empty vs non-empty", []byte{}, []byte{1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bytesEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("bytesEqual(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestTransactionDuplicateLocks(t *testing.T) {
	txn := NewTransaction(1, SERIALIZABLE)
	key := []byte("key")

	txn.AddSharedLock(key)
	txn.AddSharedLock(key)

	txn.RemoveSharedLock(key)

	txn.AddExclusiveLock(key)
	txn.AddExclusiveLock(key)

	txn.RemoveExclusiveLock(key)
}

func TestTransactionSavePoint(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	point := txn.SavePoint()
	if point < 0 {
		t.Errorf("expected non-negative savepoint, got %d", point)
	}
}

func TestTransactionGetID(t *testing.T) {
	tests := []uint64{1, 100, 999999, 18446744073709551615}

	for _, id := range tests {
		txn := NewTransaction(id, READ_COMMITTED)
		if txn.GetID() != id {
			t.Errorf("expected txn ID %d, got %d", id, txn.GetID())
		}
	}
}

func TestTransactionMultipleStateTransitions(t *testing.T) {
	txn := NewTransaction(1, SERIALIZABLE)

	txn.Abort()
	if txn.GetState() != ABORTED {
		t.Errorf("expected ABORTED, got %v", txn.GetState())
	}

	if txn.GetID() != 1 {
		t.Error("txn ID should still be accessible after abort")
	}
}

func TestTransactionRecordOperation(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	op := undo.NewPageWriteOp(1, 0, 0, []byte{1, 2, 3})

	err := txn.RecordOperation(op)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestTransactionCleanupCallbacksExecution(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	callOrder := []int{}
	txn.RegisterCleanupCallback(func() error {
		callOrder = append(callOrder, 1)
		return nil
	})
	txn.RegisterCleanupCallback(func() error {
		callOrder = append(callOrder, 2)
		return nil
	})

	err := txn.ExecuteCleanupCallbacks()
	if err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	if len(callOrder) != 2 {
		t.Errorf("expected 2 callbacks, got %d", len(callOrder))
	}
	if callOrder[0] != 1 || callOrder[1] != 2 {
		t.Errorf("expected callbacks in order [1,2], got %v", callOrder)
	}
}

func TestTransactionCleanupCallbackError(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	txn.RegisterCleanupCallback(func() error {
		return nil
	})
	txn.RegisterCleanupCallback(func() error {
		return nil
	})

	err := txn.ExecuteCleanupCallbacks()
	if err != nil {
		t.Fatalf("cleanup should succeed: %v", err)
	}
}

func TestTransactionConcurrentLockOperations(t *testing.T) {
	txn := NewTransaction(1, SERIALIZABLE)
	done := make(chan bool, 10)

	for i := 0; i < 5; i++ {
		go func(idx int) {
			key := []byte{byte(idx)}
			txn.AddSharedLock(key)
			txn.RemoveSharedLock(key)
			done <- true
		}(i)
	}

	for i := 0; i < 5; i++ {
		go func(idx int) {
			key := []byte{byte(idx + 5)}
			txn.AddExclusiveLock(key)
			txn.RemoveExclusiveLock(key)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

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
