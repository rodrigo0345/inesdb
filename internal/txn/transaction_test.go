package txn

import (
	"testing"

	"github.com/rodrigo0345/omag/internal/txn/undo"
)

// TestNewTransaction tests transaction creation
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

// TestTransactionState tests transaction state transitions
func TestTransactionState(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	// Initially ACTIVE
	if txn.GetState() != ACTIVE {
		t.Errorf("initial state should be ACTIVE, got %v", txn.GetState())
	}

	// Commit
	txn.Commit()
	if txn.GetState() != COMMITTED {
		t.Errorf("expected COMMITTED state, got %v", txn.GetState())
	}

	// Test abort on a new transaction
	txn2 := NewTransaction(2, READ_UNCOMMITTED)
	txn2.Abort()
	if txn2.GetState() != ABORTED {
		t.Errorf("expected ABORTED state, got %v", txn2.GetState())
	}
}

// TestTransactionSharedLocks tests shared lock operations
func TestTransactionSharedLocks(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)
	key1 := []byte("key1")
	key2 := []byte("key2")

	// Add shared locks
	txn.AddSharedLock(key1)
	txn.AddSharedLock(key2)

	// Remove shared lock
	txn.RemoveSharedLock(key1)

	// Remove another
	txn.RemoveSharedLock(key2)
}

// TestTransactionExclusiveLocks tests exclusive lock operations
func TestTransactionExclusiveLocks(t *testing.T) {
	txn := NewTransaction(1, SERIALIZABLE)
	key1 := []byte("key1")
	key2 := []byte("key2")

	// Add exclusive locks
	txn.AddExclusiveLock(key1)
	txn.AddExclusiveLock(key2)

	// Remove exclusive lock
	txn.RemoveExclusiveLock(key1)

	// Remove another
	txn.RemoveExclusiveLock(key2)
}

// TestRemoveSharedLockNonExistent tests removing non-existent shared lock
func TestRemoveSharedLockNonExistent(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)
	key := []byte("nonexistent")

	// Should not panic when removing non-existent lock
	txn.RemoveSharedLock(key)
}

// TestRemoveExclusiveLockNonExistent tests removing non-existent exclusive lock
func TestRemoveExclusiveLockNonExistent(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)
	key := []byte("nonexistent")

	// Should not panic when removing non-existent lock
	txn.RemoveExclusiveLock(key)
}

// TestTransactionMixedLocks tests mixing shared and exclusive locks
func TestTransactionMixedLocks(t *testing.T) {
	txn := NewTransaction(1, SERIALIZABLE)
	keyA := []byte("keyA")
	keyB := []byte("keyB")

	// Add mixed locks
	txn.AddSharedLock(keyA)
	txn.AddExclusiveLock(keyB)

	// Remove them
	txn.RemoveSharedLock(keyA)
	txn.RemoveExclusiveLock(keyB)
}

// TestTransactionGetUndoLog tests getting undo log
func TestTransactionGetUndoLog(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)
	undoLog := txn.GetUndoLog()

	if undoLog == nil {
		t.Fatal("expected non-nil undo log")
	}
}

// TestTransactionGetIsolationLevel tests getting isolation level
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

// TestBytesEqual tests the bytesEqual helper function
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

// TestTransactionDuplicateLocks tests handling of duplicate locks
func TestTransactionDuplicateLocks(t *testing.T) {
	txn := NewTransaction(1, SERIALIZABLE)
	key := []byte("key")

	// Add same lock twice
	txn.AddSharedLock(key)
	txn.AddSharedLock(key) // Add duplicate

	// Remove should only remove first occurrence
	txn.RemoveSharedLock(key)

	// Add exclusive lock multiple times
	txn.AddExclusiveLock(key)
	txn.AddExclusiveLock(key)

	txn.RemoveExclusiveLock(key)
}

// TestTransactionSavePoint tests savepoint functionality
func TestTransactionSavePoint(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	// Get savepoint - should work
	point := txn.SavePoint()
	if point < 0 {
		t.Errorf("expected non-negative savepoint, got %d", point)
	}
}

// TestTransactionGetID tests getting transaction ID
func TestTransactionGetID(t *testing.T) {
	tests := []uint64{1, 100, 999999, 18446744073709551615} // Max uint64

	for _, id := range tests {
		txn := NewTransaction(id, READ_COMMITTED)
		if txn.GetID() != id {
			t.Errorf("expected txn ID %d, got %d", id, txn.GetID())
		}
	}
}

// TestTransactionMultipleStateTransitions tests multiple state changes
func TestTransactionMultipleStateTransitions(t *testing.T) {
	txn := NewTransaction(1, SERIALIZABLE)

	// ACTIVE -> ABORTED
	txn.Abort()
	if txn.GetState() != ABORTED {
		t.Errorf("expected ABORTED, got %v", txn.GetState())
	}

	// Can still get ID after abort
	if txn.GetID() != 1 {
		t.Error("txn ID should still be accessible after abort")
	}
}

// TestTransactionRecordOperation tests recording operations
func TestTransactionRecordOperation(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	// Create a page write operation
	op := undo.NewPageWriteOp(1, 0, 0, []byte{1, 2, 3})

	// Record it
	err := txn.RecordOperation(op)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}
