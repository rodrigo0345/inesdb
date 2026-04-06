package transaction_manager

import (
	"testing"
)

// TestNewTransaction tests transaction creation
func TestNewTransaction_Begin(t *testing.T) {
	txnMgr := NewTransactionManager(nil)

	txn := txnMgr.Begin()

	if txn == nil {
		t.Fatal("expected transaction to be non-nil")
	}
	if txn.GetID() == 0 {
		t.Fatal("expected non-zero transaction ID")
	}
	if txn.state != ACTIVE {
		t.Fatalf("expected state ACTIVE, got %v", txn.state)
	}
}

// TestTransaction_GetID tests transaction ID getter
func TestTransaction_GetID(t *testing.T) {
	txnMgr := NewTransactionManager(nil)

	txn1 := txnMgr.Begin()
	txn2 := txnMgr.Begin()

	if txn1.GetID() == txn2.GetID() {
		t.Fatal("expected different IDs for different transactions")
	}
	if txn2.GetID() != txn1.GetID()+1 {
		t.Fatalf("expected sequential IDs, got %d and %d", txn1.GetID(), txn2.GetID())
	}
}

// TestTransaction_AddUndo tests adding undo entries
func TestTransaction_AddUndo(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	entry1 := UndoEntry([]byte("undo1"))
	entry2 := UndoEntry([]byte("undo2"))

	txn.AddUndo(entry1)
	txn.AddUndo(entry2)

	if len(txn.undoLog) != 2 {
		t.Fatalf("expected 2 undo entries, got %d", len(txn.undoLog))
	}
	if string(txn.undoLog[0]) != "undo1" {
		t.Fatalf("expected first undo entry 'undo1', got '%s'", string(txn.undoLog[0]))
	}
	if string(txn.undoLog[1]) != "undo2" {
		t.Fatalf("expected second undo entry 'undo2', got '%s'", string(txn.undoLog[1]))
	}
}

// TestTransaction_StateTransitions tests state transitions
func TestTransaction_StateTransitions(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	if txn.state != ACTIVE {
		t.Fatalf("initial state should be ACTIVE, got %v", txn.state)
	}

	// Commit
	txnMgr.Commit(txn)
	if txn.state != COMMITTED {
		t.Fatalf("state should be COMMITTED after commit, got %v", txn.state)
	}
}

// TestCommit_Success tests successful transaction commit
func TestCommit_Success(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	err := txnMgr.Commit(txn)
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	if txn.state != COMMITTED {
		t.Fatalf("expected state COMMITTED, got %v", txn.state)
	}
}

// TestAbort_Success tests successful transaction abort
func TestAbort_Success(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	// Add some undo entries
	txn.AddUndo(UndoEntry([]byte("undo1")))
	txn.AddUndo(UndoEntry([]byte("undo2")))

	err := txnMgr.Abort(txn)
	if err != nil {
		t.Fatalf("failed to abort: %v", err)
	}

	if txn.state != ABORTED {
		t.Fatalf("expected state ABORTED, got %v", txn.state)
	}
}

// TestSetBufferPool tests setting buffer pool manager
func TestSetBufferPool(t *testing.T) {
	txnMgr := NewTransactionManager(nil)

	mockBPM := struct{ FlushAll func() error }{}
	txnMgr.SetBufferPool(mockBPM)

	if txnMgr.bufferPool == nil {
		t.Fatal("expected bufferPool to be set")
	}
}

// TestMultipleTransactions tests multiple concurrent transactions
func TestMultipleTransactions(t *testing.T) {
	txnMgr := NewTransactionManager(nil)

	// Create multiple transactions
	txns := make([]*Transaction, 5)
	for i := 0; i < 5; i++ {
		txns[i] = txnMgr.Begin()
	}

	// Verify all have different IDs and are ACTIVE
	seenIDs := make(map[uint64]bool)
	for i, txn := range txns {
		if txn.state != ACTIVE {
			t.Fatalf("txn %d: expected state ACTIVE, got %v", i, txn.state)
		}
		if seenIDs[txn.GetID()] {
			t.Fatalf("txn %d: duplicate ID %d", i, txn.GetID())
		}
		seenIDs[txn.GetID()] = true
	}

	// Commit some, abort others
	txnMgr.Commit(txns[0])
	txnMgr.Commit(txns[1])
	txnMgr.Abort(txns[2])
	txnMgr.Commit(txns[3])
	txnMgr.Abort(txns[4])

	if txns[0].state != COMMITTED {
		t.Fatal("txns[0] should be committed")
	}
	if txns[2].state != ABORTED {
		t.Fatal("txns[2] should be aborted")
	}
}

// TestAbort_WithUndoLog tests abort properly handles undo log
func TestAbort_WithUndoLog(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	// Add undo entries
	undoEntries := []UndoEntry{
		UndoEntry([]byte("change1")),
		UndoEntry([]byte("change2")),
		UndoEntry([]byte("change3")),
	}
	for _, entry := range undoEntries {
		txn.AddUndo(entry)
	}

	if len(txn.undoLog) != 3 {
		t.Fatalf("expected 3 undo entries, got %d", len(txn.undoLog))
	}

	err := txnMgr.Abort(txn)
	if err != nil {
		t.Fatalf("failed to abort: %v", err)
	}

	if txn.state != ABORTED {
		t.Fatalf("expected ABORTED, got %v", txn.state)
	}

	// Undo log should still exist (used for rollback)
	if len(txn.undoLog) != 3 {
		t.Fatalf("expected undo log to be preserved, got %d entries", len(txn.undoLog))
	}
}

// TestLockManager_Initialization tests lock manager creation
func TestLockManager_Initialization(t *testing.T) {
	lockMgr := NewLockManager()

	if lockMgr == nil {
		t.Fatal("expected lock manager to be non-nil")
	}
}

// TestLockManager_AcquireSharedLock tests acquiring shared lock
func TestLockManager_AcquireSharedLock(t *testing.T) {
	lockMgr := NewLockManager()
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	key := []byte("resource1")
	err := lockMgr.LockShared(txn, key)
	if err != nil {
		t.Fatalf("failed to acquire shared lock: %v", err)
	}

	txnMgr.Commit(txn)
}

// TestLockManager_AcquireExclusiveLock tests acquiring exclusive lock
func TestLockManager_AcquireExclusiveLock(t *testing.T) {
	lockMgr := NewLockManager()
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	key := []byte("resource2")
	err := lockMgr.LockExclusive(txn, key)
	if err != nil {
		t.Fatalf("failed to acquire exclusive lock: %v", err)
	}

	txnMgr.Commit(txn)
}

// TestLockManager_ReleaseLock tests releasing lock
func TestLockManager_ReleaseLock(t *testing.T) {
	lockMgr := NewLockManager()
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	key := []byte("resource3")
	lockMgr.LockExclusive(txn, key)

	err := lockMgr.Unlock(txn, key)
	if err != nil {
		t.Fatalf("failed to release lock: %v", err)
	}

	txnMgr.Commit(txn)
}

// TestTransactionManager_SequentialIDs tests that transaction IDs are sequential
func TestTransactionManager_SequentialIDs(t *testing.T) {
	txnMgr := NewTransactionManager(nil)

	ids := make([]uint64, 10)
	for i := 0; i < 10; i++ {
		txn := txnMgr.Begin()
		ids[i] = txn.GetID()

		if i > 0 && ids[i] != ids[i-1]+1 {
			t.Fatalf("expected sequential IDs, got %d after %d", ids[i], ids[i-1])
		}

		txnMgr.Commit(txn)
	}
}

// TestTransaction_TxnState_Constant tests transaction state constants
func TestTransaction_TxnState_Constants(t *testing.T) {
	if ACTIVE != 0 {
		t.Fatalf("expected ACTIVE to be 0, got %d", ACTIVE)
	}
	if COMMITTED != 1 {
		t.Fatalf("expected COMMITTED to be 1, got %d", COMMITTED)
	}
	if ABORTED != 2 {
		t.Fatalf("expected ABORTED to be 2, got %d", ABORTED)
	}
}

// TestTransaction_EmptyUndoLog tests transaction with empty undo log
func TestTransaction_EmptyUndoLog(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	// Don't add any undo entries
	if len(txn.undoLog) != 0 {
		t.Fatalf("expected empty undo log, got %d entries", len(txn.undoLog))
	}

	// Should still be able to commit
	err := txnMgr.Commit(txn)
	if err != nil {
		t.Fatalf("failed to commit transaction with empty undo log: %v", err)
	}
}

// TestLockManager_MultipleTransactions tests locks with multiple transactions
func TestLockManager_MultipleTransactions(t *testing.T) {
	lockMgr := NewLockManager()
	txnMgr := NewTransactionManager(nil)

	txn1 := txnMgr.Begin()
	txn2 := txnMgr.Begin()

	key := []byte("shared_resource")

	// Both transactions can acquire shared locks
	err1 := lockMgr.LockShared(txn1, key)
	err2 := lockMgr.LockShared(txn2, key)

	if err1 != nil || err2 != nil {
		t.Fatal("both transactions should be able to acquire shared lock")
	}

	lockMgr.Unlock(txn1, key)
	lockMgr.Unlock(txn2, key)

	txnMgr.Commit(txn1)
	txnMgr.Commit(txn2)
}
