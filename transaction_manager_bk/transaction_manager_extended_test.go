package transaction_manager

import (
	"testing"
)

// MockFlushAll is a mock buffer pool with FlushAll capability
type MockFlushAll struct {
	flushCalled bool
}

func (m *MockFlushAll) FlushAll() error {
	m.flushCalled = true
	return nil
}

// TestCommit_WithBufferPool tests commit with buffer pool set
func TestCommit_WithBufferPool(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	mockBPM := &MockFlushAll{}
	txnMgr.SetBufferPool(mockBPM)

	txn := txnMgr.Begin()
	err := txnMgr.Commit(txn)

	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	if txn.state != COMMITTED {
		t.Fatalf("expected state COMMITTED, got %v", txn.state)
	}
	if !mockBPM.flushCalled {
		t.Fatal("expected FlushAll to be called on buffer pool")
	}
}

// TestCommit_WithUndoLog tests commit with undo log entries
func TestCommit_WithUndoLog(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	// Add undo entries
	txn.AddUndo(UndoEntry([]byte("change1")))
	txn.AddUndo(UndoEntry([]byte("change2")))

	if len(txn.undoLog) != 2 {
		t.Fatalf("expected 2 undo entries, got %d", len(txn.undoLog))
	}

	err := txnMgr.Commit(txn)
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	if txn.state != COMMITTED {
		t.Fatalf("expected state COMMITTED, got %v", txn.state)
	}

	// Undo log should be preserved even after commit
	if len(txn.undoLog) != 2 {
		t.Fatalf("expected undo log preserved, got %d entries", len(txn.undoLog))
	}
}

// TestAbort_WithMultipleUndoEntries tests abort with many undo entries
func TestAbort_WithMultipleUndoEntries(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	// Add many undo entries
	for i := 0; i < 10; i++ {
		txn.AddUndo(UndoEntry([]byte{byte(i)}))
	}

	err := txnMgr.Abort(txn)
	if err != nil {
		t.Fatalf("failed to abort: %v", err)
	}

	if txn.state != ABORTED {
		t.Fatalf("expected state ABORTED, got %v", txn.state)
	}

	// Undo log should be preserved (for rollback)
	if len(txn.undoLog) != 10 {
		t.Fatalf("expected 10 undo entries, got %d", len(txn.undoLog))
	}
}

// TestTransaction_StateNotModifiedBeforeCommit tests transaction state
func TestTransaction_StateNotModifiedBeforeCommit(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	if txn.state != ACTIVE {
		t.Fatalf("new transaction should be ACTIVE, got %v", txn.state)
	}

	// Add undo entries
	txn.AddUndo(UndoEntry([]byte("test")))

	// State should still be ACTIVE
	if txn.state != ACTIVE {
		t.Fatalf("state should remain ACTIVE, got %v", txn.state)
	}

	txnMgr.Commit(txn)
	if txn.state != COMMITTED {
		t.Fatalf("state should be COMMITTED after commit, got %v", txn.state)
	}
}

// TestMultipleAborts tests multiple abort operations
func TestMultipleAborts(t *testing.T) {
	txnMgr := NewTransactionManager(nil)

	// Create and abort multiple transactions
	for i := 0; i < 5; i++ {
		txn := txnMgr.Begin()
		txn.AddUndo(UndoEntry([]byte{byte(i)}))

		err := txnMgr.Abort(txn)
		if err != nil {
			t.Fatalf("iteration %d: failed to abort: %v", i, err)
		}

		if txn.state != ABORTED {
			t.Fatalf("iteration %d: expected state ABORTED, got %v", i, txn.state)
		}
	}
}

// TestMultipleCommits tests multiple commit operations
func TestMultipleCommits(t *testing.T) {
	txnMgr := NewTransactionManager(nil)

	// Create and commit multiple transactions
	for i := 0; i < 5; i++ {
		txn := txnMgr.Begin()
		txn.AddUndo(UndoEntry([]byte{byte(i)}))

		err := txnMgr.Commit(txn)
		if err != nil {
			t.Fatalf("iteration %d: failed to commit: %v", i, err)
		}

		if txn.state != COMMITTED {
			t.Fatalf("iteration %d: expected state COMMITTED, got %v", i, txn.state)
		}
	}
}

// TestAbort_EmptyUndoLog tests abort with no undo entries
func TestAbort_EmptyUndoLog(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	// Don't add any undo entries
	if len(txn.undoLog) != 0 {
		t.Fatalf("expected empty undo log, got %d entries", len(txn.undoLog))
	}

	err := txnMgr.Abort(txn)
	if err != nil {
		t.Fatalf("failed to abort with empty undo log: %v", err)
	}

	if txn.state != ABORTED {
		t.Fatalf("expected state ABORTED, got %v", txn.state)
	}
}

// TestTransaction_IDUniqueness tests that transaction IDs are unique
func TestTransaction_IDUniqueness(t *testing.T) {
	txnMgr := NewTransactionManager(nil)

	// Create many transactions and track IDs
	ids := make(map[uint64]bool)
	for i := 0; i < 20; i++ {
		txn := txnMgr.Begin()
		id := txn.GetID()

		if ids[id] {
			t.Fatalf("duplicate transaction ID: %d", id)
		}
		ids[id] = true

		// Can mix commits and aborts
		if i%2 == 0 {
			txnMgr.Commit(txn)
		} else {
			txnMgr.Abort(txn)
		}
	}

	// Verify all IDs were unique
	if len(ids) != 20 {
		t.Fatalf("expected 20 unique IDs, got %d", len(ids))
	}
}

// TestTransaction_StateAfterAbort tests state transitions
func TestTransaction_StateAfterAbort(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	if txn.state != ACTIVE {
		t.Fatalf("initial state should be ACTIVE, got %v", txn.state)
	}

	txnMgr.Abort(txn)
	if txn.state != ABORTED {
		t.Fatalf("after abort, state should be ABORTED, got %v", txn.state)
	}
}

// TestVerifyAbortSetsBit tests the abort sets state correctly
func TestVerifyCommitBit(t *testing.T) {
	txnMgr := NewTransactionManager(nil)
	txn := txnMgr.Begin()

	if txn.state != ACTIVE {
		t.Fatalf("expected ACTIVE, got %v", txn.state)
	}

	txnMgr.Commit(txn)
	if txn.state != COMMITTED {
		t.Fatalf("expected COMMITTED, got %v", txn.state)
	}
	if txn.state == ABORTED {
		t.Fatal("state should not be ABORTED")
	}
}

// TestCommit_BufferPoolNotImplemented tests handling of buffer pool without FlushAll
func TestCommit_BufferPoolNotImplemented(t *testing.T) {
	txnMgr := NewTransactionManager(nil)

	// Set a buffer pool object without FlushAll method
	type SimpleObject struct{}
	txnMgr.SetBufferPool(&SimpleObject{})

	txn := txnMgr.Begin()
	err := txnMgr.Commit(txn)

	if err != nil {
		t.Fatalf("should not error when buffer pool doesn't implement FlushAll: %v", err)
	}

	if txn.state != COMMITTED {
		t.Fatalf("expected state COMMITTED, got %v", txn.state)
	}
}
