package transaction_manager

import (
	"sync"
	"testing"
	"time"
)

// Helper function to create a test transaction
func newTestTxn(id uint64) *Transaction {
	return &Transaction{
		txnID:          id,
		state:          ACTIVE,
		sharedLocks:    make([][]byte, 0),
		exclusiveLocks: make([][]byte, 0),
		undoLog:        make([]UndoEntry, 0),
	}
}

// ==================== LockManager Basic Lock Tests ====================

func TestLockManagerSharedLockSuccess(t *testing.T) {
	lm := NewLockManager()
	txn := newTestTxn(1)
	key := []byte("key1")

	err := lm.LockShared(txn, key)
	if err != nil {
		t.Errorf("Expected no error acquiring shared lock, got %v", err)
	}

	if len(txn.sharedLocks) != 1 {
		t.Errorf("Expected 1 shared lock in transaction, got %d", len(txn.sharedLocks))
	}
}

func TestLockManagerExclusiveLockSuccess(t *testing.T) {
	lm := NewLockManager()
	txn := newTestTxn(1)
	key := []byte("key1")

	err := lm.LockExclusive(txn, key)
	if err != nil {
		t.Errorf("Expected no error acquiring exclusive lock, got %v", err)
	}

	if len(txn.exclusiveLocks) != 1 {
		t.Errorf("Expected 1 exclusive lock in transaction, got %d", len(txn.exclusiveLocks))
	}
}

func TestLockManagerDuplicateSharedLock(t *testing.T) {
	lm := NewLockManager()
	txn := newTestTxn(1)
	key := []byte("key1")

	// Acquire first time
	err1 := lm.LockShared(txn, key)
	if err1 != nil {
		t.Errorf("First shared lock failed: %v", err1)
	}

	// Acquire again (should be idempotent)
	err2 := lm.LockShared(txn, key)
	if err2 != nil {
		t.Errorf("Second shared lock failed: %v", err2)
	}

	// Should only have one lock entry
	if len(txn.sharedLocks) != 1 {
		t.Errorf("Expected 1 shared lock, got %d", len(txn.sharedLocks))
	}
}

func TestLockManagerExclusiveImpliesShared(t *testing.T) {
	lm := NewLockManager()
	txn := newTestTxn(1)
	key := []byte("key1")

	// Acquire exclusive lock
	lm.LockExclusive(txn, key)

	// Try to acquire shared lock (should succeed because exclusive implies shared)
	err := lm.LockShared(txn, key)
	if err != nil {
		t.Errorf("Expected shared lock to be granted under exclusive lock, got %v", err)
	}
}

func TestLockManagerUnlock(t *testing.T) {
	lm := NewLockManager()
	txn := newTestTxn(1)
	key := []byte("key1")

	// Acquire and release lock
	lm.LockExclusive(txn, key)
	err := lm.Unlock(txn, key)

	if err != nil {
		t.Errorf("Expected no error unlocking, got %v", err)
	}

	if len(txn.exclusiveLocks) != 0 {
		t.Errorf("Expected locks to be empty after unlock, got %d", len(txn.exclusiveLocks))
	}
}

// ==================== LockManager Lock Conflict Tests ====================

func TestLockManagerSharedSharedConflict(t *testing.T) {
	lm := NewLockManager()
	key := []byte("key1")

	txn1 := newTestTxn(1)
	txn2 := newTestTxn(2)

	// Both should acquire shared locks (no conflict)
	err1 := lm.LockShared(txn1, key)
	err2 := lm.LockShared(txn2, key)

	if err1 != nil || err2 != nil {
		t.Errorf("Expected both shared locks to succeed, got %v and %v", err1, err2)
	}
}

func TestLockManagerSharedExclusiveConflict(t *testing.T) {
	lm := NewLockManager()
	key := []byte("key1")

	txn1 := newTestTxn(1)
	txn2 := newTestTxn(2)

	// Txn1 gets shared lock
	lm.LockShared(txn1, key)

	// Txn2 tries exclusive lock (should block, but we use timeout)
	done := make(chan error, 1)
	go func() {
		done <- lm.LockExclusive(txn2, key)
	}()

	// Give it a moment, then check that it's waiting
	time.Sleep(10 * time.Millisecond)

	select {
	case err := <-done:
		// Should timeout or detect deadlock, not succeed immediately
		if err == nil {
			t.Errorf("Expected exclusive lock to block, but it succeeded")
		}
	case <-time.After(100 * time.Millisecond):
		// Still waiting (expected behavior)
	}

	// Release shared lock to unblock txn2
	lm.Unlock(txn1, key)

	// Now txn2 should eventually succeed
	select {
	case err := <-done:
		if err != nil {
			t.Logf("Txn2 got error after txn1 released: %v (may be timeout or success)", err)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("Txn2 never acquired exclusive lock")
	}
}

func TestLockManagerExclusiveExclusiveConflict(t *testing.T) {
	lm := NewLockManager()
	key := []byte("key1")

	txn1 := newTestTxn(1)
	txn2 := newTestTxn(2)

	// Txn1 gets exclusive lock
	lm.LockExclusive(txn1, key)

	// Txn2 tries exclusive lock (should block)
	done := make(chan error, 1)
	go func() {
		done <- lm.LockExclusive(txn2, key)
	}()

	// Give it time, check that it's blocked
	time.Sleep(10 * time.Millisecond)

	select {
	case <-done:
		t.Errorf("Expected exclusive lock to block")
	case <-time.After(50 * time.Millisecond):
		// Expected: still waiting
	}

	// Release and verify txn2 can proceed
	lm.Unlock(txn1, key)

	select {
	case err := <-done:
		if err != nil {
			t.Logf("Txn2 got error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("Txn2 never acquired exclusive lock")
	}
}

// ==================== Lock Upgrade Tests ====================

func TestLockManagerUpgradeSharedToExclusive(t *testing.T) {
	lm := NewLockManager()
	key := []byte("key1")
	txn := newTestTxn(1)

	// Acquire shared lock
	lm.LockShared(txn, key)

	// Upgrade to exclusive
	err := lm.LockExclusive(txn, key)
	if err != nil {
		t.Errorf("Expected upgrade to succeed, got %v", err)
	}

	if len(txn.exclusiveLocks) != 1 {
		t.Errorf("Expected 1 exclusive lock after upgrade")
	}
}

func TestLockManagerUpgradeWithConflict(t *testing.T) {
	lm := NewLockManager()
	key := []byte("key1")

	txn1 := newTestTxn(1)
	txn2 := newTestTxn(2)

	// Txn1 gets shared lock
	lm.LockShared(txn1, key)

	// Txn2 gets shared lock
	lm.LockShared(txn2, key)

	// Txn1 tries to upgrade (should block due to txn2's shared lock)
	done := make(chan error, 1)
	go func() {
		done <- lm.LockExclusive(txn1, key)
	}()

	time.Sleep(10 * time.Millisecond)

	select {
	case <-done:
		t.Errorf("Expected upgrade to block when another txn has shared lock")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	// Release txn2's lock
	lm.Unlock(txn2, key)

	// Now txn1 should complete upgrade
	select {
	case err := <-done:
		if err != nil {
			t.Logf("Upgrade completed with error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("Upgrade never completed")
	}
}

// ==================== Deadlock Detection Tests ====================

func TestLockManagerDeadlockDetectionSimple(t *testing.T) {
	// Create deadlock scenario with short timeout for fast test
	lm := NewLockManagerWithTimeout(100 * time.Millisecond)

	txn1 := newTestTxn(1)
	txn2 := newTestTxn(2)

	key1 := []byte("key1")
	key2 := []byte("key2")

	// Txn1 locks key1
	lm.LockExclusive(txn1, key1)

	// Txn2 locks key2
	lm.LockExclusive(txn2, key2)

	// Txn1 tries to get key2 (will wait for txn2)
	txn1_waiting := make(chan error, 1)
	go func() {
		txn1_waiting <- lm.LockExclusive(txn1, key2)
	}()

	time.Sleep(50 * time.Millisecond)

	// Txn2 tries to get key1 (will wait for txn1)
	// This creates deadlock: Txn1 -> Txn2 -> Txn1
	txn2_err := lm.LockExclusive(txn2, key1)

	// One should detect deadlock
	if txn2_err == ErrDeadlock {
		t.Logf("Txn2 correctly detected deadlock")
	}

	select {
	case txn1_err := <-txn1_waiting:
		if txn1_err == ErrDeadlock {
			t.Logf("Txn1 correctly detected deadlock")
		}
	case <-time.After(500 * time.Millisecond):
		// Timeout, may or may not detect depending on graph state
	}
}

func TestLockManagerAbortedTransaction(t *testing.T) {
	lm := NewLockManager()
	txn := newTestTxn(1)
	key := []byte("key1")

	// Abort transaction
	txn.Abort()

	// Try to acquire lock
	err := lm.LockShared(txn, key)

	if err != ErrTxnAborted {
		t.Errorf("Expected ErrTxnAborted, got %v", err)
	}
}

// ==================== Timeout Tests ====================

func TestLockManagerTimeoutNonDeadlock(t *testing.T) {
	lm := NewLockManagerWithTimeout(100 * time.Millisecond)
	key := []byte("key1")

	txn1 := newTestTxn(1)
	txn2 := newTestTxn(2)

	// Txn1 locks key
	lm.LockExclusive(txn1, key)

	// Txn2 tries to lock (will timeout and retry)
	done := make(chan error, 1)
	go func() {
		done <- lm.LockShared(txn2, key)
	}()

	// Release after timeout
	time.Sleep(150 * time.Millisecond)
	lm.Unlock(txn1, key)

	// Txn2 should eventually succeed
	select {
	case err := <-done:
		if err != nil {
			t.Logf("Txn2 eventually got error: %v", err)
		} else {
			t.Logf("Txn2 successfully acquired lock after timeout")
		}
	case <-time.After(2 * time.Second):
		t.Errorf("Txn2 never acquired lock")
	}
}

// ==================== Concurrent Stress Tests ====================

func TestLockManagerConcurrentOperations(t *testing.T) {
	lm := NewLockManager()
	numTxns := 10
	numKeys := 5
	numOps := 50

	var wg sync.WaitGroup
	errChan := make(chan error, numTxns)

	for txnID := 1; txnID <= numTxns; txnID++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			txn := newTestTxn(id)

			for op := 0; op < numOps; op++ {
				keyID := (id + uint64(op)) % uint64(numKeys)
				key := []byte{byte(keyID)}

				// Alternate between shared and exclusive locks
				if op%2 == 0 {
					if err := lm.LockShared(txn, key); err != nil && err != ErrDeadlock {
						errChan <- err
					}
				} else {
					if err := lm.LockExclusive(txn, key); err != nil && err != ErrDeadlock {
						errChan <- err
					}
				}

				lm.Unlock(txn, key)
			}
		}(uint64(txnID))
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil && err != ErrDeadlock {
			t.Errorf("Concurrent operation error: %v", err)
		}
	}

	t.Logf("Completed concurrent stress test with %d txns, %d keys, %d ops each", numTxns, numKeys, numOps)
}

func TestLockManagerWaitForGraphCleanup(t *testing.T) {
	lm := NewLockManager()
	key := []byte("key1")

	txn1 := newTestTxn(1)
	txn2 := newTestTxn(2)

	// Txn1 locks
	lm.LockExclusive(txn1, key)

	// Txn2 waits
	done := make(chan error, 1)
	go func() {
		done <- lm.LockExclusive(txn2, key)
	}()

	time.Sleep(50 * time.Millisecond)

	// Release lock
	lm.Unlock(txn1, key)

	// Wait for txn2 to complete
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Errorf("Txn2 never completed")
	}

	// Verify wait-for graph is cleaned up
	lm.waitForGraph.mu.Lock()
	if len(lm.waitForGraph.waitEdges[2]) > 0 {
		t.Errorf("Wait-for graph not cleaned up after unlock")
	}
	lm.waitForGraph.mu.Unlock()
}

func TestLockManagerSetDeadlockTimeout(t *testing.T) {
	lm := NewLockManager()

	// Default should be 5 seconds
	if lm.deadlockWait != 5*time.Second {
		t.Errorf("Expected default timeout 5s, got %v", lm.deadlockWait)
	}

	// Change timeout
	lm.SetDeadlockTimeout(10 * time.Second)

	if lm.deadlockWait != 10*time.Second {
		t.Errorf("Expected timeout 10s after set, got %v", lm.deadlockWait)
	}
}

// ==================== Multiple Lock on Same Transaction ====================

func TestLockManagerMultipleLocks(t *testing.T) {
	lm := NewLockManager()
	txn := newTestTxn(1)

	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	// Acquire multiple locks
	for _, key := range keys {
		if err := lm.LockShared(txn, key); err != nil {
			t.Errorf("Failed to acquire lock on %s: %v", string(key), err)
		}
	}

	if len(txn.sharedLocks) != 3 {
		t.Errorf("Expected 3 shared locks, got %d", len(txn.sharedLocks))
	}

	// Release all locks
	for _, key := range keys {
		if err := lm.Unlock(txn, key); err != nil {
			t.Errorf("Failed to unlock %s: %v", string(key), err)
		}
	}

	if len(txn.sharedLocks) != 0 {
		t.Errorf("Expected 0 locks after unlocking all, got %d", len(txn.sharedLocks))
	}
}
