package txn

import (
	"os"
	"path/filepath"
	"testing"

	wallog "github.com/rodrigo0345/omag/internal/txn/log"
)

// TestRecoveryEndToEnd tests the complete recovery flow:
// 1. Create operations in a transaction
// 2. Commit transaction (data should be in Operations list)
// 3. Simulate crash (close WAL, recreate)
// 4. Run recovery (should replay operations from WAL)
// 5. Verify data is restored
func TestRecoveryEndToEnd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "omag-recovery-e2e-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")

	// === PHASE 1: Initial write with transaction ===
	t.Logf("Phase 1: Initial write with transaction")

	walMgr, err := wallog.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}

	// Create test data
	testKey := []byte("test_key_1")
	testValue := []byte("test_value_1")
	txnID := uint64(100)

	// Record the operations that were performed in this transaction (BEFORE committing)
	walMgr.AddTransactionOperation(txnID, wallog.PUT, testKey, testValue)

	// Simulate a transaction that has committed
	// First, log the commitment to WAL
	commitRec := &wallog.WALRecord{
		TxnID: txnID,
		Type:  wallog.COMMIT,
	}
	_, err = walMgr.AppendLogRecord(commitRec)
	if err != nil {
		t.Fatalf("failed to append commit record: %v", err)
	}

	// Flush WAL
	walMgr.Close()

	// === PHASE 2: Simulate crash and recovery ===
	t.Logf("Phase 2: Simulate crash and recovery")

	// Recreate WAL manager (simulating restart)
	walMgr2, err := wallog.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to recreate WAL manager: %v", err)
	}

	// Run crash recovery
	recoveryState, err := walMgr2.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Verify recovery state
	if !recoveryState.CommittedTxns[txnID] {
		t.Errorf("transaction %d should be committed after recovery", txnID)
	}

	if len(recoveryState.Operations) == 0 {
		t.Errorf("expected operations to be recovered, got 0")
	}

	if len(recoveryState.Operations) > 0 {
		op := recoveryState.Operations[0]
		if op.TxnID != txnID {
			t.Errorf("expected operation txn ID %d, got %d", txnID, op.TxnID)
		}
		if op.Type != wallog.PUT {
			t.Errorf("expected operation type PUT, got %d", op.Type)
		}
		if string(op.Key) != string(testKey) {
			t.Errorf("expected key %s, got %s", string(testKey), string(op.Key))
		}
		if string(op.Value) != string(testValue) {
			t.Errorf("expected value %s, got %s", string(testValue), string(op.Value))
		}
	}

	t.Logf("Recovery successful: %d committed txns, %d operations recovered",
		len(recoveryState.CommittedTxns), len(recoveryState.Operations))

	walMgr2.Close()
}

// TestRecoveryMultipleOperations tests recovery with multiple operations
func TestRecoveryMultipleOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "omag-recovery-multi-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")

	// === Write phase ===
	walMgr, err := wallog.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}

	txnID := uint64(50)

	// Add multiple operations
	operations := [][2][]byte{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, kv := range operations {
		walMgr.AddTransactionOperation(txnID, wallog.PUT, kv[0], kv[1])
	}

	// Commit transaction
	commitRec := &wallog.WALRecord{
		TxnID: txnID,
		Type:  wallog.COMMIT,
	}
	walMgr.AppendLogRecord(commitRec)
	walMgr.Close()

	// === Recovery phase ===
	walMgr2, err := wallog.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to recreate WAL manager: %v", err)
	}

	recoveryState, err := walMgr2.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Verify all operations were recovered
	if len(recoveryState.Operations) != len(operations) {
		t.Errorf("expected %d operations, got %d", len(operations), len(recoveryState.Operations))
	}

	for i, kv := range operations {
		if i >= len(recoveryState.Operations) {
			break
		}
		op := recoveryState.Operations[i]
		if string(op.Key) != string(kv[0]) {
			t.Errorf("operation %d: expected key %s, got %s", i, string(kv[0]), string(op.Key))
		}
		if string(op.Value) != string(kv[1]) {
			t.Errorf("operation %d: expected value %s, got %s", i, string(kv[1]), string(op.Value))
		}
	}

	t.Logf("Recovered %d operations successfully", len(recoveryState.Operations))
	walMgr2.Close()
}

// TestRecoveryAbortedTransactionCleanup tests that aborted transactions don't leave operations
func TestRecoveryAbortedTransactionCleanup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "omag-recovery-abort-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")

	walMgr, err := wallog.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}

	// Add operations for transaction
	txnID := uint64(75)
	walMgr.AddTransactionOperation(txnID, wallog.PUT, []byte("key1"), []byte("value1"))
	walMgr.AddTransactionOperation(txnID, wallog.PUT, []byte("key2"), []byte("value2"))

	// Abort the transaction
	walMgr.CleanupTransactionOperations(txnID)

	// Log ABORT in WAL
	abortRec := &wallog.WALRecord{
		TxnID: txnID,
		Type:  wallog.ABORT,
	}
	walMgr.AppendLogRecord(abortRec)
	walMgr.Close()

	// === Recovery phase ===
	walMgr2, err := wallog.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to recreate WAL manager: %v", err)
	}

	recoveryState, err := walMgr2.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Verify no operations recovered (transaction was aborted)
	if len(recoveryState.Operations) != 0 {
		t.Errorf("expected 0 operations for aborted transaction, got %d", len(recoveryState.Operations))
	}

	if !recoveryState.AbortedTxns[txnID] {
		t.Errorf("transaction %d should be marked as aborted", txnID)
	}

	t.Logf("Aborted transaction cleaned up successfully, 0 operations recovered")
	walMgr2.Close()
}
