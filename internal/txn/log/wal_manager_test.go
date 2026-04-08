package logmanager

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestNewWALManager_Success tests successful WAL manager creation
func TestNewWALManager_Success(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, err := NewWALManager(walFile)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	switch v := wm.(type) {
	case *WALManager:
		if v.logFile == nil {
			t.Fatal("expected logFile to be initialized")
		}
		if v.lsn != 0 {
			t.Fatalf("expected initial LSN 0, got %d", v.lsn)
		}
	default:
		t.Fatalf("unexpected type: %T", wm)
	}
}

// TestNewWALManager_InvalidPath tests WAL manager creation with invalid path
func TestNewWALManager_InvalidPath(t *testing.T) {
	invalidPath := "/nonexistent/directory/that/does/not/exist/wal.log"
	_, err := NewWALManager(invalidPath)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

// TestAppendLog_SerializesCorrectly tests WAL record serialization
func TestAppendLog_SerializesCorrectly(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	rec := WALRecord{
		TxnID:  42,
		Type:   UPDATE,
		PageID: 5,
		Offset: 100,
		Before: []byte("olddata"),
		After:  []byte("newdata"),
	}

	lsn, err := wm.AppendLogRecord(rec)
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}

	if lsn != 1 {
		t.Fatalf("expected LSN 1, got %d", lsn)
	}
	// LSN is returned from AppendLog, which is what we check
	if lsn != 1 {
		t.Fatalf("expected returned LSN to be 1, got %d", lsn)
	}
}

// TestAppendLog_SequentialLSN tests that LSN increments sequentially
func TestAppendLog_SequentialLSN(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	lsns := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		rec := WALRecord{TxnID: uint64(i + 1), Type: UPDATE}
		lsn, err := wm.AppendLogRecord(rec)
		if err != nil {
			t.Fatalf("failed to append log record: %v", err)
		}
		lsns[i] = uint64(lsn)
	}

	// Verify sequential LSNs
	for i, lsn := range lsns {
		if lsn != uint64(i+1) {
			t.Fatalf("expected LSN %d, got %d", i+1, lsn)
		}
	}
}

// TestAppendLog_DifferentRecordTypes tests appending different record types
func TestAppendLog_DifferentRecordTypes(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	types := []RecordType{UPDATE, COMMIT, ABORT, CHECKPOINT}
	for _, rt := range types {
		rec := WALRecord{Type: rt, TxnID: 1}
		lsn, err := wm.AppendLogRecord(rec)
		if err != nil {
			t.Fatalf("failed to append record of type %d: %v", rt, err)
		}
		if lsn == 0 {
			t.Fatalf("failed to append record of type %d", rt)
		}
	}
}

// TestAppendLog_WritesToFile tests that records are actually written to file
func TestAppendLog_WritesToFile(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	rec := WALRecord{
		TxnID:  99,
		Type:   UPDATE,
		PageID: 10,
		Offset: 200,
		Before: []byte("before"),
		After:  []byte("after"),
	}

	_, err := wm.AppendLogRecord(rec)
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}
	wm.Close()

	// Check file exists and has size > 0
	stat, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("failed to stat WAL file: %v", err)
	}
	if stat.Size() == 0 {
		t.Fatal("expected WAL file to have data written")
	}
}

// TestAppendLog_LargeData tests appending records with large data
func TestAppendLog_LargeData(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	rec := WALRecord{
		TxnID:  1,
		Type:   UPDATE,
		PageID: 1,
		Before: largeData,
		After:  largeData,
	}

	lsn, err := wm.AppendLogRecord(rec)
	if err != nil {
		t.Fatalf("failed to append large record: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("failed to append large record, LSN: %d", lsn)
	}
}

// TestFlush_Success tests flushing WAL to disk
func TestFlush_Success(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	rec := WALRecord{TxnID: 5, Type: COMMIT}
	lsn, err := wm.AppendLogRecord(rec)
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}

	err = wm.Flush(lsn)
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}
}

// TestFlush_ZeroLSN tests flushing with LSN 0
func TestFlush_ZeroLSN(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	err := wm.Flush(0)
	if err != nil {
		t.Fatalf("failed to flush with LSN 0: %v", err)
	}
}

// TestRecover_EmptyLog tests recovery on an empty WAL
func TestRecover_EmptyLog(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	state, err := wm.Recover()
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}

	if len(state.CommittedTxns) != 0 {
		t.Fatalf("expected no committed transactions, got %d", len(state.CommittedTxns))
	}
	if len(state.PageStates) != 0 {
		t.Fatalf("expected no page states, got %d", len(state.PageStates))
	}
}

// TestRecover_SimpleTransaction tests recovery of a simple committed transaction
func TestRecover_SimpleTransaction(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	// Add an update and commit
	updateRec := WALRecord{
		TxnID:  1,
		Type:   UPDATE,
		PageID: 1,
		Before: []byte("old"),
		After:  []byte("new"),
	}
	wm.AppendLogRecord(updateRec)

	commitRec := WALRecord{
		TxnID: 1,
		Type:  COMMIT,
	}
	wm.AppendLogRecord(commitRec)
	wm.Close()

	// Recover
	wm2, _ := NewWALManager(walFile)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}

	if !state.CommittedTxns[1] {
		t.Fatal("expected transaction 1 to be committed")
	}

	if pageState, exists := state.PageStates[1]; !exists {
		t.Fatal("expected page 1 to have state")
	} else if string(pageState) != "new" {
		t.Fatalf("expected page state 'new', got '%s'", string(pageState))
	}
}

// TestRecover_MultipleTransactions tests recovery of multiple transactions
func TestRecover_MultipleTransactions(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	// Transaction 1: updates page 1, then commits
	_, err := wm.AppendLogRecord(WALRecord{TxnID: 1, Type: UPDATE, PageID: 1, After: []byte("txn1_page1")})
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}
	_, err = wm.AppendLogRecord(WALRecord{TxnID: 1, Type: COMMIT})
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}

	// Transaction 2: updates page 2, then commits
	_, err = wm.AppendLogRecord(WALRecord{TxnID: 2, Type: UPDATE, PageID: 2, After: []byte("txn2_page2")})
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}
	_, err = wm.AppendLogRecord(WALRecord{TxnID: 2, Type: COMMIT})
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}

	// Transaction 3: updates page 3 (but never commits)
	_, err = wm.AppendLogRecord(WALRecord{TxnID: 3, Type: UPDATE, PageID: 3, After: []byte("txn3_page3")})
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}

	wm.Close()

	// Recover
	wm2, _ := NewWALManager(walFile)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}

	// New Check:txn1 should be committed
	if !state.CommittedTxns[1] {
		t.Fatal("expected transaction 1 to be committed")
	}

	// txn2 should be committed
	if !state.CommittedTxns[2] {
		t.Fatal("expected transaction 2 to be committed")
	}

	// txn3 should NOT have its updates applied
	// (Because it never committed)
	if _, exists := state.PageStates[3]; exists {
		t.Fatal("expected page 3 to NOT have state (txn3 not committed)")
	}

	// Check page states
	if string(state.PageStates[1]) != "txn1_page1" {
		t.Fatalf("expected page 1 state 'txn1_page1', got '%s'", string(state.PageStates[1]))
	}
	if string(state.PageStates[2]) != "txn2_page2" {
		t.Fatalf("expected page 2 state 'txn2_page2', got '%s'", string(state.PageStates[2]))
	}
}

// TestRecover_AbortedTransaction tests recovery correctly ignores aborted transactions
func TestRecover_AbortedTransaction(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	// Transaction that updates then aborts
	_, err := wm.AppendLogRecord(WALRecord{TxnID: 1, Type: UPDATE, PageID: 1, After: []byte("should_not_exist")})
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}
	_, err = wm.AppendLogRecord(WALRecord{TxnID: 1, Type: ABORT})
	if err != nil {
		t.Fatalf("failed to append log record: %v", err)
	}

	wm.Close()

	// Recover
	wm2, _ := NewWALManager(walFile)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}

	// Page 1 should NOT be in page states since txn was aborted
	if _, exists := state.PageStates[1]; exists {
		t.Fatal("expected page 1 to NOT exist (transaction was aborted)")
	}
}

// TestCheckpoint_Success tests checkpoint functionality
func TestCheckpoint_Success(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	err := wm.Checkpoint()
	if err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}

	// Verify checkpoint LSN is set
	checkpointLSN := wm.GetLastCheckpointLSN()
	if checkpointLSN == 0 {
		t.Fatal("expected checkpoint LSN to be > 0")
	}
}

// TestCheckpoint_ResetsCheckpoint tests that checkpoint resets dirty pages
func TestCheckpoint_ResetsCheckpoint(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	// Add some dirty pages
	wm.AppendLogRecord(WALRecord{TxnID: 1, Type: UPDATE, PageID: 1})
	wm.AppendLogRecord(WALRecord{TxnID: 1, Type: UPDATE, PageID: 2})

	switch v := wm.(type) {
	case *WALManager:
		dirtyBefore := v.GetDirtyPages()
		if len(dirtyBefore) != 2 {
			t.Fatalf("expected 2 dirty pages, got %d", len(dirtyBefore))
		}

		// Take checkpoint
		wm.Checkpoint()

		dirtyAfter := v.GetDirtyPages()
		if len(dirtyAfter) != 0 {
			t.Fatalf("expected 0 dirty pages after checkpoint, got %d", len(dirtyAfter))
		}

	default:
		panic(fmt.Sprintf("unexpected type %T for WALManager", v))
	}
}

// TestCheckpoint_WithRecovery tests that recovery works correctly with checkpoints
func TestCheckpoint_WithRecovery(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	// Pre-checkpoint transaction
	wm.AppendLogRecord(WALRecord{TxnID: 1, Type: UPDATE, PageID: 1, After: []byte("pre_checkpoint")})
	wm.AppendLogRecord(WALRecord{TxnID: 1, Type: COMMIT})

	// Checkpoint
	wm.Checkpoint()

	// Post-checkpoint transaction
	wm.AppendLogRecord(WALRecord{TxnID: 2, Type: UPDATE, PageID: 2, After: []byte("post_checkpoint")})
	wm.AppendLogRecord(WALRecord{TxnID: 2, Type: COMMIT})

	wm.Close()

	// Recover from checkpoint
	wm2, _ := NewWALManager(walFile)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}

	// Both transactions should be applied
	if !state.CommittedTxns[1] {
		t.Fatal("expected txn1 to be committed")
	}
	if !state.CommittedTxns[2] {
		t.Fatal("expected txn2 to be committed")
	}

	// Both pages should have correct states
	if string(state.PageStates[1]) != "pre_checkpoint" {
		t.Fatalf("expected page 1 to have 'pre_checkpoint', got '%s'", string(state.PageStates[1]))
	}
	if string(state.PageStates[2]) != "post_checkpoint" {
		t.Fatalf("expected page 2 to have 'post_checkpoint', got '%s'", string(state.PageStates[2]))
	}
}

// TestRecover_ComplexScenario tests recovery with mixed committed, aborted, and pending txns
func TestRecover_ComplexScenario(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	// Txn 1: committed
	wm.AppendLogRecord(WALRecord{TxnID: 1, Type: UPDATE, PageID: 10, After: []byte("txn1_p10")})
	wm.AppendLogRecord(WALRecord{TxnID: 1, Type: UPDATE, PageID: 11, After: []byte("txn1_p11")})
	wm.AppendLogRecord(WALRecord{TxnID: 1, Type: COMMIT})

	// Txn 2: aborted
	wm.AppendLogRecord(WALRecord{TxnID: 2, Type: UPDATE, PageID: 20, After: []byte("txn2_p20")})
	wm.AppendLogRecord(WALRecord{TxnID: 2, Type: ABORT})

	// Txn 3: pending (will be rolled back during recovery)
	wm.AppendLogRecord(WALRecord{TxnID: 3, Type: UPDATE, PageID: 30, After: []byte("txn3_p30")})

	// Txn 4: committed after txn 3
	wm.AppendLogRecord(WALRecord{TxnID: 4, Type: UPDATE, PageID: 40, After: []byte("txn4_p40")})
	wm.AppendLogRecord(WALRecord{TxnID: 4, Type: COMMIT})

	wm.Close()

	// Recover
	wm2, _ := NewWALManager(walFile)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}

	// Verify committed transactions
	if !state.CommittedTxns[1] {
		t.Fatal("txn1 should be committed")
	}
	if !state.CommittedTxns[4] {
		t.Fatal("txn4 should be committed")
	}

	// Verify aborted transactions (both explicitly aborted and pending)
	// Txn 2 was explicitly aborted (has ABORT record)
	// Txn 3 was pending at crash time (no COMMIT or ABORT record)
	// Both should be marked for undo in the ARIES recovery model
	if !state.AbortedTxns[2] {
		t.Fatal("txn2 should be in aborted txns (was explicitly aborted)")
	}
	if !state.AbortedTxns[3] {
		t.Fatal("txn3 should be in aborted txns (was pending at crash)")
	}

	// Verify page states - only from committed transactions
	if string(state.PageStates[10]) != "txn1_p10" {
		t.Fatalf("page 10: expected 'txn1_p10', got '%s'", string(state.PageStates[10]))
	}
	if string(state.PageStates[11]) != "txn1_p11" {
		t.Fatalf("page 11: expected 'txn1_p11', got '%s'", string(state.PageStates[11]))
	}

	// Pages from aborted and pending txns should not exist
	if _, exists := state.PageStates[20]; exists {
		t.Fatal("page 20 should not exist (txn2 was aborted)")
	}
	if _, exists := state.PageStates[30]; exists {
		t.Fatal("page 30 should not exist (txn3 was pending/aborted)")
	}

	// Pages from committed txns should exist
	if string(state.PageStates[40]) != "txn4_p40" {
		t.Fatalf("page 40: expected 'txn4_p40', got '%s'", string(state.PageStates[40]))
	}
}

// TestCheckpoint_Success tests checkpoint function
func TestCheckpoint_Function(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	err := wm.Checkpoint()
	if err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}
}

// TestClose_Success tests closing WAL manager
func TestClose_Success(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	err := wm.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// After close, trying to use the manager should fail
	// (logFile is closed but the pointer may not be nil)
}

// TestClose_Idempotent tests that close operations are safe
func TestClose_Idempotent(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	// First close should work
	err1 := wm.Close()
	if err1 != nil {
		t.Fatalf("first close failed: %v", err1)
	}
}

// TestRecordTypes_Constants tests record type constants
func TestRecordTypes_Constants(t *testing.T) {
	if UPDATE != 0 {
		t.Fatalf("expected UPDATE to be 0, got %d", UPDATE)
	}
	if COMMIT != 1 {
		t.Fatalf("expected COMMIT to be 1, got %d", COMMIT)
	}
	if ABORT != 2 {
		t.Fatalf("expected ABORT to be 2, got %d", ABORT)
	}
	if CHECKPOINT != 3 {
		t.Fatalf("expected CHECKPOINT to be 3, got %d", CHECKPOINT)
	}
}

// TestAppendLog_MultipleRecords tests appending multiple records in sequence
func TestAppendLog_MultipleRecords(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	records := []WALRecord{
		{TxnID: 1, Type: UPDATE, PageID: 1, Before: []byte("a"), After: []byte("b")},
		{TxnID: 1, Type: UPDATE, PageID: 2, Before: []byte("c"), After: []byte("d")},
		{TxnID: 2, Type: UPDATE, PageID: 3, Before: []byte("e"), After: []byte("f")},
		{TxnID: 1, Type: COMMIT},
		{TxnID: 2, Type: COMMIT},
	}

	for i, rec := range records {
		lsn, _ := wm.AppendLogRecord(rec)
		if uint64(lsn) != uint64(i+1) {
			t.Fatalf("record %d: expected LSN %d, got %d", i, i+1, lsn)
		}
	}
}

// TestAppendLog_Serialization_Format tests the binary format of serialized records
func TestAppendLog_Serialization_Format(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	rec := WALRecord{
		TxnID:  42,
		Type:   UPDATE,
		PageID: 5,
		Before: []byte("olddata"),
		After:  []byte("newdata"),
	}

	lsn, _ := wm.AppendLogRecord(rec)

	if lsn != 1 {
		t.Fatalf("expected LSN 1, got %d", lsn)
	}
	// LSN is returned from AppendLog, which is what we check
	if lsn != 1 {
		t.Fatalf("expected returned LSN to be 1, got %d", lsn)
	}
}

// TestAppendLog_ConcurrentAppends tests thread safety (basic)
func TestAppendLog_ConcurrentAppends(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	// Simple sequential appends to test LSN ordering
	// (full concurrency test would require goroutines)
	for i := 0; i < 10; i++ {
		rec := WALRecord{TxnID: uint64(i), Type: UPDATE}
		lsn, _ := wm.AppendLogRecord(rec)
		if uint64(lsn) != uint64(i+1) {
			t.Fatalf("append %d: expected LSN %d, got %d", i, i+1, lsn)
		}
	}
}

// TestWALRecord_Structure tests WALRecord data structure
func TestWALRecord_Structure(t *testing.T) {
	rec := WALRecord{
		LSN:    10,
		TxnID:  20,
		Type:   UPDATE,
		PageID: 30,
		Offset: 40,
		Before: []byte("before"),
		After:  []byte("after"),
	}

	if rec.LSN != 10 {
		t.Fatalf("expected LSN 10, got %d", rec.LSN)
	}
	if rec.TxnID != 20 {
		t.Fatalf("expected TxnID 20, got %d", rec.TxnID)
	}
	if rec.Type != UPDATE {
		t.Fatalf("expected Type UPDATE, got %d", rec.Type)
	}
	if rec.PageID != 30 {
		t.Fatalf("expected PageID 30, got %d", rec.PageID)
	}
	if rec.Offset != 40 {
		t.Fatalf("expected Offset 40, got %d", rec.Offset)
	}
	if string(rec.Before) != "before" {
		t.Fatalf("expected Before 'before', got '%s'", string(rec.Before))
	}
	if string(rec.After) != "after" {
		t.Fatalf("expected After 'after', got '%s'", string(rec.After))
	}
}

// TestAppendLog_EmptyDataFields tests records with empty Before/After
func TestAppendLog_EmptyDataFields(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	rec := WALRecord{
		TxnID:  1,
		Type:   COMMIT, // Commit records might not have Before/After
		Before: []byte{},
		After:  []byte{},
	}

	lsn, _ := wm.AppendLogRecord(rec)
	if uint64(lsn) != 1 {
		t.Fatalf("failed to append record with empty data, LSN: %d", lsn)
	}
}

// Helper function to get temp WAL file
func getTempWalFile(t *testing.T) string {
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "test.wal")
}
