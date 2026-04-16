package acid_compliance

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/page"
	wallog "github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/testutil"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	"github.com/rodrigo0345/omag/internal/txn/write_handler"
)

func TestCrashRecovery_BasicRecovery(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	wm, _ := wallog.NewWALManager(walPath)

	rec := &wallog.WALRecord{
		TxnID:  1,
		Type:   wallog.UPDATE,
		PageID: page.ResourcePageID(1),
		Before: []byte{1, 2, 3},
		After:  []byte{4, 5, 6},
	}

	wm.AppendLogRecord(rec)
	wm.Close()

	wm2, _ := wallog.NewWALManager(walPath)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if state == nil {
		t.Fatal("recovery state should not be nil")
	}
}

func TestCrashRecovery_CommittedTransaction(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	wm, _ := wallog.NewWALManager(walPath)

	txnID := uint64(42)

	updateRec := &wallog.WALRecord{
		TxnID:  txnID,
		Type:   wallog.UPDATE,
		PageID: page.ResourcePageID(1),
		Before: []byte{10},
		After:  []byte{20},
	}
	wm.AppendLogRecord(updateRec)

	commitRec := &wallog.WALRecord{
		TxnID: txnID,
		Type:  wallog.COMMIT,
	}
	wm.AppendLogRecord(commitRec)
	wm.Close()

	wm2, _ := wallog.NewWALManager(walPath)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if _, exists := state.CommittedTxns[txnID]; !exists {
		t.Errorf("committed transaction %d not found in recovery state", txnID)
	}
}

func TestCrashRecovery_AbortedTransaction(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	wm, _ := wallog.NewWALManager(walPath)

	txnID := uint64(99)

	updateRec := &wallog.WALRecord{
		TxnID:  txnID,
		Type:   wallog.UPDATE,
		PageID: page.ResourcePageID(2),
		Before: []byte{30},
		After:  []byte{40},
	}
	wm.AppendLogRecord(updateRec)

	abortRec := &wallog.WALRecord{
		TxnID: txnID,
		Type:  wallog.ABORT,
	}
	wm.AppendLogRecord(abortRec)
	wm.Close()

	wm2, _ := wallog.NewWALManager(walPath)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if _, exists := state.AbortedTxns[txnID]; !exists {
		t.Errorf("aborted transaction %d not found in recovery state", txnID)
	}
}

func TestCrashRecovery_MultipleTransactions(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	wm, _ := wallog.NewWALManager(walPath)

	committed := []uint64{1, 2, 3}
	aborted := []uint64{4, 5}

	for _, txnID := range committed {
		updateRec := &wallog.WALRecord{
			TxnID:  txnID,
			Type:   wallog.UPDATE,
			PageID: page.ResourcePageID(txnID),
			Before: []byte{byte(txnID)},
			After:  []byte{byte(txnID * 2)},
		}
		wm.AppendLogRecord(updateRec)

		commitRec := &wallog.WALRecord{
			TxnID: txnID,
			Type:  wallog.COMMIT,
		}
		wm.AppendLogRecord(commitRec)
	}

	for _, txnID := range aborted {
		updateRec := &wallog.WALRecord{
			TxnID:  txnID,
			Type:   wallog.UPDATE,
			PageID: page.ResourcePageID(txnID),
			Before: []byte{byte(txnID)},
			After:  []byte{byte(txnID * 2)},
		}
		wm.AppendLogRecord(updateRec)

		abortRec := &wallog.WALRecord{
			TxnID: txnID,
			Type:  wallog.ABORT,
		}
		wm.AppendLogRecord(abortRec)
	}

	wm.Close()

	wm2, _ := wallog.NewWALManager(walPath)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if len(state.CommittedTxns) != len(committed) {
		t.Errorf("expected %d committed txns, got %d", len(committed), len(state.CommittedTxns))
	}

	if len(state.AbortedTxns) != len(aborted) {
		t.Errorf("expected %d aborted txns, got %d", len(aborted), len(state.AbortedTxns))
	}

	for _, txnID := range committed {
		if _, exists := state.CommittedTxns[txnID]; !exists {
			t.Errorf("committed txn %d not in recovery state", txnID)
		}
	}

	for _, txnID := range aborted {
		if _, exists := state.AbortedTxns[txnID]; !exists {
			t.Errorf("aborted txn %d not in recovery state", txnID)
		}
	}
}

func TestCrashRecovery_IntermediateState(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	wm, _ := wallog.NewWALManager(walPath)

	unknownTxn := uint64(777)
	updateRec := &wallog.WALRecord{
		TxnID:  unknownTxn,
		Type:   wallog.UPDATE,
		PageID: page.ResourcePageID(10),
		Before: []byte{100},
		After:  []byte{200},
	}
	wm.AppendLogRecord(updateRec)

	wm.Close()

	wm2, _ := wallog.NewWALManager(walPath)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if _, inCommitted := state.CommittedTxns[unknownTxn]; inCommitted {
		t.Error("incomplete transaction should not be in committed set")
	}
}

func TestCrashRecovery_DurabilityAfterCrash(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "omag-test-")
	defer os.RemoveAll(tmpDir)

	bpm := testutil.NewTestBufferPoolManager(t, tmpDir)
	walPath := filepath.Join(tmpDir, "test.wal")
	wm, _ := wallog.NewWALManager(walPath)

	storage := testutil.NewInMemoryStorageEngine()
	rollbackMgr := rollback.NewRollbackManager(bpm)
	handler := write_handler.NewDefaultWriteHandler(storage, rollbackMgr, bpm, wm)

	txn := txn_unit.NewTransaction(1, txn_unit.READ_COMMITTED)

	writeOp := write_handler.WriteOperation{
		Key:      []byte("crash_test_key"),
		Value:    []byte("crash_test_value"),
		PageID:   page.ResourcePageID(1),
		Offset:   0,
		IsDelete: false,
	}

	err := handler.HandleWrite(txn, writeOp)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	wm.Flush(0)
	wm.Close()

	wm2, _ := wallog.NewWALManager(walPath)
	defer wm2.Close()

	state, err := wm2.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if state == nil {
		t.Fatal("recovery state should not be nil")
	}

	stored, err := storage.Get([]byte("crash_test_key"))
	if err != nil {
		t.Fatalf("data lost after crash recovery: %v", err)
	}

	if string(stored) != "crash_test_value" {
		t.Errorf("durability compromised: expected 'crash_test_value', got '%s'", string(stored))
	}
}
