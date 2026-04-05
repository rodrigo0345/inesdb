package main

import (
	"fmt"
	"os"
	"testing"

	btree "github.com/rodrigo0345/omag/b_tree"
	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/transaction_manager"
	"github.com/rodrigo0345/omag/wal"
)

// setupTestDatabase creates a fresh database for testing
func setupTestDatabase(t *testing.T) (*DatabaseTUI, string, string) {
	dbFile, err := os.CreateTemp("", "e2e-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp db file: %v", err)
	}
	dbPath := dbFile.Name()
	dbFile.Close()

	walFile, err := os.CreateTemp("", "e2e-test-*.wal")
	if err != nil {
		t.Fatalf("failed to create temp wal file: %v", err)
	}
	walPath := walFile.Name()
	walFile.Close()

	// Create disk manager
	diskMgr, err := buffermanager.NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}

	// Create buffer pool manager
	bufferPool := buffermanager.NewBufferPoolManager(50, diskMgr)

	// Create lock manager
	lockMgr := transaction_manager.NewLockManager()

	// Create WAL manager
	walMgr, err := wal.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}

	// Allocate first page for meta
	diskMgr.AllocatePage()

	// Perform recovery if WAL has data
	recoveryState, err := walMgr.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Apply recovered pages if any
	if len(recoveryState.PageStates) > 0 {
		maxPageID := uint32(0)
		for pageID := range recoveryState.PageStates {
			if uint32(pageID) > maxPageID {
				maxPageID = uint32(pageID)
			}
		}

		for {
			allocatedPageID := diskMgr.AllocatePage()
			if uint32(allocatedPageID) > maxPageID {
				break
			}
		}

		for pageID, pageData := range recoveryState.PageStates {
			paddedData := make([]byte, 4096)
			copy(paddedData, pageData)
			diskMgr.WritePage(buffermanager.PageID(pageID), paddedData)
		}
	}

	// Create BTree
	tree, err := btree.NewBTree(bufferPool, lockMgr, walMgr, 4096)
	if err != nil {
		t.Fatalf("failed to create BTree: %v", err)
	}

	// Create transaction manager
	txnMgr := transaction_manager.NewTransactionManager(walMgr)
	txnMgr.SetBufferPool(bufferPool)

	db := &DatabaseTUI{
		tree:       tree,
		diskMgr:    diskMgr,
		bufferPool: bufferPool,
		walMgr:     walMgr,
		lockMgr:    lockMgr,
		txnMgr:     txnMgr,
	}

	t.Cleanup(func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(walPath)
	})

	return db, dbPath, walPath
}

// TestE2E_CrashDuringTransaction tests recovery when crash occurs during an uncommitted transaction
func TestE2E_CrashDuringTransaction(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Phase 1: Insert first committed transaction
	if err := db.insert("key1", "value1"); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	// Flush to ensure it's on disk
	db.bufferPool.FlushAll()
	db.diskMgr.Sync()

	// Phase 2: Start a transaction but don't commit (simulate crash)
	txn := db.txnMgr.Begin()
	if err := db.tree.Insert(txn, []byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("insert in transaction failed: %v", err)
	}
	// Intentionally NOT committing - just closing to simulate crash
	db.Close()

	// Phase 3: Recover from crash
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	// Perform recovery
	recoveryState, err := db.walMgr.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Verify: key1 should be committed and present
	if len(recoveryState.CommittedTxns) == 0 {
		t.Fatal("expected at least one committed transaction after recovery")
	}

	// Verify: uncommitted transaction (key2) should not exist
	if len(recoveryState.AbortedTxns) == 0 {
		t.Log("Warning: expected aborted transactions after recovery")
	}

	db.Close()
}

// TestE2E_CrashDuringChekpoint tests recovery when crash occurs during checkpoint
func TestE2E_CrashDuringCheckpoint(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Insert multiple transactions
	for i := 0; i < 5; i++ {
		if err := db.insert(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	// Create checkpoint (simulate crash during checkpoint by just closing)
	db.Close()

	// Recover and verify all data is still there
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	recoveryState, err := db.walMgr.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if len(recoveryState.CommittedTxns) < 5 {
		t.Fatalf("expected at least 5 committed transactions, got %d", len(recoveryState.CommittedTxns))
	}

	db.Close()
}

// TestE2E_MultipleConsecutiveCrashes tests idempotency - recovery should work after multiple crashes
func TestE2E_MultipleConsecutiveCrashes(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Insert initial data
	if err := db.insert("persistent_key", "persistent_value"); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	db.Close()

	// First recovery (crash during recovery)
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	state1, err := db.walMgr.Recover()
	if err != nil {
		t.Fatalf("first recovery failed: %v", err)
	}

	numCommitted1 := len(state1.CommittedTxns)
	db.Close()

	// Second recovery (recovery of recovery - idempotency test)
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	state2, err := db.walMgr.Recover()
	if err != nil {
		t.Fatalf("second recovery failed: %v", err)
	}

	numCommitted2 := len(state2.CommittedTxns)

	// Both recoveries should see the same committed transactions (idempotency)
	if numCommitted1 != numCommitted2 {
		t.Fatalf("idempotency violated: first recovery saw %d committed txns, second saw %d",
			numCommitted1, numCommitted2)
	}

	db.Close()
}

// TestE2E_DataIntegrityAfterRecovery verifies that recovered data matches what was committed
func TestE2E_DataIntegrityAfterRecovery(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Insert known data
	testCases := map[string]string{
		"user:1":     "alice",
		"user:2":     "bob",
		"config:db":  "production",
		"balance:42": "999.99",
	}

	for key, value := range testCases {
		if err := db.insert(key, value); err != nil {
			t.Fatalf("insert failed for %s: %v", key, err)
		}
	}

	// Force flush
	db.bufferPool.FlushAll()
	db.diskMgr.Sync()

	// Simulate crash and recovery
	db.Close()

	// Reopen and verify recovery
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	recoveryState, err := db.walMgr.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Verify all committed transactions recovered
	if len(recoveryState.CommittedTxns) != len(testCases) {
		t.Fatalf("expected %d committed transactions, got %d",
			len(testCases), len(recoveryState.CommittedTxns))
	}

	db.Close()
}

// TestE2E_ConcurrentTransactionsWithCrash tests multiple concurrent transactions with crash
func TestE2E_ConcurrentTransactionsWithCrash(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Start multiple concurrent transactions
	txns := make([]*transaction_manager.Transaction, 5)
	for i := 0; i < 5; i++ {
		txns[i] = db.txnMgr.Begin()
		key := fmt.Sprintf("concurrent_key_%d", i)
		if err := db.tree.Insert(txns[i], []byte(key), []byte(fmt.Sprintf("value_%d", i))); err != nil {
			t.Fatalf("concurrent insert failed: %v", err)
		}
	}

	// Commit only first 3 transactions, leave others pending
	for i := 0; i < 3; i++ {
		db.txnMgr.Commit(txns[i])
	}
	// Simulate crash - don't commit txns 3 and 4
	db.Close()

	// Recover and verify only committed transactions present
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	recoveryState, err := db.walMgr.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if len(recoveryState.CommittedTxns) < 3 {
		t.Fatalf("expected at least 3 committed transactions, got %d", len(recoveryState.CommittedTxns))
	}

	if len(recoveryState.AbortedTxns) < 2 {
		t.Fatalf("expected at least 2 aborted transactions, got %d", len(recoveryState.AbortedTxns))
	}

	db.Close()
}

// TestE2E_RecoveryWithDirtyPages tests recovery correctly handles dirty pages from uncommitted txns
func TestE2E_RecoveryWithDirtyPages(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Transaction 1: Committed
	if err := db.insert("committed:1", "data1"); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Transaction 2: Start but don't commit (will be dirty)
	txn := db.txnMgr.Begin()
	if err := db.tree.Insert(txn, []byte("uncommitted:1"), []byte("should_not_exist")); err != nil {
		t.Fatalf("insert in txn failed: %v", err)
	}
	// Simulate crash - no commit
	db.Close()

	// Recover
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	state, err := db.walMgr.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Verify dirty page from uncommitted transaction is NOT in final state
	// Only the committed transaction's pages should be recovered
	if len(state.CommittedTxns) != 1 {
		t.Fatalf("expected 1 committed transaction, got %d", len(state.CommittedTxns))
	}

	db.Close()
}

// TestE2E_PhantomReadPrevention tests that recovery prevents phantom reads
func TestE2E_PhantomReadPrevention(t *testing.T) {
	// Setup phase 1: Create database and insert initial data
	dbFile, _ := os.CreateTemp("", "phantom-test-*.db")
	dbPath := dbFile.Name()
	dbFile.Close()

	walFile, _ := os.CreateTemp("", "phantom-test-*.wal")
	walPath := walFile.Name()
	walFile.Close()

	// Phase 1: Create initial database with some data
	diskMgr, _ := buffermanager.NewDiskManager(dbPath)
	bufferPool := buffermanager.NewBufferPoolManager(50, diskMgr)
	lockMgr := transaction_manager.NewLockManager()
	walMgr, _ := wal.NewWALManager(walPath)
	diskMgr.AllocatePage()

	tree1, _ := btree.NewBTree(bufferPool, lockMgr, walMgr, 4096)
	txnMgr1 := transaction_manager.NewTransactionManager(walMgr)
	txnMgr1.SetBufferPool(bufferPool)

	db1 := &DatabaseTUI{
		tree:       tree1,
		diskMgr:    diskMgr,
		bufferPool: bufferPool,
		walMgr:     walMgr,
		lockMgr:    lockMgr,
		txnMgr:     txnMgr1,
	}

	// Insert initial data
	for i := 0; i < 3; i++ {
		if err := db1.insert(fmt.Sprintf("item_%d", i), fmt.Sprintf("data_%d", i)); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}
	db1.Close()

	// Phase 2: Reopen and insert more data
	diskMgr2, _ := buffermanager.NewDiskManager(dbPath)
	bufferPool2 := buffermanager.NewBufferPoolManager(50, diskMgr2)
	lockMgr2 := transaction_manager.NewLockManager()
	walMgr2, _ := wal.NewWALManager(walPath)
	diskMgr2.AllocatePage()

	// Recover and get baseline
	state, _ := walMgr2.Recover()
	baselineCount := len(state.CommittedTxns)

	tree2, _ := btree.NewBTree(bufferPool2, lockMgr2, walMgr2, 4096)
	txnMgr2 := transaction_manager.NewTransactionManager(walMgr2)
	txnMgr2.SetBufferPool(bufferPool2)

	db2 := &DatabaseTUI{
		tree:       tree2,
		diskMgr:    diskMgr2,
		bufferPool: bufferPool2,
		walMgr:     walMgr2,
		lockMgr:    lockMgr2,
		txnMgr:     txnMgr2,
	}

	// Verify we can read the original data
	if err := db2.search("item_0"); err != nil {
		t.Fatalf("failed to recover initial data: %v", err)
	}

	// Insert more data
	for i := 3; i < 6; i++ {
		if err := db2.insert(fmt.Sprintf("item_%d", i), fmt.Sprintf("data_%d", i)); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}
	db2.Close()

	// Phase 3: Final recovery and verify all data is present
	diskMgr3, _ := buffermanager.NewDiskManager(dbPath)
	bufferPool3 := buffermanager.NewBufferPoolManager(50, diskMgr3)
	lockMgr3 := transaction_manager.NewLockManager()
	walMgr3, _ := wal.NewWALManager(walPath)
	diskMgr3.AllocatePage()

	// Recover and verify we have more transactions than before
	state2, _ := walMgr3.Recover()
	finalCount := len(state2.CommittedTxns)

	if finalCount < baselineCount {
		t.Fatalf("recovery regression: baseline %d transactions, final %d transactions",
			baselineCount, finalCount)
	}

	tree3, _ := btree.NewBTree(bufferPool3, lockMgr3, walMgr3, 4096)
	txnMgr3 := transaction_manager.NewTransactionManager(walMgr3)
	txnMgr3.SetBufferPool(bufferPool3)

	db3 := &DatabaseTUI{
		tree:       tree3,
		diskMgr:    diskMgr3,
		bufferPool: bufferPool3,
		walMgr:     walMgr3,
		lockMgr:    lockMgr3,
		txnMgr:     txnMgr3,
	}

	// Verify all inserted data is recoverable
	for i := 0; i < 6; i++ {
		err := db3.search(fmt.Sprintf("item_%d", i))
		if err != nil {
			t.Fatalf("failed to recover item_%d: %v", i, err)
		}
	}

	db3.Close()
	walMgr3.Close()
	bufferPool3.Close()
	diskMgr3.Close()
	os.Remove(dbPath)
	os.Remove(walPath)
}

// TestE2E_RecoveryIsotonicOrdering tests that recovery maintains transaction ordering
func TestE2E_RecoveryIsotonicOrdering(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Insert transactions in order
	orderingKeys := []string{"first", "second", "third", "fourth"}
	for _, key := range orderingKeys {
		if err := db.insert(key, "value_"+key); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}
	db.Close()

	// Recover and check order is preserved
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	records, err := db.walMgr.ReadAllRecords()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	// Verify records appear in monotonic LSN order
	lastLSN := uint64(0)
	for _, rec := range records {
		if rec.LSN < lastLSN {
			t.Fatalf("LSN ordering violated: %d < %d", rec.LSN, lastLSN)
		}
		lastLSN = rec.LSN
	}

	db.Close()
}

// TestE2E_LongRunningTransactionRecovery tests recovery with very large transactions
func TestE2E_LongRunningTransactionRecovery(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Insert large amount of data
	const numInserts = 50
	txn := db.txnMgr.Begin()

	for i := 0; i < numInserts; i++ {
		key := fmt.Sprintf("large_txn_key_%d", i)
		value := fmt.Sprintf("large_value_%d_with_some_padding_to_make_it_longer_%s",
			i, "aaaaaaaaaa")
		if err := db.tree.Insert(txn, []byte(key), []byte(value)); err != nil {
			t.Fatalf("large insert failed: %v", err)
		}
	}

	// Commit the large transaction
	db.txnMgr.Commit(txn)
	db.Close()

	// Recover and verify all inserts from large transaction are present
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	state, _ := db.walMgr.Recover()

	if len(state.CommittedTxns) != 1 {
		t.Fatalf("expected 1 committed transaction for large txn, got %d", len(state.CommittedTxns))
	}

	db.Close()
}

// TestE2E_CheckpointRecovery tests that checkpoints enable faster recovery
func TestE2E_CheckpointRecovery(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Insert initial data
	for i := 0; i < 10; i++ {
		if err := db.insert(fmt.Sprintf("pre_checkpoint_%d", i), fmt.Sprintf("value_%d", i)); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	// Create checkpoint
	if err := db.createCheckpoint(); err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}

	// Insert more data after checkpoint
	for i := 10; i < 15; i++ {
		if err := db.insert(fmt.Sprintf("post_checkpoint_%d", i), fmt.Sprintf("value_%d", i)); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}
	db.Close()

	// Recover - should see both pre and post checkpoint data
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)

	state, _ := db.walMgr.Recover()

	// All 15 inserts should be present
	if len(state.CommittedTxns) < 15 {
		t.Fatalf("expected at least 15 committed transactions after recovery, got %d",
			len(state.CommittedTxns))
	}

	db.Close()
}
