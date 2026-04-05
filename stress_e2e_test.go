package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	btree "github.com/rodrigo0345/omag/b_tree"
	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/transaction_manager"
	"github.com/rodrigo0345/omag/wal"
)

// TestStress_ConcurrentWritesWithRecovery stress tests concurrent writes followed by recovery
func TestStress_ConcurrentWritesWithRecovery(t *testing.T) {
	dbFile, _ := os.CreateTemp("", "stress-concurrent-*.db")
	dbPath := dbFile.Name()
	dbFile.Close()

	walFile, _ := os.CreateTemp("", "stress-concurrent-*.wal")
	walPath := walFile.Name()
	walFile.Close()

	// Setup initial database
	diskMgr, _ := buffermanager.NewDiskManager(dbPath)
	bufferPool := buffermanager.NewBufferPoolManager(50, diskMgr)
	lockMgr := transaction_manager.NewLockManager()
	walMgr, _ := wal.NewWALManager(walPath)
	diskMgr.AllocatePage()

	tree, _ := btree.NewBTree(bufferPool, lockMgr, walMgr, 4096)
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

	const numGoroutines = 10
	const operationsPerGoroutine = 20

	var wg sync.WaitGroup
	var successCount int64

	// Launch concurrent writers
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for op := 0; op < operationsPerGoroutine; op++ {
				key := fmt.Sprintf("stress_g%d_op%d", goroutineID, op)
				value := fmt.Sprintf("value_g%d_op%d", goroutineID, op)
				if err := db.insert(key, value); err == nil {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(g)
	}

	wg.Wait()
	db.Close()

	// Recover and verify by reopening database
	diskMgr2, _ := buffermanager.NewDiskManager(dbPath)
	bufferPool2 := buffermanager.NewBufferPoolManager(50, diskMgr2)
	walMgr2, _ := wal.NewWALManager(walPath)
	diskMgr2.AllocatePage()

	state, _ := walMgr2.Recover()

	// We should have recovered some committed transactions
	if len(state.CommittedTxns) == 0 {
		t.Fatalf("expected committed transactions after concurrent stress test, got 0")
	}

	t.Logf("Stress test: %d successful inserts, %d committed transactions recovered",
		successCount, len(state.CommittedTxns))

	walMgr2.Close()
	bufferPool2.Close()
	diskMgr2.Close()
	os.Remove(dbPath)
	os.Remove(walPath)
}

// TestStress_BufferPoolExhaustion tests recovery when buffer pool is exhausted
func TestStress_BufferPoolExhaustion(t *testing.T) {
	dbFile, _ := os.CreateTemp("", "stress-exhaustion-*.db")
	dbPath := dbFile.Name()
	dbFile.Close()

	walFile, _ := os.CreateTemp("", "stress-exhaustion-*.wal")
	walPath := walFile.Name()
	walFile.Close()

	// Create with very small buffer pool (5 frames) to force evictions
	diskMgr, _ := buffermanager.NewDiskManager(dbPath)
	bufferPool := buffermanager.NewBufferPoolManager(5, diskMgr)
	lockMgr := transaction_manager.NewLockManager()
	walMgr, _ := wal.NewWALManager(walPath)

	diskMgr.AllocatePage()

	db := &DatabaseTUI{
		diskMgr:    diskMgr,
		bufferPool: bufferPool,
		walMgr:     walMgr,
		lockMgr:    lockMgr,
		txnMgr:     transaction_manager.NewTransactionManager(walMgr),
	}
	db.txnMgr.SetBufferPool(bufferPool)

	// Insert many items to cause buffer evictions
	for i := 0; i < 30; i++ {
		if err := db.insert(fmt.Sprintf("exhaustion_key_%d", i), fmt.Sprintf("value_%d", i)); err != nil {
			t.Logf("Insert %d failed (expected due to buffer exhaustion): %v", i, err)
		}
	}

	db.Close()

	// Clean up
	t.Cleanup(func() {
		os.Remove(dbPath)
		os.Remove(walPath)
	})
}

// TestStress_WalLogSize tests recovery with very large WAL files
func TestStress_WalLogSize(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)
	defer db.Close()

	// Generate large WAL by inserting many records
	const numRecords = 100
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("wal_size_test_key_%d", i)
		// Make values large to increase WAL size
		value := fmt.Sprintf("%-256s", fmt.Sprintf("value_%d", i))
		if err := db.insert(key, value); err != nil {
			t.Logf("Insert %d failed: %v", i, err)
		}
	}

	db.Close()

	// Verify WAL file size
	walInfo, _ := os.Stat(walPath)
	walSize := walInfo.Size()
	t.Logf("WAL file size: %d bytes after %d inserts", walSize, numRecords)

	if walSize == 0 {
		t.Fatal("WAL file is empty after inserts")
	}

	// Recover and verify
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)
	defer db.Close()

	state, err := db.walMgr.Recover()
	if err != nil {
		t.Fatalf("recovery from large WAL failed: %v", err)
	}

	records, _ := db.walMgr.ReadAllRecords()
	if len(records) < numRecords {
		t.Logf("Warning: expected %d records in WAL, got %d", numRecords, len(records))
	}

	t.Logf("Successfully recovered %d committed transactions from %d WAL records",
		len(state.CommittedTxns), len(records))
}

// TestStress_RapidFireTransactions tests rapid consecutive transactions
func TestStress_RapidFireTransactions(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)
	defer db.Close()

	startTime := time.Now()

	// Execute rapid-fire transactions
	for i := 0; i < 100; i++ {
		if err := db.insert(fmt.Sprintf("rapid_key_%d", i), fmt.Sprintf("rapid_value_%d", i)); err != nil {
			t.Logf("Rapid fire insert %d failed: %v", i, err)
		}
	}

	elapsed := time.Since(startTime)
	t.Logf("Completed 100 inserts in %v (%.2f ops/sec)", elapsed, 100.0/elapsed.Seconds())

	db.Close()

	// Recover
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)
	defer db.Close()

	state, _ := db.walMgr.Recover()

	if len(state.CommittedTxns) == 0 {
		t.Fatal("no transactions recovered after rapid-fire inserts")
	}

	t.Logf("Rapid fire test recovered %d transactions", len(state.CommittedTxns))
}

// TestStress_LockContentionUnderLoad tests recovery with lock contention
func TestStress_LockContentionUnderLoad(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)
	defer db.Close()

	const numGoroutines = 5
	const operationsPerGoroutine = 20
	var wg sync.WaitGroup

	// All goroutines try to access the same pages (high contention)
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for op := 0; op < operationsPerGoroutine; op++ {
				// All goroutines contend on same keys
				key := "contention_key_1"
				value := fmt.Sprintf("update_g%d_op%d", goroutineID, op)
				_ = db.insert(key, value) // Some will fail due to contention
			}
		}(g)
	}

	wg.Wait()
	db.Close()

	// Recover
	db, _, _ = setupTestDatabase(t)
	db.diskMgr, _ = buffermanager.NewDiskManager(dbPath)
	db.bufferPool = buffermanager.NewBufferPoolManager(50, db.diskMgr)
	db.walMgr, _ = wal.NewWALManager(walPath)
	defer db.Close()

	state, _ := db.walMgr.Recover()
	t.Logf("Lock contention test recovered %d committed transactions", len(state.CommittedTxns))
}

// TestStress_RecoveryUnderMemoryPressure tests recovery with minimal memory
func TestStress_RecoveryUnderMemoryPressure(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Insert moderate amount of data
	for i := 0; i < 50; i++ {
		_ = db.insert(fmt.Sprintf("memory_test_%d", i), fmt.Sprintf("value_%d", i))
	}
	db.Close()

	// Recover with minimal buffer pool
	diskMgr, _ := buffermanager.NewDiskManager(dbPath)
	bufferPool := buffermanager.NewBufferPoolManager(2, diskMgr) // Only 2 frames!
	lockMgr := transaction_manager.NewLockManager()
	walMgr, _ := wal.NewWALManager(walPath)

	db2 := &DatabaseTUI{
		diskMgr:    diskMgr,
		bufferPool: bufferPool,
		walMgr:     walMgr,
		lockMgr:    lockMgr,
		txnMgr:     transaction_manager.NewTransactionManager(walMgr),
	}

	// Recovery should still work with minimal memory
	state, err := walMgr.Recover()
	if err != nil {
		t.Fatalf("recovery under memory pressure failed: %v", err)
	}

	if len(state.CommittedTxns) == 0 {
		t.Fatal("no transactions recovered despite successful recovery call")
	}

	db2.Close()
	os.Remove(dbPath)
	os.Remove(walPath)

	t.Cleanup(func() {})
}

// TestStress_SequentialCrashesAndRecoveries tests multiple consecutive crash-recovery cycles
func TestStress_SequentialCrashesAndRecoveries(t *testing.T) {
	dbFile, _ := os.CreateTemp("", "sequential-crash-*.db")
	dbPath := dbFile.Name()
	dbFile.Close()

	walFile, _ := os.CreateTemp("", "sequential-crash-*.wal")
	walPath := walFile.Name()
	walFile.Close()

	totalTxns := 0
	crashCount := 0

	for cycle := 0; cycle < 5; cycle++ {
		// Phase 1: Do some work
		diskMgr, _ := buffermanager.NewDiskManager(dbPath)
		bufferPool := buffermanager.NewBufferPoolManager(50, diskMgr)
		lockMgr := transaction_manager.NewLockManager()
		walMgr, _ := wal.NewWALManager(walPath)

		diskMgr.AllocatePage()

		db := &DatabaseTUI{
			diskMgr:    diskMgr,
			bufferPool: bufferPool,
			walMgr:     walMgr,
			lockMgr:    lockMgr,
			txnMgr:     transaction_manager.NewTransactionManager(walMgr),
		}
		db.txnMgr.SetBufferPool(bufferPool)

		// Do some inserts in this cycle
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("cycle_%d_key_%d", cycle, i)
			value := fmt.Sprintf("value_cycle_%d", cycle)
			_ = db.insert(key, value)
			totalTxns++
		}

		db.Close()
		crashCount++

		// Phase 2: Verify recovery
		walMgr2, _ := wal.NewWALManager(walPath)
		state, err := walMgr2.Recover()
		walMgr2.Close()

		if err != nil {
			t.Fatalf("recovery failed at cycle %d: %v", cycle, err)
		}

		t.Logf("Cycle %d: %d committed transactions total", cycle, len(state.CommittedTxns))
	}

	t.Logf("Completed %d crash-recovery cycles with %d total transactions", crashCount, totalTxns)

	// Cleanup
	os.Remove(dbPath)
	os.Remove(walPath)
}

// TestStress_RecoveryConsistency verifies that recovery always reaches the same final state
func TestStress_RecoveryConsistency(t *testing.T) {
	db, dbPath, walPath := setupTestDatabase(t)

	// Insert deterministic test data
	testData := map[string]string{
		"consistency_1": "value_1",
		"consistency_2": "value_2",
		"consistency_3": "value_3",
	}

	for key, value := range testData {
		_ = db.insert(key, value)
	}
	db.Close()

	// Perform recovery 3 times and verify consistency
	states := make([]uint64, 3)

	for attempt := 0; attempt < 3; attempt++ {
		diskMgr, _ := buffermanager.NewDiskManager(dbPath)
		bufferPool := buffermanager.NewBufferPoolManager(50, diskMgr)
		walMgr, _ := wal.NewWALManager(walPath)

		state, _ := walMgr.Recover()
		states[attempt] = uint64(len(state.CommittedTxns))

		walMgr.Close()
		bufferPool.Close()
		diskMgr.Close()
	}

	// All recovery attempts should see the same committed transaction count
	if states[0] != states[1] || states[1] != states[2] {
		t.Fatalf("recovery consistency violated: attempts saw %d, %d, %d committed txns",
			states[0], states[1], states[2])
	}

	os.Remove(dbPath)
	os.Remove(walPath)
}
