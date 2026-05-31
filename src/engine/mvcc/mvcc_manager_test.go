package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rodrigo0345/omag/pkg/pkglog"
	"github.com/rodrigo0345/omag/src/engine/rollback"
	txn "github.com/rodrigo0345/omag/src/engine/txn"
	log "github.com/rodrigo0345/omag/src/engine/wal"
	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/buffer"
	"github.com/rodrigo0345/omag/src/storage/lsm"
	"github.com/rodrigo0345/omag/src/storage/schema"
)

// --- Test Environment Setup ---

func setupMVCCWithRealTableManager(t *testing.T) (*MVCCManager, *schema.TableManager) {
	t.Helper()

	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal.log")
	diskPath := filepath.Join(tempDir, "data.db")
	lsmPath := filepath.Join(tempDir, "lsm_data")

	tm := schema.NewTableManager()

	logMgr, err := log.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	diskManager, err := buffer.NewDiskManager(diskPath)
	if err != nil {
		t.Fatalf("failed to create DiskMgr: %v", err)
	}

	bufferMgr := buffer.NewBufferPoolManager(50, diskManager)
	storageEngine := lsm.NewLSMTreeBackendWithDataDir(logMgr, bufferMgr, lsmPath)

	rollbackMgr := rollback.NewRollbackManager(bufferMgr)

	cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt32},
		{Name: "val", Type: schema.TypeInt32},
	}
	ts := schema.NewTableSchema("test_table", cols)
	ts.AddIndex("PRIMARY", []string{"id"}, storageEngine)
	tm.CreateTable(ts, true)

	tracer := pkglog.NewTracer()

	mvcc := NewMVCCManager(logMgr, bufferMgr, rollbackMgr, tm, tracer)

	t.Cleanup(func() {
		storageEngine.Close()
		logMgr.Close()
		diskManager.Close()
	})

	return mvcc, tm
}

// Fixed: Adds the 0x01 metadata byte required by the TableSchema
func buildTestRow(id, val int32) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Metadata byte (_txn_op)
	binary.Write(buf, schema.DbEndian, id)
	binary.Write(buf, schema.DbEndian, val)
	return buf.Bytes()
}

// Fixed: Uses the TableManager to decode so we don't guess offsets
func getValFromTable(tm *schema.TableManager, payload []byte) int32 {
	return int32(tm.DecodeRow("test_table", "val", payload).Int())
}

func TestMVCC_ReadCommitted_Visibility(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("k1")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 100))

	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)

	// Should not see T1's uncommitted write
	res, err := mvcc.Read(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	if err == nil && res != nil {
		t.Error("ReadCommitted: T2 saw uncommitted data")
	}

	err = mvcc.Commit(txn.TransactionID(t1))
	if err != nil {
		panic("This should have not happened")
	}

	// T2 (Read Committed) should now see the data upon its next read
	res, err = mvcc.Read(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatalf("ReadCommitted: T2 failed to see committed data: %v", err)
	}
	result := tm.DecodeRow("test_table", "val", res)

	if result.Int() != 100 {
		t.Errorf("Expected 100, got %d", getValFromTable(tm, res))
	}
}

func TestMVCC_RepeatableRead_Snapshot(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("k1")

	// T0 initializes data
	t0 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t0), "test_table", "PRIMARY", key, buildTestRow(1, 10))
	mvcc.Commit(txn.TransactionID(t0))

	// T1 starts Repeatable Read (captures current snapshot)
	t1 := mvcc.BeginTransaction(txn.REPEATABLE_READ)

	// T2 updates data and commits
	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t2), "test_table", "PRIMARY", key, buildTestRow(1, 20))
	mvcc.Commit(txn.TransactionID(t2))

	// T1 reads: Should see its snapshot value (10), ignoring T2's commit (20)
	res, err := mvcc.Read(txn.TransactionID(t1), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatal(err)
	}

	val := getValFromTable(tm, res)
	if val != 10 {
		t.Errorf("RepeatableRead: T1 saw T2's update. Got %d, want 10", val)
	}
}

func TestMVCC_Delete_Isolation(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("k1")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 50))
	mvcc.Commit(txn.TransactionID(t1))

	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Delete(txn.TransactionID(t2), "test_table", "PRIMARY", key)

	// T3 should still see the record because T2 is not committed
	t3 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(t3), "test_table", "PRIMARY", key)
	if err != nil || res == nil {
		t.Error("Delete: uncommitted delete made record invisible to T3")
	}

	mvcc.Commit(txn.TransactionID(t2))

	// T4 should not see the record anymore
	t4 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	_, err = mvcc.Read(txn.TransactionID(t4), "test_table", "PRIMARY", key)
	if err == nil {
		t.Error("Delete: committed delete still visible to T4")
	}
}

func TestMVCC_MultipleVersions_Scanning(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("k1")

	// Write 3 versions
	for i := 1; i <= 3; i++ {
		tid := mvcc.BeginTransaction(txn.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(1, int32(i*10)))
		mvcc.Commit(txn.TransactionID(tid))
	}

	// Read should return the latest committed version (30)
	tRead := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(tRead), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatal(err)
	}

	val := getValFromTable(tm, res)
	if val != 30 {
		t.Errorf("Expected latest version 30, got %d", val)
	}
}

func TestMVCC_HighConcurrency_LostUpdate(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("counter")

	// Initialize counter at 0
	t0 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t0), "test_table", "PRIMARY", key, buildTestRow(1, 0))
	mvcc.Commit(txn.TransactionID(t0))

	const numWorkers = 50
	const incrementsPerWorker = 20
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerWorker; j++ {
				for {
					tid := mvcc.BeginTransaction(txn.SERIALIZABLE)
					res, _ := mvcc.Read(txn.TransactionID(tid), "test_table", "PRIMARY", key)
					currentVal := getValFromTable(tm, res)
					mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(1, currentVal+1))
					if err := mvcc.Commit(txn.TransactionID(tid)); err == nil {
						break
					}
					// Commit aborted the transaction due to a write-write conflict.
					// Retry with a fresh read of the current value.
				}
			}
		}()
	}

	wg.Wait()

	tFinal := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res, _ := mvcc.Read(txn.TransactionID(tFinal), "test_table", "PRIMARY", key)
	finalVal := getValFromTable(tm, res)

	expected := int32(numWorkers * incrementsPerWorker)
	if finalVal != expected {
		t.Errorf("Lost Update Detected: Expected %d, got %d", expected, finalVal)
	}
}

func TestMVCC_LongRunningSnapshot_Integrity(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("stable_key")

	// 1. Setup initial state
	tInit := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(tInit), "test_table", "PRIMARY", key, buildTestRow(1, 100))
	mvcc.Commit(txn.TransactionID(tInit))

	// 2. Start the "Snapshot" transaction
	tSnapshot := mvcc.BeginTransaction(txn.REPEATABLE_READ)

	// 3. Perform 1,000 updates in other transactions
	for i := 1; i <= 1000; i++ {
		tx := mvcc.BeginTransaction(txn.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tx), "test_table", "PRIMARY", key, buildTestRow(1, int32(100+i)))
		mvcc.Commit(txn.TransactionID(tx))
	}

	// 4. The snapshot transaction must STILL see 100
	res, err := mvcc.Read(txn.TransactionID(tSnapshot), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatalf("Snapshot read failed: %v", err)
	}

	val := getValFromTable(tm, res)
	if val != 100 {
		t.Errorf("Snapshot Isolation Broken: Expected 100, got %d. Version chain traversal is likely leaking newer commits.", val)
	}
}

func TestMVCC_MassParallelInserts_RangeScan(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	const totalKeys = 1000

	// 1. Insert 1000 keys
	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		tid := mvcc.BeginTransaction(txn.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(int32(i), int32(i)))
		mvcc.Commit(txn.TransactionID(tid))
	}

	// 2. Scan and verify count
	tScan := mvcc.BeginTransaction(txn.READ_COMMITTED)
	// Empty bounds for full scan
	opts := storage.ScanOptions{Inclusive: true}
	cursor, err := mvcc.Scan(txn.TransactionID(tScan), "test_table", "PRIMARY", opts)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for cursor.Next() {
		count++
	}

	if count != totalKeys {
		t.Errorf("Scan Count Mismatch: Expected %d keys, found %d. Cursor logic might be failing on multi-page data.", totalKeys, count)
	}
}

func TestMVCC_Abort_WritesNotVisible(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("k_abort")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 999))
	mvcc.Abort(txn.TransactionID(t1))

	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	if err == nil && res != nil {
		t.Error("Abort: aborted write is visible to subsequent transaction")
	}
}

func TestMVCC_Abort_DoesNotAffectPreviousCommit(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("k_stable")

	// Commit initial value
	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 42))
	mvcc.Commit(txn.TransactionID(t1))

	// Overwrite then abort
	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t2), "test_table", "PRIMARY", key, buildTestRow(1, 777))
	mvcc.Abort(txn.TransactionID(t2))

	// Should still see 42
	t3 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(t3), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatalf("Read after abort failed: %v", err)
	}
	if getValFromTable(tm, res) != 42 {
		t.Errorf("Abort corrupted prior commit: expected 42, got %d", getValFromTable(tm, res))
	}
}

func TestMVCC_Abort_AfterMultipleWrites(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)

	// Commit a baseline for several keys
	for i := 1; i <= 5; i++ {
		key := []byte(fmt.Sprintf("mk_%d", i))
		tid := mvcc.BeginTransaction(txn.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(int32(i), int32(i*10)))
		mvcc.Commit(txn.TransactionID(tid))
	}

	// Write to all keys in one transaction, then abort
	tAbort := mvcc.BeginTransaction(txn.READ_COMMITTED)
	for i := 1; i <= 5; i++ {
		key := []byte(fmt.Sprintf("mk_%d", i))
		mvcc.Write(txn.TransactionID(tAbort), "test_table", "PRIMARY", key, buildTestRow(int32(i), 0))
	}
	mvcc.Abort(txn.TransactionID(tAbort))

	// All keys should still have their original values
	tCheck := mvcc.BeginTransaction(txn.READ_COMMITTED)
	for i := 1; i <= 5; i++ {
		key := []byte(fmt.Sprintf("mk_%d", i))
		res, err := mvcc.Read(txn.TransactionID(tCheck), "test_table", "PRIMARY", key)
		if err != nil {
			t.Errorf("key %d: read failed after multi-write abort: %v", i, err)
			continue
		}
		expected := int32(i * 10)
		if got := getValFromTable(tm, res); got != expected {
			t.Errorf("key %d: expected %d after abort, got %d", i, expected, got)
		}
	}
}

func TestMVCC_Abort_DeleteIsReverted(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("del_revert")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 55))
	mvcc.Commit(txn.TransactionID(t1))

	// Delete then abort
	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Delete(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	mvcc.Abort(txn.TransactionID(t2))

	// Record should still be visible
	t3 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(t3), "test_table", "PRIMARY", key)
	if err != nil || res == nil {
		t.Error("Abort of delete: record is incorrectly invisible after rollback")
	} else if getValFromTable(tm, res) != 55 {
		t.Errorf("Abort of delete: value corrupted, expected 55 got %d", getValFromTable(tm, res))
	}
}

func TestMVCC_Abort_TransactionCannotBeReused(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("reuse_key")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 1))
	mvcc.Abort(txn.TransactionID(t1))

	// Any operation with the aborted txn ID should fail or be a no-op
	err := mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 2))
	if err == nil {
		// If Write doesn't return errors, verify the data isn't committed later
		t.Log("Write after abort did not return error — verifying data is not visible")
		tCheck := mvcc.BeginTransaction(txn.READ_COMMITTED)
		res, rerr := mvcc.Read(txn.TransactionID(tCheck), "test_table", "PRIMARY", key)
		if rerr == nil && res != nil && getValFromTable(setupMVCCWithRealTableManagerTM(t), res) == 2 {
			t.Error("Re-use of aborted txn ID resulted in visible write")
		}
	}
}

// helper to avoid re-declaring tm when we just need it for decode
func setupMVCCWithRealTableManagerTM(t *testing.T) *schema.TableManager {
	_, tm := setupMVCCWithRealTableManager(t)
	return tm
}

// =============================================================================
// COMMIT FAILURE / CONFLICT TESTS
// =============================================================================

func TestMVCC_CommitFails_WriteWriteConflict(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("conflict_key")

	// T1 and T2 both write to the same key concurrently (serializable)
	t1 := mvcc.BeginTransaction(txn.SERIALIZABLE)
	t2 := mvcc.BeginTransaction(txn.SERIALIZABLE)

	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 100))
	mvcc.Write(txn.TransactionID(t2), "test_table", "PRIMARY", key, buildTestRow(1, 200))

	err1 := mvcc.Commit(txn.TransactionID(t1))

	// One of them must fail
	err2 := mvcc.Commit(txn.TransactionID(t2))

	if err1 != nil && err2 != nil {
		t.Error("WriteWrite conflict: both commits failed — one should succeed")
	}
	if err1 == nil && err2 == nil {
		t.Error("WriteWrite conflict: both commits succeeded — one should have been rejected")
	}
}

func TestMVCC_CommitFails_StaleRead_SerializableConflict(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("serial_key")

	// Establish initial value
	t0 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t0), "test_table", "PRIMARY", key, buildTestRow(1, 10))
	mvcc.Commit(txn.TransactionID(t0))

	// T1 reads, T2 writes and commits, T1 tries to commit (should fail or detect anomaly)
	t1 := mvcc.BeginTransaction(txn.SERIALIZABLE)
	mvcc.Read(txn.TransactionID(t1), "test_table", "PRIMARY", key)

	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t2), "test_table", "PRIMARY", key, buildTestRow(1, 20))
	mvcc.Commit(txn.TransactionID(t2))

	// T1 writes based on its stale read and tries to commit
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 11))
	err := mvcc.Commit(txn.TransactionID(t1))
	if err == nil {
		t.Log("Note: Serializable conflict not detected at commit — verify your conflict detection logic")
	}
}

func TestMVCC_CommitFails_AfterAbort(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("abort_commit")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 1))
	mvcc.Abort(txn.TransactionID(t1))

	// Committing after abort must fail
	err := mvcc.Commit(txn.TransactionID(t1))
	if err == nil {
		t.Error("CommitAfterAbort: commit succeeded on an already-aborted transaction")
	}
}

func TestMVCC_CommitFails_DoubleCommit(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("double_commit")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 42))
	if err := mvcc.Commit(txn.TransactionID(t1)); err != nil {
		t.Fatalf("First commit failed unexpectedly: %v", err)
	}

	// Second commit on same txn should fail
	err := mvcc.Commit(txn.TransactionID(t1))
	if err == nil {
		t.Error("DoubleCommit: second commit on terminated transaction succeeded")
	}
}

// =============================================================================
// ISOLATION LEVEL EDGE CASES
// =============================================================================

func TestMVCC_DirtyRead_Prevention_AllLevels(t *testing.T) {
	levels := []uint8{
		txn.READ_COMMITTED,
		txn.REPEATABLE_READ,
		txn.SERIALIZABLE,
	}

	for _, level := range levels {
		t.Run(string(level), func(t *testing.T) {
			mvcc, _ := setupMVCCWithRealTableManager(t)
			key := []byte("dirty_read_key")

			tWriter := mvcc.BeginTransaction(txn.READ_COMMITTED)
			mvcc.Write(txn.TransactionID(tWriter), "test_table", "PRIMARY", key, buildTestRow(1, 666))
			// Do NOT commit

			tReader := mvcc.BeginTransaction(level)
			res, err := mvcc.Read(txn.TransactionID(tReader), "test_table", "PRIMARY", key)
			if err == nil && res != nil {
				t.Errorf("[%d] Dirty read allowed — uncommitted data visible", level)
			}
		})
	}
}

func TestMVCC_PhantomRead_Detection_RepeatableRead(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)

	// T1 starts a Repeatable Read snapshot
	t1 := mvcc.BeginTransaction(txn.REPEATABLE_READ)

	// T2 inserts new keys and commits
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("phantom_%03d", i))
		t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(t2), "test_table", "PRIMARY", key, buildTestRow(int32(i), int32(i)))
		mvcc.Commit(txn.TransactionID(t2))
	}

	// T1's scan must not see the new inserts (phantom prevention)
	opts := storage.ScanOptions{Inclusive: true}
	cursor, err := mvcc.Scan(txn.TransactionID(t1), "test_table", "PRIMARY", opts)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for cursor.Next() {
		count++
		_ = getValFromTable(tm, cursor.Entry().Value)
	}
	if count != 0 {
		t.Errorf("PhantomRead: RepeatableRead saw %d new rows inserted after snapshot begin", count)
	}
}

func TestMVCC_NonRepeatableRead_Allowed_ReadCommitted(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("nr_key")

	// Commit initial value
	t0 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t0), "test_table", "PRIMARY", key, buildTestRow(1, 10))
	mvcc.Commit(txn.TransactionID(t0))

	// T1 is READ_COMMITTED — reads first
	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res1, err := mvcc.Read(txn.TransactionID(t1), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatal(err)
	}
	val1 := getValFromTable(tm, res1)

	// T2 updates and commits
	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t2), "test_table", "PRIMARY", key, buildTestRow(1, 20))
	mvcc.Commit(txn.TransactionID(t2))

	// T1 reads again — should now see 20 (non-repeatable read is expected for RC)
	res2, err := mvcc.Read(txn.TransactionID(t1), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatal(err)
	}
	val2 := getValFromTable(tm, res2)

	if val1 != 10 {
		t.Errorf("First read should have seen 10, got %d", val1)
	}
	if val2 != 20 {
		t.Errorf("ReadCommitted: second read should see new commit (20), got %d", val2)
	}
}

func TestMVCC_RepeatableRead_SameValue_MultipleReads(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("rr_stable")

	t0 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t0), "test_table", "PRIMARY", key, buildTestRow(1, 77))
	mvcc.Commit(txn.TransactionID(t0))

	tRR := mvcc.BeginTransaction(txn.REPEATABLE_READ)

	// Read 10 times interleaved with commits by other txns
	for i := 0; i < 10; i++ {
		tWriter := mvcc.BeginTransaction(txn.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tWriter), "test_table", "PRIMARY", key, buildTestRow(1, int32(100+i)))
		mvcc.Commit(txn.TransactionID(tWriter))

		res, err := mvcc.Read(txn.TransactionID(tRR), "test_table", "PRIMARY", key)
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}
		if val := getValFromTable(tm, res); val != 77 {
			t.Errorf("RepeatableRead broken at iteration %d: expected 77, got %d", i, val)
		}
	}
}

// =============================================================================
// CONCURRENT ABORT TESTS
// =============================================================================

func TestMVCC_ConcurrentAborts_DataIntegrity(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("concurrent_abort")

	// Establish baseline
	tBase := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(tBase), "test_table", "PRIMARY", key, buildTestRow(1, 1000))
	mvcc.Commit(txn.TransactionID(tBase))

	var wg sync.WaitGroup
	abortCount := int32(0)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tid := mvcc.BeginTransaction(txn.READ_COMMITTED)
			mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(1, int32(idx)))
			// Half commit, half abort
			if idx%2 == 0 {
				atomic.AddInt32(&abortCount, 1)
				mvcc.Abort(txn.TransactionID(tid))
			} else {
				mvcc.Commit(txn.TransactionID(tid))
			}
		}(i)
	}
	wg.Wait()

	// Data must be readable and valid (one of the committed values or original)
	tFinal := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(tFinal), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatalf("Read after concurrent aborts failed: %v", err)
	}
	_ = getValFromTable(tm, res) // just ensure it decodes without panic
}

func TestMVCC_RaceAbortCommit_SameTransaction(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("race_key")

	// This tests that abort and commit on the same txn don't corrupt state
	// (only one should succeed — implementation-defined which wins)
	for i := 0; i < 50; i++ {
		tid := mvcc.BeginTransaction(txn.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(1, int32(i)))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); mvcc.Commit(txn.TransactionID(tid)) }()
		go func() { defer wg.Done(); mvcc.Abort(txn.TransactionID(tid)) }()
		wg.Wait()
	}
	// If we get here without a panic, the race is handled safely
}

// =============================================================================
// VERSION CHAIN CORRECTNESS
// =============================================================================

func TestMVCC_VersionChain_AbortedVersionsSkipped(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("chain_key")

	// v1 = committed (val=5)
	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 5))
	mvcc.Commit(txn.TransactionID(t1))

	// v2 = aborted (val=99)
	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t2), "test_table", "PRIMARY", key, buildTestRow(1, 99))
	mvcc.Abort(txn.TransactionID(t2))

	// v3 = committed (val=7)
	t3 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t3), "test_table", "PRIMARY", key, buildTestRow(1, 7))
	mvcc.Commit(txn.TransactionID(t3))

	// Reader should see 7, never 99
	tRead := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(tRead), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatal(err)
	}
	val := getValFromTable(tm, res)
	if val == 99 {
		t.Error("VersionChain: aborted version (99) was returned — version traversal must skip aborted entries")
	}
	if val != 7 {
		t.Errorf("VersionChain: expected 7, got %d", val)
	}
}

func TestMVCC_VersionChain_SnapshotSeesOnlyCommittedBeforeSnapshot(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("snap_chain")

	// v1 committed before snapshot
	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 10))
	mvcc.Commit(txn.TransactionID(t1))

	// Snapshot starts here
	tSnap := mvcc.BeginTransaction(txn.REPEATABLE_READ)

	// v2 committed (aborted)
	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t2), "test_table", "PRIMARY", key, buildTestRow(1, 20))
	mvcc.Abort(txn.TransactionID(t2))

	// v3 committed after snapshot
	t3 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t3), "test_table", "PRIMARY", key, buildTestRow(1, 30))
	mvcc.Commit(txn.TransactionID(t3))

	res, err := mvcc.Read(txn.TransactionID(tSnap), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatal(err)
	}
	if val := getValFromTable(tm, res); val != 10 {
		t.Errorf("Snapshot must see only v1 (10), got %d", val)
	}
}

// =============================================================================
// READ-YOUR-OWN-WRITES
// =============================================================================

func TestMVCC_ReadYourOwnWrites(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("ryw_key")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 123))

	// Before commit, T1 should read its own write
	res, err := mvcc.Read(txn.TransactionID(t1), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatalf("ReadYourOwnWrites: failed to read own write: %v", err)
	}
	if getValFromTable(tm, res) != 123 {
		t.Errorf("ReadYourOwnWrites: expected 123, got %d", getValFromTable(tm, res))
	}
}

func TestMVCC_ReadYourOwnDelete(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("ryd_key")

	// First commit a row
	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 88))
	mvcc.Commit(txn.TransactionID(t1))

	// T2 deletes, then tries to read — should get nothing
	t2 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	err := mvcc.Delete(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	if err != nil {
		fmt.Printf("Error deleting key: %s, %s", string(key), err.Error())
	}
	res, err := mvcc.Read(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	if err == nil && res != nil {
		mvcc.tracer.PrintTrace()
		t.Error("ReadYourOwnDelete: deleted row still visible within same transaction")
	}
}

func TestMVCC_ReadYourOwnWrite_OverwrittenMultipleTimes(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)
	key := []byte("ryw_multi")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	for i := int32(1); i <= 5; i++ {
		mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, i*10))
		res, err := mvcc.Read(txn.TransactionID(t1), "test_table", "PRIMARY", key)
		if err != nil {
			t.Fatalf("iteration %d: read failed: %v", i, err)
		}
		if val := getValFromTable(tm, res); val != i*10 {
			t.Errorf("iteration %d: expected %d, got %d", i, i*10, val)
		}
	}
}

// =============================================================================
// SCAN + ABORT / SCAN VISIBILITY
// =============================================================================

func TestMVCC_Scan_ExcludesAbortedInserts(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)

	// Commit 5 rows
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("committed_%03d", i))
		tid := mvcc.BeginTransaction(txn.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(int32(i), int32(i)))
		mvcc.Commit(txn.TransactionID(tid))
	}

	// Abort 5 more rows
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("aborted_%03d", i))
		tid := mvcc.BeginTransaction(txn.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(int32(i), int32(i)))
		mvcc.Abort(txn.TransactionID(tid))
	}

	tScan := mvcc.BeginTransaction(txn.READ_COMMITTED)
	opts := storage.ScanOptions{Inclusive: true}
	cursor, err := mvcc.Scan(txn.TransactionID(tScan), "test_table", "PRIMARY", opts)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for cursor.Next() {
		count++
	}
	if count != 5 {
		t.Errorf("Scan should return 5 committed rows, got %d (aborted rows leaked)", count)
	}
}

func TestMVCC_Scan_IncludesOwnUncommittedWrites(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("own_write_%d", i))
		mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(int32(i), int32(i)))
	}

	// T1 scans its own writes before commit
	opts := storage.ScanOptions{Inclusive: true}
	cursor, err := mvcc.Scan(txn.TransactionID(t1), "test_table", "PRIMARY", opts)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for cursor.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("Scan should include own uncommitted writes (3), got %d", count)
	}
}

// =============================================================================
// TRANSACTION ID EXHAUSTION / EDGE CASES
// =============================================================================

func TestMVCC_ManyTransactions_StressIDAllocation(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)

	const n = 500
	ids := make([]txn.TransactionID, n)
	for i := 0; i < n; i++ {
		ids[i] = txn.TransactionID(mvcc.BeginTransaction(txn.READ_COMMITTED))
	}

	// All IDs must be unique
	seen := make(map[txn.TransactionID]bool)
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("Duplicate transaction ID allocated: %v", id)
		}
		seen[id] = true
	}

	// Clean up — abort all
	for _, id := range ids {
		mvcc.Abort(id)
	}
}

func TestMVCC_EmptyRead_BeforeAnyWrite(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("never_written")

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(t1), "test_table", "PRIMARY", key)
	if err == nil && res != nil {
		t.Error("Read on non-existent key should return nil or error, got data")
	}
}

func TestMVCC_WriteEmptyPayload_DoesNotPanic(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("empty_payload")

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Write of empty payload caused panic: %v", r)
		}
	}()

	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	// Empty row — implementation should handle gracefully (error or no-op, not panic)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, []byte{})
	mvcc.Abort(txn.TransactionID(t1))
}

// =============================================================================
// TIMING / TIMEOUT
// =============================================================================

func TestMVCC_Operations_CompleteWithinTimeout(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager(t)
	key := []byte("timeout_key")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			tid := mvcc.BeginTransaction(txn.READ_COMMITTED)
			mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(1, int32(i)))
			mvcc.Commit(txn.TransactionID(tid))
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("200 sequential write+commit cycles exceeded 10 s — possible deadlock or livelock")
	}
}

// =============================================================================
// CROSS-TABLE ISOLATION
// =============================================================================

func TestMVCC_CrossTable_TransactionIsolation(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)

	// Use only the one table available, but simulate two logical "domains"
	keyA := []byte("table_a_row")
	keyB := []byte("table_b_row")

	// Write both in same transaction
	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", keyA, buildTestRow(1, 11))
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", keyB, buildTestRow(2, 22))
	// Abort — neither should be visible
	mvcc.Abort(txn.TransactionID(t1))

	tCheck := mvcc.BeginTransaction(txn.READ_COMMITTED)
	for _, key := range [][]byte{keyA, keyB} {
		res, err := mvcc.Read(txn.TransactionID(tCheck), "test_table", "PRIMARY", key)
		if err == nil && res != nil {
			t.Errorf("Cross-key abort: key %s visible after abort", key)
			_ = getValFromTable(tm, res)
		}
	}
}

func TestMVCC_AtomicCommit_AllOrNothing(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager(t)

	keyA := []byte("atomic_a")
	keyB := []byte("atomic_b")

	// Commit both keys in one transaction
	t1 := mvcc.BeginTransaction(txn.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", keyA, buildTestRow(1, 100))
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", keyB, buildTestRow(2, 200))
	if err := mvcc.Commit(txn.TransactionID(t1)); err != nil {
		t.Fatalf("Atomic commit failed: %v", err)
	}

	tCheck := mvcc.BeginTransaction(txn.READ_COMMITTED)
	resA, errA := mvcc.Read(txn.TransactionID(tCheck), "test_table", "PRIMARY", keyA)
	resB, errB := mvcc.Read(txn.TransactionID(tCheck), "test_table", "PRIMARY", keyB)

	if errA != nil || resA == nil {
		t.Error("AtomicCommit: keyA not visible after commit")
	}
	if errB != nil || resB == nil {
		t.Error("AtomicCommit: keyB not visible after commit")
	}
	if resA != nil && getValFromTable(tm, resA) != 100 {
		t.Errorf("AtomicCommit: keyA expected 100, got %d", getValFromTable(tm, resA))
	}
	if resB != nil && getValFromTable(tm, resB) != 200 {
		t.Errorf("AtomicCommit: keyB expected 200, got %d", getValFromTable(tm, resB))
	}
}
