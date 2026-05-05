package isolation

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/mocks"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

// --- Test Environment Setup ---

func setupMVCCWithRealTableManager() (*MVCCManager, *schema.TableManager) {
	tm := schema.NewTableManager()

	cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt32},
		{Name: "val", Type: schema.TypeInt32},
	}
	ts := schema.NewTableSchema("test_table", cols)
	ts.AddIndex("PRIMARY", []string{"id"}, mocks.NewMockStorage())
	tm.CreateTable(ts, true)

	// Note: Ensure your MVCCManager has access to the TableManager internally
	mvcc := NewMVCCManager(nil, nil, nil, tm)
	return mvcc, tm
}

// Fixed: Adds the 0x01 metadata byte required by the TableSchema
func buildTestRow(id, val int32) []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Metadata byte (_txn_op)
	binary.Write(buf, binary.BigEndian, id)
	binary.Write(buf, binary.BigEndian, val)
	return buf.Bytes()
}

// Fixed: Uses the TableManager to decode so we don't guess offsets
func getValFromTable(tm *schema.TableManager, payload []byte) int32 {
	return int32(tm.DecodeRow("test_table", "val", payload).Int())
}

func TestMVCC_ReadCommitted_Visibility(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager()
	key := []byte("k1")

	t1 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 100))

	t2 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)

	// Should not see T1's uncommitted write
	res, err := mvcc.Read(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	if err == nil && res != nil {
		t.Error("ReadCommitted: T2 saw uncommitted data")
	}

	mvcc.Commit(txn.TransactionID(t1))

	// T2 (Read Committed) should now see the data upon its next read
	res, err = mvcc.Read(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatalf("ReadCommitted: T2 failed to see committed data: %v", err)
	}

	if getValFromTable(tm, res) != 100 {
		t.Errorf("Expected 100, got %d", getValFromTable(tm, res))
	}
}

func TestMVCC_RepeatableRead_Snapshot(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager()
	key := []byte("k1")

	// T0 initializes data
	t0 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t0), "test_table", "PRIMARY", key, buildTestRow(1, 10))
	mvcc.Commit(txn.TransactionID(t0))

	// T1 starts Repeatable Read (captures current snapshot)
	t1 := mvcc.BeginTransaction(txn_unit.REPEATABLE_READ)

	// T2 updates data and commits
	t2 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
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
	mvcc, _ := setupMVCCWithRealTableManager()
	key := []byte("k1")

	t1 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 50))
	mvcc.Commit(txn.TransactionID(t1))

	t2 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Delete(txn.TransactionID(t2), "test_table", "PRIMARY", key)

	// T3 should still see the record because T2 is not committed
	t3 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(t3), "test_table", "PRIMARY", key)
	if err != nil || res == nil {
		t.Error("Delete: uncommitted delete made record invisible to T3")
	}

	mvcc.Commit(txn.TransactionID(t2))

	// T4 should not see the record anymore
	t4 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	_, err = mvcc.Read(txn.TransactionID(t4), "test_table", "PRIMARY", key)
	if err == nil {
		t.Error("Delete: committed delete still visible to T4")
	}
}

func TestMVCC_MultipleVersions_Scanning(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager()
	key := []byte("k1")

	// Write 3 versions
	for i := 1; i <= 3; i++ {
		tid := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(1, int32(i*10)))
		mvcc.Commit(txn.TransactionID(tid))
	}

	// Read should return the latest committed version (30)
	tRead := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
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
	mvcc, tm := setupMVCCWithRealTableManager()
	key := []byte("counter")

	// Initialize counter at 0
	t0 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
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
				// In a real DB, you'd have a lock manager here.
				// For MVCC test, we simulate sequential updates.
				tid := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
				res, _ := mvcc.Read(txn.TransactionID(tid), "test_table", "PRIMARY", key)

				currentVal := getValFromTable(tm, res)
				mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(1, currentVal+1))
				mvcc.Commit(txn.TransactionID(tid))
			}
		}()
	}

	wg.Wait()

	tFinal := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	res, _ := mvcc.Read(txn.TransactionID(tFinal), "test_table", "PRIMARY", key)
	finalVal := getValFromTable(tm, res)

	expected := int32(numWorkers * incrementsPerWorker)
	if finalVal != expected {
		t.Errorf("Lost Update Detected: Expected %d, got %d", expected, finalVal)
	}
}

func TestMVCC_LongRunningSnapshot_Integrity(t *testing.T) {
	mvcc, tm := setupMVCCWithRealTableManager()
	key := []byte("stable_key")

	// 1. Setup initial state
	tInit := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(tInit), "test_table", "PRIMARY", key, buildTestRow(1, 100))
	mvcc.Commit(txn.TransactionID(tInit))

	// 2. Start the "Snapshot" transaction
	tSnapshot := mvcc.BeginTransaction(txn_unit.REPEATABLE_READ)

	// 3. Perform 1,000 updates in other transactions
	for i := 1; i <= 1000; i++ {
		tx := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
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
	mvcc, _ := setupMVCCWithRealTableManager()
	const totalKeys = 1000

	// 1. Insert 1000 keys
	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		tid := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(int32(i), int32(i)))
		mvcc.Commit(txn.TransactionID(tid))
	}

	// 2. Scan and verify count
	tScan := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
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
