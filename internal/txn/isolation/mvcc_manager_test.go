package isolation

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/mocks"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

// --- Test Environment Setup ---

func setupMVCCWithRealTableManager() (*MVCCManager, *schema.TableManager) {
	tm := schema.NewTableManager()

	// Define schema: id(int32), val(int32)
	cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt32},
		{Name: "val", Type: schema.TypeInt32},
	}
	ts := schema.NewTableSchema("test_table", cols)

	// Add indexes with mock storage engines
	ts.AddIndex("PRIMARY", []string{"id"}, mocks.NewMockStorage())
	tm.CreateTable(ts, true)

	mvcc := NewMVCCManager(nil, nil, nil, tm)
	return mvcc, tm
}

func buildTestRow(id, val int32) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, id)
	binary.Write(buf, binary.BigEndian, val)
	return buf.Bytes()
}

func getValFromPayload(payload []byte) int32 {
	// payload[0] is OpInsert/OpDelete, data starts at payload[1]
	// id is 4 bytes, val is next 4 bytes
	return int32(binary.BigEndian.Uint32(payload[5:9]))
}

// --- Isolation Tests ---

func TestMVCC_ReadCommitted_Visibility(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager()
	key := []byte("k1")

	// T1 writes but does not commit
	t1 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 100))

	// T2 starts
	t2 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)

	// T2 should not see T1's uncommitted write
	_, err := mvcc.Read(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	if err == nil {
		t.Error("ReadCommitted: T2 saw uncommitted data")
	}

	// T1 commits
	mvcc.Commit(txn.TransactionID(t1))

	// T2 should now see the data
	res, err := mvcc.Read(txn.TransactionID(t2), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatalf("ReadCommitted: T2 failed to see committed data: %v", err)
	}
	if getValFromPayload(res) != 100 {
		t.Errorf("ReadCommitted: Expected 100, got %d", getValFromPayload(res))
	}
}

func TestMVCC_RepeatableRead_Snapshot(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager()
	key := []byte("k1")

	// T0 initializes data
	t0 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t0), "test_table", "PRIMARY", key, buildTestRow(1, 10))
	mvcc.Commit(txn.TransactionID(t0))

	// T1 starts Repeatable Read (captures snapshot)
	t1 := mvcc.BeginTransaction(txn_unit.REPEATABLE_READ)

	// T2 updates data and commits
	t2 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t2), "test_table", "PRIMARY", key, buildTestRow(1, 20))
	mvcc.Commit(txn.TransactionID(t2))

	// T1 reads
	// Should see initial value (10), not updated value (20)
	res, err := mvcc.Read(txn.TransactionID(t1), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatal(err)
	}
	if getValFromPayload(res) != 10 {
		t.Errorf("RepeatableRead: T1 saw T2's update. Got %d, want 10", getValFromPayload(res))
	}
}

// --- Consistency & Tombstone Tests ---

func TestMVCC_Delete_Isolation(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager()
	key := []byte("k1")

	// Initialize
	t1 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", key, buildTestRow(1, 50))
	mvcc.Commit(txn.TransactionID(t1))

	// T2 deletes but uncommitted
	t2 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Delete(txn.TransactionID(t2), "test_table", "PRIMARY", key)

	// T3 should still see the record
	t3 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(t3), "test_table", "PRIMARY", key)
	if err != nil || res == nil {
		t.Error("Delete: uncommitted delete made record invisible to T3")
	}

	// T2 commits
	mvcc.Commit(txn.TransactionID(t2))

	// T4 should not see the record
	t4 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	_, err = mvcc.Read(txn.TransactionID(t4), "test_table", "PRIMARY", key)
	if err == nil {
		t.Error("Delete: committed delete still visible to T4")
	}
}

func TestMVCC_Rollback_Cleanup(t *testing.T) {
	// Note: Testing actual physical cleanup requires RollbackManager implementation.
	// This test verifies transaction state cleanup in MVCCManager.
	mvcc, _ := setupMVCCWithRealTableManager()

	// Mock rollback manager to avoid nil panics if Abort calls it
	mvcc.rollbackManager = &rollback.RollbackManager{}

	t1 := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	mvcc.Write(txn.TransactionID(t1), "test_table", "PRIMARY", []byte("k1"), buildTestRow(1, 10))

	if err := mvcc.Abort(txn.TransactionID(t1)); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	mvcc.mu.RLock()
	_, exists := mvcc.transactions[txn.TransactionID(t1)]
	mvcc.mu.RUnlock()

	if exists {
		t.Error("Transaction still exists in manager after Abort")
	}
}

func TestMVCC_MultipleVersions_Scanning(t *testing.T) {
	mvcc, _ := setupMVCCWithRealTableManager()
	key := []byte("k1")

	// Write 3 versions
	for i := 1; i <= 3; i++ {
		tid := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
		mvcc.Write(txn.TransactionID(tid), "test_table", "PRIMARY", key, buildTestRow(1, int32(i*10)))
		mvcc.Commit(txn.TransactionID(tid))
	}

	// Read should return the latest version (30)
	tRead := mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	res, err := mvcc.Read(txn.TransactionID(tRead), "test_table", "PRIMARY", key)
	if err != nil {
		t.Fatal(err)
	}
	if getValFromPayload(res) != 30 {
		t.Errorf("Expected latest version 30, got %d", getValFromPayload(res))
	}
}
