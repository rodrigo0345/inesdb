package database

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/synchronization"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

func TestOpenMVCCLSM(t *testing.T) {
	tmp := t.TempDir()

	engine, err := OpenMVCCLSM(Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	if engine == nil {
		t.Fatal("OpenMVCCLSM() returned nil engine")
	}
	if engine.IsolationManager() == nil {
		t.Fatal("expected isolation manager")
	}
	if engine.StorageEngine() == nil {
		t.Fatal("expected storage engine")
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestEngineCreateTableAndTxnRoundTrip(t *testing.T) {
	tmp := t.TempDir()

	engine, err := OpenMVCCLSM(Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	ts := schema.NewTableSchema("users", "id")
	if err := ts.AddColumn("id", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn() error = %v", err)
	}
	if err := ts.AddColumn("name", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn() error = %v", err)
	}

	if err := engine.CreateTable(ts); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	txnID := engine.BeginTransaction(txn_unit.SERIALIZABLE, "users", ts)
	if err := engine.Write(txnID, []byte("user-1"), []byte(`{"id":"user-1","name":"Ada"}`)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Commit(txnID); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	readTxn := engine.BeginTransaction(txn_unit.READ_COMMITTED, "users", ts)
	value, err := engine.Read(readTxn, []byte("user-1"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(value) != `{"id":"user-1","name":"Ada"}` {
		t.Fatalf("unexpected value: %s", string(value))
	}
	if err := engine.Commit(readTxn); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func TestEngineScanUsesLSMBackend(t *testing.T) {
	tmp := t.TempDir()

	engine, err := OpenMVCCLSM(Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	ts := schema.NewTableSchema("users", "id")
	if err := ts.AddColumn("id", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn() error = %v", err)
	}
	if err := ts.AddColumn("name", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn() error = %v", err)
	}
	if err := engine.CreateTable(ts); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	insertTxn := engine.BeginTransaction(txn_unit.SERIALIZABLE, "users", ts)
	for _, kv := range []struct {
		key   string
		value string
	}{
		{key: "alpha", value: `{"id":"alpha","name":"Ada"}`},
		{key: "bravo", value: `{"id":"bravo","name":"Bea"}`},
		{key: "charlie", value: `{"id":"charlie","name":"Cid"}`},
	} {
		if err := engine.Write(insertTxn, []byte(kv.key), []byte(kv.value)); err != nil {
			t.Fatalf("Write(%s) error = %v", kv.key, err)
		}
	}
	if err := engine.Commit(insertTxn); err != nil {
		t.Fatalf("Commit(insertTxn) error = %v", err)
	}

	updateTxn := engine.BeginTransaction(txn_unit.SERIALIZABLE, "users", ts)
	if err := engine.Write(updateTxn, []byte("bravo"), []byte(`{"id":"bravo","name":"Bea-Updated"}`)); err != nil {
		t.Fatalf("Write(update bravo) error = %v", err)
	}
	if err := engine.Delete(updateTxn, []byte("alpha")); err != nil {
		t.Fatalf("Delete(alpha) error = %v", err)
	}
	if err := engine.Commit(updateTxn); err != nil {
		t.Fatalf("Commit(updateTxn) error = %v", err)
	}

	entries, err := engine.Scan([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	got := make(map[string]string, len(entries))
	for _, entry := range entries {
		got[string(entry.Key)] = string(entry.Value)
	}

	want := map[string]string{
		"bravo":   `{"id":"bravo","name":"Bea-Updated"}`,
		"charlie": `{"id":"charlie","name":"Cid"}`,
	}

	if len(got) != len(want) {
		t.Fatalf("Scan() entry count = %d, want %d", len(got), len(want))
	}
	for key, wantValue := range want {
		if got[key] != wantValue {
			t.Fatalf("Scan() key %q = %q, want %q", key, got[key], wantValue)
		}
	}
	if _, exists := got["alpha"]; exists {
		t.Fatalf("Scan() unexpectedly returned deleted key alpha")
	}
}

func TestEngineUsesSeparateTableBackends(t *testing.T) {
	tmp := t.TempDir()

	engine, err := OpenMVCCLSM(Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	users := schema.NewTableSchema("users", "id")
	if err := users.AddColumn("id", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn(users.id) error = %v", err)
	}
	if err := users.AddColumn("name", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn(users.name) error = %v", err)
	}

	orders := schema.NewTableSchema("orders", "id")
	if err := orders.AddColumn("id", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn(orders.id) error = %v", err)
	}
	if err := orders.AddColumn("status", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn(orders.status) error = %v", err)
	}

	if err := engine.CreateTable(users); err != nil {
		t.Fatalf("CreateTable(users) error = %v", err)
	}
	if err := engine.CreateTable(orders); err != nil {
		t.Fatalf("CreateTable(orders) error = %v", err)
	}
	if engine.TableStorageEngine("users") == nil || engine.TableStorageEngine("orders") == nil {
		t.Fatal("expected table-specific storage engines")
	}
	if engine.TableStorageEngine("users") == engine.TableStorageEngine("orders") {
		t.Fatal("expected distinct storage engines per table")
	}

	userTxn := engine.BeginTransaction(txn_unit.SERIALIZABLE, "users", users)
	if err := engine.Write(userTxn, []byte("shared-key"), []byte(`{"id":"shared-key","name":"Ada"}`)); err != nil {
		t.Fatalf("Write(users) error = %v", err)
	}
	if err := engine.Commit(userTxn); err != nil {
		t.Fatalf("Commit(users) error = %v", err)
	}

	orderTxn := engine.BeginTransaction(txn_unit.SERIALIZABLE, "orders", orders)
	if err := engine.Write(orderTxn, []byte("shared-key"), []byte(`{"id":"shared-key","status":"Pending"}`)); err != nil {
		t.Fatalf("Write(orders) error = %v", err)
	}
	if err := engine.Commit(orderTxn); err != nil {
		t.Fatalf("Commit(orders) error = %v", err)
	}

	readUserTxn := engine.BeginTransaction(txn_unit.READ_COMMITTED, "users", users)
	userValue, err := engine.Read(readUserTxn, []byte("shared-key"))
	if err != nil {
		t.Fatalf("Read(users) error = %v", err)
	}
	if string(userValue) != `{"id":"shared-key","name":"Ada"}` {
		t.Fatalf("Read(users) = %s, want users value", string(userValue))
	}
	if err := engine.Commit(readUserTxn); err != nil {
		t.Fatalf("Commit(readUsers) error = %v", err)
	}

	readOrderTxn := engine.BeginTransaction(txn_unit.READ_COMMITTED, "orders", orders)
	orderValue, err := engine.Read(readOrderTxn, []byte("shared-key"))
	if err != nil {
		t.Fatalf("Read(orders) error = %v", err)
	}
	if string(orderValue) != `{"id":"shared-key","status":"Pending"}` {
		t.Fatalf("Read(orders) = %s, want orders value", string(orderValue))
	}
	if err := engine.Commit(readOrderTxn); err != nil {
		t.Fatalf("Commit(readOrders) error = %v", err)
	}

	entries, err := engine.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Scan() entry count = %d, want 2", len(entries))
	}

	var sawUser, sawOrder bool
	for _, entry := range entries {
		switch string(entry.Value) {
		case `{"id":"shared-key","name":"Ada"}`:
			sawUser = true
		case `{"id":"shared-key","status":"Pending"}`:
			sawOrder = true
		}
	}
	if !sawUser || !sawOrder {
		t.Fatalf("Scan() did not return both table values: sawUser=%v sawOrder=%v", sawUser, sawOrder)
	}
}

func TestOpenMVCCLSM_ReplaysCommittedTransactionsOnStartup(t *testing.T) {
	tmp := t.TempDir()
	opts := Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	}

	firstEngine, err := OpenMVCCLSM(opts)
	if err != nil {
		t.Fatalf("OpenMVCCLSM() first open error = %v", err)
	}

	users := schema.NewTableSchema("users", "id")
	if err := users.AddColumn("id", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn(users.id) error = %v", err)
	}
	if err := users.AddColumn("name", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn(users.name) error = %v", err)
	}
	if err := firstEngine.CreateTable(users); err != nil {
		t.Fatalf("CreateTable(users) error = %v", err)
	}

	txnID := firstEngine.BeginTransaction(txn_unit.SERIALIZABLE, "users", users)
	if err := firstEngine.Write(txnID, []byte("user-1"), []byte(`{"id":"user-1","name":"Grace"}`)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := firstEngine.Commit(txnID); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if err := firstEngine.Close(); err != nil {
		t.Fatalf("Close() first engine error = %v", err)
	}

	secondEngine, err := OpenMVCCLSM(opts)
	if err != nil {
		t.Fatalf("OpenMVCCLSM() second open error = %v", err)
	}
	defer func() { _ = secondEngine.Close() }()

	if secondEngine.TableStorageEngine("users") == nil {
		t.Fatal("expected users table storage engine after restart")
	}

	readTxn := secondEngine.BeginTransaction(txn_unit.READ_COMMITTED, "users", users)
	value, err := secondEngine.Read(readTxn, []byte("user-1"))
	if err != nil {
		t.Fatalf("Read() after restart error = %v", err)
	}
	if string(value) != `{"id":"user-1","name":"Grace"}` {
		t.Fatalf("Read() after restart = %s, want recovered value", string(value))
	}
	if err := secondEngine.Commit(readTxn); err != nil {
		t.Fatalf("Commit(readTxn) error = %v", err)
	}
}

func TestOpenMVCCLSM_SeedsTxnIDFromRecovery(t *testing.T) {
	tmp := t.TempDir()
	opts := Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	}

	firstEngine, err := OpenMVCCLSM(opts)
	if err != nil {
		t.Fatalf("OpenMVCCLSM() first open error = %v", err)
	}

	var lastTxnID int64
	for i := 0; i < 6; i++ {
		lastTxnID = firstEngine.BeginTransaction(txn_unit.READ_COMMITTED, "", nil)
		if err := firstEngine.Commit(lastTxnID); err != nil {
			t.Fatalf("Commit(%d) error = %v", lastTxnID, err)
		}
	}

	if err := firstEngine.Close(); err != nil {
		t.Fatalf("Close() first engine error = %v", err)
	}

	secondEngine, err := OpenMVCCLSM(opts)
	if err != nil {
		t.Fatalf("OpenMVCCLSM() second open error = %v", err)
	}
	defer func() { _ = secondEngine.Close() }()

	nextTxnID := secondEngine.BeginTransaction(txn_unit.READ_COMMITTED, "", nil)
	if nextTxnID <= lastTxnID {
		t.Fatalf("BeginTransaction() after restart = %d, want > %d", nextTxnID, lastTxnID)
	}
	if err := secondEngine.Commit(nextTxnID); err != nil {
		t.Fatalf("Commit(%d) after restart error = %v", nextTxnID, err)
	}
}

func TestOpenMVCCLSM_IgnoresRaftLeadershipConfigAtStartup(t *testing.T) {
	tmp := t.TempDir()
	engine, err := OpenMVCCLSM(Options{
		DBPath:     filepath.Join(tmp, "db.db"),
		LSMDataDir: filepath.Join(tmp, "lsm"),
		WALPath:    filepath.Join(tmp, "wal.log"),
		ReplicationConfig: synchronization.ReplicationConfig{
			Strategy:     synchronization.SyncStrategyRaft,
			Backend:      synchronization.ReplicationBackendMaelstrom,
			ReadPolicy:   synchronization.SyncPolicySynchronous,
			WritePolicy:  synchronization.SyncPolicyQuorum,
			MinWriteAcks: 1,
			LocalNodeID:  "n2",
			LeaderNodeID: "n1",
			CurrentTerm:  3,
		},
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	txnID := engine.BeginTransaction(txn_unit.SERIALIZABLE, "", nil)
	if err := engine.Write(txnID, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Write() should remain local before commit, got error = %v", err)
	}
	err = engine.Commit(txnID)
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Commit() error = %v, want leadership-not-configured error", err)
	}
}

func TestEngine_UpdateRaftLeadership_DynamicTransition(t *testing.T) {
	tmp := t.TempDir()
	engine, err := OpenMVCCLSM(Options{
		DBPath:     filepath.Join(tmp, "db.db"),
		LSMDataDir: filepath.Join(tmp, "lsm"),
		WALPath:    filepath.Join(tmp, "wal.log"),
		ReplicationConfig: synchronization.ReplicationConfig{
			Strategy:     synchronization.SyncStrategyRaft,
			Backend:      synchronization.ReplicationBackendMaelstrom,
			ReadPolicy:   synchronization.SyncPolicySynchronous,
			WritePolicy:  synchronization.SyncPolicyQuorum,
			MinWriteAcks: 1,
			LocalNodeID:  "n2",
			LeaderNodeID: "n2",
			CurrentTerm:  2,
		},
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	if err := engine.UpdateRaftLeadership("n2", "n2", 2); err != nil {
		t.Fatalf("UpdateRaftLeadership(initial leader) error = %v", err)
	}

	txnID := engine.BeginTransaction(txn_unit.SERIALIZABLE, "", nil)
	if err := engine.Write(txnID, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Write() as leader error = %v", err)
	}
	if err := engine.Commit(txnID); err != nil {
		t.Fatalf("Commit() as leader error = %v", err)
	}

	if err := engine.UpdateRaftLeadership("n2", "", 3); err != nil {
		t.Fatalf("UpdateRaftLeadership(leader crash) error = %v", err)
	}
	txnID = engine.BeginTransaction(txn_unit.SERIALIZABLE, "", nil)
	if err := engine.Write(txnID, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Write() during election should remain local, got = %v", err)
	}
	err = engine.Commit(txnID)
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Commit() during election error = %v, want leadership-not-configured", err)
	}
	_ = engine.Abort(txnID)

	if err := engine.UpdateRaftLeadership("n2", "n3", 4); err != nil {
		t.Fatalf("UpdateRaftLeadership(new leader) error = %v", err)
	}
	txnID = engine.BeginTransaction(txn_unit.SERIALIZABLE, "", nil)
	if err := engine.Write(txnID, []byte("k3"), []byte("v3")); err != nil {
		t.Fatalf("Write() as follower should remain local, got = %v", err)
	}
	err = engine.Commit(txnID)
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Fatalf("Commit() as follower error = %v, want not-leader", err)
	}
	_ = engine.Abort(txnID)

	err = engine.UpdateRaftLeadership("n2", "n2", 3)
	if err == nil || !strings.Contains(err.Error(), "stale raft term") {
		t.Fatalf("UpdateRaftLeadership(stale term) error = %v, want stale-term error", err)
	}
}

