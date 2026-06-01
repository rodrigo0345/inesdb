package engine_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ajitpratap0/GoSQLX/pkg/gosqlx"
	dbengine "github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/query/executor"
	txn_unit "github.com/rodrigo0345/omag/src/engine/txn"
)

// openDBAt opens a database engine rooted at dir (no cleanup registered —
// callers are responsible for calling Close).
func openDBAt(t *testing.T, dir string) dbengine.Database {
	t.Helper()
	db, err := dbengine.OpenMVCCLSM(dbengine.Options{
		DBPath:     filepath.Join(dir, "test.db"),
		LSMDataDir: filepath.Join(dir, "lsm"),
		WALPath:    filepath.Join(dir, "test.wal"),
	})
	if err != nil {
		t.Fatalf("openDBAt(%s): %v", dir, err)
	}
	return db
}

// runSQL parses and executes a SQL statement in its own auto-committed txn.
func runSQL(t *testing.T, db dbengine.Database, sql string) *executor.Result {
	t.Helper()
	parsed, err := gosqlx.Parse(sql)
	if err != nil {
		t.Fatalf("parse [%s]: %v", sql, err)
	}
	txnID := db.BeginTransaction(txn_unit.SERIALIZABLE)
	var result *executor.Result
	for _, stmt := range parsed.Statements {
		result, err = executor.Execute(db, txnID, stmt)
		if err != nil {
			_ = db.Abort(txnID)
			t.Fatalf("exec [%s]: %v", sql, err)
		}
	}
	if err := db.Commit(txnID); err != nil {
		t.Fatalf("commit [%s]: %v", sql, err)
	}
	return result
}

// runSQLExpectError executes a SQL statement and expects it to fail.
func runSQLExpectError(t *testing.T, db dbengine.Database, sql string) error {
	t.Helper()
	parsed, err := gosqlx.Parse(sql)
	if err != nil {
		return err
	}
	txnID := db.BeginTransaction(txn_unit.SERIALIZABLE)
	var execErr error
	for _, stmt := range parsed.Statements {
		_, execErr = executor.Execute(db, txnID, stmt)
		if execErr != nil {
			_ = db.Abort(txnID)
			return execErr
		}
	}
	_ = db.Commit(txnID)
	return nil
}

// TestRecovery_CleanShutdown verifies that rows inserted before a clean
// Close() are still readable after reopening the database.
func TestRecovery_CleanShutdown(t *testing.T) {
	dir := t.TempDir()

	// --- First run: create table and insert rows ---
	db1 := openDBAt(t, dir)
	runSQL(t, db1, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
	runSQL(t, db1, "INSERT INTO users (id, name) VALUES (1, 'alice')")
	runSQL(t, db1, "INSERT INTO users (id, name) VALUES (2, 'bob')")
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	// --- Second run: data must still be there ---
	db2 := openDBAt(t, dir)
	defer db2.Close()

	res := runSQL(t, db2, "SELECT * FROM users")
	if res.RowsAffected != 2 {
		t.Errorf("expected 2 rows after restart, got %d", res.RowsAffected)
	}
}

// TestRecovery_MultipleTablesAndRows confirms that multiple tables with
// multiple rows all survive a restart.
func TestRecovery_MultipleTablesAndRows(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	runSQL(t, db1, "CREATE TABLE a (id INT PRIMARY KEY, v INT)")
	runSQL(t, db1, "CREATE TABLE b (id INT PRIMARY KEY, v INT)")
	for i := 1; i <= 5; i++ {
		runSQL(t, db1, fmt.Sprintf("INSERT INTO a (id, v) VALUES (%d, %d)", i, i*10))
		runSQL(t, db1, fmt.Sprintf("INSERT INTO b (id, v) VALUES (%d, %d)", i, i*100))
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2 := openDBAt(t, dir)
	defer db2.Close()

	resA := runSQL(t, db2, "SELECT * FROM a")
	if resA.RowsAffected != 5 {
		t.Errorf("table a: expected 5 rows, got %d", resA.RowsAffected)
	}
	resB := runSQL(t, db2, "SELECT * FROM b")
	if resB.RowsAffected != 5 {
		t.Errorf("table b: expected 5 rows, got %d", resB.RowsAffected)
	}
}

// TestRecovery_DropTable ensures that a table dropped before shutdown stays
// gone after restart.
func TestRecovery_DropTable(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	runSQL(t, db1, "CREATE TABLE tmp (id INT PRIMARY KEY)")
	runSQL(t, db1, "INSERT INTO tmp (id) VALUES (1)")
	runSQL(t, db1, "DROP TABLE tmp")
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2 := openDBAt(t, dir)
	defer db2.Close()

	err := runSQLExpectError(t, db2, "SELECT * FROM tmp")
	if err == nil {
		t.Error("expected error querying dropped table, got nil")
	}
}

// TestRecovery_UpdateRow ensures that the latest value is visible after a
// restart, not the original value.
func TestRecovery_UpdateRow(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	runSQL(t, db1, "CREATE TABLE kv (id INT PRIMARY KEY, v INT)")
	runSQL(t, db1, "INSERT INTO kv (id, v) VALUES (1, 10)")
	runSQL(t, db1, "UPDATE kv SET v = 99 WHERE id = 1")
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2 := openDBAt(t, dir)
	defer db2.Close()

	res := runSQL(t, db2, "SELECT * FROM kv WHERE id = 1")
	if res.RowsAffected != 1 {
		t.Fatalf("expected 1 row, got %d", res.RowsAffected)
	}
	// The value stored in column index 1 (v) must be 99.
	v := res.Rows[0][1]
	var vInt int64
	switch val := v.(type) {
	case int32:
		vInt = int64(val)
	case int64:
		vInt = val
	}
	if vInt != 99 {
		t.Errorf("expected v=99 after restart, got %v", v)
	}
}

// TestRecovery_DeleteRow verifies that deleted rows are absent and remaining
// rows are present after a restart.
func TestRecovery_DeleteRow(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	runSQL(t, db1, "CREATE TABLE items (id INT PRIMARY KEY, v INT)")
	runSQL(t, db1, "INSERT INTO items (id, v) VALUES (1, 1)")
	runSQL(t, db1, "INSERT INTO items (id, v) VALUES (2, 2)")
	runSQL(t, db1, "INSERT INTO items (id, v) VALUES (3, 3)")
	runSQL(t, db1, "DELETE FROM items WHERE id = 2")
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2 := openDBAt(t, dir)
	defer db2.Close()

	res := runSQL(t, db2, "SELECT * FROM items")
	if res.RowsAffected != 2 {
		t.Errorf("expected 2 rows after delete+restart, got %d", res.RowsAffected)
	}
}

// TestRecovery_AbortedTxnNotRecovered verifies that rows written inside an
// aborted transaction are never visible, even after a restart.
func TestRecovery_AbortedTxnNotRecovered(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	runSQL(t, db1, "CREATE TABLE ghost (id INT PRIMARY KEY)")

	// Write inside a transaction that is explicitly aborted — never committed.
	parsed, _ := gosqlx.Parse("INSERT INTO ghost (id) VALUES (42)")
	txnID := db1.BeginTransaction(txn_unit.SERIALIZABLE)
	for _, stmt := range parsed.Statements {
		_, _ = executor.Execute(db1, txnID, stmt)
	}
	_ = db1.Abort(txnID) // explicit abort, not committed

	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2 := openDBAt(t, dir)
	defer db2.Close()

	res := runSQL(t, db2, "SELECT * FROM ghost")
	if res.RowsAffected != 0 {
		t.Errorf("aborted rows must not appear after restart, got %d rows", res.RowsAffected)
	}
}

// TestRecovery_WALCrashRecovery simulates a crash (no Close call) and verifies
// that WAL replay on the next startup recovers all committed rows.
func TestRecovery_WALCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	runSQL(t, db1, "CREATE TABLE crash (id INT PRIMARY KEY, v INT)")
	runSQL(t, db1, "INSERT INTO crash (id, v) VALUES (1, 100)")
	runSQL(t, db1, "INSERT INTO crash (id, v) VALUES (2, 200)")
	// Simulate crash: intentionally skip db1.Close() so the memtable is NOT
	// flushed to SSTables. Data survives only through the WAL.
	// (We still need a clean catalog on disk, so explicitly save it via Close
	// only of the buffer pool — but since we can't do that directly, we rely on
	// the WAL having been synced at commit time.)
	//
	// For the purposes of this test the catalog IS written at CreateTable time
	// (before the "crash"), so schema recovery works. The row data must come
	// from WAL replay because the memtable was never flushed.
	_ = db1 // intentionally not closed — simulates crash

	db2 := openDBAt(t, dir)
	defer db2.Close()

	res := runSQL(t, db2, "SELECT * FROM crash")
	if res.RowsAffected != 2 {
		t.Errorf("WAL crash recovery: expected 2 rows, got %d", res.RowsAffected)
	}
}

// TestRecovery_SubsequentWritesAfterRestart verifies that normal SQL
// operations continue to work correctly after recovery.
func TestRecovery_SubsequentWritesAfterRestart(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	runSQL(t, db1, "CREATE TABLE cont (id INT PRIMARY KEY, v INT)")
	runSQL(t, db1, "INSERT INTO cont (id, v) VALUES (1, 1)")
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2 := openDBAt(t, dir)
	defer db2.Close()

	// Existing row must be visible.
	res := runSQL(t, db2, "SELECT * FROM cont")
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 pre-existing row, got %d", res.RowsAffected)
	}

	// New writes after restart must work.
	runSQL(t, db2, "INSERT INTO cont (id, v) VALUES (2, 2)")
	runSQL(t, db2, "UPDATE cont SET v = 99 WHERE id = 1")
	runSQL(t, db2, "DELETE FROM cont WHERE id = 2")

	res = runSQL(t, db2, "SELECT * FROM cont")
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row after post-restart DML, got %d", res.RowsAffected)
	}
	v := res.Rows[0][1]
	var vInt int64
	switch val := v.(type) {
	case int32:
		vInt = int64(val)
	case int64:
		vInt = val
	}
	if vInt != 99 {
		t.Errorf("expected v=99 after update, got %v", v)
	}
}
