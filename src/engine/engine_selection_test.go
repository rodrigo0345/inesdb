package engine_test

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Default and explicit LSM
// ---------------------------------------------------------------------------

func TestEngineSelection_DefaultIsLSM(t *testing.T) {
	db := openFreshDB(t)
	// No ENGINE option — should work exactly as before.
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (2, 'Bob')`)

	res := mustSQL(t, db, `SELECT id, name FROM t`)
	if len(res.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(res.Rows))
	}
}

func TestEngineSelection_ExplicitLSM(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, PRIMARY KEY (id)) ENGINE = LSM`)
	mustSQL(t, db, `INSERT INTO t (id) VALUES (42)`)

	res := mustSQL(t, db, `SELECT id FROM t`)
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(42) {
		t.Errorf("unexpected result: %v", res.Rows)
	}
}

// ---------------------------------------------------------------------------
// BTree engine
// ---------------------------------------------------------------------------

func TestEngineSelection_BTree_CRUD(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT, PRIMARY KEY (id)) ENGINE = BTREE`)

	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (2, 'Bob')`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (3, 'Carol')`)

	res := mustSQL(t, db, `SELECT id, name FROM t`)
	if len(res.Rows) != 3 {
		t.Errorf("expected 3 rows from BTree table, got %d", len(res.Rows))
	}
}

func TestEngineSelection_BTree_UpdateDelete(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, val INT, PRIMARY KEY (id)) ENGINE = BTREE`)
	mustSQL(t, db, `INSERT INTO t (id, val) VALUES (1, 10)`)
	mustSQL(t, db, `INSERT INTO t (id, val) VALUES (2, 20)`)

	mustSQL(t, db, `UPDATE t SET val = 99 WHERE id = 1`)
	mustSQL(t, db, `DELETE FROM t WHERE id = 2`)

	res := mustSQL(t, db, `SELECT id, val FROM t`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row after delete, got %d", len(res.Rows))
	}
	if res.Rows[0][1] != int64(99) {
		t.Errorf("expected val=99 after update, got %v", res.Rows[0][1])
	}
}

func TestEngineSelection_BTree_WithWhere(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id)) ENGINE = BTREE`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (2, 'Bob')`)

	res := mustSQL(t, db, `SELECT name FROM t WHERE id = 1`)
	if len(res.Rows) != 1 || res.Rows[0][0] != "Alice" {
		t.Errorf("WHERE filter on BTree failed: %v", res.Rows)
	}
}

// ---------------------------------------------------------------------------
// Vector engine (basic table operations — see vector_test.go for search)
// ---------------------------------------------------------------------------

func TestEngineSelection_Vector_BasicInsertSelect(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db, `INSERT INTO t (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO t (id) VALUES (2)`)

	res := mustSQL(t, db, `SELECT id FROM t`)
	if len(res.Rows) != 2 {
		t.Errorf("expected 2 rows from VECTOR table, got %d", len(res.Rows))
	}
}

// ---------------------------------------------------------------------------
// Invalid engine name
// ---------------------------------------------------------------------------

func TestEngineSelection_InvalidEngine(t *testing.T) {
	db := openFreshDB(t)
	err := trySQL(t, db, `CREATE TABLE t (id INT NOT NULL, PRIMARY KEY (id)) ENGINE = NOSUCHENGINE`)
	if err == nil {
		t.Fatal("expected error for unknown ENGINE, got nil")
	}
	if !strings.Contains(err.Error(), "unknown storage engine") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Engine persists across restart
// ---------------------------------------------------------------------------

func TestEngineSelection_PersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	mustSQL(t, db1, `CREATE TABLE lsm_tbl (id INT NOT NULL, PRIMARY KEY (id)) ENGINE = LSM`)
	mustSQL(t, db1, `CREATE TABLE vec_tbl (id INT NOT NULL, PRIMARY KEY (id)) ENGINE = VECTOR`)
	mustSQL(t, db1, `INSERT INTO lsm_tbl (id) VALUES (1)`)
	mustSQL(t, db1, `INSERT INTO vec_tbl (id) VALUES (2)`)
	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2 := openDBAt(t, dir)
	defer db2.Close()

	resLSM := mustSQL(t, db2, `SELECT id FROM lsm_tbl`)
	if len(resLSM.Rows) != 1 {
		t.Errorf("expected 1 row in lsm_tbl after restart, got %d", len(resLSM.Rows))
	}
	resVec := mustSQL(t, db2, `SELECT id FROM vec_tbl`)
	if len(resVec.Rows) != 1 {
		t.Errorf("expected 1 row in vec_tbl after restart, got %d", len(resVec.Rows))
	}
}

// ---------------------------------------------------------------------------
// Two tables with different engines in the same DB
// ---------------------------------------------------------------------------

func TestEngineSelection_TwoTablesDifferentEngines(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE lsm_t (id INT NOT NULL, PRIMARY KEY (id)) ENGINE = LSM`)
	mustSQL(t, db, `CREATE TABLE vec_t (id INT NOT NULL, PRIMARY KEY (id)) ENGINE = VECTOR`)

	mustSQL(t, db, `INSERT INTO lsm_t (id) VALUES (10)`)
	mustSQL(t, db, `INSERT INTO vec_t (id) VALUES (20)`)

	r1 := mustSQL(t, db, `SELECT id FROM lsm_t`)
	r2 := mustSQL(t, db, `SELECT id FROM vec_t`)
	if len(r1.Rows) != 1 || r1.Rows[0][0] != int64(10) {
		t.Errorf("lsm_t unexpected result: %v", r1.Rows)
	}
	if len(r2.Rows) != 1 || r2.Rows[0][0] != int64(20) {
		t.Errorf("vec_t unexpected result: %v", r2.Rows)
	}
}
