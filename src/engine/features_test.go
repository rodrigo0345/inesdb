package engine_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ajitpratap0/GoSQLX/pkg/gosqlx"
	dbengine "github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/query/executor"
	txn_unit "github.com/rodrigo0345/omag/src/engine/txn"
)

// --- helpers shared with recovery_test.go ---

func mustSQL(t *testing.T, db dbengine.Database, sql string) *executor.Result {
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

func trySQL(t *testing.T, db dbengine.Database, sql string) error {
	t.Helper()
	parsed, err := gosqlx.Parse(sql)
	if err != nil {
		return err
	}
	txnID := db.BeginTransaction(txn_unit.SERIALIZABLE)
	for _, stmt := range parsed.Statements {
		_, err = executor.Execute(db, txnID, stmt)
		if err != nil {
			_ = db.Abort(txnID)
			return err
		}
	}
	return db.Commit(txnID)
}

func openFreshDB(t *testing.T) dbengine.Database {
	t.Helper()
	tmpDir := t.TempDir()
	db, err := dbengine.OpenMVCCLSM(dbengine.Options{
		DBPath:     filepath.Join(tmpDir, "test.db"),
		LSMDataDir: filepath.Join(tmpDir, "lsm"),
		WALPath:    filepath.Join(tmpDir, "test.wal"),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// ---------------------------------------------------------------------------
// INSERT positional fix
// ---------------------------------------------------------------------------

func TestInsert_Positional_SingleColumn(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE nums (n INT PRIMARY KEY)")

	mustSQL(t, db, "INSERT INTO nums VALUES (42)")
	mustSQL(t, db, "INSERT INTO nums VALUES (100)")

	res := mustSQL(t, db, "SELECT * FROM nums")
	if res.RowsAffected != 2 {
		t.Fatalf("expected 2 rows, got %d", res.RowsAffected)
	}
	for _, row := range res.Rows {
		v := toInt64(row[0])
		if v != 42 && v != 100 {
			t.Errorf("unexpected value %v", row[0])
		}
	}
}

func TestInsert_Positional_MultiColumn(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE xy (x INT PRIMARY KEY, y INT)")

	mustSQL(t, db, "INSERT INTO xy VALUES (1, 10)")
	mustSQL(t, db, "INSERT INTO xy VALUES (2, 20)")

	res := mustSQL(t, db, "SELECT * FROM xy WHERE x = 1")
	if res.RowsAffected != 1 {
		t.Fatalf("expected 1 row, got %d", res.RowsAffected)
	}
	y := toInt64(res.Rows[0][1])
	if y != 10 {
		t.Errorf("expected y=10, got %v", y)
	}
}

func TestInsert_Positional_UserOriginalBug(t *testing.T) {
	// Reproduces the exact bug report: INSERT INTO user VALUES (1000) showed 0.
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE "user" (id INT PRIMARY KEY)`)
	mustSQL(t, db, `INSERT INTO "user" VALUES (1000)`)

	res := mustSQL(t, db, `SELECT * FROM "user"`)
	if res.RowsAffected != 1 {
		t.Fatalf("expected 1 row, got %d", res.RowsAffected)
	}
	if toInt64(res.Rows[0][0]) != 1000 {
		t.Errorf("expected 1000, got %v", res.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// AUTO_INCREMENT
// ---------------------------------------------------------------------------

func TestAutoIncrement_Basic(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE events (id SERIAL PRIMARY KEY, name VARCHAR)")

	mustSQL(t, db, "INSERT INTO events (name) VALUES ('a')")
	mustSQL(t, db, "INSERT INTO events (name) VALUES ('b')")
	mustSQL(t, db, "INSERT INTO events (name) VALUES ('c')")

	res := mustSQL(t, db, "SELECT * FROM events")
	if res.RowsAffected != 3 {
		t.Fatalf("expected 3 rows, got %d", res.RowsAffected)
	}

	ids := make(map[int64]bool)
	for _, row := range res.Rows {
		id := toInt64(row[0])
		if id <= 0 {
			t.Errorf("expected positive auto-increment id, got %v", id)
		}
		if ids[id] {
			t.Errorf("duplicate auto-increment id %d", id)
		}
		ids[id] = true
	}
	if len(ids) != 3 {
		t.Errorf("expected 3 distinct ids, got %d", len(ids))
	}
}

func TestAutoIncrement_Monotonic(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE counters (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")

	for i := 0; i < 5; i++ {
		mustSQL(t, db, fmt.Sprintf("INSERT INTO counters (v) VALUES (%d)", i))
	}

	res := mustSQL(t, db, "SELECT * FROM counters")
	if res.RowsAffected != 5 {
		t.Fatalf("expected 5 rows, got %d", res.RowsAffected)
	}

	var prev int64 = 0
	for _, row := range res.Rows {
		id := toInt64(row[0])
		if id <= prev {
			t.Errorf("ids not monotonically increasing: %d <= %d", id, prev)
		}
		prev = id
	}
}

func TestAutoIncrement_ResumeAfterRestart(t *testing.T) {
	dir := t.TempDir()

	db1, _ := dbengine.OpenMVCCLSM(dbengine.Options{
		DBPath:     filepath.Join(dir, "test.db"),
		LSMDataDir: filepath.Join(dir, "lsm"),
		WALPath:    filepath.Join(dir, "test.wal"),
	})
	mustSQL(t, db1, "CREATE TABLE seq_t (id SERIAL PRIMARY KEY, v INT)")
	mustSQL(t, db1, "INSERT INTO seq_t (v) VALUES (10)")
	mustSQL(t, db1, "INSERT INTO seq_t (v) VALUES (20)")
	_ = db1.Close()

	db2, _ := dbengine.OpenMVCCLSM(dbengine.Options{
		DBPath:     filepath.Join(dir, "test.db"),
		LSMDataDir: filepath.Join(dir, "lsm"),
		WALPath:    filepath.Join(dir, "test.wal"),
	})
	defer db2.Close()

	mustSQL(t, db2, "INSERT INTO seq_t (v) VALUES (30)")

	res := mustSQL(t, db2, "SELECT * FROM seq_t")
	if res.RowsAffected != 3 {
		t.Fatalf("expected 3 rows after restart, got %d", res.RowsAffected)
	}
	ids := make(map[int64]bool)
	for _, row := range res.Rows {
		id := toInt64(row[0])
		if ids[id] {
			t.Errorf("duplicate id after restart: %d", id)
		}
		ids[id] = true
	}
}

// ---------------------------------------------------------------------------
// UUID
// ---------------------------------------------------------------------------

func TestUUID_AutoGeneration(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE tokens (id UUID PRIMARY KEY, label VARCHAR)")

	mustSQL(t, db, "INSERT INTO tokens (label) VALUES ('x')")
	mustSQL(t, db, "INSERT INTO tokens (label) VALUES ('y')")

	res := mustSQL(t, db, "SELECT * FROM tokens")
	if res.RowsAffected != 2 {
		t.Fatalf("expected 2 rows, got %d", res.RowsAffected)
	}

	uuids := make(map[string]bool)
	for _, row := range res.Rows {
		u, ok := row[0].(string)
		if !ok {
			t.Fatalf("expected uuid string, got %T", row[0])
		}
		if len(u) != 36 {
			t.Errorf("expected 36-char UUID, got %q (len %d)", u, len(u))
		}
		if uuids[u] {
			t.Errorf("duplicate UUID %q", u)
		}
		uuids[u] = true
	}
}

func TestUUID_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	db1, _ := dbengine.OpenMVCCLSM(dbengine.Options{
		DBPath:     filepath.Join(dir, "test.db"),
		LSMDataDir: filepath.Join(dir, "lsm"),
		WALPath:    filepath.Join(dir, "test.wal"),
	})
	mustSQL(t, db1, "CREATE TABLE uids (id UUID PRIMARY KEY, label VARCHAR)")
	mustSQL(t, db1, "INSERT INTO uids (label) VALUES ('hello')")
	res1 := mustSQL(t, db1, "SELECT * FROM uids")
	origUUID := res1.Rows[0][0].(string)
	_ = db1.Close()

	db2, _ := dbengine.OpenMVCCLSM(dbengine.Options{
		DBPath:     filepath.Join(dir, "test.db"),
		LSMDataDir: filepath.Join(dir, "lsm"),
		WALPath:    filepath.Join(dir, "test.wal"),
	})
	defer db2.Close()

	res2 := mustSQL(t, db2, "SELECT * FROM uids")
	if res2.RowsAffected != 1 {
		t.Fatalf("UUID row not found after restart")
	}
	if res2.Rows[0][0].(string) != origUUID {
		t.Errorf("UUID changed after restart: want %q got %q", origUUID, res2.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// UNIQUE constraint
// ---------------------------------------------------------------------------

func TestUnique_InsertDuplicate(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE emails (id INT PRIMARY KEY, addr VARCHAR UNIQUE)")

	mustSQL(t, db, "INSERT INTO emails (id, addr) VALUES (1, 'a@b.com')")
	err := trySQL(t, db, "INSERT INTO emails (id, addr) VALUES (2, 'a@b.com')")
	if err == nil {
		t.Fatal("expected UNIQUE violation, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestUnique_InsertDistinct(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE tags (id INT PRIMARY KEY, name VARCHAR UNIQUE)")

	mustSQL(t, db, "INSERT INTO tags (id, name) VALUES (1, 'go')")
	mustSQL(t, db, "INSERT INTO tags (id, name) VALUES (2, 'rust')")

	res := mustSQL(t, db, "SELECT * FROM tags")
	if res.RowsAffected != 2 {
		t.Errorf("expected 2 rows, got %d", res.RowsAffected)
	}
}

func TestUnique_UpdateViolation(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE accounts (id INT PRIMARY KEY, email VARCHAR UNIQUE)")
	mustSQL(t, db, "INSERT INTO accounts (id, email) VALUES (1, 'alice@x.com')")
	mustSQL(t, db, "INSERT INTO accounts (id, email) VALUES (2, 'bob@x.com')")

	err := trySQL(t, db, "UPDATE accounts SET email = 'alice@x.com' WHERE id = 2")
	if err == nil {
		t.Fatal("expected UNIQUE violation on UPDATE, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestUnique_UpdateSameRow(t *testing.T) {
	// Updating a row to the same unique value it already has must succeed.
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE items (id INT PRIMARY KEY, sku VARCHAR UNIQUE)")
	mustSQL(t, db, "INSERT INTO items (id, sku) VALUES (1, 'SKU-001')")

	// Update to same value — should not violate UNIQUE.
	mustSQL(t, db, "UPDATE items SET sku = 'SKU-001' WHERE id = 1")
}

func TestUnique_AfterDelete(t *testing.T) {
	// After deleting a row with a unique value, that value should be reusable.
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE codes (id INT PRIMARY KEY, code VARCHAR UNIQUE)")
	mustSQL(t, db, "INSERT INTO codes (id, code) VALUES (1, 'ABC')")
	mustSQL(t, db, "DELETE FROM codes WHERE id = 1")

	// Re-insert the same unique value — must succeed now.
	mustSQL(t, db, "INSERT INTO codes (id, code) VALUES (2, 'ABC')")
}

func TestUnique_TableLevelConstraint(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, "CREATE TABLE pairs (a INT, b INT, PRIMARY KEY (a), UNIQUE (b))")
	mustSQL(t, db, "INSERT INTO pairs (a, b) VALUES (1, 10)")

	err := trySQL(t, db, "INSERT INTO pairs (a, b) VALUES (2, 10)")
	if err == nil {
		t.Fatal("expected UNIQUE violation on table-level constraint")
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int32:
		return int64(x)
	case int64:
		return x
	}
	return 0
}
