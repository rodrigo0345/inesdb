package engine_test

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// NOT NULL enforcement
// ---------------------------------------------------------------------------

func TestNullable_NotNullColumnOmitted(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)

	// id is NOT NULL and omitted → error
	err := trySQL(t, db, `INSERT INTO t (name) VALUES ('Alice')`)
	if err == nil {
		t.Fatal("expected NOT NULL violation, got nil")
	}
	if !strings.Contains(err.Error(), "not-null constraint") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNullable_NotNullColumnExplicitNull(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)

	// Explicit NULL on NOT NULL column → error
	err := trySQL(t, db, `INSERT INTO t (id, name) VALUES (NULL, 'Bob')`)
	if err == nil {
		t.Fatal("expected NOT NULL violation for explicit NULL, got nil")
	}
	if !strings.Contains(err.Error(), "not-null constraint") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNullable_UpdateNotNullColumnToNull(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)

	err := trySQL(t, db, `UPDATE t SET id = NULL WHERE id = 1`)
	if err == nil {
		t.Fatal("expected NOT NULL violation on UPDATE, got nil")
	}
	if !strings.Contains(err.Error(), "not-null constraint") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNullable_PKColumnImplicitlyNotNull(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))`)

	// PK column is implicitly NOT NULL.
	err := trySQL(t, db, `INSERT INTO t (name) VALUES ('Alice')`)
	if err == nil {
		t.Fatal("expected NOT NULL violation on PK column, got nil")
	}
	if !strings.Contains(err.Error(), "not-null constraint") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNullable_OmittedNullableColumnDefaultsToNull(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db, `INSERT INTO t (id) VALUES (1)`)

	res := mustSQL(t, db, `SELECT name FROM t WHERE id = 1`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != nil {
		t.Errorf("expected NULL for omitted nullable column, got %v", res.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// NULL insert / retrieve
// ---------------------------------------------------------------------------

func TestNullable_ExplicitNullInsertAndSelect(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, NULL)`)

	res := mustSQL(t, db, `SELECT name FROM t WHERE id = 1`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != nil {
		t.Errorf("expected NULL value, got %v", res.Rows[0][0])
	}
}

func TestNullable_NullAndNonNullValues(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, score INT)`)
	mustSQL(t, db, `INSERT INTO t (id, score) VALUES (1, 100)`)
	mustSQL(t, db, `INSERT INTO t (id, score) VALUES (2, NULL)`)
	mustSQL(t, db, `INSERT INTO t (id) VALUES (3)`)

	res := mustSQL(t, db, `SELECT id, score FROM t`)
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
}

// ---------------------------------------------------------------------------
// IS NULL / IS NOT NULL predicates
// ---------------------------------------------------------------------------

func TestNullable_IsNullPredicate(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (2, NULL)`)
	mustSQL(t, db, `INSERT INTO t (id) VALUES (3)`)

	res := mustSQL(t, db, `SELECT id FROM t WHERE name IS NULL`)
	if len(res.Rows) != 2 {
		t.Errorf("expected 2 NULL-name rows, got %d", len(res.Rows))
	}
}

func TestNullable_IsNotNullPredicate(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (2, NULL)`)
	mustSQL(t, db, `INSERT INTO t (id) VALUES (3)`)

	res := mustSQL(t, db, `SELECT id FROM t WHERE name IS NOT NULL`)
	if len(res.Rows) != 1 {
		t.Errorf("expected 1 non-NULL-name row, got %d", len(res.Rows))
	}
}

func TestNullable_IsNullCompound(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (2, NULL)`)

	res := mustSQL(t, db, `SELECT id FROM t WHERE name IS NULL AND id = 2`)
	if len(res.Rows) != 1 {
		t.Errorf("expected 1 row matching compound IS NULL, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != int64(2) {
		t.Errorf("expected id=2, got %v", res.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// NULL in UPDATE
// ---------------------------------------------------------------------------

func TestNullable_UpdateToNull(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)

	mustSQL(t, db, `UPDATE t SET name = NULL WHERE id = 1`)

	res := mustSQL(t, db, `SELECT name FROM t WHERE id = 1`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != nil {
		t.Errorf("expected NULL after UPDATE SET NULL, got %v", res.Rows[0][0])
	}
}

func TestNullable_UpdateToNullThenQueryIsNull(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO t (id, name) VALUES (2, 'Bob')`)

	mustSQL(t, db, `UPDATE t SET name = NULL WHERE id = 1`)

	res := mustSQL(t, db, `SELECT id FROM t WHERE name IS NULL`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 IS NULL row after update, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != int64(1) {
		t.Errorf("expected id=1, got %v", res.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// Persistence (nullable survives restart)
// ---------------------------------------------------------------------------

func TestNullable_PersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	mustSQL(t, db1, `CREATE TABLE t (id INT NOT NULL, name TEXT)`)
	mustSQL(t, db1, `INSERT INTO t (id, name) VALUES (1, NULL)`)
	if err := db1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2 := openDBAt(t, dir)
	defer db2.Close()

	// NOT NULL constraint still enforced.
	err := trySQL(t, db2, `INSERT INTO t (name) VALUES ('test')`)
	if err == nil {
		t.Fatal("expected NOT NULL violation after restart")
	}

	// NULL value still readable.
	res := mustSQL(t, db2, `SELECT name FROM t WHERE id = 1`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != nil {
		t.Errorf("expected NULL value after restart, got %v", res.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// FK SET NULL (now enabled)
// ---------------------------------------------------------------------------

func TestNullable_FKSetNull(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL)`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	// Deleting parent with SET NULL should set child FK column to NULL.
	mustSQL(t, db, `DELETE FROM users WHERE id = 1`)

	res := mustSQL(t, db, `SELECT user_id FROM orders WHERE id = 10`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 order row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != nil {
		t.Errorf("expected user_id to be NULL after SET NULL, got %v", res.Rows[0][0])
	}
}

func TestNullable_FKSetNullOnNotNullColumn(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL)`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	// SET NULL on a NOT NULL FK column → error.
	err := trySQL(t, db, `DELETE FROM users WHERE id = 1`)
	if err == nil {
		t.Fatal("expected error: SET NULL on NOT NULL FK column")
	}
	if !strings.Contains(err.Error(), "not-null constraint") {
		t.Errorf("unexpected error: %v", err)
	}
}
