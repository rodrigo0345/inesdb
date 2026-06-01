package engine_test

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Basic constraint enforcement
// ---------------------------------------------------------------------------

func TestForeignKey_InsertChildMissingParent(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)

	err := trySQL(t, db, `INSERT INTO orders (id, user_id) VALUES (1, 99)`)
	if err == nil {
		t.Fatal("expected FK violation, got nil")
	}
	if !strings.Contains(err.Error(), "not present in table") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestForeignKey_InsertChildValidParent(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)

	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)
}

func TestForeignKey_DeleteParentWithChildRestrict(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)

	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	err := trySQL(t, db, `DELETE FROM users WHERE id = 1`)
	if err == nil {
		t.Fatal("expected FK violation on delete, got nil")
	}
	if !strings.Contains(err.Error(), "still referenced from table") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestForeignKey_DeleteParentNoChildren(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)

	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `DELETE FROM users WHERE id = 1`)
}

// ---------------------------------------------------------------------------
// Syntax variants
// ---------------------------------------------------------------------------

func TestForeignKey_ColumnLevelSyntax(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	// column-level REFERENCES clause
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT REFERENCES users(id), PRIMARY KEY (id))`)

	err := trySQL(t, db, `INSERT INTO orders (id, user_id) VALUES (1, 42)`)
	if err == nil {
		t.Fatal("expected FK violation")
	}

	mustSQL(t, db, `INSERT INTO users (id) VALUES (42)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (1, 42)`)
}

func TestForeignKey_TableLevelSyntax(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (7)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (1, 7)`)

	err := trySQL(t, db, `INSERT INTO orders (id, user_id) VALUES (2, 99)`)
	if err == nil {
		t.Fatal("expected FK violation")
	}
}

// ---------------------------------------------------------------------------
// ON DELETE CASCADE
// ---------------------------------------------------------------------------

func TestForeignKey_OnDeleteCascade(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (11, 1)`)

	// Deleting parent should cascade-delete children.
	mustSQL(t, db, `DELETE FROM users WHERE id = 1`)

	res := mustSQL(t, db, `SELECT id FROM orders WHERE user_id = 1`)
	if res.RowsAffected != 0 || len(res.Rows) != 0 {
		t.Errorf("expected child rows to be deleted by cascade, got %d rows", len(res.Rows))
	}
}

func TestForeignKey_OnDeleteRestrictExplicit(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	err := trySQL(t, db, `DELETE FROM users WHERE id = 1`)
	if err == nil {
		t.Fatal("expected RESTRICT FK violation")
	}
	if !strings.Contains(err.Error(), "still referenced") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestForeignKey_MultiLevelCascade(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE a (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE b (id INT, a_id INT, PRIMARY KEY (id), FOREIGN KEY (a_id) REFERENCES a(id) ON DELETE CASCADE)`)
	mustSQL(t, db, `CREATE TABLE c (id INT, b_id INT, PRIMARY KEY (id), FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE)`)

	mustSQL(t, db, `INSERT INTO a (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO b (id, a_id) VALUES (10, 1)`)
	mustSQL(t, db, `INSERT INTO c (id, b_id) VALUES (100, 10)`)

	mustSQL(t, db, `DELETE FROM a WHERE id = 1`)

	resB := mustSQL(t, db, `SELECT id FROM b`)
	if len(resB.Rows) != 0 {
		t.Errorf("expected b rows to be cascade-deleted, got %d", len(resB.Rows))
	}
	resC := mustSQL(t, db, `SELECT id FROM c`)
	if len(resC.Rows) != 0 {
		t.Errorf("expected c rows to be cascade-deleted, got %d", len(resC.Rows))
	}
}

// ---------------------------------------------------------------------------
// ON UPDATE CASCADE / RESTRICT
// ---------------------------------------------------------------------------

func TestForeignKey_OnUpdateCascade(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE CASCADE)`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	// Update parent PK — child FK column should cascade.
	mustSQL(t, db, `UPDATE users SET id = 2 WHERE id = 1`)

	res := mustSQL(t, db, `SELECT user_id FROM orders WHERE id = 10`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 order row, got %d", len(res.Rows))
	}
	// user_id should now be 2 (decoded as int64 by schemaValueToNative)
	if v := res.Rows[0][0]; v != int64(2) {
		t.Errorf("expected user_id=2 after cascade update, got %v (%T)", v, v)
	}
}

func TestForeignKey_OnUpdateRestrictDefault(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	err := trySQL(t, db, `UPDATE users SET id = 999 WHERE id = 1`)
	if err == nil {
		t.Fatal("expected FK RESTRICT on update, got nil")
	}
	if !strings.Contains(err.Error(), "still referenced") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

func TestForeignKey_SelfReferential(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE employees (id INT, manager_id INT, PRIMARY KEY (id))`)
	// Note: self-referential FK — the table already exists when we check the reference.
	// Actually we can't do self-ref in CREATE TABLE since the table doesn't exist yet;
	// this tests that the error is helpful.
	err := trySQL(t, db, `CREATE TABLE emp2 (id INT, manager_id INT REFERENCES emp2(id), PRIMARY KEY (id))`)
	if err == nil {
		t.Fatal("expected error: self-ref FK table doesn't exist yet at CREATE TABLE time")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestForeignKey_ReferencedColumnNotFound(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)

	err := trySQL(t, db, `CREATE TABLE orders (id INT, user_id INT REFERENCES users(nonexistent), PRIMARY KEY (id))`)
	if err == nil {
		t.Fatal("expected error for missing referenced column")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestForeignKey_ReferencedTableNotFound(t *testing.T) {
	db := openFreshDB(t)

	err := trySQL(t, db, `CREATE TABLE orders (id INT, user_id INT REFERENCES ghost_table(id), PRIMARY KEY (id))`)
	if err == nil {
		t.Fatal("expected error for missing referenced table")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestForeignKey_UpdateChildFKToInvalidParent(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	err := trySQL(t, db, `UPDATE orders SET user_id = 999 WHERE id = 10`)
	if err == nil {
		t.Fatal("expected FK violation on child update to non-existent parent")
	}
	if !strings.Contains(err.Error(), "not present in table") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestForeignKey_UpdateChildFKToValidParent(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO users (id) VALUES (2)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	// Changing FK column to another valid parent should succeed.
	mustSQL(t, db, `UPDATE orders SET user_id = 2 WHERE id = 10`)
}

func TestForeignKey_MultipleFKsOnSameTable(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE products (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE customers (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE line_items (
		id         INT,
		product_id INT,
		customer_id INT,
		PRIMARY KEY (id),
		FOREIGN KEY (product_id) REFERENCES products(id),
		FOREIGN KEY (customer_id) REFERENCES customers(id)
	)`)

	mustSQL(t, db, `INSERT INTO products (id) VALUES (10)`)
	mustSQL(t, db, `INSERT INTO customers (id) VALUES (20)`)
	mustSQL(t, db, `INSERT INTO line_items (id, product_id, customer_id) VALUES (1, 10, 20)`)

	err := trySQL(t, db, `INSERT INTO line_items (id, product_id, customer_id) VALUES (2, 10, 999)`)
	if err == nil {
		t.Fatal("expected FK violation for customer_id")
	}

	err = trySQL(t, db, `INSERT INTO line_items (id, product_id, customer_id) VALUES (2, 999, 20)`)
	if err == nil {
		t.Fatal("expected FK violation for product_id")
	}
}

func TestForeignKey_DeleteUnreferencedParentSucceeds(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)

	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO users (id) VALUES (2)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	// Deleting user 2 (unreferenced) should succeed.
	mustSQL(t, db, `DELETE FROM users WHERE id = 2`)

	// Deleting user 1 (referenced) should fail.
	err := trySQL(t, db, `DELETE FROM users WHERE id = 1`)
	if err == nil {
		t.Fatal("expected FK violation for referenced user 1")
	}
}

func TestForeignKey_ZeroValueFKMustMatchParent(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE categories (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE items (id INT, cat_id INT, PRIMARY KEY (id), FOREIGN KEY (cat_id) REFERENCES categories(id))`)

	// cat_id=0 with no category with id=0 → violation.
	err := trySQL(t, db, `INSERT INTO items (id, cat_id) VALUES (1, 0)`)
	if err == nil {
		t.Fatal("expected FK violation for zero-value FK column with no matching parent")
	}

	// Now insert a parent with id=0 explicitly.
	mustSQL(t, db, `INSERT INTO categories (id) VALUES (0)`)
	mustSQL(t, db, `INSERT INTO items (id, cat_id) VALUES (1, 0)`)
}

// ---------------------------------------------------------------------------
// Persistence (FK survives restart)
// ---------------------------------------------------------------------------

func TestForeignKey_PersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	db1 := openDBAt(t, dir)
	mustSQL(t, db1, `CREATE TABLE users (id INT, PRIMARY KEY (id))`)
	mustSQL(t, db1, `CREATE TABLE orders (id INT, user_id INT, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users(id))`)
	mustSQL(t, db1, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db1, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)
	if err := db1.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Reopen: FK constraint must still be enforced.
	db2 := openDBAt(t, dir)
	defer db2.Close()

	err := trySQL(t, db2, `INSERT INTO orders (id, user_id) VALUES (20, 99)`)
	if err == nil {
		t.Fatal("expected FK violation after restart")
	}
	if !strings.Contains(err.Error(), "not present in table") {
		t.Errorf("unexpected error after restart: %v", err)
	}
}
