package engine_test

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Helpers shared across join tests
// ---------------------------------------------------------------------------

func setupUsersOrders(t *testing.T) interface{ Close() error } {
	t.Helper()
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, name TEXT NOT NULL, active BOOL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, total INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)`)
	mustSQL(t, db, `INSERT INTO users (id, name, active) VALUES (2, 'Bob', true)`)
	mustSQL(t, db, `INSERT INTO users (id, name, active) VALUES (3, 'Carol', false)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (11, 1, 200)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (12, 2, 50)`)
	return db
}

// ---------------------------------------------------------------------------
// INNER JOIN
// ---------------------------------------------------------------------------

func TestJoin_InnerJoin_Basic(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, total INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (11, 1, 200)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (12, 2, 50)`)

	res := mustSQL(t, db, `SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id`)
	if len(res.Rows) != 3 {
		t.Errorf("expected 3 joined rows, got %d", len(res.Rows))
	}
}

func TestJoin_InnerJoin_NoMatches(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	// No orders → INNER JOIN yields 0 rows.

	res := mustSQL(t, db, `SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id`)
	if len(res.Rows) != 0 {
		t.Errorf("expected 0 rows for empty right table, got %d", len(res.Rows))
	}
}

func TestJoin_InnerJoin_MultipleMatchesPerLeft(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id) VALUES (1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (11, 1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (12, 1)`)

	res := mustSQL(t, db, `SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id`)
	if len(res.Rows) != 3 {
		t.Errorf("expected 3 rows (one user × three orders), got %d", len(res.Rows))
	}
}

func TestJoin_InnerJoin_WithWhere(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, name TEXT NOT NULL, active BOOL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, total INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)`)
	mustSQL(t, db, `INSERT INTO users (id, name, active) VALUES (2, 'Bob', false)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (11, 2, 50)`)

	// Only active users' orders.
	res := mustSQL(t, db, `SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = true`)
	if len(res.Rows) != 1 {
		t.Errorf("expected 1 row (active user only), got %d", len(res.Rows))
	}
}

func TestJoin_InnerJoin_UnqualifiedWhereAfterJoin(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (11, 2)`)

	// Qualified WHERE to avoid ambiguity between u.id and o.id.
	res := mustSQL(t, db, `SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.id = 1`)
	if len(res.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(res.Rows))
	}
}

// ---------------------------------------------------------------------------
// LEFT JOIN
// ---------------------------------------------------------------------------

func TestJoin_LeftJoin_AllLeftRowsPresent(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (3, 'Carol')`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)
	// Bob and Carol have no orders.

	res := mustSQL(t, db, `SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id`)
	// Alice matches → 1 row; Bob and Carol don't match → 1 row each with NULL right side.
	if len(res.Rows) != 3 {
		t.Errorf("expected 3 rows (all users), got %d", len(res.Rows))
	}
}

func TestJoin_LeftJoin_UnmatchedRowsHaveNullRightColumns(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, total INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (10, 1, 500)`)

	res := mustSQL(t, db, `SELECT u.id, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id`)
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}

	// Find Bob's row (no order → o.total should be nil).
	foundNull := false
	for _, row := range res.Rows {
		if row[1] == nil {
			foundNull = true
		}
	}
	if !foundNull {
		t.Error("expected NULL for o.total in unmatched LEFT JOIN row, found none")
	}
}

func TestJoin_LeftJoin_Mixed(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (3, 'Carol')`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (11, 1)`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (12, 2)`)

	res := mustSQL(t, db, `SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id`)
	// Alice: 2 matches, Bob: 1 match, Carol: 0 matches (1 NULL row).
	if len(res.Rows) != 4 {
		t.Errorf("expected 4 rows (2+1+1), got %d", len(res.Rows))
	}
}

// ---------------------------------------------------------------------------
// Column projection
// ---------------------------------------------------------------------------

func TestJoin_QualifiedProjection(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, total INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id, total) VALUES (10, 1, 300)`)

	res := mustSQL(t, db, `SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id`)
	if len(res.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(res.Columns), res.Columns)
	}
	if res.Columns[0] != "name" || res.Columns[1] != "total" {
		t.Errorf("unexpected columns: %v", res.Columns)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "Alice" {
		t.Errorf("expected name='Alice', got %v", res.Rows[0][0])
	}
	if res.Rows[0][1] != int64(300) {
		t.Errorf("expected total=300, got %v", res.Rows[0][1])
	}
}

func TestJoin_StarProjection(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustSQL(t, db, `INSERT INTO orders (id, user_id) VALUES (10, 1)`)

	res := mustSQL(t, db, `SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id`)
	// users: id, name → 2 cols; orders: id, user_id → 2 cols = 4 total.
	if len(res.Columns) != 4 {
		t.Errorf("expected 4 columns for SELECT *, got %d: %v", len(res.Columns), res.Columns)
	}
	if len(res.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(res.Rows))
	}
}

// ---------------------------------------------------------------------------
// Three-table join
// ---------------------------------------------------------------------------

func TestJoin_ThreeTableInnerJoin(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE a (id INT NOT NULL, val TEXT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE b (id INT NOT NULL, a_id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE c (id INT NOT NULL, b_id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `INSERT INTO a (id, val) VALUES (1, 'alpha')`)
	mustSQL(t, db, `INSERT INTO b (id, a_id) VALUES (10, 1)`)
	mustSQL(t, db, `INSERT INTO c (id, b_id) VALUES (100, 10)`)

	res := mustSQL(t, db, `SELECT a.val FROM a INNER JOIN b ON a.id = b.a_id INNER JOIN c ON b.id = c.b_id`)
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row from three-table join, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "alpha" {
		t.Errorf("expected val='alpha', got %v", res.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

func TestJoin_NonExistentTable(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, PRIMARY KEY (id))`)

	err := trySQL(t, db, `SELECT * FROM users u INNER JOIN ghost_table g ON u.id = g.user_id`)
	if err == nil {
		t.Fatal("expected error for non-existent right table, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestJoin_UnsupportedJoinType(t *testing.T) {
	db := openFreshDB(t)
	mustSQL(t, db, `CREATE TABLE users (id INT NOT NULL, PRIMARY KEY (id))`)
	mustSQL(t, db, `CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, PRIMARY KEY (id))`)

	err := trySQL(t, db, `SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id`)
	if err == nil {
		t.Fatal("expected error for RIGHT JOIN, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported JOIN type") {
		t.Errorf("unexpected error: %v", err)
	}
}
