package executor_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ajitpratap0/GoSQLX/pkg/gosqlx"
	"github.com/rodrigo0345/omag/src/query/executor"
	txn_unit "github.com/rodrigo0345/omag/src/engine/txn"
)

func TestExecutor_SelectProjection(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE proj_t (id INT PRIMARY KEY, name TEXT, age INT)")
	execSQL(t, db, "INSERT INTO proj_t (id, name, age) VALUES (1, 'Alice', 30)")
	execSQL(t, db, "INSERT INTO proj_t (id, name, age) VALUES (2, 'Bob', 25)")
	execSQL(t, db, "INSERT INTO proj_t (id, name, age) VALUES (3, 'Charlie', 35)")

	res := execSQL(t, db, "SELECT id, name FROM proj_t")
	if res.RowsAffected != 3 {
		t.Fatalf("expected 3 rows, got %d", res.RowsAffected)
	}
	if len(res.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(res.Columns), res.Columns)
	}
	if res.Columns[0] != "id" || res.Columns[1] != "name" {
		t.Errorf("columns: want [id name], got %v", res.Columns)
	}
	for i, row := range res.Rows {
		if len(row) != 2 {
			t.Errorf("row %d: expected 2 values, got %d", i, len(row))
		}
		// id is int64, name is string
		if _, ok := row[0].(int64); !ok {
			t.Errorf("row %d col 0: expected int64, got %T", i, row[0])
		}
		if _, ok := row[1].(string); !ok {
			t.Errorf("row %d col 1: expected string, got %T", i, row[1])
		}
	}
}

func TestExecutor_OrderBy_DESC(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE desc_t (id INT PRIMARY KEY, score INT)")
	execSQL(t, db, "INSERT INTO desc_t (id, score) VALUES (1, 30)")
	execSQL(t, db, "INSERT INTO desc_t (id, score) VALUES (2, 10)")
	execSQL(t, db, "INSERT INTO desc_t (id, score) VALUES (3, 20)")

	res := execSQL(t, db, "SELECT * FROM desc_t ORDER BY score DESC")
	if res.RowsAffected != 3 {
		t.Fatalf("expected 3 rows, got %d", res.RowsAffected)
	}

	want := []int64{30, 20, 10}
	for i, row := range res.Rows {
		s, ok := row[1].(int64)
		if !ok {
			t.Fatalf("row %d score not int64: %T %v", i, row[1], row[1])
		}
		if s != want[i] {
			t.Errorf("row %d: want score %d, got %d", i, want[i], s)
		}
	}
}

func TestExecutor_LimitOffset(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE page_t (id INT PRIMARY KEY, val INT)")
	for i := 1; i <= 5; i++ {
		execSQL(t, db, fmt.Sprintf("INSERT INTO page_t (id, val) VALUES (%d, %d)", i, i*10))
	}

	// LIMIT 2 — first two rows.
	res := execSQL(t, db, "SELECT * FROM page_t ORDER BY id ASC LIMIT 2")
	if res.RowsAffected != 2 {
		t.Errorf("LIMIT 2: expected 2 rows, got %d", res.RowsAffected)
	}
	if id := res.Rows[0][0].(int64); id != 1 {
		t.Errorf("LIMIT 2: first row id = %d, want 1", id)
	}

	// LIMIT 2 OFFSET 2 — rows 3 and 4.
	res = execSQL(t, db, "SELECT * FROM page_t ORDER BY id ASC LIMIT 2 OFFSET 2")
	if res.RowsAffected != 2 {
		t.Errorf("LIMIT 2 OFFSET 2: expected 2 rows, got %d", res.RowsAffected)
	}
	if id := res.Rows[0][0].(int64); id != 3 {
		t.Errorf("LIMIT 2 OFFSET 2: first row id = %d, want 3", id)
	}

	// LIMIT 10 OFFSET 4 — only the last row remains.
	res = execSQL(t, db, "SELECT * FROM page_t ORDER BY id ASC LIMIT 10 OFFSET 4")
	if res.RowsAffected != 1 {
		t.Errorf("LIMIT 10 OFFSET 4: expected 1 row, got %d", res.RowsAffected)
	}
	if id := res.Rows[0][0].(int64); id != 5 {
		t.Errorf("LIMIT 10 OFFSET 4: row id = %d, want 5", id)
	}
}

func TestExecutor_ComplexWhere_AND(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE and_t (id INT PRIMARY KEY, age INT, name TEXT)")
	execSQL(t, db, "INSERT INTO and_t (id, age, name) VALUES (1, 25, 'Alice')")
	execSQL(t, db, "INSERT INTO and_t (id, age, name) VALUES (2, 17, 'Bob')")
	execSQL(t, db, "INSERT INTO and_t (id, age, name) VALUES (3, 30, 'Alice')")
	execSQL(t, db, "INSERT INTO and_t (id, age, name) VALUES (4, 22, 'Charlie')")

	res := execSQL(t, db, "SELECT * FROM and_t WHERE age > 20 AND name = 'Alice'")
	if res.RowsAffected != 2 {
		t.Fatalf("WHERE age > 20 AND name = 'Alice': expected 2 rows, got %d", res.RowsAffected)
	}
	for i, row := range res.Rows {
		// schema order: id(0), age(1), name(2)
		age, ok := row[1].(int64)
		if !ok {
			t.Errorf("row %d age not int64: %T", i, row[1])
			continue
		}
		name, ok := row[2].(string)
		if !ok {
			t.Errorf("row %d name not string: %T", i, row[2])
			continue
		}
		if age <= 20 {
			t.Errorf("row %d: age %d does not satisfy > 20", i, age)
		}
		if name != "Alice" {
			t.Errorf("row %d: name %q does not satisfy = 'Alice'", i, name)
		}
	}
}

func TestExecutor_MultiRowUpdate(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE upd_t (id INT PRIMARY KEY, status INT)")
	for i := 1; i <= 5; i++ {
		execSQL(t, db, fmt.Sprintf("INSERT INTO upd_t (id, status) VALUES (%d, 0)", i))
	}

	res := execSQL(t, db, "UPDATE upd_t SET status = 1")
	if res.RowsAffected != 5 {
		t.Errorf("UPDATE all: expected 5 rows affected, got %d", res.RowsAffected)
	}

	res = execSQL(t, db, "SELECT * FROM upd_t WHERE status = 1")
	if res.RowsAffected != 5 {
		t.Errorf("after UPDATE: expected 5 rows with status=1, got %d", res.RowsAffected)
	}

	res = execSQL(t, db, "SELECT * FROM upd_t WHERE status = 0")
	if res.RowsAffected != 0 {
		t.Errorf("after UPDATE: expected 0 rows with status=0, got %d", res.RowsAffected)
	}
}

func TestExecutor_MultiRowDelete(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE del_t (id INT PRIMARY KEY, grp INT)")
	for i := 1; i <= 6; i++ {
		grp := 1
		if i > 3 {
			grp = 2
		}
		execSQL(t, db, fmt.Sprintf("INSERT INTO del_t (id, grp) VALUES (%d, %d)", i, grp))
	}

	res := execSQL(t, db, "DELETE FROM del_t WHERE grp = 1")
	if res.RowsAffected != 3 {
		t.Errorf("DELETE grp=1: expected 3 affected, got %d", res.RowsAffected)
	}

	res = execSQL(t, db, "SELECT * FROM del_t")
	if res.RowsAffected != 3 {
		t.Errorf("after DELETE: expected 3 remaining rows, got %d", res.RowsAffected)
	}
}

func TestExecutor_DropTable_ThenSelect_ReturnsError(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE drop_t (id INT PRIMARY KEY, val INT)")
	execSQL(t, db, "INSERT INTO drop_t (id, val) VALUES (1, 10)")
	execSQL(t, db, "DROP TABLE drop_t")

	// Execute SELECT manually so we can capture the error without t.Fatalf.
	txnID := db.BeginTransaction(txn_unit.SERIALIZABLE)
	parsed, err := gosqlx.Parse("SELECT * FROM drop_t")
	if err != nil {
		_ = db.Abort(txnID)
		t.Fatalf("parse: %v", err)
	}
	_, execErr := executor.Execute(db, txnID, parsed.Statements[0])
	_ = db.Abort(txnID)
	if execErr == nil {
		t.Error("expected error selecting from dropped table, got nil")
	}
}

func TestExecutor_UpdateNonExistentRow(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE ghost_t (id INT PRIMARY KEY, val INT)")
	execSQL(t, db, "INSERT INTO ghost_t (id, val) VALUES (1, 10)")

	res := execSQL(t, db, "UPDATE ghost_t SET val = 99 WHERE id = 999")
	if res.RowsAffected != 0 {
		t.Errorf("UPDATE non-existent: expected 0 rows affected, got %d", res.RowsAffected)
	}

	// Original row untouched.
	check := execSQL(t, db, "SELECT * FROM ghost_t WHERE id = 1")
	if check.RowsAffected != 1 {
		t.Errorf("original row missing after no-op UPDATE")
	}
}

// TestExecutor_BlogWorkflow simulates a minimal blog platform: two users, four
// posts, one title update, one post deletion. Verifies row counts and field
// values at each step.
func TestExecutor_BlogWorkflow(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	execSQL(t, db, "CREATE TABLE posts (id INT PRIMARY KEY, author_id INT, title TEXT)")

	execSQL(t, db, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
	execSQL(t, db, "INSERT INTO users (id, name) VALUES (2, 'Bob')")

	execSQL(t, db, "INSERT INTO posts (id, author_id, title) VALUES (1, 1, 'Hello World')")
	execSQL(t, db, "INSERT INTO posts (id, author_id, title) VALUES (2, 1, 'Go Tips')")
	execSQL(t, db, "INSERT INTO posts (id, author_id, title) VALUES (3, 2, 'Bobs Post')")
	execSQL(t, db, "INSERT INTO posts (id, author_id, title) VALUES (4, 1, 'Another Post')")

	// Alice has 3 posts.
	res := execSQL(t, db, "SELECT * FROM posts WHERE author_id = 1")
	if res.RowsAffected != 3 {
		t.Errorf("Alice posts: expected 3, got %d", res.RowsAffected)
	}

	// Update a post title.
	res = execSQL(t, db, "UPDATE posts SET title = 'Updated: Hello World' WHERE id = 1")
	if res.RowsAffected != 1 {
		t.Errorf("UPDATE title: expected 1 affected, got %d", res.RowsAffected)
	}

	// Verify updated title — schema order: id(0), author_id(1), title(2).
	res = execSQL(t, db, "SELECT * FROM posts WHERE id = 1")
	if res.RowsAffected != 1 {
		t.Fatalf("post id=1 not found after title update")
	}
	if title, ok := res.Rows[0][2].(string); !ok || title != "Updated: Hello World" {
		t.Errorf("title: want 'Updated: Hello World', got %v", res.Rows[0][2])
	}

	// Delete Bob's post.
	res = execSQL(t, db, "DELETE FROM posts WHERE id = 3")
	if res.RowsAffected != 1 {
		t.Errorf("DELETE post: expected 1 affected, got %d", res.RowsAffected)
	}

	// Total posts now 3.
	res = execSQL(t, db, "SELECT * FROM posts")
	if res.RowsAffected != 3 {
		t.Errorf("after delete: expected 3 total posts, got %d", res.RowsAffected)
	}

	// Bob has 0 posts remaining.
	res = execSQL(t, db, "SELECT * FROM posts WHERE author_id = 2")
	if res.RowsAffected != 0 {
		t.Errorf("Bob posts after delete: expected 0, got %d", res.RowsAffected)
	}
}

// TestExecutor_ConcurrentInventory simulates two concurrent reservations from a
// single inventory item. Each goroutine uses a read-then-write SERIALIZABLE
// transaction and retries on write-write conflict. The final quantity must
// reflect both deductions with no lost updates.
func TestExecutor_ConcurrentInventory(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE inventory (item_id INT PRIMARY KEY, qty INT)")
	execSQL(t, db, "INSERT INTO inventory (item_id, qty) VALUES (1, 10)")

	const (
		reservations = 2
		each         = 3
	)

	var wg sync.WaitGroup
	errCh := make(chan error, reservations)

	for range reservations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				txnID := db.BeginTransaction(txn_unit.SERIALIZABLE)

				// Read current qty.
				rParsed, err := gosqlx.Parse("SELECT * FROM inventory WHERE item_id = 1")
				if err != nil {
					_ = db.Abort(txnID)
					errCh <- fmt.Errorf("parse SELECT: %w", err)
					return
				}
				res, err := executor.Execute(db, txnID, rParsed.Statements[0])
				if err != nil || len(res.Rows) == 0 {
					_ = db.Abort(txnID)
					errCh <- fmt.Errorf("SELECT inventory: %w", err)
					return
				}
				currentQty := res.Rows[0][1].(int64)
				newQty := currentQty - each

				// Write new qty.
				updateSQL := fmt.Sprintf("UPDATE inventory SET qty = %d WHERE item_id = 1", newQty)
				uParsed, err := gosqlx.Parse(updateSQL)
				if err != nil {
					_ = db.Abort(txnID)
					errCh <- fmt.Errorf("parse UPDATE: %w", err)
					return
				}
				if _, err = executor.Execute(db, txnID, uParsed.Statements[0]); err != nil {
					_ = db.Abort(txnID)
					errCh <- fmt.Errorf("UPDATE inventory: %w", err)
					return
				}

				// Commit — on conflict the transaction is auto-aborted; retry.
				if db.Commit(txnID) == nil {
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("goroutine error: %v", err)
	}

	res := execSQL(t, db, "SELECT * FROM inventory WHERE item_id = 1")
	if len(res.Rows) == 0 {
		t.Fatal("inventory row missing after reservations")
	}
	finalQty := res.Rows[0][1].(int64)
	want := int64(10 - reservations*each)
	if finalQty != want {
		t.Errorf("lost update detected: expected qty=%d, got %d", want, finalQty)
	}
}
