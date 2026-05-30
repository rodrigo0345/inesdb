package executor_test

import (
    "os"
    "path/filepath"
    "testing"

    "github.com/ajitpratap0/GoSQLX/pkg/gosqlx"
    "github.com/rodrigo0345/omag/src/engine"
    "github.com/rodrigo0345/omag/src/query/executor"
    txn_unit "github.com/rodrigo0345/omag/src/engine/txn"
)

func execSQL(t *testing.T, db engine.Database, sql string) *executor.Result {
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

func openTestDB(t *testing.T) engine.Database {
    t.Helper()
    tmpDir := t.TempDir()
    db, err := engine.OpenMVCCLSM(engine.Options{
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

func TestExecutor_CRUD(t *testing.T) {
    db := openTestDB(t)

    execSQL(t, db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")

    execSQL(t, db, "INSERT INTO t (id, val) VALUES (1, 10)")
    execSQL(t, db, "INSERT INTO t (id, val) VALUES (2, 20)")
    execSQL(t, db, "INSERT INTO t (id, val) VALUES (3, 30)")

    res := execSQL(t, db, "SELECT * FROM t")
    if res.RowsAffected != 3 {
        t.Errorf("SELECT: expected 3 rows, got %d", res.RowsAffected)
    }

    res = execSQL(t, db, "SELECT * FROM t WHERE val > 10")
    if res.RowsAffected != 2 {
        t.Errorf("WHERE: expected 2 rows, got %d", res.RowsAffected)
    }

    res = execSQL(t, db, "UPDATE t SET val = 99 WHERE id = 1")
    if res.RowsAffected != 1 {
        t.Errorf("UPDATE: expected 1 affected, got %d", res.RowsAffected)
    }

    res = execSQL(t, db, "SELECT * FROM t WHERE id = 1")
    if res.RowsAffected != 1 {
        t.Errorf("SELECT after UPDATE: expected 1 row, got %d", res.RowsAffected)
    }

    res = execSQL(t, db, "DELETE FROM t WHERE id = 2")
    if res.RowsAffected != 1 {
        t.Errorf("DELETE: expected 1 affected, got %d", res.RowsAffected)
    }

    res = execSQL(t, db, "SELECT * FROM t")
    if res.RowsAffected != 2 {
        t.Errorf("SELECT after DELETE: expected 2 rows, got %d", res.RowsAffected)
    }

    execSQL(t, db, "DROP TABLE t")
    _ = os.Stderr
}

func TestExecutor_OrderBy(t *testing.T) {
    db := openTestDB(t)

    execSQL(t, db, "CREATE TABLE vals (id INT PRIMARY KEY, score INT)")
    execSQL(t, db, "INSERT INTO vals (id, score) VALUES (1, 30)")
    execSQL(t, db, "INSERT INTO vals (id, score) VALUES (2, 10)")
    execSQL(t, db, "INSERT INTO vals (id, score) VALUES (3, 20)")

    res := execSQL(t, db, "SELECT * FROM vals ORDER BY score ASC")
    if res.RowsAffected != 3 {
        t.Fatalf("expected 3 rows, got %d", res.RowsAffected)
    }
    // rows should be ordered 10, 20, 30
    scores := []int64{10, 20, 30}
    for i, row := range res.Rows {
        // score is second column
        if len(row) < 2 {
            t.Fatalf("row %d too short: %v", i, row)
        }
        s, ok := row[1].(int64)
        if !ok {
            t.Fatalf("row %d score not int64: %T %v", i, row[1], row[1])
        }
        if s != scores[i] {
            t.Errorf("row %d: expected score %d, got %d", i, scores[i], s)
        }
    }
}

func TestExecutor_ExplicitTransaction(t *testing.T) {
    db := openTestDB(t)

    execSQL(t, db, "CREATE TABLE txtest (id INT PRIMARY KEY, val INT)")
    execSQL(t, db, "INSERT INTO txtest (id, val) VALUES (1, 100)")

    // Explicit transaction that gets rolled back.
    txnID := db.BeginTransaction(txn_unit.SERIALIZABLE)

    parsed, _ := gosqlx.Parse("INSERT INTO txtest (id, val) VALUES (2, 200)")
    _, err := executor.Execute(db, txnID, parsed.Statements[0])
    if err != nil {
        t.Fatalf("insert in txn: %v", err)
    }

    _ = db.Abort(txnID)

    // Row 2 should not be visible.
    res := execSQL(t, db, "SELECT * FROM txtest")
    if res.RowsAffected != 1 {
        t.Errorf("after rollback: expected 1 row, got %d", res.RowsAffected)
    }
}
