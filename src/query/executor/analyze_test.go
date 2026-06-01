package executor_test

import (
	"errors"
	"testing"
	"time"

	"github.com/rodrigo0345/omag/src/query/executor"
	txn_unit "github.com/rodrigo0345/omag/src/engine/txn"
	"github.com/rodrigo0345/omag/pkg/pkglog"
)

func TestExecAnalyze_BasicStats(t *testing.T) {
	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE people (id INT PRIMARY KEY, age INT)")
	execSQL(t, db, "INSERT INTO people (id, age) VALUES (1, 30)")
	execSQL(t, db, "INSERT INTO people (id, age) VALUES (2, 25)")
	execSQL(t, db, "INSERT INTO people (id, age) VALUES (3, 40)")

	txnID := db.BeginTransaction(txn_unit.READ_COMMITTED)
	result, err := executor.ExecAnalyze(db, txnID, "people")
	if err != nil {
		t.Fatalf("ExecAnalyze: %v", err)
	}
	if err := db.Commit(txnID); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if result.Tag != "ANALYZE" {
		t.Errorf("expected tag ANALYZE, got %q", result.Tag)
	}
	if len(result.Rows) != 2 { // id and age columns
		t.Errorf("expected 2 column rows, got %d", len(result.Rows))
	}

	stats, ok := db.GetTelemetry().GetTableStats("people")
	if !ok {
		t.Fatal("expected table stats to be stored in telemetry")
	}
	if stats.RowCount != 3 {
		t.Errorf("expected row count 3, got %d", stats.RowCount)
	}
	ageStats, ok := stats.Columns["age"]
	if !ok {
		t.Fatal("expected age column stats")
	}
	if ageStats.DistinctCount != 3 {
		t.Errorf("expected 3 distinct age values, got %d", ageStats.DistinctCount)
	}
	if ageStats.NullCount != 0 {
		t.Errorf("expected 0 nulls, got %d", ageStats.NullCount)
	}
}

func TestExecAnalyze_MissingTable(t *testing.T) {
	db := openTestDB(t)
	txnID := db.BeginTransaction(txn_unit.READ_COMMITTED)
	_, err := executor.ExecAnalyze(db, txnID, "nonexistent")
	_ = db.Abort(txnID)
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

func TestExecAnalyze_EmptyTable(t *testing.T) {
	db := openTestDB(t)
	execSQL(t, db, "CREATE TABLE empty_tbl (id INT PRIMARY KEY, val INT)")

	txnID := db.BeginTransaction(txn_unit.READ_COMMITTED)
	_, err := executor.ExecAnalyze(db, txnID, "empty_tbl")
	if err != nil {
		t.Fatalf("ExecAnalyze empty: %v", err)
	}
	_ = db.Commit(txnID)

	stats, _ := db.GetTelemetry().GetTableStats("empty_tbl")
	if stats.RowCount != 0 {
		t.Errorf("expected 0 rows, got %d", stats.RowCount)
	}
}

func TestTelemetry_SpanLifecycle(t *testing.T) {
	db := openTestDB(t)
	tel := db.GetTelemetry()

	// Simulate what the session does for each statement.
	span := tel.StartSpan(42, "SELECT * FROM t", "SELECT", "t")
	if span == nil {
		t.Fatal("StartSpan returned nil")
	}
	if span.TxnID != 42 {
		t.Errorf("expected TxnID 42, got %d", span.TxnID)
	}
	if span.StatementType != "SELECT" {
		t.Errorf("expected SELECT, got %q", span.StatementType)
	}

	time.Sleep(time.Millisecond)
	tel.EndSpan(span, 5, nil)

	if tel.TotalQueries.Load() != 1 {
		t.Errorf("expected 1 total query, got %d", tel.TotalQueries.Load())
	}
	if tel.TotalErrors.Load() != 0 {
		t.Errorf("expected 0 errors, got %d", tel.TotalErrors.Load())
	}

	spans := tel.RecentSpans(1)
	if len(spans) != 1 {
		t.Fatalf("expected 1 recent span, got %d", len(spans))
	}
	if spans[0].RowsAffected != 5 {
		t.Errorf("expected RowsAffected=5, got %d", spans[0].RowsAffected)
	}
	if spans[0].Duration < time.Millisecond {
		t.Errorf("expected Duration >= 1ms, got %v", spans[0].Duration)
	}
}

func TestTelemetry_ErrorSpan(t *testing.T) {
	tel := pkglog.NewTelemetryCollector()
	span := tel.StartSpan(1, "bad query", "SELECT", "t")
	tel.EndSpan(span, 0, errors.New("query failed"))

	if tel.TotalErrors.Load() != 1 {
		t.Errorf("expected 1 error, got %d", tel.TotalErrors.Load())
	}
	if tel.RecentSpans(1)[0].Error == "" {
		t.Error("expected non-empty error in span")
	}
}

func TestTelemetry_ConflictRecording(t *testing.T) {
	db := openTestDB(t)
	execSQL(t, db, "CREATE TABLE conflict_tbl (id INT PRIMARY KEY, val INT)")
	execSQL(t, db, "INSERT INTO conflict_tbl (id, val) VALUES (1, 10)")

	// Both t1 and t2 write to the same key; the second commit must conflict.
	t1 := db.BeginTransaction(txn_unit.SERIALIZABLE)
	t2 := db.BeginTransaction(txn_unit.SERIALIZABLE)

	// Row format: [0x01 _txn_op][null_id=0][null_val=0][id=int32LE][val=int32LE]
	row := func(id, val int32) []byte {
		b := make([]byte, 11)
		b[0] = 0x01                           // _txn_op
		b[1] = 0x00                           // null flag for id (not null)
		b[2] = 0x00                           // null flag for val (not null)
		b[3] = byte(id); b[4] = byte(id >> 8); b[5] = byte(id >> 16); b[6] = byte(id >> 24)
		b[7] = byte(val); b[8] = byte(val >> 8); b[9] = byte(val >> 16); b[10] = byte(val >> 24)
		return b
	}
	key := []byte{0, 0, 0, 1}

	if err := db.Write(t1, "conflict_tbl", key, row(1, 99)); err != nil {
		t.Fatalf("t1 write: %v", err)
	}
	if err := db.Write(t2, "conflict_tbl", key, row(1, 77)); err != nil {
		t.Fatalf("t2 write: %v", err)
	}

	if err := db.Commit(t1); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	_ = db.Commit(t2) // expected to conflict; error is OK

	if db.GetTelemetry().TotalConflicts.Load() == 0 {
		t.Error("expected at least one conflict to be recorded")
	}
}

func TestTelemetry_AbortRecording(t *testing.T) {
	db := openTestDB(t)
	txnID := db.BeginTransaction(txn_unit.READ_COMMITTED)
	_ = db.Abort(txnID)

	if db.GetTelemetry().TotalAborts.Load() == 0 {
		t.Error("expected at least one abort to be recorded")
	}
}

func TestTelemetry_TableStats(t *testing.T) {
	tel := pkglog.NewTelemetryCollector()

	stats := &pkglog.TableStats{
		TableName: "users",
		RowCount:  100,
		Columns: map[string]pkglog.ColumnStat{
			"id": {DistinctCount: 100},
		},
	}
	tel.SetTableStats(stats)

	got, ok := tel.GetTableStats("users")
	if !ok {
		t.Fatal("expected stats to be retrievable")
	}
	if got.RowCount != 100 {
		t.Errorf("expected 100 rows, got %d", got.RowCount)
	}

	_, ok = tel.GetTableStats("nonexistent")
	if ok {
		t.Error("expected false for nonexistent table")
	}
}
