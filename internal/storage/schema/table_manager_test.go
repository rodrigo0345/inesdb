package schema

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/mocks"
)

func setupTestManager() (*TableManager, *mocks.MockStorageEngine, *mocks.MockStorageEngine) {
	tm := NewTableManager()
	primary := mocks.NewMockStorage()
	secondary := mocks.NewMockStorage()

	cols := []Column{
		{Name: "id", Type: TypeInt32},
		{Name: "age", Type: TypeInt32},
		{Name: "active", Type: TypeBool},
		{Name: "name", Type: TypeString},
	}
	ts := NewTableSchema("users", cols)
	ts.AddIndex("PRIMARY", []string{"id"}, primary)
	ts.AddIndex("idx_age", []string{"age"}, secondary)

	tm.CreateTable(ts, true)
	return tm, primary, secondary
}

func TestTableManager_Lifecycle(t *testing.T) {
	tm := NewTableManager()
	ts := NewTableSchema("test_table", []Column{{Name: "id", Type: TypeInt32}})
	ts.AddIndex("PRIMARY", []string{"id"}, mocks.NewMockStorage())

	if err := tm.CreateTable(ts, true); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	tables := tm.GetAllTables()
	if len(tables) != 1 || tables[0] != "test_table" {
		t.Errorf("expected 1 table, got %v", tables)
	}

	if err := tm.DropTable("test_table"); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	if _, err := tm.GetTableSchema("test_table"); err == nil {
		t.Error("table should not exist after drop")
	}
}

func TestTableManager_ConcurrentAccess(t *testing.T) {
	tm := NewTableManager()
	var wg sync.WaitGroup
	count := 100

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("table_%d", n)
			ts := NewTableSchema(name, []Column{{Name: "id", Type: TypeInt32}})
			ts.AddIndex("PRIMARY", []string{"id"}, mocks.NewMockStorage())
			tm.CreateTable(ts, false)
			tm.GetAllTables()
		}(i)
	}

	wg.Wait()
	if len(tm.GetAllTables()) != count {
		t.Errorf("concurrency issue: expected %d tables, got %d", count, len(tm.GetAllTables()))
	}
}

func TestTableManager_WriteConsistency(t *testing.T) {
	tm, primary, secondary := setupTestManager()

	pKey := []byte("physical-1")
	row := buildRow(int32(1), int32(30), true, "Alice")

	op := WriteOperation{
		TableName: "users",
		Key:       pKey,
		Value:     row,
	}

	if err := tm.Write(op); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Verify Primary: Key -> Full Row
	pVal, _ := primary.Get(pKey)
	if !bytes.Equal(pVal, row) {
		t.Error("primary storage data mismatch")
	}

	// Verify Secondary: Age(30) -> Physical Key
	ageKey := buildInt32(30)
	sVal, _ := secondary.Get(ageKey)
	if !bytes.Equal(sVal, pKey) {
		t.Errorf("secondary index points to wrong key. Got %s, want %s", sVal, pKey)
	}
}

func TestTableManager_DeleteConsistency(t *testing.T) {
	tm, primary, secondary := setupTestManager()

	pKey := []byte("p1")
	row := buildRow(int32(1), int32(30), true, "Alice")
	tm.Write(WriteOperation{"users", pKey, row})

	delOp := DeleteOperation{
		TableName:   "users",
		Key:         pKey,
		BeforeImage: row,
	}

	if err := tm.Delete(delOp); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Check primary
	if val, _ := primary.Get(pKey); val != nil {
		t.Error("primary record remains")
	}

	// Check secondary
	if val, _ := secondary.Get(buildInt32(30)); val != nil {
		t.Error("secondary index remains")
	}
}

// --- SCAN / QUERY TESTS ---

func TestTableManager_TwoHopScan(t *testing.T) {
	tm, _, _ := setupTestManager()

	// Insert data: Alice(30), Bob(20)
	tm.Write(WriteOperation{"users", []byte("pk_a"), buildRow(int32(1), int32(30), true, "Alice")})
	tm.Write(WriteOperation{"users", []byte("pk_b"), buildRow(int32(2), int32(20), true, "Bob")})

	// Scan by Age (Secondary Index)
	// Even though secondary storage only holds physical keys,
	// the TableManager should return the full row via TwoHopCursor.
	cursor, err := tm.Scan("users", "idx_age", storage.ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	foundBob := false
	for cursor.Next() {
		entry := cursor.Entry()
		if bytes.Contains(entry.Value, []byte("Bob")) {
			foundBob = true
		}
	}

	if !foundBob {
		t.Error("two-hop resolution failed to retrieve Bob's full row")
	}
}

func TestTableManager_FullTableScan(t *testing.T) {
	tm, _, _ := setupTestManager()
	tm.Write(WriteOperation{"users", []byte("1"), buildRow(int32(1), int32(10), true, "A")})
	tm.Write(WriteOperation{"users", []byte("2"), buildRow(int32(2), int32(20), true, "B")})

	cursor, _ := tm.FullTableScan("users", storage.ScanOptions{})
	count := 0
	for cursor.Next() {
		count++
	}

	if count != 2 {
		t.Errorf("FullTableScan expected 2 rows, got %d", count)
	}
}

func TestTableManager_ProjectionEdgeCases(t *testing.T) {
	tm, _, _ := setupTestManager()
	row := buildRow(int32(1), int32(30), true, "Alice")
	tm.Write(WriteOperation{"users", []byte("pk1"), row})

	// Project non-existent column
	opts := storage.ScanOptions{
		Projection: []string{"ghost_col"},
	}
	cursor, _ := tm.Scan("users", "PRIMARY", opts)
	cursor.Next()
	if len(cursor.Entry().Value) != 0 {
		t.Error("projection of non-existent column should return empty buffer")
	}
}

func TestTableManager_ScanOptionsStacking(t *testing.T) {
	tm, _, _ := setupTestManager()

	// Insert 5 rows
	for i := int32(1); i <= 5; i++ {
		tm.Write(WriteOperation{"users", buildInt32(i), buildRow(i, i*10, true, "Player")})
	}

	// Options: Filter (Age > 15) -> Offset 1 -> Limit 1
	// Values: [20, 30, 40, 50] (After filter)
	// Values: [30, 40, 50] (After offset 1)
	// Values: [30] (After limit 1)
	opts := storage.ScanOptions{
		ComplexFilter: storage.RowFilterFunction(func(row storage.ScanEntry) bool {
			age := tm.DecodeRow("users", "age", row.Value).Int()
			return age > 15
		}),
		Offset: 1,
		Limit:  1,
	}

	cursor, _ := tm.Scan("users", "idx_age", opts)
	cursor.Next()
	age := tm.DecodeRow("users", "age", cursor.Entry().Value)

	if age.Int() != 30 {
		t.Errorf("stacked options failed. Got age %d, want 30", age.Int())
	}
}

func TestTableManager_ProjectionCorruption(t *testing.T) {
	// Test the internal projection helper for malformed data
	cols := []Column{{Name: "name", Type: TypeString}}

	// Value is too short to contain string length header (4 bytes)
	_, err := extractProjectedColumns(cols, []byte{1, 2}, []string{"name"})
	if err == nil {
		t.Error("expected error for truncated string payload")
	}

	// Value says string is length 10, but only 2 bytes provided
	corruptPayload := []byte{0, 0, 0, 10, 1, 2}
	_, err = extractProjectedColumns(cols, corruptPayload, []string{"name"})
	if err == nil {
		t.Error("expected error for payload shorter than specified length")
	}
}
