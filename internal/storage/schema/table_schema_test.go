package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/mocks"
)

func setupTestSchema() (*TableSchema, *mocks.MockStorageEngine, *mocks.MockStorageEngine) {
	cols := []Column{
		{Name: "id", Type: TypeInt32},
		{Name: "age", Type: TypeInt32},
		{Name: "name", Type: TypeString},
	}
	ts := NewTableSchema("users", cols)

	primary := mocks.NewMockStorage()
	secondary := mocks.NewMockStorage()

	ts.AddIndex("PRIMARY", []string{"id"}, primary)
	ts.AddIndex("idx_age", []string{"age"}, secondary)

	return ts, primary, secondary
}

func TestTableManager_WritePropagation(t *testing.T) {
	tm := NewTableManager()
	ts, primary, secondary := setupTestSchema()
	tm.CreateTable(ts, true)

	// Mock data: id=1, age=25, name="Bob"
	payload := bytes.Join([][]byte{
		{0x01}, // metadata
		buildInt32(1),
		buildInt32(25),
		buildString("Bob"),
	}, nil)

	physicalKey := []byte("p-key-v1")

	op := WriteOperation{
		TableName: "users",
		Key:       physicalKey,
		Value:     payload,
	}

	if err := tm.Write(op); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify Primary Storage
	val, _ := primary.Get(physicalKey)
	if !bytes.Equal(val, payload) {
		t.Errorf("Primary storage mismatch. Got %v, want %v", val, payload)
	}

	// Verify Secondary Storage maps Age (25) -> Physical Key
	ageKey := buildInt32(25)
	ptr, _ := secondary.Get(ageKey)
	if !bytes.Equal(ptr, physicalKey) {
		t.Errorf("Secondary storage pointer mismatch. Got %x, want %x", ptr, physicalKey)
	}
}

func TestTableManager_DeleteCleanup(t *testing.T) {
	tm := NewTableManager()
	ts, primary, secondary := setupTestSchema()
	tm.CreateTable(ts, true)

	physicalKey := []byte("p-key-v1")
	payload := buildRow(int32(1), int32(25), "Alice")

	tm.Write(WriteOperation{"users", physicalKey, payload})

	delOp := DeleteOperation{
		TableName:   "users",
		Key:         physicalKey,
		BeforeImage: payload,
	}

	if err := tm.Delete(delOp); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if val, _ := primary.Get(physicalKey); val != nil {
		t.Error("Primary record was not deleted")
	}
	if ptr, _ := secondary.Get(buildInt32(25)); ptr != nil {
		t.Error("Secondary index entry was not purged")
	}
}

func TestTableManager_TwoHopScanAndFiltering(t *testing.T) {
	tm := NewTableManager()
	ts, _, _ := setupTestSchema()
	tm.CreateTable(ts, true)

	pKey1 := []byte("pk1")
	row1 := buildRow(int32(1), int32(30), "Alice")
	pKey2 := []byte("pk2")
	row2 := buildRow(int32(2), int32(25), "Bob")

	tm.Write(WriteOperation{"users", pKey1, row1})
	tm.Write(WriteOperation{"users", pKey2, row2})

	// Scan 'idx_age' for Age > 28
	opts := storage.ScanOptions{
		ComplexFilter: storage.RowFilterFunction(func(row storage.ScanEntry) bool {
			value := ts.DecodeRow("age", row.Value)
			age := value.Int()
			return age > 28
		}),
	}

	cursor, err := tm.Scan("users", "idx_age", opts)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	defer cursor.Close()

	count := 0
	for cursor.Next() {
		count++
		entry := cursor.Entry()
		if !bytes.Contains(entry.Value, []byte("Alice")) {
			t.Errorf("Unexpected row returned: %s", entry.Value)
		}
	}

	if count != 1 {
		t.Errorf("Expected 1 row after filtering, got %d", count)
	}
}

func buildInt32(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func buildString(s string) []byte {
	b := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(b[0:4], uint32(len(s)))
	copy(b[4:], s)
	return b
}

func buildRow(values ...interface{}) []byte {
	buf := new(bytes.Buffer)

	buf.WriteByte(0x01)

	for _, v := range values {
		switch t := v.(type) {
		case int32:
			binary.Write(buf, binary.BigEndian, t)
		case int64:
			binary.Write(buf, binary.BigEndian, t)
		case bool:
			binary.Write(buf, binary.BigEndian, t)
		case string:
			binary.Write(buf, binary.BigEndian, uint32(len(t)))
			buf.WriteString(t)
		}
	}
	return buf.Bytes()
}

func setupFullManager() (*TableManager, *TableSchema) {
	tm := NewTableManager()
	cols := []Column{
		{Name: "id", Type: TypeInt32},
		{Name: "score", Type: TypeInt32},
		{Name: "active", Type: TypeBool},
		{Name: "name", Type: TypeString},
	}
	ts := NewTableSchema("players", cols)
	ts.AddIndex("PRIMARY", []string{"id"}, mocks.NewMockStorage())
	ts.AddIndex("idx_score", []string{"score"}, mocks.NewMockStorage())
	tm.CreateTable(ts, true)
	return tm, ts
}

// --- TESTS ---

func TestTableManager_TableManagement(t *testing.T) {
	tm := NewTableManager()
	ts, _, _ := setupTestSchema() // From previous context

	// 1. Create duplicate table
	err := tm.CreateTable(ts, true)
	if err != nil {
		t.Fatal(err)
	}
	err = tm.CreateTable(ts, true)
	if err == nil {
		t.Error("Expected error on duplicate table creation")
	}

	// 2. Add Index to existing table
	idx := IndexDefinition{Name: "idx_new", Columns: []string{"name"}}
	err = tm.CreateIndex("players_non_existent", idx, mocks.NewMockStorage())
	if err == nil {
		t.Error("Expected error creating index on missing table")
	}

	err = tm.CreateIndex("users", idx, mocks.NewMockStorage())
	if err != nil {
		t.Errorf("Failed to add index: %v", err)
	}
}

func TestTableManager_ScanPagination(t *testing.T) {
	tm, _ := setupFullManager()

	// Insert 10 players with scores 10, 20, ..., 100
	for i := int32(1); i <= 10; i++ {
		payload := buildRow(i, i*10, true, fmt.Sprintf("Player%d", i))
		tm.Write(WriteOperation{"players", buildInt32(i), payload})
	}

	// Test: Offset 3, Limit 2 (Should get scores 40, 50)
	opts := storage.ScanOptions{
		Offset: 3,
		Limit:  2,
	}

	cursor, _ := tm.Scan("players", "idx_score", opts)
	ts, err := tm.GetTableSchema("players")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	defer cursor.Close()

	var results []int32
	for cursor.Next() {
		entry := cursor.Entry()
		score := ts.DecodeRow("score", entry.Value)
		results = append(results, int32(score.Int()))
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results[0] != 40 || results[1] != 50 {
		t.Errorf("Pagination failed. Got %v", results)
	}
}

func TestTableManager_ComplexFilterLogic(t *testing.T) {
	tm, _ := setupFullManager()

	tm.Write(WriteOperation{"players", []byte("p1"), buildRow(int32(1), int32(90), true, "Alice")})
	tm.Write(WriteOperation{"players", []byte("p2"), buildRow(int32(2), int32(95), false, "Bob")})
	tm.Write(WriteOperation{"players", []byte("p3"), buildRow(int32(3), int32(50), true, "Charlie")})

	// Filter: Active == True AND Score > 60
	opts := storage.ScanOptions{
		ComplexFilter: storage.RowFilterFunction(func(row storage.ScanEntry) bool {
			  // Use the DecodeRow method from the TableManager
			  score := tm.DecodeRow("players", "score", row.Value).Int()
			  active := tm.DecodeRow("players", "active", row.Value).Bool()

			  return active && score > 60
		    }),
	}

	cursor, _ := tm.Scan("players", "PRIMARY", opts)
	count := 0
	for cursor.Next() {
		count++
		aliceString := tm.DecodeRow("players", "name", cursor.Entry().Value).String()
		if !bytes.Contains([]byte(aliceString), []byte("Alice")) {
			t.Errorf("Expected Alice, got %s", cursor.Entry().Value)
		}
	}

	if count != 1 {
		t.Errorf("Expected exactly 1 result, got %d", count)
	}
}

func TestTableManager_ProjectionAndKeyOnly(t *testing.T) {
	tm, _ := setupFullManager()
	payload := buildRow(int32(1), int32(100), true, "ProPlayer")
	tm.Write(WriteOperation{"players", []byte("pk1"), payload})

	// 1. Test KeyOnly
	optsKey := storage.ScanOptions{KeyOnly: true}
	cursor, _ := tm.Scan("players", "PRIMARY", optsKey)
	cursor.Next()
	if cursor.Entry().Value != nil {
		t.Error("KeyOnly scan should return nil value")
	}
	cursor.Close()

	// 2. Test Partial Projection (Score and Name only)
	optsProj := storage.ScanOptions{
		Projection: []string{"score", "name"},
	}
	cursor, _ = tm.Scan("players", "PRIMARY", optsProj)
	cursor.Next()

	val := cursor.Entry().Value
	// Score is 4 bytes, Name is 4 (len) + 9 (string) = 13 bytes. Total 17.
	if len(val) != 17 {
		t.Errorf("Projected payload length mismatch. Got %d, want 17", len(val))
	}

	score := int32(binary.BigEndian.Uint32(val[0:4]))
	if score != 100 {
		t.Errorf("Projected score mismatch: %d", score)
	}

	nameLen := binary.BigEndian.Uint32(val[4:8])
	name := string(val[8 : 8+nameLen])
	if name != "ProPlayer" {
		t.Errorf("Projected name mismatch: %s", name)
	}
}

func TestTableManager_ReverseScan(t *testing.T) {
	tm, _ := setupFullManager()
	tm.Write(WriteOperation{"players", buildInt32(1), buildRow(int32(1), int32(10), true, "A")})
	tm.Write(WriteOperation{"players", buildInt32(2), buildRow(int32(2), int32(20), true, "B")})

	opts := storage.ScanOptions{Reverse: true}
	cursor, _ := tm.Scan("players", "idx_score", opts)

	cursor.Next()
	scoreFirst := tm.DecodeRow("players", "score", cursor.Entry().Value)
	if scoreFirst.Int() != 20 {
		t.Errorf("Reverse scan failed, expected score 20 first, got %d", scoreFirst.Int())
	}
}

func TestTableManager_ErrorPaths(t *testing.T) {
	tm := NewTableManager()

	// Missing Table
	_, err := tm.Scan("ghost_table", "PRIMARY", storage.ScanOptions{})
	if err == nil {
		t.Error("Expected error scanning missing table")
	}

	// Missing Index
	ts, _, _ := setupTestSchema()
	tm.CreateTable(ts, true)
	_, err = tm.Scan("users", "non_existent_idx", storage.ScanOptions{})
	if err == nil {
		t.Error("Expected error scanning missing index")
	}
}
