package schema

import (
	"bytes"
	"testing"
)

type MockStorageEngine struct {
	data map[string][]byte
}

func NewMockStorageEngine() *MockStorageEngine {
	return &MockStorageEngine{
		data: make(map[string][]byte),
	}
}

func (m *MockStorageEngine) Put(key []byte, value []byte) error {
	keyStr := string(key)
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	m.data[keyStr] = valCopy
	return nil
}

func (m *MockStorageEngine) Get(key []byte) ([]byte, error) {
	keyStr := string(key)
	if val, exists := m.data[keyStr]; exists {
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		return valCopy, nil
	}
	return nil, nil
}

func (m *MockStorageEngine) Delete(key []byte) error {
	keyStr := string(key)
	delete(m.data, keyStr)
	return nil
}

func TestSchemaManager_CreateTable(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("name", DataTypeString, false)

	err := manager.CreateTable(schema)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	err = manager.CreateTable(schema)
	if err == nil {
		t.Fatalf("expected error creating duplicate table")
	}
}

func TestSchemaManager_GetTable(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	manager.CreateTable(schema)

	retrieved, err := manager.GetTable("users")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	if retrieved.Name != "users" {
		t.Fatalf("table name mismatch")
	}

	_, err = manager.GetTable("nonexistent")
	if err == nil {
		t.Fatalf("expected error getting non-existent table")
	}
}

func TestSchemaManager_DropTable(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	manager.CreateTable(schema)

	if !manager.TableExists("users") {
		t.Fatalf("expected table to exist")
	}

	err := manager.DropTable("users")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	if manager.TableExists("users") {
		t.Fatalf("expected table to not exist after drop")
	}

	err = manager.DropTable("nonexistent")
	if err == nil {
		t.Fatalf("expected error dropping non-existent table")
	}
}

func TestSchemaManager_Persistence(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("email", DataTypeString, false)
	schema.AddIndex("email_idx", IndexTypeSecondary, []string{"email"}, true)

	err := manager.CreateTable(schema)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	schemaKey := []byte("__schema:users")
	if data, ok := storage.data[string(schemaKey)]; !ok || len(data) == 0 {
		t.Fatalf("schema not persisted to storage")
	}

	manager2 := NewSchemaManager(storage)
	err = manager2.LoadSchema("users")
	if err != nil {
		t.Fatalf("failed to load schema: %v", err)
	}

	retrieved, _ := manager2.GetTable("users")
	if retrieved.Name != "users" {
		t.Fatalf("loaded schema has wrong name")
	}

	if len(retrieved.Columns) != 2 {
		t.Fatalf("loaded schema has wrong number of columns")
	}
}

func TestSchemaManager_UpdateTable(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	manager.CreateTable(schema)

	retrieved, _ := manager.GetTable("users")
	retrieved.AddColumn("name", DataTypeString, false)

	err := manager.UpdateTable(retrieved)
	if err != nil {
		t.Fatalf("failed to update table: %v", err)
	}

	updated, _ := manager.GetTable("users")
	if len(updated.Columns) != 2 {
		t.Fatalf("expected 2 columns after update, got %d", len(updated.Columns))
	}
}

func TestSchemaManager_IndexOperations(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("email", DataTypeString, false)
	manager.CreateTable(schema)

	err := manager.AddIndex("users", "email_idx", IndexTypeSecondary, []string{"email"}, true)
	if err != nil {
		t.Fatalf("failed to add index: %v", err)
	}

	if !manager.IndexExists("users", "email_idx") {
		t.Fatalf("expected index to exist")
	}

	err = manager.RemoveIndex("users", "email_idx")
	if err != nil {
		t.Fatalf("failed to remove index: %v", err)
	}

	if manager.IndexExists("users", "email_idx") {
		t.Fatalf("expected index to not exist after removal")
	}
}

func TestSchemaManager_ColumnOperations(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	manager.CreateTable(schema)

	if !manager.ColumnExists("users", "id") {
		t.Fatalf("expected id column to exist")
	}

	if manager.ColumnExists("users", "nonexistent") {
		t.Fatalf("expected nonexistent column to not exist")
	}

	if manager.ColumnExists("nonexistent", "id") {
		t.Fatalf("expected column check to fail for non-existent table")
	}
}

func TestSchemaManager_ListTables(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	for i := 1; i <= 3; i++ {
		schema := NewTableSchema("table"+string(rune(48+i)), "id")
		schema.AddColumn("id", DataTypeInt64, false)
		manager.CreateTable(schema)
	}

	tables := manager.ListTables()
	if len(tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(tables))
	}
}

func TestSchemaManager_GetPrimaryKeyColumn(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	schema := NewTableSchema("users", "user_id")
	schema.AddColumn("user_id", DataTypeInt64, false)
	manager.CreateTable(schema)

	pk, err := manager.GetPrimaryKeyColumn("users")
	if err != nil {
		t.Fatalf("failed to get primary key: %v", err)
	}

	if pk != "user_id" {
		t.Fatalf("expected 'user_id', got %q", pk)
	}

	_, err = manager.GetPrimaryKeyColumn("nonexistent")
	if err == nil {
		t.Fatalf("expected error for non-existent table")
	}
}

func TestSchemaManager_IsSchemaKey(t *testing.T) {
	storage := NewMockStorageEngine()
	manager := NewSchemaManager(storage)

	schemaKey := []byte("__schema:users")
	dataKey := []byte("user:123")

	if !manager.IsSchemaKey(schemaKey) {
		t.Fatalf("expected schema key to be recognized")
	}

	if manager.IsSchemaKey(dataKey) {
		t.Fatalf("expected data key to not be recognized as schema key")
	}
}

func TestSecondaryIndexEntry_Encoding(t *testing.T) {
	entry := &SecondaryIndexEntry{
		IndexKey:    []byte("test@example.com"),
		PrimaryKeys: [][]byte{[]byte("user:1"), []byte("user:2"), []byte("user:3")},
	}

	encoded, err := EncodeSecondaryIndexEntry(entry)
	if err != nil {
		t.Fatalf("failed to encode entry: %v", err)
	}

	decoded, err := DecodeSecondaryIndexEntry(encoded)
	if err != nil {
		t.Fatalf("failed to decode entry: %v", err)
	}

	if len(decoded.PrimaryKeys) != 3 {
		t.Fatalf("expected 3 primary keys, got %d", len(decoded.PrimaryKeys))
	}

	for i := 0; i < 3; i++ {
		if !bytes.Equal(decoded.PrimaryKeys[i], entry.PrimaryKeys[i]) {
			t.Fatalf("primary key %d mismatch", i)
		}
	}
}

func TestSecondaryIndexManager_AddToIndex(t *testing.T) {
	storage := NewMockStorageEngine()
	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("email", DataTypeString, false)
	schema.AddIndex("email_idx", IndexTypeSecondary, []string{"email"}, false)

	indexMgr := NewSecondaryIndexManager("users", schema, storage)

	err := indexMgr.AddToIndex("email_idx", []byte("test@example.com"), []byte("user:1"))
	if err != nil {
		t.Fatalf("failed to add index entry: %v", err)
	}

	pks, err := indexMgr.GetPrimaryKeysForIndexValue("email_idx", []byte("test@example.com"))
	if err != nil {
		t.Fatalf("failed to get index entries: %v", err)
	}

	if len(pks) != 1 {
		t.Fatalf("expected 1 primary key, got %d", len(pks))
	}

	if !bytes.Equal(pks[0], []byte("user:1")) {
		t.Fatalf("primary key mismatch")
	}
}

func TestSecondaryIndexManager_RemoveFromIndex(t *testing.T) {
	storage := NewMockStorageEngine()
	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("email", DataTypeString, false)
	schema.AddIndex("email_idx", IndexTypeSecondary, []string{"email"}, false)

	indexMgr := NewSecondaryIndexManager("users", schema, storage)

	indexMgr.AddToIndex("email_idx", []byte("test@example.com"), []byte("user:1"))
	indexMgr.AddToIndex("email_idx", []byte("test@example.com"), []byte("user:2"))

	err := indexMgr.RemoveFromIndex("email_idx", []byte("test@example.com"), []byte("user:1"))
	if err != nil {
		t.Fatalf("failed to remove index entry: %v", err)
	}

	pks, _ := indexMgr.GetPrimaryKeysForIndexValue("email_idx", []byte("test@example.com"))
	if len(pks) != 1 {
		t.Fatalf("expected 1 primary key, got %d", len(pks))
	}

	indexMgr.RemoveFromIndex("email_idx", []byte("test@example.com"), []byte("user:2"))

	pks, _ = indexMgr.GetPrimaryKeysForIndexValue("email_idx", []byte("test@example.com"))
	if len(pks) != 0 {
		t.Fatalf("expected 0 primary keys, got %d", len(pks))
	}
}
