package schema

import (
	"fmt"
	"sync"
)

type SchemaManager struct {
	schemas map[string]*TableSchema
	mu      sync.RWMutex

	storageBackend interface {
		Put(key []byte, value []byte) error
		Get(key []byte) ([]byte, error)
		Delete(key []byte) error
	}

	schemaNamespace string
}

func NewSchemaManager(storageBackend interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}) *SchemaManager {
	return &SchemaManager{
		schemas:         make(map[string]*TableSchema),
		storageBackend:  storageBackend,
		schemaNamespace: "__schema:",
	}
}

func (sm *SchemaManager) CreateTable(schema *TableSchema) error {
	if err := schema.Validate(); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.schemas[schema.Name]; exists {
		return fmt.Errorf("table %q already exists", schema.Name)
	}

	schemaJSON, err := schema.ToJSON()
	if err != nil {
		return err
	}

	key := sm.getSchemaKey(schema.Name)
	if err := sm.storageBackend.Put(key, schemaJSON); err != nil {
		return fmt.Errorf("failed to persist schema: %w", err)
	}

	sm.schemas[schema.Name] = schema
	return nil
}

func (sm *SchemaManager) DropTable(tableName string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.schemas[tableName]; !exists {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	key := sm.getSchemaKey(tableName)
	if err := sm.storageBackend.Delete(key); err != nil {
		return fmt.Errorf("failed to delete schema: %w", err)
	}

	delete(sm.schemas, tableName)
	return nil
}

func (sm *SchemaManager) GetTable(tableName string) (*TableSchema, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	schema, exists := sm.schemas[tableName]
	if !exists {
		return nil, fmt.Errorf("table %q not found", tableName)
	}

	return schema, nil
}

func (sm *SchemaManager) LoadSchema(tableName string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.schemas[tableName]; exists {
		return fmt.Errorf("table %q already loaded", tableName)
	}

	key := sm.getSchemaKey(tableName)
	schemaJSON, err := sm.storageBackend.Get(key)
	if err != nil {
		return fmt.Errorf("table %q not found in storage: %w", tableName, err)
	}

	schema, err := FromJSON(schemaJSON)
	if err != nil {
		return fmt.Errorf("failed to deserialize schema: %w", err)
	}

	sm.schemas[tableName] = schema
	return nil
}

func (sm *SchemaManager) LoadAllSchemas() ([]string, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return nil, fmt.Errorf("LoadAllSchemas not implemented - requires storage engine iterator support")
}

func (sm *SchemaManager) UpdateTable(schema *TableSchema) error {
	if err := schema.Validate(); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.schemas[schema.Name]; !exists {
		return fmt.Errorf("table %q does not exist", schema.Name)
	}

	schemaJSON, err := schema.ToJSON()
	if err != nil {
		return err
	}

	key := sm.getSchemaKey(schema.Name)
	if err := sm.storageBackend.Put(key, schemaJSON); err != nil {
		return fmt.Errorf("failed to persist schema: %w", err)
	}

	sm.schemas[schema.Name] = schema
	return nil
}

func (sm *SchemaManager) AddIndex(tableName string, indexName string, indexType IndexType, columns []string, isUnique bool) error {
	sm.mu.Lock()
	schema, exists := sm.schemas[tableName]
	if !exists {
		sm.mu.Unlock()
		return fmt.Errorf("table %q does not exist", tableName)
	}
	sm.mu.Unlock()

	if err := schema.AddIndex(indexName, indexType, columns, isUnique); err != nil {
		return err
	}

	return sm.UpdateTable(schema)
}

func (sm *SchemaManager) RemoveIndex(tableName string, indexName string) error {
	sm.mu.Lock()
	schema, exists := sm.schemas[tableName]
	if !exists {
		sm.mu.Unlock()
		return fmt.Errorf("table %q does not exist", tableName)
	}

	if _, exists := schema.Indexes[indexName]; !exists {
		sm.mu.Unlock()
		return fmt.Errorf("index %q does not exist", indexName)
	}

	delete(schema.Indexes, indexName)
	sm.mu.Unlock()

	return sm.UpdateTable(schema)
}

func (sm *SchemaManager) ListTables() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	tables := make([]string, 0, len(sm.schemas))
	for name := range sm.schemas {
		tables = append(tables, name)
	}

	return tables
}

func (sm *SchemaManager) TableExists(tableName string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	_, exists := sm.schemas[tableName]
	return exists
}

func (sm *SchemaManager) ColumnExists(tableName string, columnName string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	schema, exists := sm.schemas[tableName]
	if !exists {
		return false
	}

	_, exists = schema.Columns[columnName]
	return exists
}

func (sm *SchemaManager) IndexExists(tableName string, indexName string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	schema, exists := sm.schemas[tableName]
	if !exists {
		return false
	}

	_, exists = schema.Indexes[indexName]
	return exists
}

func (sm *SchemaManager) GetPrimaryKeyColumn(tableName string) (string, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	schema, exists := sm.schemas[tableName]
	if !exists {
		return "", fmt.Errorf("table %q not found", tableName)
	}

	return schema.PrimaryKey, nil
}

func (sm *SchemaManager) getSchemaKey(tableName string) []byte {
	return []byte(sm.schemaNamespace + tableName)
}

func (sm *SchemaManager) GetSchemaNamespace() string {
	return sm.schemaNamespace
}

func (sm *SchemaManager) IsSchemaKey(key []byte) bool {
	keyStr := string(key)
	return len(keyStr) > len(sm.schemaNamespace) && keyStr[:len(sm.schemaNamespace)] == sm.schemaNamespace
}
