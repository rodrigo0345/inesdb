package schema

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage"
)

type TableManager struct {
	schemas map[string]*TableSchema
	mu      sync.RWMutex
}

var _ ITableManager = (*TableManager)(nil)

func NewTableManager() *TableManager {
	return &TableManager{
		schemas: make(map[string]*TableSchema),
	}
}

func (tm *TableManager) CreateTable(schema ITableSchema, errorOnExists bool) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	name := schema.GetName()
	if _, exists := tm.schemas[name]; exists {
		if errorOnExists {
			return fmt.Errorf("table %s already exists", name)
		}
		return nil
	}

	tableSchema, ok := schema.(*TableSchema)
	if !ok {
		tableSchema = NewTableSchema(
			schema.GetName(),
			schema.GetColumns(),
		)
		for _, idx := range schema.GetAllIndexes() {
			tableSchema.AddIndex(idx.Name, idx.Columns, idx.Engine)
		}
	}

	tm.schemas[name] = tableSchema
	return nil
}

func (tm *TableManager) DropTable(tableName string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.schemas[tableName]; !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	delete(tm.schemas, tableName)
	return nil
}

func (tm *TableManager) CreateIndex(tableName string, index IndexDefinition, indexBackend storage.IStorageEngine) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if len(index.Columns) == 0 {
		return fmt.Errorf("index columns cannot be empty")
	}

	schema, exists := tm.schemas[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	if schema.GetIndex(index.Name) != nil {
		return fmt.Errorf("index %s already exists on table %s", index.Name, tableName)
	}

	schema.AddIndex(index.Name, index.Columns, indexBackend)

	return nil
}

func (tm *TableManager) GetTableSchema(tableName string) (ITableSchema, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	schema, exists := tm.schemas[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	return schema, nil
}

func (tm *TableManager) GetAllTables() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tables := make([]string, 0, len(tm.schemas))
	for name := range tm.schemas {
		tables = append(tables, name)
	}
	return tables
}

func (tm *TableManager) GetPrimaryStorage(tableName string) (storage.IStorageEngine, error) {
	schema, err := tm.GetTableSchema(tableName)
	if err != nil {
		return nil, err
	}

	idx := schema.GetIndex("PRIMARY")
	if idx == nil || idx.Engine == nil {
		return nil, fmt.Errorf("primary storage engine not found for table %s", tableName)
	}

	return idx.Engine, nil
}

func (tm *TableManager) Write(op WriteOperation) error {
	tableName := op.TableName

	tm.mu.RLock()
	tableSchema, exists := tm.schemas[tableName]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// 1. Extract values for all defined indexes from the row payload
	indexKeys, err := tableSchema.ExtractIndexValues(op.Value)
	if err != nil {
		return fmt.Errorf("failed to extract index values for table %s: %w", tableName, err)
	}

	// 2. Iterate through all indexes defined in the schema
	for indexName, extractedKey := range indexKeys {
		indexData := tableSchema.GetIndex(indexName)
		if indexData == nil || indexData.Engine == nil {
			return fmt.Errorf("no storage backend found for index %s", indexName)
		}

		if indexName == "PRIMARY" {
			// PRIMARY Index: [Physical Key] -> [Full Row Payload]
			if err := indexData.Engine.Put(op.Key, op.Value); err != nil {
				return fmt.Errorf("primary write failed: %w", err)
			}
		} else {
			// Secondary Index: [Extracted Columns] -> [Physical Key]
			// We store op.Key (the primary pointer) as the value in secondary storages
			if err := indexData.Engine.Put(extractedKey, op.Key); err != nil {
				return fmt.Errorf("secondary index %s write failed: %w", indexName, err)
			}
		}
	}

	return nil
}

// Delete removes the record from the PRIMARY index and purges stale entries from secondary indexes.
// It requires the BeforeImage to know which secondary index keys to remove.
func (tm *TableManager) Delete(op DeleteOperation) error {
	tableName := op.TableName

	tm.mu.RLock()
	tableSchema, exists := tm.schemas[tableName]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// 1. Purge Secondary Indexes
	// Since secondary indexes are keyed by column values, we MUST have the previous data
	// (BeforeImage) to find and delete the correct index entries.
	if op.BeforeImage != nil {
		indexKeys, err := tableSchema.ExtractIndexValues(op.BeforeImage)
		if err != nil {
			return fmt.Errorf("failed to extract index values for deletion: %w", err)
		}

		for indexName, extractedKey := range indexKeys {
			if indexName == "PRIMARY" {
				continue
			}

			indexData := tableSchema.GetIndex(indexName)
			if indexData != nil && indexData.Engine != nil {
				if err := indexData.Engine.Delete(extractedKey); err != nil {
					return fmt.Errorf("secondary index %s delete failed: %w", indexName, err)
				}
			}
		}
	}

	// 2. Remove from Primary Storage
	primaryStorage, err := tm.GetPrimaryStorage(tableName)
	if err != nil {
		return err
	}

	if err := primaryStorage.Delete(op.Key); err != nil {
		return fmt.Errorf("primary delete failed: %w", err)
	}

	return nil
}

func (tm *TableManager) Scan(tableName string, indexName string, opts storage.ScanOptions) (storage.ICursor, error) {
	tm.mu.RLock()
	tableSchema, exists := tm.schemas[tableName]
	tm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	targetIndex := tableSchema.GetIndex(indexName)
	if targetIndex == nil || targetIndex.Engine == nil {
		return nil, fmt.Errorf("index %s not found on table %s", indexName, tableName)
	}

	// Do not pass Limit/Offset to the raw engine; decorators must handle them after ghost-resolution and filtering.
	engineOpts := storage.ScanOptions{
		LowerBound: opts.LowerBound,
		UpperBound: opts.UpperBound,
		Inclusive:  opts.Inclusive,
		Reverse:    opts.Reverse,
	}

	rawCursor, err := targetIndex.Engine.Scan(engineOpts)
	if err != nil {
		return nil, err
	}

	var cursor storage.ICursor = rawCursor

	// Resolve Secondary Index two-hop reads
	if indexName != "PRIMARY" {
		primaryStorage, err := tm.GetPrimaryStorage(tableName)
		if err != nil {
			rawCursor.Close()
			return nil, err
		}
		cursor = &TwoHopCursor{
			secondaryCursor: rawCursor,
			primaryEngine:   primaryStorage,
		}
	}

	// Predicate Filter
	if opts.ComplexFilter != nil {
		cursor = NewFilterCursor(cursor, opts.ComplexFilter)
	}

	// Offset (Must occur after filtering so only valid rows are skipped)
	if opts.Offset > 0 {
		cursor = NewOffsetCursor(cursor, opts.Offset)
	}

	// Limit (Must occur after offset so we count exactly what is yielded)
	if opts.Limit > 0 {
		cursor = NewLimitCursor(cursor, opts.Limit)
	}

	// Projection / Key-Only (Executed last to preserve payload data for the ComplexFilter)
	if opts.KeyOnly {
		cursor = NewProjectCursor(cursor, func(k, v []byte) ([]byte, []byte) {
			return k, nil // Strip payload
		})
	} else if len(opts.Projection) > 0 {
		cursor = NewProjectCursor(cursor, func(k, v []byte) ([]byte, []byte) {
			projectedValue, err := extractProjectedColumns(tableSchema.Columns, v, opts.Projection)
			if err != nil {
				return k, nil // Or handle projection error
			}
			return k, projectedValue
		})
	}

	return cursor, nil
}

// FullTableScan bypasses secondary index checks and forces a primary index sweep.
func (tm *TableManager) FullTableScan(tableName string, opts storage.ScanOptions) (storage.ICursor, error) {
	return tm.Scan(tableName, "PRIMARY", opts)
}

func extractProjectedColumns(schemaCols []Column, value []byte, projection []string) ([]byte, error) {
	if len(value) == 0 {
		return nil, nil
	}

	projectMap := make(map[string]bool, len(projection))
	for _, p := range projection {
		projectMap[p] = true
	}

	offset := 0
	payloadLen := len(value)
	var projectedBuf []byte

	for i := range schemaCols {
		col := &schemaCols[i]
		start := offset

		switch col.Type {
		case TypeInt32:
			offset += 4
		case TypeInt64, TypeFloat64:
			offset += 8
		case TypeBool, TypeMetadata:
			offset += 1
		case TypeString:
			if offset+4 > payloadLen {
				return nil, fmt.Errorf("unexpected EOF reading string header")
			}
			strLen := int(binary.BigEndian.Uint32(value[offset : offset+4]))
			offset += 4 + strLen
		}

		if offset > payloadLen {
			return nil, fmt.Errorf("unexpected EOF reading payload")
		}

		if projectMap[col.Name] {
			projectedBuf = append(projectedBuf, value[start:offset]...)
		}
	}

	return projectedBuf, nil
}

func (tm *TableManager) DecodeRow(tableName string, columnName string, row []byte) Value {
      tm.mu.RLock()
      tableSchema, exists := tm.schemas[tableName]
      tm.mu.RUnlock()

      if !exists {
            return Value{err: fmt.Errorf("table %s does not exist", tableName)}
      }

      return tableSchema.DecodeRow(columnName, row)
}
