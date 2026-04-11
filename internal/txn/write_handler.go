package txn

import (
	"encoding/json"
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/storage/schema"
)

type WriteOperation struct {
	Key        []byte
	Value      []byte
	PageID     page.ResourcePageID
	Offset     uint16
	IsDelete   bool
	TableName  string
	SchemaInfo *schema.TableSchema
	PrimaryKey []byte
}

type WriteHandler interface {
	HandleWrite(txn *Transaction, writeOp WriteOperation) error
	SetIndexContext(tableSchema *schema.TableSchema, indexMgr *schema.SecondaryIndexManager) error
}

// ExtractIndexValues extracts indexed column values from serialized row data
// Returns a map of indexName -> indexValue ([]byte)
func ExtractIndexValues(tableSchema *schema.TableSchema, serializedData []byte) (map[string][]byte, error) {
	if tableSchema == nil || len(tableSchema.Indexes) == 0 {
		return make(map[string][]byte), nil // No indexes, return empty map
	}

	// Deserialize row data (assuming JSON format)
	var rowData map[string]interface{}
	if err := json.Unmarshal(serializedData, &rowData); err != nil {
		return nil, fmt.Errorf("failed to deserialize row data: %w", err)
	}

	indexValues := make(map[string][]byte)

	// For each index, extract the indexed column values
	for indexName, index := range tableSchema.Indexes {
		var indexValue interface{}

		if len(index.Columns) == 1 {
			// Single-column index
			colName := index.Columns[0]
			val, exists := rowData[colName]
			if !exists {
				// Column not in data, skip
				continue
			}
			indexValue = val
		} else {
			// Multi-column index: combine values
			values := make([]interface{}, 0)
			for _, colName := range index.Columns {
				val, exists := rowData[colName]
				if !exists {
					// Missing column in composite index, skip this index
					continue
				}
				values = append(values, val)
			}
			if len(values) != len(index.Columns) {
				continue // Skip if not all columns present
			}
			indexValue = values
		}

		// Serialize index value to bytes
		indexBytes, err := json.Marshal(indexValue)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize index value for %q: %w", indexName, err)
		}

		indexValues[indexName] = indexBytes
	}

	return indexValues, nil
}
