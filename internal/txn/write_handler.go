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

func ExtractIndexValues(tableSchema *schema.TableSchema, serializedData []byte) (map[string][]byte, error) {
	if tableSchema == nil || len(tableSchema.Indexes) == 0 {
		return make(map[string][]byte), nil
	}

	var rowData map[string]interface{}
	if err := json.Unmarshal(serializedData, &rowData); err != nil {
		return nil, fmt.Errorf("failed to deserialize row data: %w", err)
	}

	indexValues := make(map[string][]byte)

	for indexName, index := range tableSchema.Indexes {
		var indexValue interface{}

		if len(index.Columns) == 1 {
			colName := index.Columns[0]
			val, exists := rowData[colName]
			if !exists {
				continue
			}
			indexValue = val
		} else {
			values := make([]interface{}, 0)
			for _, colName := range index.Columns {
				val, exists := rowData[colName]
				if !exists {
					continue
				}
				values = append(values, val)
			}
			if len(values) != len(index.Columns) {
				continue
			}
			indexValue = values
		}

		indexBytes, err := json.Marshal(indexValue)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize index value for %q: %w", indexName, err)
		}

		indexValues[indexName] = indexBytes
	}

	return indexValues, nil
}
