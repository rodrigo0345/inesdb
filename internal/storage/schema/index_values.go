package schema

import (
	"encoding/json"
	"fmt"
)

func ExtractIndexValues(tableSchema *TableSchema, serializedData []byte) (map[string][]byte, error) {
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

		indexBytes, err := json.Marshal(indexValue)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize index value for %q: %w", indexName, err)
		}

		indexValues[indexName] = indexBytes
	}

	return indexValues, nil
}
