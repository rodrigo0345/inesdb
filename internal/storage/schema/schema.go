package schema

import (
	"fmt"
)

type DataType string

const (
	DataTypeInt64    DataType = "int64"
	DataTypeString   DataType = "string"
	DataTypeBytes    DataType = "bytes"
	DataTypeFloat64  DataType = "float64"
	DataTypeBool     DataType = "bool"
	DataTypeNullable DataType = "nullable"
)

type Column struct {
	Name     string
	DataType DataType
	Nullable bool
}

type IndexType string

const (
	IndexTypePrimary   IndexType = "primary"
	IndexTypeSecondary IndexType = "secondary"
	IndexTypeUnique    IndexType = "unique"
)

type Index struct {
	Name     string
	Type     IndexType
	Columns  []string
	IsUnique bool
	IsSparse bool
}

type TableSchema struct {
	Name       string
	Columns    map[string]*Column
	ColumnList []string
	PrimaryKey string
	Indexes    map[string]*Index
	CreatedAt  int64
	ModifiedAt int64
}

func NewTableSchema(name string, primaryKey string) *TableSchema {
	return &TableSchema{
		Name:       name,
		Columns:    make(map[string]*Column),
		ColumnList: make([]string, 0),
		PrimaryKey: primaryKey,
		Indexes:    make(map[string]*Index),
		CreatedAt:  0,
		ModifiedAt: 0,
	}
}

func (ts *TableSchema) AddColumn(name string, dataType DataType, nullable bool) error {
	if _, exists := ts.Columns[name]; exists {
		return fmt.Errorf("column %q already exists", name)
	}

	col := &Column{
		Name:     name,
		DataType: dataType,
		Nullable: nullable,
	}

	ts.Columns[name] = col
	ts.ColumnList = append(ts.ColumnList, name)
	return nil
}

func (ts *TableSchema) AddIndex(name string, indexType IndexType, columns []string, isUnique bool) error {
	if _, exists := ts.Indexes[name]; exists {
		return fmt.Errorf("index %q already exists", name)
	}

	for _, colName := range columns {
		if _, exists := ts.Columns[colName]; !exists {
			return fmt.Errorf("column %q does not exist", colName)
		}
	}

	if indexType == IndexTypePrimary {
		if len(columns) != 1 || columns[0] != ts.PrimaryKey {
			return fmt.Errorf("primary key index must contain exactly the primary key column %q", ts.PrimaryKey)
		}
		if isUnique {
			return fmt.Errorf("primary key is implicitly unique")
		}
	}

	idx := &Index{
		Name:     name,
		Type:     indexType,
		Columns:  columns,
		IsUnique: isUnique || indexType == IndexTypePrimary,
		IsSparse: false,
	}

	ts.Indexes[name] = idx
	return nil
}

func (ts *TableSchema) GetColumn(name string) (*Column, error) {
	if col, exists := ts.Columns[name]; exists {
		return col, nil
	}
	return nil, fmt.Errorf("column %q not found", name)
}

func (ts *TableSchema) GetIndex(name string) (*Index, error) {
	if idx, exists := ts.Indexes[name]; exists {
		return idx, nil
	}
	return nil, fmt.Errorf("index %q not found", name)
}

func (ts *TableSchema) GetIndexesForColumn(columnName string) []*Index {
	var indexes []*Index
	for _, idx := range ts.Indexes {
		for _, col := range idx.Columns {
			if col == columnName {
				indexes = append(indexes, idx)
				break
			}
		}
	}
	return indexes
}

func (ts *TableSchema) HasPrimaryKey() bool {
	pkIndexName := ts.PrimaryKey + "_pk"
	_, exists := ts.Indexes[pkIndexName]
	return exists
}

func (ts *TableSchema) Validate() error {
	if ts.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if ts.PrimaryKey == "" {
		return fmt.Errorf("primary key not set")
	}

	if _, exists := ts.Columns[ts.PrimaryKey]; !exists {
		return fmt.Errorf("primary key column %q does not exist", ts.PrimaryKey)
	}

	if len(ts.Columns) == 0 {
		return fmt.Errorf("table must have at least one column")
	}

	return nil
}

func (ts *TableSchema) ColumnDataType(columnName string) (DataType, error) {
	col, err := ts.GetColumn(columnName)
	if err != nil {
		return "", err
	}
	return col.DataType, nil
}
