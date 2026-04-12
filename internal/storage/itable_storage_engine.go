package storage

import "github.com/rodrigo0345/omag/internal/storage/schema"

type ITableStorageEngine interface {
	IStorageEngine

	CreateTable(tableSchema *schema.TableSchema) error
	DropTable(tableName string) error
	GetTableSchema(tableName string) (*schema.TableSchema, error)

	TablePut(tableName string, key []byte, value []byte) error
	TableGet(tableName string, key []byte) ([]byte, error)
	TableDelete(tableName string, key []byte) error

	CreateIndex(tableName string, indexName string, indexType schema.IndexType, columns []string) error
	DropIndex(tableName string, indexName string) error

	IndexGet(tableName string, indexName string, key []byte) ([][]byte, error)
	IndexScan(tableName string, indexName string, startKey []byte, endKey []byte) ([][]byte, error)

	UpdateSecondaryIndexes(tableName string, operation string, oldRow map[string][]byte, newRow map[string][]byte) error
}

type TableStorageOptions struct {
	AutoIndexUpdate bool

	MaxIndexesPerTable int
}
