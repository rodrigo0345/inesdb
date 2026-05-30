package engine

import (
	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/schema"
)

type TransactionID = uint64

// the implementation is in internal/isolation, but it was defined here to avoid circular dependencies
type IIsolationManager interface {
	// Transaction Lifecycle
	BeginTransaction(isolationLevel uint8) int64
	Commit(txnID TransactionID) error
	Abort(txnID TransactionID) error

	// Data Operations (DML) - target is used instead of tableName
	// because I assume the user already contacted TableManager to get the storage engine for the table, given a certain query
	Read(txnID TransactionID, tableName, indexName string, key []byte) ([]byte, error)
	Write(txnID TransactionID, tableName, indexName string, key []byte, value []byte) error
	Delete(txnID TransactionID, tableName, indexName string, key []byte) error
	Scan(txnID TransactionID, target storage.IStorageEngine, opts storage.ScanOptions) (storage.ICursor, error)

	// Schema Operations (DDL)
	CreateTable(txnID TransactionID, schema schema.ITableSchema) error
	UpdateTable(txnID TransactionID, schema schema.ITableSchema) error
	DropTable(txnID TransactionID, tableName string) error
	CreateIndex(txnID TransactionID, tableName string, index schema.IndexDefinition) error
	DropIndex(txnID TransactionID, tableName string, indexName string) error

	Close() error
}
