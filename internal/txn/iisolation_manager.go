package txn

import "github.com/rodrigo0345/omag/internal/storage/schema"

type IIsolationManager interface {
	BeginTransaction(isolationLevel uint8, tableName string, tableSchema *schema.TableSchema) int64
	Read(txnID int64, Key []byte) ([]byte, error)
	Write(txnID int64, Key []byte, Value []byte) error
	Commit(txnID int64) error
	Abort(txnID int64) error
	Close() error
}
