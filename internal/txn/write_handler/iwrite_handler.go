package write_handler

import (
	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
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

type IWriteHandler interface {
	HandleWrite(txn *txn_unit.Transaction, writeOp WriteOperation) error
	SetIndexContext(tableSchema *schema.TableSchema, indexMgr *schema.SecondaryIndexManager) error
}
