package txn_unit

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/undo"
)

type TxnState int

const (
	ACTIVE TxnState = iota
	COMMITTED
	ABORTED
)

const (
	READ_UNCOMMITTED uint8 = iota
	READ_COMMITTED
	REPEATABLE_READ
	SERIALIZABLE
)

type Transaction struct {
	txnID            uint64
	state            TxnState
	sharedLocks      [][]byte
	exclusiveLocks   [][]byte
	undoLog          *undo.UndoLog
	isolationLevel   uint8
	tableName        string
	tableSchema      *schema.TableSchema
	cleanupCallbacks []func() error
	operations       []log.RecoveryOperation // Operations for crash recovery
	txn_write_prefix string // used to manage isolation level copies
}

func NewTransaction(txnID uint64, isolationLevel uint8) *Transaction {
	return &Transaction{
		txnID:            txnID,
		state:            ACTIVE,
		sharedLocks:      make([][]byte, 0),
		exclusiveLocks:   make([][]byte, 0),
		undoLog:          undo.NewUndoLog(txnID),
		isolationLevel:   isolationLevel,
		tableName:        "",
		tableSchema:      nil,
		cleanupCallbacks: make([]func() error, 0),
		operations:       make([]log.RecoveryOperation, 0),
		txn_write_prefix: fmt.Sprintf("#txn_%d_%d", txnID, isolationLevel),
	}
}

func (t *Transaction) SetTableContext(tableName string, tableSchema *schema.TableSchema) {
	t.tableName = tableName
	t.tableSchema = tableSchema
}

func (t *Transaction) GetTableContext() (string, *schema.TableSchema) {
	return t.tableName, t.tableSchema
}

func (t *Transaction) GetID() uint64 {
	return t.txnID
}

func (t *Transaction) RecordOperation(op undo.Operation) error {
	return t.undoLog.RecordOp(op)
}

func (t *Transaction) AddUndo(op undo.Operation) error {
	return t.undoLog.RecordOp(op)
}

func (t *Transaction) Rollback(bufferMgr buffer.IBufferPoolManager) error {
	return t.undoLog.Rollback(bufferMgr)
}

func (t *Transaction) RollbackToPoint(point int, bufferMgr buffer.IBufferPoolManager) error {
	return t.undoLog.RollbackToPoint(point, bufferMgr)
}

func (t *Transaction) SavePoint() int {
	return t.undoLog.SavePoint()
}

func (t *Transaction) Abort() {
	t.state = ABORTED
}

func (t *Transaction) GetState() TxnState {
	return t.state
}

func (t *Transaction) GetUndoLog() *undo.UndoLog {
	return t.undoLog
}

func (t *Transaction) GetIsolationLevel() uint8 {
	return t.isolationLevel
}

func (t *Transaction) GetSharedLocks() [][]byte {
	return t.sharedLocks
}

func (t *Transaction) GetExclusiveLocks() [][]byte {
	return t.exclusiveLocks
}

func (t *Transaction) GetWritePrefix() string {
	return t.txn_write_prefix
}

func (t *Transaction) Commit() {
	t.state = COMMITTED
}

func (t *Transaction) AddSharedLock(key []byte) {
	t.sharedLocks = append(t.sharedLocks, key)
}

func (t *Transaction) AddExclusiveLock(key []byte) {
	t.exclusiveLocks = append(t.exclusiveLocks, key)
}

func (t *Transaction) RemoveSharedLock(key []byte) {
	for i, k := range t.sharedLocks {
		if bytesEqual(k, key) {
			t.sharedLocks = append(t.sharedLocks[:i], t.sharedLocks[i+1:]...)
			return
		}
	}
}

func (t *Transaction) RemoveExclusiveLock(key []byte) {
	for i, k := range t.exclusiveLocks {
		if bytesEqual(k, key) {
			t.exclusiveLocks = append(t.exclusiveLocks[:i], t.exclusiveLocks[i+1:]...)
			return
		}
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (t *Transaction) RegisterCleanupCallback(cleanup func() error) {
	t.cleanupCallbacks = append(t.cleanupCallbacks, cleanup)
}

func (t *Transaction) ExecuteCleanupCallbacks() error {
	for _, cleanup := range t.cleanupCallbacks {
		if cleanup != nil {
			if err := cleanup(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Transaction) RecordRecoveryOperation(opType log.RecordType, key []byte, value []byte) {
	op := log.RecoveryOperation{
		TxnID: t.txnID,
		Type:  opType,
		Key:   key,
		Value: value,
	}
	t.operations = append(t.operations, op)
}

func (t *Transaction) GetRecoveryOperations() []log.RecoveryOperation {
	return t.operations
}
