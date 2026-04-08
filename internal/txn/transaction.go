package transaction_manager

import (
	"github.com/rodrigo0345/omag/buffermanager"
)

type TxnState int

const (
	ACTIVE TxnState = iota
	COMMITTED
	ABORTED
)

type Transaction struct {
	txnID          uint64
	state          TxnState
	sharedLocks    [][]byte
	exclusiveLocks [][]byte
	undoLog        *UndoLog
	isolationLevel uint8
}

// NewTransaction creates a new transaction with initialized undo log
func NewTransaction(txnID uint64, isolationLevel uint8) *Transaction {
	return &Transaction{
		txnID:          txnID,
		state:          ACTIVE,
		sharedLocks:    make([][]byte, 0),
		exclusiveLocks: make([][]byte, 0),
		undoLog:        NewUndoLog(txnID),
		isolationLevel: isolationLevel,
	}
}

func (t *Transaction) GetID() uint64 {
	return t.txnID
}

// RecordOperation adds a reversible operation to the undo log
func (t *Transaction) RecordOperation(op Operation) error {
	return t.undoLog.RecordOp(op)
}

// Deprecated: Use RecordOperation instead
// Kept for backward compatibility during migration
func (t *Transaction) AddUndo(op Operation) error {
	return t.undoLog.RecordOp(op)
}

// Rollback reverts all recorded operations in reverse order
func (t *Transaction) Rollback(bufferMgr buffermanager.IBufferPoolManager) error {
	return t.undoLog.Rollback(bufferMgr)
}

// RollbackToPoint reverts operations up to a savepoint
func (t *Transaction) RollbackToPoint(point int, bufferMgr buffermanager.IBufferPoolManager) error {
	return t.undoLog.RollbackToPoint(point, bufferMgr)
}

// SavePoint returns current position in undo log for later rollback
func (t *Transaction) SavePoint() int {
	return t.undoLog.SavePoint()
}

func (t *Transaction) Abort() {
	t.state = ABORTED
}

func (t *Transaction) GetState() TxnState {
	return t.state
}

// GetUndoLog returns the undo log for this transaction
func (t *Transaction) GetUndoLog() *UndoLog {
	return t.undoLog
}

// GetIsolationLevel returns the isolation level for this transaction
func (t *Transaction) GetIsolationLevel() uint8 {
	return t.isolationLevel
}
