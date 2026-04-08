package txn

import (
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn/undo"
)

type TxnState int

const (
	ACTIVE TxnState = iota
	COMMITTED
	ABORTED
)

// Isolation levels
const (
	READ_UNCOMMITTED uint8 = iota
	READ_COMMITTED
	REPEATABLE_READ
	SERIALIZABLE
)

type Transaction struct {
	txnID          uint64
	state          TxnState
	sharedLocks    [][]byte
	exclusiveLocks [][]byte
	undoLog        *undo.UndoLog
	isolationLevel uint8
}

// NewTransaction creates a new transaction with initialized undo log
func NewTransaction(txnID uint64, isolationLevel uint8) *Transaction {
	return &Transaction{
		txnID:          txnID,
		state:          ACTIVE,
		sharedLocks:    make([][]byte, 0),
		exclusiveLocks: make([][]byte, 0),
		undoLog:        undo.NewUndoLog(txnID),
		isolationLevel: isolationLevel,
	}
}

func (t *Transaction) GetID() uint64 {
	return t.txnID
}

// RecordOperation adds a reversible operation to the undo log
func (t *Transaction) RecordOperation(op undo.Operation) error {
	return t.undoLog.RecordOp(op)
}

// Deprecated: Use RecordOperation instead
// Kept for backward compatibility during migration
func (t *Transaction) AddUndo(op undo.Operation) error {
	return t.undoLog.RecordOp(op)
}

// Rollback reverts all recorded operations in reverse order
func (t *Transaction) Rollback(bufferMgr buffer.IBufferPoolManager) error {
	return t.undoLog.Rollback(bufferMgr)
}

// RollbackToPoint reverts operations up to a savepoint
func (t *Transaction) RollbackToPoint(point int, bufferMgr buffer.IBufferPoolManager) error {
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
func (t *Transaction) GetUndoLog() *undo.UndoLog {
	return t.undoLog
}

// GetIsolationLevel returns the isolation level for this transaction
func (t *Transaction) GetIsolationLevel() uint8 {
	return t.isolationLevel
}

// Commit marks this transaction as committed
func (t *Transaction) Commit() {
	t.state = COMMITTED
}

// AddSharedLock adds a shared lock on the given key
func (t *Transaction) AddSharedLock(key []byte) {
	t.sharedLocks = append(t.sharedLocks, key)
}

// AddExclusiveLock adds an exclusive lock on the given key
func (t *Transaction) AddExclusiveLock(key []byte) {
	t.exclusiveLocks = append(t.exclusiveLocks, key)
}

// RemoveSharedLock removes a shared lock on the given key
func (t *Transaction) RemoveSharedLock(key []byte) {
	for i, k := range t.sharedLocks {
		if bytesEqual(k, key) {
			t.sharedLocks = append(t.sharedLocks[:i], t.sharedLocks[i+1:]...)
			return
		}
	}
}

// RemoveExclusiveLock removes an exclusive lock on the given key
func (t *Transaction) RemoveExclusiveLock(key []byte) {
	for i, k := range t.exclusiveLocks {
		if bytesEqual(k, key) {
			t.exclusiveLocks = append(t.exclusiveLocks[:i], t.exclusiveLocks[i+1:]...)
			return
		}
	}
}

// bytesEqual compares two byte slices for equality
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
