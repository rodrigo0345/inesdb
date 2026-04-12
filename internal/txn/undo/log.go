package undo

import (
	"fmt"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
)

type UndoLog struct {
	txnID      uint64
	operations []Operation
	mu         sync.RWMutex
}

func NewUndoLog(txnID uint64) *UndoLog {
	return &UndoLog{
		txnID:      txnID,
		operations: make([]Operation, 0, 16),
	}
}

func (ul *UndoLog) RecordOp(op Operation) error {
	if op == nil {
		return fmt.Errorf("cannot record nil operation for transaction %d", ul.txnID)
	}

	ul.mu.Lock()
	defer ul.mu.Unlock()

	ul.operations = append(ul.operations, op)
	return nil
}

func (ul *UndoLog) Rollback(bufferMgr buffer.IBufferPoolManager) error {
	ul.mu.Lock()
	defer ul.mu.Unlock()

	for i := len(ul.operations) - 1; i >= 0; i-- {
		op := ul.operations[i]
		if err := op.Undo(bufferMgr); err != nil {
			return fmt.Errorf("txn %d rollback failed at op %d (op_id=%d): %w",
				ul.txnID, i, op.GetID(), err)
		}
	}

	ul.operations = ul.operations[:0]
	return nil
}

func (ul *UndoLog) RollbackToPoint(point int, bufferMgr buffer.IBufferPoolManager) error {
	ul.mu.Lock()
	defer ul.mu.Unlock()

	if point < 0 || point > len(ul.operations) {
		return fmt.Errorf("invalid rollback point %d for txn %d (total ops=%d)",
			point, ul.txnID, len(ul.operations))
	}

	for i := len(ul.operations) - 1; i >= point; i-- {
		op := ul.operations[i]
		if err := op.Undo(bufferMgr); err != nil {
			return fmt.Errorf("txn %d rollback to point %d failed at index %d (op_id=%d): %w",
				ul.txnID, point, i, op.GetID(), err)
		}
	}

	ul.operations = ul.operations[:point]
	return nil
}

func (ul *UndoLog) SavePoint() int {
	ul.mu.RLock()
	defer ul.mu.RUnlock()
	return len(ul.operations)
}

func (ul *UndoLog) GetOperationCount() int {
	ul.mu.RLock()
	defer ul.mu.RUnlock()
	return len(ul.operations)
}

func (ul *UndoLog) GetTxnID() uint64 {
	return ul.txnID
}

func (ul *UndoLog) Clear() {
	ul.mu.Lock()
	defer ul.mu.Unlock()
	ul.operations = ul.operations[:0]
}

func (ul *UndoLog) IsEmpty() bool {
	ul.mu.RLock()
	defer ul.mu.RUnlock()
	return len(ul.operations) == 0
}

func (ul *UndoLog) GetOperations() []Operation {
	ul.mu.RLock()
	defer ul.mu.RUnlock()

	opsCopy := make([]Operation, len(ul.operations))
	copy(opsCopy, ul.operations)
	return opsCopy
}
