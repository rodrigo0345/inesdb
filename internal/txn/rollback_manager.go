package txn

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/txn/undo"
)

// RollbackManager provides utilities for safe transaction rollback
// Coordinates between isolation strategies and undo operations
type RollbackManager struct {
	bufferMgr buffer.IBufferPoolManager
	nextOpID  uint64 // Atomic counter for operation IDs
	mu        sync.RWMutex
}

// NewRollbackManager creates a manager for handling transaction rollbacks
func NewRollbackManager(bufferMgr buffer.IBufferPoolManager) *RollbackManager {
	return &RollbackManager{
		bufferMgr: bufferMgr,
		nextOpID:  1,
	}
}

// RecordPageWrite creates and records a page write operation
// Returns the operation ID for reference
func (rm *RollbackManager) RecordPageWrite(
	txn *Transaction,
	pageID page.ResourcePageID,
	offset uint16,
	beforeData []byte,
) (uint64, error) {
	opID := atomic.AddUint64(&rm.nextOpID, 1)

	op := undo.NewPageWriteOp(opID, pageID, offset, beforeData)

	if err := txn.RecordOperation(op); err != nil {
		return 0, fmt.Errorf("failed to record page write operation: %w", err)
	}

	return opID, nil
}

// RollbackTransaction performs complete rollback of a transaction
// Calls optional callbacks before/after rollback for cleanup
func (rm *RollbackManager) RollbackTransaction(
	txn *Transaction,
	onBeforeRollback func() error,
	onAfterRollback func() error,
) error {
	if txn == nil {
		return fmt.Errorf("cannot rollback nil transaction")
	}

	if txn.GetState() == COMMITTED {
		return fmt.Errorf("cannot rollback committed transaction %d", txn.GetID())
	}

	if onBeforeRollback != nil {
		if err := onBeforeRollback(); err != nil {
			return fmt.Errorf("pre-rollback callback failed: %w", err)
		}
	}

	if err := txn.Rollback(rm.bufferMgr); err != nil {
		return fmt.Errorf("rollback of txn %d failed: %w", txn.GetID(), err)
	}

	if onAfterRollback != nil {
		if err := onAfterRollback(); err != nil {
			return fmt.Errorf("post-rollback callback failed: %w", err)
		}
	}

	return nil
}

// HasOperations returns true if the transaction has recorded any operations
func (rm *RollbackManager) HasOperations(txn *Transaction) bool {
	return txn.GetUndoLog().GetOperationCount() > 0
}

// RollbackToSavePoint performs partial rollback to a savepoint
func (rm *RollbackManager) RollbackToSavePoint(txn *Transaction, savePoint int) error {
	if txn == nil {
		return fmt.Errorf("cannot rollback nil transaction")
	}

	if savePoint < 0 {
		return fmt.Errorf("invalid savepoint: %d", savePoint)
	}

	return txn.RollbackToPoint(savePoint, rm.bufferMgr)
}

// GetOperationCount returns the number of operations recorded for a transaction
func (rm *RollbackManager) GetOperationCount(txn *Transaction) int {
	if txn == nil {
		return 0
	}
	return txn.GetUndoLog().GetOperationCount()
}
