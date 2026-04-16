package rollback

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	"github.com/rodrigo0345/omag/internal/txn/undo"
)

type RollbackManager struct {
	bufferMgr buffer.IBufferPoolManager
	nextOpID  uint64
	mu        sync.RWMutex
}

func NewRollbackManager(bufferMgr buffer.IBufferPoolManager) *RollbackManager {
	return &RollbackManager{
		bufferMgr: bufferMgr,
		nextOpID:  1,
	}
}

func (rm *RollbackManager) RecordPageWrite(
	txn *txn_unit.Transaction,
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

func (rm *RollbackManager) RollbackTransaction(
	txn *txn_unit.Transaction,
	onBeforeRollback func() error,
	onAfterRollback func() error,
) error {
	if txn == nil {
		return fmt.Errorf("cannot rollback nil transaction")
	}

	if txn.GetState() == txn_unit.COMMITTED {
		return fmt.Errorf("cannot rollback committed transaction %d", txn.GetID())
	}

	if onBeforeRollback != nil {
		if err := onBeforeRollback(); err != nil {
			return fmt.Errorf("pre-rollback callback failed: %w", err)
		}
	}

	if err := txn.ExecuteCleanupCallbacks(); err != nil {
		return fmt.Errorf("cleanup failed for txn %d: %w", txn.GetID(), err)
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

func (rm *RollbackManager) HasOperations(txn *txn_unit.Transaction) bool {
	return txn.GetUndoLog().GetOperationCount() > 0
}

func (rm *RollbackManager) RollbackToSavePoint(txn *txn_unit.Transaction, savePoint int) error {
	if txn == nil {
		return fmt.Errorf("cannot rollback nil transaction")
	}

	if savePoint < 0 {
		return fmt.Errorf("invalid savepoint: %d", savePoint)
	}

	return txn.RollbackToPoint(savePoint, rm.bufferMgr)
}

func (rm *RollbackManager) GetOperationCount(txn *txn_unit.Transaction) int {
	if txn == nil {
		return 0
	}
	return txn.GetUndoLog().GetOperationCount()
}

func (rm *RollbackManager) RegisterIndexCleanup(txn *txn_unit.Transaction, cleanup func() error) {
	if txn != nil && cleanup != nil {
		txn.RegisterCleanupCallback(cleanup)
	}
}
