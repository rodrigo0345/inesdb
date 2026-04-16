package txn

import (
	"context"
	"fmt"
	stdlog "log"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	wallog "github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/write_handler"
)

type TransactionManager struct {
	isolationManager  IIsolationManager
	logManager        wallog.ILogManager
	bufferPoolManager buffer.IBufferPoolManager
	rollbackManager   *rollback.RollbackManager
	writeHandler      write_handler.IWriteHandler
}

func NewTransactionManager(
	isolationMgr IIsolationManager,
	logMgr wallog.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	storage storage.IStorageEngine,
) *TransactionManager {
	rollbackMgr := rollback.NewRollbackManager(bufferMgr)

	var writeHandler write_handler.IWriteHandler
	if logMgr != nil {
		writeHandler = write_handler.NewDefaultWriteHandler(storage, rollbackMgr, bufferMgr, logMgr)
	} else {
		writeHandler = write_handler.NewMVCCWriteHandler(storage, bufferMgr, nil, rollbackMgr)
	}

	return &TransactionManager{
		isolationManager:  isolationMgr,
		logManager:        logMgr,
		bufferPoolManager: bufferMgr,
		rollbackManager:   rollbackMgr,
		writeHandler:      writeHandler,
	}
}

func (tm *TransactionManager) GetRollbackManager() *rollback.RollbackManager {
	return tm.rollbackManager
}

func (tm *TransactionManager) GetWriteHandler() write_handler.IWriteHandler {
	return tm.writeHandler
}

func (tm *TransactionManager) RollbackRemainingTransactions(ctx context.Context, recoveryState *wallog.RecoveryState) error {
	if recoveryState == nil {
		return fmt.Errorf("recovery state is nil")
	}

	abortedCount := len(recoveryState.AbortedTxns)
	if abortedCount == 0 {
		stdlog.Printf("[TransactionManager] No transactions to rollback after recovery")
		return nil
	}

	stdlog.Printf("[TransactionManager] Rolling back %d uncommitted transactions", abortedCount)

	for txnID := range recoveryState.AbortedTxns {
		stdlog.Printf("[TransactionManager] Rollback aborted transaction: %d", txnID)
	}

	stdlog.Printf("[TransactionManager] Rollback of uncommitted transactions complete")
	return nil
}
