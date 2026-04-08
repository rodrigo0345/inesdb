package transaction_manager

import (
	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/logmanager"
)

type TransactionManager struct {
	isolationManager  IIsolationManager
	logManager        logmanager.ILogManager
	bufferPoolManager buffermanager.IBufferPoolManager
	rollbackManager   *RollbackManager // Coordinates undo/rollback
	writeHandler      WriteHandler     // Coordinates writes/WAL/undo
	// indexManager      IIndexManager       - vai conter o primary e secondary index
}

// NewTransactionManager creates a new transaction manager with all components
func NewTransactionManager(
	isolationMgr IIsolationManager,
	logMgr logmanager.ILogManager,
	bufferMgr buffermanager.IBufferPoolManager,
	storage StorageEngine,
) *TransactionManager {
	rollbackMgr := NewRollbackManager(bufferMgr)

	// Choose write handler based on configuration
	var writeHandler WriteHandler
	if logMgr != nil {
		// WAL-based system (2PL, OCC)
		writeHandler = NewDefaultWriteHandler(storage, rollbackMgr, bufferMgr, logMgr)
	} else {
		// MVCC or other snapshot-based system
		writeHandler = NewMVCCWriteHandler(storage, bufferMgr, nil)
	}

	return &TransactionManager{
		isolationManager:  isolationMgr,
		logManager:        logMgr,
		bufferPoolManager: bufferMgr,
		rollbackManager:   rollbackMgr,
		writeHandler:      writeHandler,
	}
}

func (tm *TransactionManager) ForceWALPushOnCommit() bool {
	switch tm.isolationManager.(type) {
	case *TwoPhaseLockingManager:
		return true
	case *OptimisticConcurrencyControlManager:
		return true
	case *MVCCManager:
		return false
	default:
		panic("unknown isolation manager type")
	}
}

// GetRollbackManager returns the rollback manager for use by isolation strategies
func (tm *TransactionManager) GetRollbackManager() *RollbackManager {
	return tm.rollbackManager
}

// GetWriteHandler returns the write handler for coordinated write operations
func (tm *TransactionManager) GetWriteHandler() WriteHandler {
	return tm.writeHandler
}
