package transaction_manager

import (
	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/logmanager"
)

type TransactionManager struct {
	isolationManager  IIsolationManager
	logManager        logmanager.ILogManager
	bufferPoolManager buffermanager.IBufferPoolManager
	// indexManager      IIndexManager  - vai conter o primary e secondary index

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
