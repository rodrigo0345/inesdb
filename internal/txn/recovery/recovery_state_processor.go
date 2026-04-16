package recovery

import (
	"fmt"
	stdlog "log"

	wallog "github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/undo"
)

type RecoveryStateProcessor struct {
	rollbackMgr *rollback.RollbackManager
}

func NewRecoveryStateProcessor(rollbackMgr *rollback.RollbackManager) *RecoveryStateProcessor {
	return &RecoveryStateProcessor{
		rollbackMgr: rollbackMgr,
	}
}

func (rsp *RecoveryStateProcessor) ProcessRecoveryState(state *wallog.RecoveryState) error {
	if state == nil {
		return fmt.Errorf("recovery state is nil")
	}

	stdlog.Printf("[RecoveryProcessor] Processing recovery state: %d committed, %d aborted txns",
		len(state.CommittedTxns), len(state.AbortedTxns))

	if err := rsp.cleanupAbortedTransactions(state); err != nil {
		return fmt.Errorf("failed to clean up aborted transactions: %w", err)
	}

	stdlog.Printf("[RecoveryProcessor] Recovery state processing complete")
	return nil
}

func (rsp *RecoveryStateProcessor) cleanupAbortedTransactions(state *wallog.RecoveryState) error {
	if len(state.AbortedTxns) == 0 {
		return nil
	}

	stdlog.Printf("[RecoveryProcessor] Cleaning up %d aborted transactions", len(state.AbortedTxns))

	for txnID := range state.AbortedTxns {
		stdlog.Printf("[RecoveryProcessor] Aborted transaction: %d", txnID)
	}

	return nil
}

func (rsp *RecoveryStateProcessor) GetRecoveredTransactionIDs(state *wallog.RecoveryState) (committed []uint64, aborted []uint64) {
	if state == nil {
		return
	}

	for txnID := range state.CommittedTxns {
		committed = append(committed, txnID)
	}

	for txnID := range state.AbortedTxns {
		aborted = append(aborted, txnID)
	}

	return
}

func (rsp *RecoveryStateProcessor) GetRecoveredUndoLog(state *wallog.RecoveryState) []*undo.Operation {
	if state == nil {
		return nil
	}


	return nil
}

func (rsp *RecoveryStateProcessor) ValidateRecoveryConsistency(state *wallog.RecoveryState) error {
	if state == nil {
		return fmt.Errorf("recovery state is nil")
	}

	if len(state.CommittedTxns) > 0 && len(state.AbortedTxns) > 0 {
		for txnID := range state.CommittedTxns {
			if _, exists := state.AbortedTxns[txnID]; exists {
				return fmt.Errorf("transaction %d appears in both committed and aborted sets", txnID)
			}
		}
	}

	stdlog.Printf("[RecoveryProcessor] Consistency validation passed")
	return nil
}
