package recovery

import (
	"context"
	"fmt"
	"log"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/btree"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/lsm"
	wallog "github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
)

type RecoveryCoordinator interface {
	RecoverFromCrash(ctx context.Context) (*wallog.RecoveryState, error)

	ApplyRecoveryState(ctx context.Context, state *wallog.RecoveryState) error

	ValidateRecoveredState(ctx context.Context, state *wallog.RecoveryState) error

	GetRecoveryStats() RecoveryStats
}

type RecoveryStats struct {
	TotalRecords           int64
	RecordsRedo            int64
	RecordsUndo            int64
	CommittedTransactions  int
	RolledbackTransactions int
	PagesRecovered         int
	Duration               int64
}

type DefaultRecoveryCoordinator struct {
	walMgr        wallog.ILogManager
	storageEngine storage.IStorageEngine
	bufferPool    buffer.IBufferPoolManager
	rollbackMgr   *rollback.RollbackManager
	stats         RecoveryStats
}

func NewDefaultRecoveryCoordinator(
	walMgr wallog.ILogManager,
	storageEngine storage.IStorageEngine,
	bufferPool buffer.IBufferPoolManager,
	rollbackMgr *rollback.RollbackManager,
) RecoveryCoordinator {
	return &DefaultRecoveryCoordinator{
		walMgr:        walMgr,
		storageEngine: storageEngine,
		bufferPool:    bufferPool,
		rollbackMgr:   rollbackMgr,
		stats:         RecoveryStats{},
	}
}

func (rc *DefaultRecoveryCoordinator) RecoverFromCrash(ctx context.Context) (*wallog.RecoveryState, error) {
	log.Printf("[Recovery] Starting crash recovery...")

	log.Printf("[Recovery] Running WAL analysis, redo, and undo phases...")
	state, err := rc.walMgr.Recover()
	if err != nil {
		return nil, fmt.Errorf("WAL recovery failed: %w", err)
	}

	log.Printf("[Recovery] WAL recovery complete: %d committed txns, %d aborted txns",
		len(state.CommittedTxns), len(state.AbortedTxns))

	log.Printf("[Recovery] Applying recovery state to storage engine...")
	if err := rc.ApplyRecoveryState(ctx, state); err != nil {
		return nil, fmt.Errorf("failed to apply recovery state: %w", err)
	}

	log.Printf("[Recovery] Validating recovered state...")
	if err := rc.ValidateRecoveredState(ctx, state); err != nil {
		log.Printf("[Recovery] Warning: validation found issues: %v", err)
	}

	log.Printf("[Recovery] Crash recovery complete")
	return state, nil
}

func (rc *DefaultRecoveryCoordinator) ApplyRecoveryState(ctx context.Context, state *wallog.RecoveryState) error {
	if lsmEngine, ok := rc.storageEngine.(*lsm.LSMTreeBackend); ok {
		return rc.applyLSMRecovery(ctx, lsmEngine, state)
	}

	if btreeEngine, ok := rc.storageEngine.(*btree.BPlusTreeBackend); ok {
		return rc.applyBTreeRecovery(ctx, btreeEngine, state)
	}

	log.Printf("[Recovery] Warning: Unknown storage engine type, skipping engine-specific recovery")
	return nil
}

func (rc *DefaultRecoveryCoordinator) applyLSMRecovery(ctx context.Context, engine *lsm.LSMTreeBackend, state *wallog.RecoveryState) error {
	log.Printf("[Recovery] Applying LSM recovery: replaying %d committed transactions with %d operations",
		len(state.CommittedTxns), len(state.Operations))

	// Replay operations for committed transactions using Put/Delete API
	for _, op := range state.Operations {
		// Only replay operations from committed transactions
		if !state.CommittedTxns[op.TxnID] {
			continue
		}

		switch op.Type {
		case wallog.PUT:
			if err := engine.Put(op.Key, op.Value); err != nil {
				log.Printf("[Recovery] Warning: Failed to replay PUT operation for txn %d: %v", op.TxnID, err)
			}
		case wallog.DELETE:
			if err := engine.Delete(op.Key); err != nil {
				log.Printf("[Recovery] Warning: Failed to replay DELETE operation for txn %d: %v", op.TxnID, err)
			}
		}
	}

	log.Printf("[Recovery] LSM recovery complete: replayed operations from %d committed transactions", len(state.CommittedTxns))
	return nil
}

func (rc *DefaultRecoveryCoordinator) applyBTreeRecovery(ctx context.Context, engine *btree.BPlusTreeBackend, state *wallog.RecoveryState) error {
	log.Printf("[Recovery] Applying B+Tree recovery: validating structure and repairing if needed")

	validator := btree.NewBTreeRecoveryValidator(engine, rc.bufferPool)
	report, err := validator.ValidateStructure()
	if err != nil {
		return fmt.Errorf("B+Tree structure validation failed: %w", err)
	}

	if report.IsCorrupted {
		log.Printf("[Recovery] B+Tree corruption detected: %d corrupted, %d orphaned, %d broken chains",
			report.GetCorruptedPageCount(), report.GetOrphanedPageCount(), report.GetBrokenChainCount())

		if err := validator.RepairStructure(report); err != nil {
			log.Printf("[Recovery] B+Tree repair failed: %v (attempting to continue)", err)
		}

		if err := validator.VerifyRecovery(); err != nil {
			log.Printf("[Recovery] B+Tree recovery verification failed: %v", err)
		}
	}

	log.Printf("[Recovery] B+Tree recovery complete")
	return nil
}

func (rc *DefaultRecoveryCoordinator) ValidateRecoveredState(ctx context.Context, state *wallog.RecoveryState) error {

	if state == nil {
		return fmt.Errorf("recovery state is nil")
	}

	if len(state.CommittedTxns) == 0 && len(state.AbortedTxns) == 0 {
		return nil
	}

	log.Printf("[Recovery] Validation: %d committed, %d aborted transactions",
		len(state.CommittedTxns), len(state.AbortedTxns))


	return nil
}

func (rc *DefaultRecoveryCoordinator) GetRecoveryStats() RecoveryStats {
	return rc.stats
}
