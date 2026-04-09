package isolation

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

// OptimisticConcurrencyControlManager implements OCC isolation
// Transactions execute optimistically without locks, then validate on commit
type OptimisticConcurrencyControlManager struct {
	transactions    map[TransactionID]*txn.Transaction
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	writeHandler    txn.WriteHandler     // For coordinating writes with WAL
	rollbackManager *txn.RollbackManager // For abort handling
	primaryIndex    txn.StorageEngine
	nextTxnID       int64
	// TODO: Add conflict detection, read set/write set tracking, validation
}

// NewOptimisticConcurrencyControlManager creates a new OCC isolation manager
func NewOptimisticConcurrencyControlManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	writeHandler txn.WriteHandler,
	rollbackMgr *txn.RollbackManager,
	primaryIndex txn.StorageEngine,
) *OptimisticConcurrencyControlManager {
	return &OptimisticConcurrencyControlManager{
		transactions:    make(map[TransactionID]*txn.Transaction),
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		writeHandler:    writeHandler,
		rollbackManager: rollbackMgr,
		primaryIndex:    primaryIndex,
		nextTxnID:       1,
	}
}

func (m *OptimisticConcurrencyControlManager) BeginTransaction(isolationLevel uint8) int64 {
	txnID := m.nextTxnID
	m.nextTxnID++

	transaction := txn.NewTransaction(uint64(txnID), isolationLevel)
	m.transactions[TransactionID(txnID)] = transaction

	// TODO: Initialize read set and write set for this transaction
	return txnID
}

func (m *OptimisticConcurrencyControlManager) Read(txnID int64, Key []byte) ([]byte, error) {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	// OCC: Track read in read set, then read value
	// TODO: Add to transaction's read set for validation phase
	_ = transaction // silence unused
	return m.primaryIndex.Get(Key)
}

func (m *OptimisticConcurrencyControlManager) Write(txnID int64, Key []byte, Value []byte) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	// OCC: Buffer writes in transaction (don't apply to storage immediately)
	// TODO: Cache writes instead of applying immediately

	// For now, use write handler to apply to storage
	writeOp := txn.WriteOperation{
		Key:    Key,
		Value:  Value,
		PageID: 0, // TODO: determine actual page ID
		Offset: 0, // TODO: determine actual offset
	}

	return m.writeHandler.HandleWrite(transaction, writeOp)
}

func (m *OptimisticConcurrencyControlManager) Commit(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	// OCC: Validation phase
	// TODO: Check for conflicts with concurrent transactions
	// TODO: If validation passes, commit; otherwise abort

	transaction.Commit()

	if m.logManager != nil {
		rec := log.WALRecord{
			TxnID: transaction.GetID(),
			Type:  log.COMMIT,
		}
		lsn, err := m.logManager.AppendLogRecord(rec)
		if err != nil {
			return err
		}
		m.logManager.Flush(lsn)
	}

	return nil
}

func (m *OptimisticConcurrencyControlManager) Abort(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	// Use RollbackManager to coordinate abort
	return m.rollbackManager.RollbackTransaction(transaction, nil, nil)
}

func (m *OptimisticConcurrencyControlManager) Close() error {
	return nil
}
