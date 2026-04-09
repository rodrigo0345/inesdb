package isolation

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

// MVCCManager implements Multi-Version Concurrency Control isolation
// Each transaction sees a consistent snapshot of data at its start time
type MVCCManager struct {
	transactions    map[TransactionID]*txn.Transaction
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	writeHandler    txn.WriteHandler     // For coordinating writes (optional WAL)
	rollbackManager *txn.RollbackManager // For abort handling
	primaryIndex    txn.StorageEngine
	nextTxnID       int64
	// TODO: Add version management, snapshot isolation, garbage collection
}

// NewMVCCManager creates a new MVCC isolation manager
func NewMVCCManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	writeHandler txn.WriteHandler,
	rollbackMgr *txn.RollbackManager,
	primaryIndex txn.StorageEngine,
) *MVCCManager {
	return &MVCCManager{
		transactions:    make(map[TransactionID]*txn.Transaction),
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		writeHandler:    writeHandler,
		rollbackManager: rollbackMgr,
		primaryIndex:    primaryIndex,
		nextTxnID:       1,
	}
}

func (m *MVCCManager) BeginTransaction(isolationLevel uint8) int64 {
	txnID := m.nextTxnID
	m.nextTxnID++

	// MVCC: Create transaction with current snapshot/version
	transaction := txn.NewTransaction(uint64(txnID), isolationLevel)
	m.transactions[TransactionID(txnID)] = transaction

	// TODO: Record transaction's start version for snapshot isolation
	return txnID
}

func (m *MVCCManager) Read(txnID int64, Key []byte) ([]byte, error) {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	// MVCC Read: Get version visible to this transaction's snapshot
	// TODO: Implement version lookup based on transaction start time
	// For now, just read current version
	_ = transaction // silence unused
	return m.primaryIndex.Get(Key)
}

func (m *MVCCManager) Write(txnID int64, Key []byte, Value []byte) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	// Create write operation for WriteHandler coordination
	writeOp := txn.WriteOperation{
		Key:    Key,
		Value:  Value,
		PageID: 0, // TODO: determine actual page ID from storage engine
		Offset: 0, // TODO: determine actual offset within page
	}

	// WriteHandler coordinates write (MVCC typically doesn't use WAL for writes)
	return m.writeHandler.HandleWrite(transaction, writeOp)
}

func (m *MVCCManager) Commit(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	transaction.Commit()

	// MVCC: Make this transaction's writes visible
	// TODO: Update global transaction view to include this transaction's version

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

func (m *MVCCManager) Abort(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	// Use RollbackManager to coordinate abort
	// MVCC advantage: no need to undo, just discard this transaction's writes
	return m.rollbackManager.RollbackTransaction(transaction, nil, nil)
}

func (m *MVCCManager) Close() error {
	return nil
}
