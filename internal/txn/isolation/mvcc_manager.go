package isolation

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type MVCCManager struct {
	transactions    map[TransactionID]*txn.Transaction
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	writeHandler    txn.WriteHandler     // For coordinating writes (optional WAL)
	rollbackManager *txn.RollbackManager // For abort handling
	primaryIndex    storage.IStorageEngine
	indexManagers   map[string]*schema.SecondaryIndexManager
	nextTxnID       int64
	indexSnapshots  map[TransactionID]map[string]string
}

// NewMVCCManager creates a new MVCC isolation manager
func NewMVCCManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	writeHandler txn.WriteHandler,
	rollbackMgr *txn.RollbackManager,
	primaryIndex storage.IStorageEngine,
	indexManagers map[string]*schema.SecondaryIndexManager,
) *MVCCManager {
	return &MVCCManager{
		transactions:    make(map[TransactionID]*txn.Transaction),
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		writeHandler:    writeHandler,
		rollbackManager: rollbackMgr,
		primaryIndex:    primaryIndex,
		indexManagers:   indexManagers,
		nextTxnID:       1,
		indexSnapshots:  make(map[TransactionID]map[string]string),
	}
}

func (m *MVCCManager) BeginTransaction(isolationLevel uint8, tableName string, tableSchema *schema.TableSchema) int64 {
	txnID := m.nextTxnID
	m.nextTxnID++

	transaction := txn.NewTransaction(uint64(txnID), isolationLevel)
	transaction.SetTableContext(tableName, tableSchema)
	m.transactions[TransactionID(txnID)] = transaction

	m.indexSnapshots[TransactionID(txnID)] = m.captureIndexSnapshot(tableName)

	return txnID
}

func (m *MVCCManager) Read(txnID int64, Key []byte) ([]byte, error) {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	_ = transaction
	return m.primaryIndex.Get(Key)
}

func (m *MVCCManager) Write(txnID int64, Key []byte, Value []byte) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	tableName, tableSchema := transaction.GetTableContext()

	if tableSchema != nil && m.indexManagers != nil {
		if indexMgr, exists := m.indexManagers[tableName]; exists && indexMgr != nil {
			if err := m.writeHandler.SetIndexContext(tableSchema, indexMgr); err != nil {
				return fmt.Errorf("failed to set index context: %w", err)
			}
		}
	}

	writeOp := txn.WriteOperation{
		Key:        Key,
		Value:      Value,
		PageID:     0, // TODO: determine actual page ID from storage engine
		Offset:     0, // TODO: determine actual offset within page
		TableName:  tableName,
		SchemaInfo: tableSchema,
		PrimaryKey: Key,
	}

	// WriteHandler coordinates write (MVCC typically doesn't use WAL for writes)
	return m.writeHandler.HandleWrite(transaction, writeOp)
}

func (m *MVCCManager) Commit(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	tableName, _ := transaction.GetTableContext()
	if tableName != "" {
		if err := m.validateIndexSnapshot(TransactionID(txnID), tableName); err != nil {
			return fmt.Errorf("index consistency check failed: %w", err)
		}
	}

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

	delete(m.indexSnapshots, TransactionID(txnID))

	return nil
}

func (m *MVCCManager) Abort(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	delete(m.indexSnapshots, TransactionID(txnID))
	return m.rollbackManager.RollbackTransaction(transaction, nil, nil)
}

func (m *MVCCManager) Close() error {
	return nil
}

// captureIndexSnapshot captures the state of indexes for the given table
func (m *MVCCManager) captureIndexSnapshot(tableName string) map[string]string {
	snapshot := make(map[string]string)

	indexMgr, exists := m.indexManagers[tableName]
	if !exists || indexMgr == nil {
		return snapshot
	}

	for _, indexName := range indexMgr.GetAllIndexNames() {
		stats, err := indexMgr.GetIndexStats(indexName)
		if err == nil && stats != nil {
			snapshot[indexName] = fmt.Sprintf("idx:%s:%d", indexName, stats.NumEntries)
		} else {
			snapshot[indexName] = fmt.Sprintf("idx:%s:exists", indexName)
		}
	}

	return snapshot
}

// validateIndexSnapshot validates that the index state hasn't changed since transaction start
func (m *MVCCManager) validateIndexSnapshot(txnID TransactionID, tableName string) error {
	startSnapshot, exists := m.indexSnapshots[txnID]
	if !exists {
		return nil
	}

	currentSnapshot := m.captureIndexSnapshot(tableName)

	for indexName, startFingerprint := range startSnapshot {
		if currentFingerprint, ok := currentSnapshot[indexName]; !ok {
			return fmt.Errorf("index %s was dropped during transaction", indexName)
		} else if currentFingerprint != startFingerprint {
			return fmt.Errorf("index %s was modified during transaction", indexName)
		}
	}

	if len(currentSnapshot) > len(startSnapshot) {
		return fmt.Errorf("new indexes added during transaction")
	}

	return nil
}
