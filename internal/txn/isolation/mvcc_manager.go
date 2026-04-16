package isolation

import (
	"fmt"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	"github.com/rodrigo0345/omag/internal/txn/write_handler"
)

type MVCCManager struct {
	mu              sync.RWMutex
	transactions    map[TransactionID]*txn_unit.Transaction
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	writeHandler    write_handler.IWriteHandler
	rollbackManager *rollback.RollbackManager
	primaryIndex    storage.IStorageEngine
	indexManagers   map[string]*schema.SecondaryIndexManager
	nextTxnID       int64
	indexSnapshots  map[TransactionID]map[string]string
}

func NewMVCCManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	writeHandler write_handler.IWriteHandler,
	rollbackMgr *rollback.RollbackManager,
	primaryIndex storage.IStorageEngine,
	indexManagers map[string]*schema.SecondaryIndexManager,
) *MVCCManager {
	return &MVCCManager{
		transactions:    make(map[TransactionID]*txn_unit.Transaction),
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
	m.mu.Lock()
	defer m.mu.Unlock()

	txnID := m.nextTxnID
	m.nextTxnID++

	transaction := txn_unit.NewTransaction(uint64(txnID), isolationLevel)
	transaction.SetTableContext(tableName, tableSchema)
	m.transactions[TransactionID(txnID)] = transaction

	m.indexSnapshots[TransactionID(txnID)] = m.captureIndexSnapshot(tableName)

	return txnID
}

func (m *MVCCManager) Read(txnID int64, Key []byte) ([]byte, error) {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	_ = transaction
	return m.primaryIndex.Get(Key)
}

func (m *MVCCManager) Write(txnID int64, Key []byte, Value []byte) error {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

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

	writeOp := write_handler.WriteOperation{
		Key:        Key,
		Value:      Value,
		PageID:     0,
		Offset:     0,
		TableName:  tableName,
		SchemaInfo: tableSchema,
		PrimaryKey: Key,
	}

	return m.writeHandler.HandleWrite(transaction, writeOp)
}

func (m *MVCCManager) Commit(txnID int64) error {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

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

	m.mu.Lock()
	delete(m.indexSnapshots, TransactionID(txnID))
	m.mu.Unlock()

	return nil
}

func (m *MVCCManager) Abort(txnID int64) error {
	m.mu.Lock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	delete(m.indexSnapshots, TransactionID(txnID))
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	// Clean up transaction operations since this transaction is being aborted
	if m.logManager != nil {
		m.logManager.CleanupTransactionOperations(transaction.GetID())
	}

	return m.rollbackManager.RollbackTransaction(transaction, nil, nil)
}

func (m *MVCCManager) Close() error {
	return nil
}

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
