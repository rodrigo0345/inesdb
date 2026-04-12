package isolation

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type OptimisticConcurrencyControlManager struct {
	transactions    map[TransactionID]*txn.Transaction
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	writeHandler    txn.WriteHandler
	rollbackManager *txn.RollbackManager
	primaryIndex    storage.IStorageEngine
	indexManagers   map[string]*schema.SecondaryIndexManager
	nextTxnID       int64
	indexSnapshots  map[TransactionID]map[string]string
	userWrites      map[TransactionID]map[string]bool
}

func NewOptimisticConcurrencyControlManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	writeHandler txn.WriteHandler,
	rollbackMgr *txn.RollbackManager,
	primaryIndex storage.IStorageEngine,
	indexManagers map[string]*schema.SecondaryIndexManager,
) *OptimisticConcurrencyControlManager {
	return &OptimisticConcurrencyControlManager{
		transactions:    make(map[TransactionID]*txn.Transaction),
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		writeHandler:    writeHandler,
		rollbackManager: rollbackMgr,
		primaryIndex:    primaryIndex,
		indexManagers:   indexManagers,
		nextTxnID:       1,
		indexSnapshots:  make(map[TransactionID]map[string]string),
		userWrites:      make(map[TransactionID]map[string]bool),
	}
}

func (m *OptimisticConcurrencyControlManager) BeginTransaction(isolationLevel uint8, tableName string, tableSchema *schema.TableSchema) int64 {
	txnID := m.nextTxnID
	m.nextTxnID++

	transaction := txn.NewTransaction(uint64(txnID), isolationLevel)
	transaction.SetTableContext(tableName, tableSchema)
	m.transactions[TransactionID(txnID)] = transaction

	m.indexSnapshots[TransactionID(txnID)] = m.captureIndexSnapshot(tableName)
	m.userWrites[TransactionID(txnID)] = make(map[string]bool)

	return txnID
}

func (m *OptimisticConcurrencyControlManager) Read(txnID int64, Key []byte) ([]byte, error) {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	_ = transaction
	return m.primaryIndex.Get(Key)
}

func (m *OptimisticConcurrencyControlManager) Write(txnID int64, Key []byte, Value []byte) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	tableName, tableSchema := transaction.GetTableContext()

	if m.userWrites[TransactionID(txnID)] != nil {
		m.userWrites[TransactionID(txnID)][tableName] = true
	}

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
		PageID:     0,
		Offset:     0,
		TableName:  tableName,
		SchemaInfo: tableSchema,
		PrimaryKey: Key,
	}

	return m.writeHandler.HandleWrite(transaction, writeOp)
}

func (m *OptimisticConcurrencyControlManager) Commit(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	tableName, _ := transaction.GetTableContext()
	if tableName != "" && m.userWrites[TransactionID(txnID)][tableName] {
		if err := m.detectIndexConflicts(TransactionID(txnID), tableName); err != nil {
			m.rollbackManager.RollbackTransaction(transaction, nil, nil)
			return fmt.Errorf("OCC index conflict detection failed: %w", err)
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
	delete(m.userWrites, TransactionID(txnID))

	return nil
}

func (m *OptimisticConcurrencyControlManager) Abort(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	delete(m.indexSnapshots, TransactionID(txnID))
	delete(m.userWrites, TransactionID(txnID))

	return m.rollbackManager.RollbackTransaction(transaction, nil, nil)
}

func (m *OptimisticConcurrencyControlManager) Close() error {
	return nil
}

func (m *OptimisticConcurrencyControlManager) captureIndexSnapshot(tableName string) map[string]string {
	snapshot := make(map[string]string)

	indexMgr, exists := m.indexManagers[tableName]
	if !exists || indexMgr == nil {
		return snapshot
	}

	for _, indexName := range indexMgr.GetAllIndexNames() {
		stats, err := indexMgr.GetIndexStats(indexName)
		if err == nil && stats != nil {
			snapshot[indexName] = fmt.Sprintf("idx:%s:%d:%d", indexName, stats.NumEntries, stats.SizeBytes)
		} else {
			snapshot[indexName] = fmt.Sprintf("idx:%s:exists", indexName)
		}
	}

	return snapshot
}

func (m *OptimisticConcurrencyControlManager) detectIndexConflicts(txnID TransactionID, tableName string) error {
	startSnapshot, exists := m.indexSnapshots[txnID]
	if !exists {
		return nil
	}

	currentSnapshot := m.captureIndexSnapshot(tableName)

	for indexName, startFingerprint := range startSnapshot {
		if currentFingerprint, ok := currentSnapshot[indexName]; !ok {
			return fmt.Errorf("index %s was dropped during transaction", indexName)
		} else if currentFingerprint != startFingerprint {
			return fmt.Errorf("write-write conflict detected on index %s", indexName)
		}
	}

	if len(currentSnapshot) > len(startSnapshot) {
		return fmt.Errorf("new indexes added during transaction")
	}

	return nil
}
