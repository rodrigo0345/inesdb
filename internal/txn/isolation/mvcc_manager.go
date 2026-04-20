package isolation

import (
	"fmt"
	"sync"
	"sync/atomic"

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
	storageResolver func(tableName string) storage.IStorageEngine
	indexManagers   map[string]*schema.SecondaryIndexManager
	nextTxnID       atomic.Int64
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
		indexSnapshots:  make(map[TransactionID]map[string]string),
	}
}

func (m *MVCCManager) SetStorageResolver(resolver func(tableName string) storage.IStorageEngine) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storageResolver = resolver
}

func (m *MVCCManager) resolveStorageEngine(tableName string) storage.IStorageEngine {
	if m.storageResolver != nil {
		if engine := m.storageResolver(tableName); engine != nil {
			return engine
		}
	}
	return m.primaryIndex
}

func (m *MVCCManager) BeginTransaction(isolationLevel uint8, tableName string, tableSchema *schema.TableSchema) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	txnID := m.nextTxnID.Add(1)

	transaction := txn_unit.NewTransaction(uint64(txnID), isolationLevel)
	transaction.SetTableContext(tableName, tableSchema)
	m.transactions[TransactionID(txnID)] = transaction

	if hasSecondaryIndexes(tableSchema) {
		if indexMgr, exists := m.indexManagers[tableName]; exists && indexMgr != nil {
			if snapshot := captureIndexSnapshotForManager(tableName, indexMgr); len(snapshot) > 0 {
				m.indexSnapshots[TransactionID(txnID)] = snapshot
			}
		}
	}

	return txnID
}

// EnsureMinNextTxnID seeds the in-memory transaction ID allocator so the next
// BeginTransaction call returns an ID greater than any recovered WAL txn ID.
func (m *MVCCManager) EnsureMinNextTxnID(lastTxnID uint64) {
	target := int64(lastTxnID)
	for {
		current := m.nextTxnID.Load()
		if current >= target {
			return
		}
		if m.nextTxnID.CompareAndSwap(current, target) {
			return
		}
	}
}

func (m *MVCCManager) GetTransactionTableContext(txnID int64) (string, *schema.TableSchema, bool) {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()
	if !ok || transaction == nil {
		return "", nil, false
	}
	tableName, tableSchema := transaction.GetTableContext()
	return tableName, tableSchema, true
}

func (m *MVCCManager) Read(txnID int64, Key []byte) ([]byte, error) {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	_ = transaction
	transaction.RecordReadKey(Key)
	tableName, _ := transaction.GetTableContext()
	storageEngine := m.resolveStorageEngine(tableName)
	if storageEngine == nil {
		return nil, fmt.Errorf("storage engine is nil")
	}
	return storageEngine.Get(Key)
}

func (m *MVCCManager) Write(txnID int64, Key []byte, Value []byte) error {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	tableName, tableSchema := transaction.GetTableContext()
	transaction.RecordWriteKey(Key)
	transaction.RecordTableAccess(tableName)

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

func (m *MVCCManager) Delete(txnID int64, Key []byte) error {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	tableName, tableSchema := transaction.GetTableContext()
	transaction.RecordWriteKey(Key)
	transaction.RecordTableAccess(tableName)

	if tableSchema != nil && m.indexManagers != nil {
		if indexMgr, exists := m.indexManagers[tableName]; exists && indexMgr != nil {
			if err := m.writeHandler.SetIndexContext(tableSchema, indexMgr); err != nil {
				return fmt.Errorf("failed to set index context: %w", err)
			}
		}
	}

	writeOp := write_handler.WriteOperation{
		Key:        Key,
		Value:      nil,
		PageID:     0,
		Offset:     0,
		IsDelete:   true,
		TableName:  tableName,
		SchemaInfo: tableSchema,
		PrimaryKey: Key,
	}

	return m.writeHandler.HandleWrite(transaction, writeOp)
}

func (m *MVCCManager) Commit(txnID int64) error {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	startSnapshot := m.indexSnapshots[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	tableName, _ := transaction.GetTableContext()
	if tableName != "" && len(startSnapshot) > 0 {
		if err := m.validateIndexSnapshot(tableName, startSnapshot); err != nil {
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
		if err := m.logManager.Flush(lsn); err != nil {
			return err
		}
	}

	m.mu.Lock()
	delete(m.transactions, TransactionID(txnID))
	delete(m.indexSnapshots, TransactionID(txnID))
	m.mu.Unlock()

	return nil
}

func (m *MVCCManager) Abort(txnID int64) error {
	m.mu.Lock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	delete(m.transactions, TransactionID(txnID))
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
	indexMgr, exists := m.indexManagers[tableName]
	if !exists || indexMgr == nil {
		return map[string]string{}
	}
	return captureIndexSnapshotForManager(tableName, indexMgr)
}

func captureIndexSnapshotForManager(tableName string, indexMgr *schema.SecondaryIndexManager) map[string]string {
	snapshot := make(map[string]string)

	for _, indexName := range indexMgr.GetAllIndexNames() {
		stats, err := indexMgr.GetIndexStats(tableName, indexName)
		if err == nil && stats != nil {
			snapshot[indexName] = fmt.Sprintf("idx:%s:%d", indexName, stats.NumEntries)
		} else {
			snapshot[indexName] = fmt.Sprintf("idx:%s:exists", indexName)
		}
	}

	return snapshot
}

func (m *MVCCManager) validateIndexSnapshot(tableName string, startSnapshot map[string]string) error {
	if len(startSnapshot) == 0 {
		return nil
	}

	indexMgr, exists := m.indexManagers[tableName]
	if !exists || indexMgr == nil {
		return fmt.Errorf("indexes for table %q are unavailable", tableName)
	}

	currentSnapshot := captureIndexSnapshotForManager(tableName, indexMgr)

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

func hasSecondaryIndexes(tableSchema *schema.TableSchema) bool {
	if tableSchema == nil {
		return false
	}
	for _, idx := range tableSchema.Indexes {
		if idx != nil && idx.Type != schema.IndexTypePrimary {
			return true
		}
	}
	return false
}
