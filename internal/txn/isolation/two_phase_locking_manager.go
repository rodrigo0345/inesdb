package isolation

import (
	"bytes"
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type TransactionID uint64

type TwoPhaseLockingManager struct {
	transactions    map[TransactionID]*txn.Transaction
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	writeHandler    txn.WriteHandler
	rollbackManager *txn.RollbackManager
	primaryIndex    storage.IStorageEngine
	indexManagers   map[string]*schema.SecondaryIndexManager
}

func NewTwoPhaseLockingManager(
	logManager log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	writeHandler txn.WriteHandler,
	rollbackMgr *txn.RollbackManager,
	primaryIndex storage.IStorageEngine,
	indexManagers map[string]*schema.SecondaryIndexManager,
) *TwoPhaseLockingManager {
	return &TwoPhaseLockingManager{
		transactions:    make(map[TransactionID]*txn.Transaction),
		logManager:      logManager,
		bufferManager:   bufferMgr,
		writeHandler:    writeHandler,
		rollbackManager: rollbackMgr,
		primaryIndex:    primaryIndex,
		indexManagers:   indexManagers,
	}
}

func (m *TwoPhaseLockingManager) BeginTransaction(isolationLevel uint8, tableName string, tableSchema *schema.TableSchema) int64 {
	txnID := int64(len(m.transactions) + 1)
	txn := txn.NewTransaction(uint64(txnID), isolationLevel)
	txn.SetTableContext(tableName, tableSchema)
	m.transactions[TransactionID(txnID)] = txn
	return txnID
}

func (m *TwoPhaseLockingManager) Read(txnID int64, Key []byte) ([]byte, error) {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	transaction.AddSharedLock(Key)
	return m.primaryIndex.Get(Key)
}

func (m *TwoPhaseLockingManager) Write(txnID int64, Key []byte, Value []byte) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	transaction.AddExclusiveLock(Key)

	tableName, tableSchema := transaction.GetTableContext()

	if err := m.acquireIndexLocks(transaction, tableName, tableSchema, Value); err != nil {
		return fmt.Errorf("failed to acquire index locks: %w", err)
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

func (m *TwoPhaseLockingManager) Commit(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	transaction.Commit()

	if m.logManager != nil {
		rec := &log.WALRecord{
			TxnID: transaction.GetID(),
			Type:  log.COMMIT,
		}
		lsn, err := m.logManager.AppendLogRecord(rec)
		if err != nil {
			return err
		}

		m.logManager.Flush(lsn)
	}

	if m.bufferManager != nil {
		if bpm, ok := m.bufferManager.(interface{ FlushAll() error }); ok {
			bpm.FlushAll()
		}
	}
	return nil
}

func (m *TwoPhaseLockingManager) Abort(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	return m.rollbackManager.RollbackTransaction(
		transaction,
		nil,
		nil,
	)
}

func (m *TwoPhaseLockingManager) Close() error {
	return nil
}

func makeIndexLockKey(tableName, indexName string, indexValue []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("__index:")
	buf.WriteString(tableName)
	buf.WriteString(":")
	buf.WriteString(indexName)
	buf.WriteString(":")
	buf.Write(indexValue)
	return buf.Bytes()
}

func (m *TwoPhaseLockingManager) acquireIndexLocks(transaction *txn.Transaction, tableName string, tableSchema *schema.TableSchema, rowData []byte) error {
	if tableSchema == nil || len(tableSchema.Indexes) == 0 {
		return nil
	}

	indexValues, err := txn.ExtractIndexValues(tableSchema, rowData)
	if err != nil {
		return fmt.Errorf("failed to extract index values for locking: %w", err)
	}

	for indexName, indexValue := range indexValues {
		lockKey := makeIndexLockKey(tableName, indexName, indexValue)
		transaction.AddExclusiveLock(lockKey)
	}

	return nil
}

func (m *TwoPhaseLockingManager) releaseIndexLocks(transaction *txn.Transaction, tableName string, tableSchema *schema.TableSchema, rowData []byte) error {
	if tableSchema == nil || len(tableSchema.Indexes) == 0 {
		return nil
	}

	indexValues, err := txn.ExtractIndexValues(tableSchema, rowData)
	if err != nil {
		return fmt.Errorf("failed to extract index values for lock release: %w", err)
	}

	for indexName, indexValue := range indexValues {
		lockKey := makeIndexLockKey(tableName, indexName, indexValue)
		transaction.RemoveExclusiveLock(lockKey)
	}

	return nil
}
