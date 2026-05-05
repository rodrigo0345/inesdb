package isolation

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

const (
	Separator uint8 = 0x00
	OpInsert  uint8 = 0x01
	OpDelete  uint8 = 0x02
)

type MVCCManager struct {
	mu              sync.RWMutex
	transactions    map[txn.TransactionID]*txn_unit.Transaction
	committedTxns   map[txn.TransactionID]bool
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	rollbackManager *rollback.RollbackManager
	tableManager    schema.ITableManager
	nextTxnID       atomic.Int64
}

func NewMVCCManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	rollbackMgr *rollback.RollbackManager,
	tableManager schema.ITableManager,
) *MVCCManager {
	return &MVCCManager{
		transactions:    make(map[txn.TransactionID]*txn_unit.Transaction),
		committedTxns:   make(map[txn.TransactionID]bool),
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		rollbackManager: rollbackMgr,
		tableManager:    tableManager,
	}
}

// --- Transaction Lifecycle ---

func (m *MVCCManager) BeginTransaction(isolationLevel uint8) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	txnID := m.nextTxnID.Add(1)
	transaction := txn_unit.NewTransaction(uint64(txnID), isolationLevel)

	activeIDs := make([]int64, 0, len(m.transactions))
	for id := range m.transactions {
		activeIDs = append(activeIDs, int64(id))
	}
	transaction.SetSnapshot(activeIDs)

	m.transactions[txn.TransactionID(txnID)] = transaction
	return txnID
}

func (m *MVCCManager) Commit(txnID txn.TransactionID) error {
	m.mu.Lock()
	transaction, ok := m.transactions[txnID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("transaction %d not found", txnID)
	}

	m.committedTxns[txnID] = true
	m.mu.Unlock()

	transaction.Commit()

	if m.logManager != nil {
		rec := log.WALRecord{TxnID: transaction.GetID(), Type: log.COMMIT}
		lsn, _ := m.logManager.AppendLogRecord(rec)
		m.logManager.Flush(lsn)
	}

	m.mu.Lock()
	delete(m.transactions, txnID)
	m.mu.Unlock()
	return nil
}

func (m *MVCCManager) Abort(txnID txn.TransactionID) error {
	m.mu.Lock()
	transaction, ok := m.transactions[txnID]
	delete(m.transactions, txnID)
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	if m.logManager != nil {
		m.logManager.CleanupTransactionOperations(transaction.GetID())
	}

	return m.rollbackManager.RollbackTransaction(transaction, nil, nil)
}

// --- Data Operations ---

func (m *MVCCManager) Read(txnID txn.TransactionID, tableName, indexName string, key []byte) ([]byte, error) {
      // FIX: Bounds must match the inverted txnID logic (^txnID)
      // Smallest suffix is ^math.MaxUint64 (0), Largest is ^0 (Max)
      opts := storage.ScanOptions{
            LowerBound: m.encodeKey(key, math.MaxUint64),
            UpperBound: m.encodeKey(key, 0),
            Inclusive:  true,
      }

      cursor, err := m.Scan(txnID, tableName, indexName, opts)
      if err != nil {
            return nil, err
      }
      defer cursor.Close()

      if cursor.Next() {
            entry := cursor.Entry()
            // Slice off the OpCode (byte 0) before returning data to the user
            if len(entry.Value) > 0 {
                  return entry.Value, nil
            }
            return nil, fmt.Errorf("corrupt empty row")
      }
      return nil, fmt.Errorf("key not found")
}

func (m *MVCCManager) isVisible(transaction *txn_unit.Transaction, xmin txn.TransactionID) bool {
      // 1. Always see our own writes
      if xmin == txn.TransactionID(transaction.GetID()) {
            return true
      }

      m.mu.RLock()
      committed := m.committedTxns[xmin]
      m.mu.RUnlock()

      switch transaction.GetIsolationLevel() {
      case txn_unit.READ_COMMITTED:
            return committed
      case txn_unit.REPEATABLE_READ:
            // FIX: If it's not committed, it's invisible (unless it was our own, handled above)
            if !committed {
                  return false
            }
            // Snapshot check: Was this txn active when we started?
            return transaction.IsVisibleInSnapshot(int64(xmin))
      default:
            return committed
      }
}

func (m *MVCCManager) Write(txnID txn.TransactionID, tableName, indexName string, key []byte, value []byte) error {
	m.mu.RLock()
	transaction, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	internalKey := m.encodeKey(key, uint64(txnID))

	// Ensure the metadata OpCode is set to Insert
	payload := make([]byte, len(value))
	copy(payload, value)
	payload[0] = OpInsert

	if err := m.tableManager.Write(schema.WriteOperation{
		TableName: tableName,
		Key:       internalKey,
		Value:     payload,
	}); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	transaction.RecordRecoveryOperation(tableName, log.PUT, internalKey, payload)
	return nil
}

func (m *MVCCManager) Delete(txnID txn.TransactionID, tableName, indexName string, key []byte) error {
	m.mu.RLock()
	transaction, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	// 1. Fetch current visible data to build a valid tombstone row.
	// We need the columns (ID, etc.) so secondary indexes can be purged.
	existingVal, err := m.Read(txnID, tableName, indexName, key)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	internalKey := m.encodeKey(key, uint64(txnID))

	// 2. Create the Tombstone (Full-row copy with OpDelete OpCode)
	payload := make([]byte, len(existingVal))
	copy(payload, existingVal)
	payload[0] = OpDelete

	if err := m.tableManager.Write(schema.WriteOperation{
		TableName: tableName,
		Key:       internalKey,
		Value:     payload,
	}); err != nil {
		return fmt.Errorf("delete write failed: %w", err)
	}

	transaction.RecordRecoveryOperation(tableName, log.DELETE, internalKey, nil)
	return nil
}

// --- MVCC Core Logic ---

func (m *MVCCManager) Scan(txnID txn.TransactionID, tableName, indexName string, opts storage.ScanOptions) (storage.ICursor, error) {
	m.mu.RLock()
	transaction, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	raw, err := m.tableManager.Scan(tableName, indexName, opts)
	if err != nil {
		return nil, err
	}

	return &MVCCCursor{
		raw:      raw,
		manager:  m,
		txn:      transaction,
		seenKeys: make(map[string]bool),
	}, nil
}

// --- Key Encoding/Decoding ---

func (m *MVCCManager) encodeKey(userKey []byte, txnID uint64) []byte {
	buf := make([]byte, len(userKey)+9)
	copy(buf, userKey)
	buf[len(userKey)] = Separator
	// Inverting txnID via bitwise NOT (^) ensures newer versions
	// appear first in lexicographical order.
	binary.BigEndian.PutUint64(buf[len(userKey)+1:], ^txnID)
	return buf
}

func (m *MVCCManager) decodeKey(internalKey []byte) ([]byte, uint64) {
	sepIdx := bytes.LastIndexByte(internalKey, Separator)
	if sepIdx == -1 {
		return internalKey, 0
	}
	userKey := internalKey[:sepIdx]
	invertedID := binary.BigEndian.Uint64(internalKey[sepIdx+1:])
	return userKey, ^invertedID
}
