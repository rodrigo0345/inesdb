package txn

import (
	"fmt"
	"sort"
	"sync"

	"github.com/rodrigo0345/omag/src/storage/buffer"
	log "github.com/rodrigo0345/omag/src/engine/wal"
	"github.com/rodrigo0345/omag/src/engine/undo"
)

type TxnState int

const (
	ACTIVE TxnState = iota
	COMMITTED
	ABORTED
)

const (
	READ_UNCOMMITTED uint8 = iota
	READ_COMMITTED
	REPEATABLE_READ
	SERIALIZABLE
)

type Transaction struct {
	txnID             uint64
	snapshotActiveTxn map[int64]bool // stores active transactions at the time of transaction start for visibility checks
	state             TxnState
	sharedLocks       [][]byte
	exclusiveLocks    [][]byte
	lockMu            sync.Mutex
	undoLog           *undo.UndoLog
	isolationLevel    uint8
	readSet           map[string]struct{}
	writeSet          map[string]struct{}
	touchedTables     map[string]struct{}
	cleanupCallbacks  []func() error
	operations        []log.RecoveryOperation // Operations for crash recovery
	txn_write_prefix  string                  // used to manage isolation level copies
}

func NewTransaction(txnID uint64, isolationLevel uint8) *Transaction {
	return &Transaction{
		txnID:            txnID,
		state:            ACTIVE,
		sharedLocks:      make([][]byte, 0),
		exclusiveLocks:   make([][]byte, 0),
		undoLog:          undo.NewUndoLog(txnID),
		isolationLevel:   isolationLevel,
		readSet:          make(map[string]struct{}),
		writeSet:         make(map[string]struct{}),
		touchedTables:    make(map[string]struct{}),
		cleanupCallbacks: make([]func() error, 0),
		operations:       make([]log.RecoveryOperation, 0),
		txn_write_prefix: fmt.Sprintf("#txn_%d_%d", txnID, isolationLevel),
	}
}

func (t *Transaction) GetID() uint64 {
	return t.txnID
}

func (t *Transaction) SetSnapshot(snapshotTxnIDs []int64) {
	t.snapshotActiveTxn = make(map[int64]bool, len(snapshotTxnIDs))
	for _, id := range snapshotTxnIDs {
		t.snapshotActiveTxn[id] = true
	}
}

func (t *Transaction) GetSnapshot() map[int64]bool {
	return t.snapshotActiveTxn
}

func (t *Transaction) IsVisibleInSnapshot(xmin int64) bool {
	// Future transactions are invisible
	if xmin > int64(t.txnID) {
		return false
	}

	// Own writes are always visible
	if xmin == int64(t.txnID) {
		return true
	}

	// If xmin is in this map, it was still running (uncommitted) when we started.
	// Therefore, its changes are invisible to this transaction.
	if _, active := t.snapshotActiveTxn[xmin]; active {
		return false
	}
	return true
}

func (t *Transaction) RecordOperation(op undo.Operation) error {
	return t.undoLog.RecordOp(op)
}

func (t *Transaction) AddUndo(op undo.Operation) error {
	return t.undoLog.RecordOp(op)
}

func (t *Transaction) Rollback(bufferMgr buffer.IBufferPoolManager) error {
	return t.undoLog.Rollback(bufferMgr)
}

func (t *Transaction) RollbackToPoint(point int, bufferMgr buffer.IBufferPoolManager) error {
	return t.undoLog.RollbackToPoint(point, bufferMgr)
}

func (t *Transaction) SavePoint() int {
	return t.undoLog.SavePoint()
}

func (t *Transaction) Abort() {
	t.state = ABORTED
}

func (t *Transaction) GetState() TxnState {
	return t.state
}

func (t *Transaction) GetUndoLog() *undo.UndoLog {
	return t.undoLog
}

func (t *Transaction) GetIsolationLevel() uint8 {
	return t.isolationLevel
}

func (t *Transaction) RecordTableAccess(tableName string) {
	if tableName == "" {
		return
	}
	if t.touchedTables == nil {
		t.touchedTables = make(map[string]struct{})
	}
	t.touchedTables[tableName] = struct{}{}
}

func (t *Transaction) RecordReadKey(key []byte) {
	if len(key) == 0 {
		return
	}
	if t.readSet == nil {
		t.readSet = make(map[string]struct{})
	}
	t.readSet[string(key)] = struct{}{}
}

func (t *Transaction) RecordWriteKey(key []byte) {
	if len(key) == 0 {
		return
	}
	if t.writeSet == nil {
		t.writeSet = make(map[string]struct{})
	}
	t.writeSet[string(key)] = struct{}{}
}

func (t *Transaction) GetReadSet() [][]byte {
	return stringSetToSortedBytes(t.readSet)
}

func (t *Transaction) GetWriteSet() [][]byte {
	return stringSetToSortedBytes(t.writeSet)
}

func (t *Transaction) GetTouchedTables() []string {
	tables := make([]string, 0, len(t.touchedTables))
	for tableName := range t.touchedTables {
		tables = append(tables, tableName)
	}
	sort.Strings(tables)
	return tables
}

func (t *Transaction) GetSharedLocks() [][]byte {
	t.lockMu.Lock()
	defer t.lockMu.Unlock()
	return cloneByteSlices(t.sharedLocks)
}

func (t *Transaction) GetExclusiveLocks() [][]byte {
	t.lockMu.Lock()
	defer t.lockMu.Unlock()
	return cloneByteSlices(t.exclusiveLocks)
}

func (t *Transaction) GetWritePrefix() string {
	return t.txn_write_prefix
}

func (t *Transaction) Commit() {
	t.state = COMMITTED
}

func (t *Transaction) AddSharedLock(key []byte) {
	t.lockMu.Lock()
	defer t.lockMu.Unlock()
	t.sharedLocks = append(t.sharedLocks, key)
}

func (t *Transaction) AddExclusiveLock(key []byte) {
	t.lockMu.Lock()
	defer t.lockMu.Unlock()
	t.exclusiveLocks = append(t.exclusiveLocks, key)
}

func (t *Transaction) RemoveSharedLock(key []byte) {
	t.lockMu.Lock()
	defer t.lockMu.Unlock()
	for i, k := range t.sharedLocks {
		if bytesEqual(k, key) {
			t.sharedLocks = append(t.sharedLocks[:i], t.sharedLocks[i+1:]...)
			return
		}
	}
}

func (t *Transaction) RemoveExclusiveLock(key []byte) {
	t.lockMu.Lock()
	defer t.lockMu.Unlock()
	for i, k := range t.exclusiveLocks {
		if bytesEqual(k, key) {
			t.exclusiveLocks = append(t.exclusiveLocks[:i], t.exclusiveLocks[i+1:]...)
			return
		}
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringSetToSortedBytes(values map[string]struct{}) [][]byte {
	if len(values) == 0 {
		return [][]byte{}
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([][]byte, 0, len(keys))
	for _, key := range keys {
		result = append(result, []byte(key))
	}
	return result
}

func cloneByteSlices(values [][]byte) [][]byte {
	if len(values) == 0 {
		return [][]byte{}
	}

	cloned := make([][]byte, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		cloned[i] = append([]byte(nil), value...)
	}
	return cloned
}

func (t *Transaction) RegisterCleanupCallback(cleanup func() error) {
	t.cleanupCallbacks = append(t.cleanupCallbacks, cleanup)
}

func (t *Transaction) ExecuteCleanupCallbacks() error {
	for _, cleanup := range t.cleanupCallbacks {
		if cleanup != nil {
			if err := cleanup(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Transaction) RecordRecoveryOperation(tableName string, opType log.RecordType, key []byte, value []byte) {
	op := log.RecoveryOperation{
		TxnID:     t.txnID,
		TableName: tableName,
		Type:      opType,
		Key:       key,
		Value:     value,
	}
	t.operations = append(t.operations, op)
}

func (t *Transaction) GetRecoveryOperations() []log.RecoveryOperation {
	return t.operations
}

// TransactionID is the unique identifier for a transaction.
type TransactionID = uint64
