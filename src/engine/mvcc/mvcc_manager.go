package mvcc

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/buffer"
	"github.com/rodrigo0345/omag/src/storage/schema"
	log "github.com/rodrigo0345/omag/src/engine/wal"
	"github.com/rodrigo0345/omag/src/engine/rollback"
	txn_unit "github.com/rodrigo0345/omag/src/engine/txn"
	"github.com/rodrigo0345/omag/pkg/pkglog"
)

const (
	Separator uint8 = 0x00
	OpInsert  uint8 = 0x01
	OpDelete  uint8 = 0x02
)

type txnStatus uint8

const (
	statusActive txnStatus = iota
	statusCommitted
	statusAborted
)

func (s txnStatus) String() string {
	switch s {
	case statusActive:
		return "active"
	case statusCommitted:
		return "committed"
	case statusAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

type txnMeta struct {
	txn    *txn_unit.Transaction
	status txnStatus
	// writtenUserKeys is the set of user-space keys this transaction wrote or
	// deleted. Populated by Write/Delete, consumed by conflict detection at
	// commit time. Keys are stored in user-key form (without the txnID suffix)
	// so they can be compared across transactions.
	writtenUserKeys map[string]struct{}
}

type MVCCManager struct {
	mu sync.RWMutex

	// transactions holds every transaction that has not been GC'd.
	// Committed and aborted entries stay here until GC sweeps them — their
	// status lets isVisible answer without a separate committedTxns map, and
	// lets GC know the low-water mark.
	transactions map[txn_unit.TransactionID]*txnMeta

	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	rollbackManager *rollback.RollbackManager
	tracer          *pkglog.Tracer // Updated: Ensure this matches your Tracer struct
	tableManager    schema.ITableManager

	nextTxnID      atomic.Int64
	minActiveTxnID atomic.Int64
}

// Updated constructor to accept the tracer
func NewMVCCManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	rollbackMgr *rollback.RollbackManager,
	tableManager schema.ITableManager,
	tracer *pkglog.Tracer,
) *MVCCManager {
	m := &MVCCManager{
		transactions:    make(map[txn_unit.TransactionID]*txnMeta),
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		rollbackManager: rollbackMgr,
		tableManager:    tableManager,
		tracer:          tracer,
	}
	m.minActiveTxnID.Store(math.MaxInt64)
	return m
}

// ---------------------------------------------------------------------------
// Transaction lifecycle
// ---------------------------------------------------------------------------

func (m *MVCCManager) BeginTransaction(isolationLevel uint8) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := m.nextTxnID.Add(1)

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("BeginTransaction: Starting TxnID %d with isolation level %d", id, isolationLevel))
	}

	t := txn_unit.NewTransaction(uint64(id), isolationLevel)

	// Snapshot: capture only the IDs of transactions that are still active.
	// A version written by txn X is invisible under REPEATABLE_READ /
	// SERIALIZABLE if X was in-flight (active) when we took this snapshot,
	// even if X commits later.
	activeIDs := make([]int64, 0, len(m.transactions))
	for tid, meta := range m.transactions {
		if meta.status == statusActive {
			activeIDs = append(activeIDs, int64(tid))
		}
	}
	t.SetSnapshot(activeIDs) // store which txns were active at the time of this txn's start for visibility checks

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("BeginTransaction: TxnID %d captured snapshot of active txns: %v", id, activeIDs))
	}

	m.transactions[txn_unit.TransactionID(id)] = &txnMeta{
		txn:             t,
		status:          statusActive,
		writtenUserKeys: make(map[string]struct{}),
	}
	m.updateMinActive()
	return id
}

func (m *MVCCManager) Commit(txnID txn_unit.TransactionID) error {
	m.mu.Lock()

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Commit: Attempting to commit TxnID %d", txnID))
	}

	meta, ok := m.transactions[txnID]
	if !ok {
		m.mu.Unlock()
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("Commit: Failed - TxnID %d not found", txnID))
		}
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if meta.status != statusActive {
		m.mu.Unlock()
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("Commit: Failed - TxnID %d is already %s", txnID, meta.status))
		}
		return fmt.Errorf("transaction %d is already %s", txnID, meta.status)
	}

	// --- Conflict detection (under the write lock) ---
	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Commit: Running conflict detection for TxnID %d", txnID))
	}

	if conflictID := m.detectWriteConflict(txnID, meta); conflictID != 0 {
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("Commit: Decision - Conflict detected with TxnID %d, aborting TxnID %d", conflictID, txnID))
		}
		// Abort atomically while still holding the lock.
		meta.status = statusAborted
		m.updateMinActive()
		m.mu.Unlock()

		m.physicalRollback(meta)
		return fmt.Errorf(
			"write conflict: transaction %d aborted — transaction %d committed "+
				"a write to the same key(s) after we began",
			txnID, conflictID,
		)
	}

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Commit: Decision - No conflicts found. Committing TxnID %d", txnID))
	}

	// No conflict — flip to committed atomically before releasing the lock.
	meta.status = statusCommitted
	m.updateMinActive()
	m.mu.Unlock()

	meta.txn.Commit()

	if m.logManager != nil {
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("Commit: Flushing WAL for TxnID %d", txnID))
		}
		rec := log.WALRecord{TxnID: meta.txn.GetID(), Type: log.COMMIT}
		lsn, _ := m.logManager.AppendLogRecord(rec)
		m.logManager.Flush(lsn)
	}

	return nil
}

func (m *MVCCManager) detectWriteConflict(txnID txn_unit.TransactionID, meta *txnMeta) txn_unit.TransactionID {
	if len(meta.writtenUserKeys) == 0 {
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("detectWriteConflict: TxnID %d has no written keys, skipping detection", txnID))
		}
		return 0
	}

	snapshot := meta.txn.GetSnapshot() // returns all active transactions at the start of this transaction
	activeAtStart := make(map[int64]struct{})
	for id, active := range snapshot {
		if !active {
			continue
		}
		activeAtStart[id] = struct{}{}
	}

	for otherID, otherMeta := range m.transactions {
		if txn_unit.TransactionID(otherID) == txnID {
			continue
		}

		if otherMeta.status != statusCommitted {
			continue
		}

		_, wasActive := activeAtStart[int64(otherID)]
		isLaterTxn := int64(otherID) > int64(txnID)

		if wasActive || isLaterTxn {
			if m.tracer != nil {
				m.tracer.Add(fmt.Sprintf("detectWriteConflict: Comparing against committed TxnID %d (wasActiveAtStart: %v, isLaterTxn: %v)", otherID, wasActive, isLaterTxn))
			}
			for key := range meta.writtenUserKeys {
				if _, exists := otherMeta.writtenUserKeys[key]; exists {
					if m.tracer != nil {
						m.tracer.Add(fmt.Sprintf("detectWriteConflict: Conflict! Both txns modified key '%s'", key))
					}
					return otherID
				}
			}
		}
	}
	return 0
}

func (m *MVCCManager) Abort(txnID txn_unit.TransactionID) error {
	m.mu.Lock()
	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Abort: Attempting to abort TxnID %d", txnID))
	}

	meta, ok := m.transactions[txnID]
	if !ok {
		m.mu.Unlock()
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("Abort: Failed - TxnID %d not found", txnID))
		}
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if meta.status != statusActive {
		m.mu.Unlock()
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("Abort: Failed - TxnID %d is already %s", txnID, meta.status))
		}
		return fmt.Errorf("transaction %d is already %s", txnID, meta.status)
	}

	meta.status = statusAborted
	m.updateMinActive()
	m.mu.Unlock()

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Abort: Triggering physical rollback for TxnID %d", txnID))
	}
	m.physicalRollback(meta)
	return nil
}

func (m *MVCCManager) physicalRollback(meta *txnMeta) {
	if m.logManager != nil {
		m.logManager.CleanupTransactionOperations(meta.txn.GetID())
	}
	m.rollbackManager.RollbackTransaction(meta.txn, nil, nil)
}

// Read now forwards strictly to Scan to prevent duplicate bound encoding.
func (m *MVCCManager) Read(txnID txn_unit.TransactionID, tableName, indexName string, key []byte) ([]byte, error) {
	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Read: TxnID %d reading key '%s' from %s.%s", txnID, key, tableName, indexName))
	}

	opts := storage.ScanOptions{
		LowerBound: key,
		UpperBound: key,
		Inclusive:  true,
	}

	cursor, err := m.Scan(txnID, tableName, indexName, opts)
	if err != nil {
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("Read: Scan setup failed for TxnID %d: %v", txnID, err))
		}
		return nil, err
	}
	defer cursor.Close()

	if cursor.Next() {
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("Read: Decision - Key '%s' found for TxnID %d", key, txnID))
		}
		return cursor.Entry().Value, nil
	}

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Read: Decision - Key '%s' not found for TxnID %d", key, txnID))
	}
	return nil, fmt.Errorf("key not found")
}

func (m *MVCCManager) Write(txnID txn_unit.TransactionID, tableName, indexName string, key []byte, value []byte) error {
	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Write: TxnID %d attempting to write key '%s' to %s.%s", txnID, key, tableName, indexName))
	}

	m.mu.RLock()
	meta, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok || meta.status != statusActive {
		return fmt.Errorf("transaction %d is not active", txnID)
	}

	internalKey := m.encodeKey(key, uint64(txnID))
	payload := append([]byte{OpInsert}, value...)

	if m.logManager != nil {
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("Write: TxnID %d logging to WAL", txnID))
		}
		rec := log.WALRecord{TxnID: meta.txn.GetID(), Type: log.UPDATE, After: payload}
		if _, err := m.logManager.AppendLogRecord(rec); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	if err := m.tableManager.Write(schema.WriteOperation{
		TableName: tableName,
		Key:       internalKey,
		Value:     payload,
	}); err != nil {
		return fmt.Errorf("table manager write failed: %w", err)
	}

	m.mu.Lock()
	meta.writtenUserKeys[string(key)] = struct{}{}
	m.mu.Unlock()

	meta.txn.RecordRecoveryOperation(tableName, log.PUT, internalKey, payload)
	if m.logManager != nil {
		m.logManager.AddTransactionOperation(meta.txn.GetID(), tableName, log.PUT, internalKey, payload)
		if il, ok := m.logManager.(log.ReplicationIntentLogger); ok {
			il.LogReplicationIntent(meta.txn.GetID(), tableName, log.PUT, internalKey, payload)
		}
	}

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Write: Decision - TxnID %d successfully wrote key '%s'", txnID, key))
	}
	return nil
}

func (m *MVCCManager) Delete(txnID txn_unit.TransactionID, tableName, indexName string, key []byte) error {
	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Delete: TxnID %d attempting to delete key '%s' from %s.%s", txnID, key, tableName, indexName))
	}

	m.mu.RLock()
	meta, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok || meta.status != statusActive {
		return fmt.Errorf("transaction %d is not active", txnID)
	}

	internalKey := m.encodeKey(key, uint64(txnID))

	var beforeImage []byte
	if v, err := m.Read(txnID, tableName, indexName, key); err == nil {
		beforeImage = v
	}
	if beforeImage == nil {
		return fmt.Errorf("cannot delete key '%s': not found", string(key))
	}
	// tableManager.Write() calls ExtractIndexValues on the payload and requires
	// a schema-valid byte sequence. Prepending OpDelete to the old row bytes
	// satisfies the schema parser while keeping entry.Value[0] == OpDelete so
	// the MVCC cursor still recognises this version as a tombstone.
	payload := append([]byte{OpDelete}, beforeImage...)

	if m.logManager != nil {
		rec := log.WALRecord{TxnID: meta.txn.GetID(), Type: log.UPDATE, Before: beforeImage, After: payload}
		if _, err := m.logManager.AppendLogRecord(rec); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	if err := m.tableManager.Write(schema.WriteOperation{
		TableName: tableName,
		Key:       internalKey,
		Value:     payload,
	}); err != nil {
		return fmt.Errorf("table manager delete write failed: %w", err)
	}

	m.mu.Lock()
	meta.writtenUserKeys[string(key)] = struct{}{}
	m.mu.Unlock()

	meta.txn.RecordRecoveryOperation(tableName, log.DELETE, internalKey, nil)
	if m.logManager != nil {
		m.logManager.AddTransactionOperation(meta.txn.GetID(), tableName, log.DELETE, internalKey, nil)
		if il, ok := m.logManager.(log.ReplicationIntentLogger); ok {
			il.LogReplicationIntent(meta.txn.GetID(), tableName, log.DELETE, internalKey, nil)
		}
	}

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Delete: Decision - TxnID %d successfully recorded delete tombstone for key '%s'", txnID, key))
	}
	return nil
}

// Scan inflates the boundaries so the underlying physical storage engine
// (LSM) can find the keys with their version suffixes attached.
func (m *MVCCManager) Scan(txnID txn_unit.TransactionID, tableName, indexName string, opts storage.ScanOptions) (storage.ICursor, error) {
	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Scan: Setup beginning for TxnID %d on %s.%s", txnID, tableName, indexName))
	}

	m.mu.RLock()
	meta, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	// 1. Convert logical bounds to physical bounds
	physicalOpts := opts
	if len(opts.LowerBound) > 0 {
		// Minimum possible version of LowerBound
		physicalOpts.LowerBound = m.encodeKey(opts.LowerBound, math.MaxUint64)
	}
	if len(opts.UpperBound) > 0 {
		// Maximum possible version of UpperBound
		physicalOpts.UpperBound = m.encodeKey(opts.UpperBound, 0)
	}

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Scan: Converted bounds -> Lower: %x, Upper: %x", physicalOpts.LowerBound, physicalOpts.UpperBound))
	}

	// 2. Clear Limit and Offset from physical query.
	// We MUST apply limits/offsets AFTER visibility filtering in MVCCCursor!
	physicalOpts.Limit = 0
	physicalOpts.Offset = 0

	raw, err := m.tableManager.Scan(tableName, indexName, physicalOpts)
	if err != nil {
		return nil, err
	}

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("Scan: Creating MVCCCursor and injecting tracer for TxnID %d", txnID))
	}

	return &MVCCCursor{
		raw:      raw,
		manager:  m,
		opts:     opts, // Keep original limits and bounds
		txn:      meta.txn,
		seenKeys: make(map[string]bool),
		tracer:   m.tracer, // INJECTING TRACER HERE
	}, nil
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

func (m *MVCCManager) isVisible(t *txn_unit.Transaction, xmin txn_unit.TransactionID) bool {
	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("isVisible: Checking if TxnID %d can see row written by TxnID %d", t.GetID(), xmin))
	}

	if xmin == t.GetID() {
		if m.tracer != nil {
			m.tracer.Add("isVisible: Decision - True (Transaction observing its own write)")
		}
		return true
	}

	m.mu.RLock()
	xminMeta, exists := m.transactions[xmin]
	m.mu.RUnlock()

	if !exists {
		if m.tracer != nil {
			m.tracer.Add("isVisible: Decision - True (Writer transaction is GC'd, meaning it's old and committed)")
		}
		return true
	}

	committed := xminMeta.status == statusCommitted

	switch t.GetIsolationLevel() {
	case txn_unit.READ_COMMITTED:
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("isVisible: Decision - READ_COMMITTED rules -> Returning %v", committed))
		}
		return committed

	case txn_unit.REPEATABLE_READ, txn_unit.SERIALIZABLE:
		if !committed {
			if m.tracer != nil {
				m.tracer.Add("isVisible: Decision - False (REPEATABLE_READ/SERIALIZABLE: writer not committed)")
			}
			return false
		}
		visible := t.IsVisibleInSnapshot(int64(xmin))
		if m.tracer != nil {
			m.tracer.Add(fmt.Sprintf("isVisible: Decision - REPEATABLE_READ/SERIALIZABLE snapshot check -> %v", visible))
		}
		return visible

	default:
		return committed
	}
}

// ---------------------------------------------------------------------------
// GC / version pruning
// ---------------------------------------------------------------------------

func (m *MVCCManager) GC() {
	m.mu.Lock()
	lowWater := m.computeMinActive()

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("GC: Started cleanup. Low water mark TxnID calculated as %d", lowWater))
	}

	prunedTxnCount := 0
	for id, meta := range m.transactions {
		if meta.status != statusActive && int64(id) < lowWater {
			delete(m.transactions, id)
			prunedTxnCount++
		}
	}
	m.mu.Unlock()

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("GC: Removed %d inactive transactions below low water mark", prunedTxnCount))
	}

	pruneKeys, err := m.collectPruneableKeys(lowWater)
	if err != nil || len(pruneKeys) == 0 {
		return
	}

	if m.tracer != nil {
		m.tracer.Add(fmt.Sprintf("GC: Identified %d obsolete row versions to prune from physical storage", len(pruneKeys)))
	}

	for _, k := range pruneKeys {
		_ = m.tableManager.Delete(k)
	}
}

func (m *MVCCManager) collectPruneableKeys(lowWater int64) ([]schema.DeleteOperation, error) {
	opts := storage.ScanOptions{Inclusive: true}
	tables := m.tableManager.GetAllTables()
	var toDelete []schema.DeleteOperation

	for _, tableName := range tables {
		tableSchema, err := m.tableManager.GetTableSchema(tableName)
		if err != nil {
			continue
		}
		for _, index := range tableSchema.GetAllIndexes() {
			indexName := index.Name
			raw, err := m.tableManager.Scan(tableName, indexName, opts)
			if err != nil {
				continue
			}

			type versionRecord struct {
				internalKey schema.DeleteOperation
				txnID       int64
				status      txnStatus
			}
			groupByUserKey := make(map[string][]versionRecord)

			for raw.Next() {
				entry := raw.Entry()
				userKey, writerID, ok := m.decodeKey(entry.Key)
				if !ok {
					continue
				}

				m.mu.RLock()
				xminMeta, exists := m.transactions[txn_unit.TransactionID(writerID)]
				m.mu.RUnlock()

				var st txnStatus
				if !exists {
					st = statusCommitted
				} else {
					st = xminMeta.status
				}

				uk := string(userKey)
				groupByUserKey[uk] = append(groupByUserKey[uk], versionRecord{
					internalKey: schema.DeleteOperation{TableName: tableName, Key: entry.Key},
					txnID:       int64(writerID),
					status:      st,
				})
			}
			raw.Close()

			for _, versions := range groupByUserKey {
				youngestCommitted := -1
				for i, v := range versions {
					if v.status == statusCommitted {
						youngestCommitted = i
						break
					}
				}
				if youngestCommitted < 0 {
					continue
				}

				for i := youngestCommitted + 1; i < len(versions); i++ {
					if versions[i].txnID < lowWater {
						toDelete = append(toDelete, versions[i].internalKey)
					}
				}

				for i := 0; i < youngestCommitted; i++ {
					v := versions[i]
					if v.status == statusAborted && v.txnID < lowWater {
						toDelete = append(toDelete, v.internalKey)
					}
				}
			}
		}
	}
	return toDelete, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (m *MVCCManager) updateMinActive() {
	m.minActiveTxnID.Store(m.computeMinActive())
}

func (m *MVCCManager) computeMinActive() int64 {
	min := int64(math.MaxInt64)
	for id, meta := range m.transactions {
		if meta.status == statusActive && int64(id) < min {
			min = int64(id)
		}
	}
	return min
}

func (m *MVCCManager) encodeKey(userKey []byte, txnID uint64) []byte {
	buf := make([]byte, len(userKey)+1+8)
	copy(buf, userKey)
	buf[len(userKey)] = 0x00 // separator
	flipped := ^txnID        // bit-flip → descending order
	binary.BigEndian.PutUint64(buf[len(userKey)+1:], flipped)
	return buf
}

func (m *MVCCManager) decodeKey(fullKey []byte) ([]byte, uint64, bool) {
	if len(fullKey) < 9 {
		return nil, 0, false
	}

	userKey := fullKey[:len(fullKey)-9] // remove separator + txn
	inverted := binary.BigEndian.Uint64(fullKey[len(fullKey)-8:])
	txnID := ^inverted

	return userKey, txnID, true
}
