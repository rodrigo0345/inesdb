package lsm

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/buffer"
	log "github.com/rodrigo0345/omag/src/engine/wal"
	applog "github.com/rodrigo0345/omag/pkg/pkglog"
)

const SSTableMaxSize = 65536
const LSMDataDir = "./lsm_data"
const MetadataFile = "metadata.json"

type LSMTreeBackend struct {
	logManager    log.ILogManager
	bufferManager buffer.IBufferPoolManager
	dataDir       string

	memtable *MemTable
	levels   [][]*SSTable

	compactionPolicy CompactionPolicy

	ssTableIdCounter atomic.Uint64
	totalItems       atomic.Int64

	compactionQueue []int
	queueMutex      sync.Mutex

	memtableLock sync.RWMutex
	levelsLock   sync.RWMutex
}

var _ storage.IStorageEngine = (*LSMTreeBackend)(nil) // compile-time check for interface implementation

// SSTableMetadata represents persisted SSTable information
type SSTableMetadata struct {
	ID    uint64 `json:"id"`
	Level int    `json:"level"`
}

// LSMMetadata represents the complete LSM state on disk
type LSMMetadata struct {
	SSTableID uint64            `json:"sstable_id"`
	SSTables  []SSTableMetadata `json:"sstables"`
}

// PersistedSSTable represents an SSTable ready for disk storage
type PersistedSSTable struct {
	ID         uint64            `json:"id"`
	Data       map[string][]byte `json:"data"`
	Tombstones map[string]bool   `json:"tombstones"`
}

func NewLSMTreeBackend(log log.ILogManager, buf buffer.IBufferPoolManager) *LSMTreeBackend {
	return NewLSMTreeBackendWithDataDir(log, buf, LSMDataDir)
}

func NewLSMTreeBackendWithDataDir(log log.ILogManager, buf buffer.IBufferPoolManager, dataDir string) *LSMTreeBackend {
	if dataDir == "" {
		dataDir = LSMDataDir
	}
	lsm := &LSMTreeBackend{
		logManager:       log,
		bufferManager:    buf,
		dataDir:          dataDir,
		memtable:         NewMemTable(),
		levels:           make([][]*SSTable, 0),
		compactionPolicy: NewGarneringCompactionPolicy(10.0, 0.5, 4),
	}
	lsm.ssTableIdCounter.Store(1)

	if err := lsm.LoadSSTables(lsm.dataDir); err != nil {
		applog.Warn("[LSMInit] failed to load persisted SSTables: %v", err)
	}

	return lsm
}

// Close gracefully shuts down the LSM backend, flushing any pending data
func (l *LSMTreeBackend) Close() error {
	applog.Info("[LSMClose] closing LSM backend")

	l.memtableLock.Lock()
	hasData := len(l.memtable.data) > 0
	l.memtableLock.Unlock()

	if hasData {
		applog.Info("[LSMClose] flushing unflushed memtable before shutdown")
		l.flush()
	}

	if err := l.updateMetadata(); err != nil {
		applog.Warn("[LSMClose] failed to update metadata on shutdown: %v", err)
		return err
	}

	applog.Info("[LSMClose] LSM backend closed successfully")
	return nil
}

func (l *LSMTreeBackend) Get(key []byte) ([]byte, error) {
	l.memtableLock.RLock()
	if val, ok := l.memtable.Get(key); ok {
		if l.memtable.IsTombstoned(key) {
			l.memtableLock.RUnlock()
			return nil, fmt.Errorf("Key %q has been deleted", string(key))
		}
		l.memtableLock.RUnlock()
		return val, nil
	}
	l.memtableLock.RUnlock()

	l.levelsLock.RLock()
	defer l.levelsLock.RUnlock()

	for lvl := 0; lvl < len(l.levels); lvl++ {
		for i := len(l.levels[lvl]) - 1; i >= 0; i-- {
			sstable := l.levels[lvl][i]
			val, err := sstable.Get(key)
			if err == ErrKeyTombstoned {
				return nil, fmt.Errorf("Key %q has been deleted", string(key))
			}
			if err == ErrKeyNotFound {
				continue
			}
			if err == nil {
				return val, nil
			}
		}
	}

	return nil, fmt.Errorf("Key %q not found in LSM tree", string(key))
}

func (l *LSMTreeBackend) Put(key []byte, value []byte) error {
	l.memtableLock.Lock()

	keyStr := string(key)
	_, exists := l.memtable.data[keyStr]
	isNew := !exists && !l.memtable.tombstones[keyStr]

	l.memtable.data[keyStr] = value
	if isNew {
		l.totalItems.Add(1)
	}

	shouldFlush := len(l.memtable.data) >= SSTableMaxSize
	l.memtableLock.Unlock()

	if shouldFlush {
		l.flush()
	}
	return nil
}

func (l *LSMTreeBackend) flush() {
	l.memtableLock.Lock()
	if len(l.memtable.data) == 0 {
		l.memtableLock.Unlock()
		return
	}
	sstable := FlushMemTable(l.ssTableIdCounter.Load(), l.memtable, 0.01)
	sstableID := l.ssTableIdCounter.Load()
	l.ssTableIdCounter.Add(1)

	l.levelsLock.Lock()
	if len(l.levels) == 0 {
		l.levels = append(l.levels, make([]*SSTable, 0))
	}
	l.levels[0] = append(l.levels[0], sstable)
	levelNum := 0
	l.levelsLock.Unlock()

	l.memtable = NewMemTable()
	l.memtableLock.Unlock()

	if err := l.persistSSTable(sstable, sstableID, levelNum); err != nil {
		applog.Warn("[LSMFlush] failed to persist SSTable %d: %v", sstableID, err)
	}

	if err := l.updateMetadata(); err != nil {
		applog.Warn("[LSMFlush] failed to update metadata: %v", err)
	}

	l.compactIfNeeded()
}

func (l *LSMTreeBackend) compactIfNeeded() {
	maxLevelTarget := l.compactionPolicy.GetNumLevels(int(l.totalItems.Load()))

	l.levelsLock.RLock()
	levelsLen := len(l.levels)
	l.levelsLock.RUnlock()

	for i := 0; i < levelsLen-1; i++ {
		limit := l.compactionPolicy.GetCapacityLimit(i, maxLevelTarget)

		l.levelsLock.RLock()
		if i < len(l.levels) && len(l.levels[i]) > limit {
			l.levelsLock.RUnlock()
			l.queueLevelForCompaction(i)
		} else {
			l.levelsLock.RUnlock()
		}
	}

	l.levelsLock.Lock()
	if len(l.levels) > 0 {
		lastLevel := len(l.levels) - 1
		limit := l.compactionPolicy.GetCapacityLimit(lastLevel, maxLevelTarget)

		if len(l.levels[lastLevel]) > limit {
			if lastLevel == maxLevelTarget-1 {
				l.levels = append(l.levels, make([]*SSTable, 0))
			} else {
				l.levelsLock.Unlock()
				l.queueLevelForCompaction(lastLevel)
				l.levelsLock.Lock()
			}
		}
	}
	l.levelsLock.Unlock()
	l.processCompactionQueue()
}

func (l *LSMTreeBackend) queueLevelForCompaction(level int) {
	l.queueMutex.Lock()
	defer l.queueMutex.Unlock()

	for _, queuedLevel := range l.compactionQueue {
		if queuedLevel == level {
			return
		}
	}
	l.compactionQueue = append(l.compactionQueue, level)
}

func (l *LSMTreeBackend) processCompactionQueue() {
	for {
		l.queueMutex.Lock()
		if len(l.compactionQueue) == 0 {
			l.queueMutex.Unlock()
			break
		}

		level := l.compactionQueue[0]
		l.compactionQueue = l.compactionQueue[1:]
		l.queueMutex.Unlock()

		l.compactLevel(level)
	}
}

func (l *LSMTreeBackend) compactLevel(level int) {
	maxLevelTarget := l.compactionPolicy.GetNumLevels(int(l.totalItems.Load()))

	l.levelsLock.Lock()
	if level+1 >= len(l.levels) {
		l.levels = append(l.levels, make([]*SSTable, 0))
	}

	allTables := append(l.levels[level+1], l.levels[level]...)
	iters := make([]*sstableIter, 0, len(allTables))
	for priority, ss := range allTables {
		iters = append(iters, newSSTableIter(ss, priority))
	}

	h := &iterHeap{}
	heap.Init(h)
	for _, it := range iters {
		if it.next() {
			heap.Push(h, it)
		}
	}

	l.levelsLock.Unlock()

	var mergedSSTables []*SSTable
	var mergedEntries []Entry
	var mergedTombstones []string
	var lastKey string

	for h.Len() > 0 {
		it := heap.Pop(h).(*sstableIter)
		k, v := it.key(), it.val()

		if k != lastKey {
			if it.IsTombstoned() {
				if level+1 == maxLevelTarget-1 {
				} else {
					mergedTombstones = append(mergedTombstones, k)
					mergedEntries = append(mergedEntries, Entry{Key: k, Value: nil})
				}
			} else {
				mergedEntries = append(mergedEntries, Entry{Key: k, Value: v})
			}
			lastKey = k

			if len(mergedEntries) >= SSTableMaxSize {
				sstable := l.createSSTableFromSortedEntries(mergedEntries, mergedTombstones)
				if sstable != nil {
					mergedSSTables = append(mergedSSTables, sstable)
				}
				mergedEntries = nil
				mergedTombstones = nil
			}
		}

		if it.next() {
			heap.Push(h, it)
		}
	}

	if len(mergedEntries) > 0 {
		sstable := l.createSSTableFromSortedEntries(mergedEntries, mergedTombstones)
		if sstable != nil {
			mergedSSTables = append(mergedSSTables, sstable)
		}
	}

	l.levelsLock.Lock()
	l.levels[level] = make([]*SSTable, 0)
	l.levels[level+1] = mergedSSTables
	l.levelsLock.Unlock()

	l.levelsLock.RLock()
	shouldCascade := level+1 < len(l.levels)
	l.levelsLock.RUnlock()

	if shouldCascade {
		l.levelsLock.RLock()
		limit := l.compactionPolicy.GetCapacityLimit(level+1, maxLevelTarget)
		cascadeNeeded := len(l.levels[level+1]) > limit
		l.levelsLock.RUnlock()

		if cascadeNeeded {
			l.queueLevelForCompaction(level + 1)
		}
	}
}

type Entry struct {
	Key   string
	Value []byte
}

func (l *LSMTreeBackend) createSSTableFromSortedEntries(entries []Entry, tombstones []string) *SSTable {
	if len(entries) == 0 {
		return nil
	}

	data := make(map[string][]byte, len(entries))
	tombstoneMap := make(map[string]bool, len(tombstones))

	for _, entry := range entries {
		data[entry.Key] = entry.Value
	}
	for _, ts := range tombstones {
		tombstoneMap[ts] = true
	}

	sstable := FlushMemTableFromMap(l.ssTableIdCounter.Load(), data, tombstoneMap, 0.01)
	l.ssTableIdCounter.Add(1)

	return sstable
}

func (l *LSMTreeBackend) Delete(key []byte) error {
	l.memtableLock.Lock()

	keyStr := string(key)
	_, exists := l.memtable.data[keyStr]
	l.memtable.tombstones[keyStr] = true
	if !exists {
		l.memtable.data[keyStr] = nil
	}

	if exists {
		l.totalItems.Add(-1)
	}

	shouldFlush := len(l.memtable.data) >= SSTableMaxSize
	l.memtableLock.Unlock()

	if shouldFlush {
		l.flush()
	}
	return nil
}

func (l *LSMTreeBackend) Scan(opts storage.ScanOptions) (storage.ICursor, error) {
	h := &iterHeap{reverse: opts.Reverse}

	// 1. Memtable Priority: Always highest (use a large constant)
	l.memtableLock.RLock()
	memIter := newSSTableIter(&SSTable{
		data:       l.memtable.data,
		tombstones: l.memtable.tombstones,
	}, 999)
	l.memtableLock.RUnlock()

	if memIter.seek(opts.LowerBound, opts.Reverse) {
		heap.Push(h, memIter)
	}

	// 2. Level Priorities: Decreasing with age
	l.levelsLock.RLock()
	var allTables []*SSTable
	for _, level := range l.levels {
		allTables = append(allTables, level...)
	}
	l.levelsLock.RUnlock()

	for i, ss := range allTables {
		// Ensure priority is lower than Memtable (999) and unique per table
		priority := 500 - i
		it := newSSTableIter(ss, priority)

		if it.seek(opts.LowerBound, opts.Reverse) {
			heap.Push(h, it)
		}
	}

	return &LSMCursor{
		h:        h,
		opts:     opts,
		seenKeys: make(map[string]bool),
	}, nil
}

func (l *LSMTreeBackend) ReplayFromWAL(recoveryState *log.RecoveryState) error {
	l.memtableLock.Lock()
	defer l.memtableLock.Unlock()

	if recoveryState == nil {
		return fmt.Errorf("recovery state is nil")
	}

	// For LSM trees, WAL recovery is handled at the buffer pool / page level.
	// The LSM tree's persistent data is already on disk in the form of SSTables.
	//
	// We do NOT reconstruct the memtable from WAL records because:
	// 1. WAL records contain page-level changes (byte offsets), not key-value operations
	// 2. The key information is lost at the page level
	// 3. Flushed memtables are already persisted as SSTables with full key-value data
	// 4. The buffer pool recovery ensures page integrity
	//
	// Recovery process for LSM:
	// - Page-level recovery: Handled by WAL + buffer pool (already done)
	// - Key-value recovery: Already persisted in SSTables on disk
	// - Uncommitted data: Lost on crash (acceptable for LSM design)

	committedCount := len(recoveryState.CommittedTxns)
	abortedCount := len(recoveryState.AbortedTxns)
	tobeRedoneCount := len(recoveryState.TobeRedone)

	applog.Debug("[LSMRecovery] LSM recovery - WAL handled at buffer pool level")
	applog.Debug("[LSMRecovery] Recovery state: %d committed txns, %d aborted txns, %d tobeRedone txns",
		committedCount, abortedCount, tobeRedoneCount)

	// IMPORTANT: LSM data recovery strategy:
	// - Page-level data: Recovered via WAL + buffer pool (ensures page integrity)
	// - Key-value data: Must be persisted separately (SSTables on disk)
	// - Current limitation: SSTables are kept in-memory only, not persisted to disk
	//
	// TODO: Implement SSTable persistence to disk for full recovery
	// Until then, data written before crash will be lost after recovery
	// (This is acceptable for in-development LSM but not for production)

	applog.Warn("[LSMRecovery] LSM SSTables are not yet persisted to disk")
	applog.Warn("[LSMRecovery] any data flushed to SSTables before crash cannot be recovered")

	// Start with a fresh memtable for new writes
	l.memtable = NewMemTable()

	applog.Info("[LSMRecovery] LSM recovery complete - ready for new writes")
	return nil
}

type replayStats struct {
	recordsProcessed  int64
	recordsApplied    int64
	recordsSkipped    int64
	txnsReplayed      int64
	errorsEncountered int64
}

func (l *LSMTreeBackend) replayAllTransactions(tobeRedone map[uint64][]uint64, lsnToRecord map[uint64]*log.WALRecord, recoveryState *log.RecoveryState, stats *replayStats) []error {
	var errors []error

	for txnID, lsns := range tobeRedone {
		// Skip if explicitly aborted (these need undo, not redo)
		if recoveryState.AbortedTxns[txnID] {
			continue
		}

		if err := l.replayTransaction(txnID, lsns, lsnToRecord, stats); err != nil {
			errors = append(errors, fmt.Errorf("txn %d replay failed: %w", txnID, err))
		} else {
			stats.txnsReplayed++
		}
	}

	return errors
}

func (l *LSMTreeBackend) replayTransaction(txnID uint64, lsns []uint64, lsnToRecord map[uint64]*log.WALRecord, stats *replayStats) error {
	if len(lsns) == 0 {
		return nil
	}

	for _, lsn := range lsns {
		stats.recordsProcessed++

		walRecord, ok := lsnToRecord[lsn]
		if !ok {
			stats.recordsSkipped++
			return fmt.Errorf("LSN %d referenced in TobeRedone not found in WAL records", lsn)
		}

		if walRecord.TxnID != txnID {
			stats.recordsSkipped++
			return fmt.Errorf("LSN %d txnID mismatch: expected %d, got %d", lsn, txnID, walRecord.TxnID)
		}

		if walRecord.Type != log.UPDATE {
			stats.recordsSkipped++
			continue
		}

		// Validate After image
		if len(walRecord.After) == 0 {
			stats.recordsSkipped++
			applog.Warn("[LSMRecovery] empty after image for LSN %d, txn %d", lsn, txnID)
			continue
		}

		// Apply the After image to memtable
		// The After image contains the value bytes; we use a synthetic key based on LSN for uniqueness
		// In production, the key should be extracted from the page data or operation metadata
		key := fmt.Sprintf("lsn_%d_txn_%d", lsn, txnID)
		l.memtable.data[key] = walRecord.After
		stats.recordsApplied++
	}

	return nil
}

func (l *LSMTreeBackend) validateRecoveryState(recoveryState *log.RecoveryState) error {
	if len(recoveryState.CommittedTxns) == 0 && len(recoveryState.TobeRedone) == 0 && len(recoveryState.UndoList) == 0 {
		// Empty recovery state is valid
		return nil
	}

	// Validate no overlap between committed and aborted
	for txnID := range recoveryState.CommittedTxns {
		if recoveryState.AbortedTxns[txnID] {
			return fmt.Errorf("transaction %d appears in both committed and aborted sets", txnID)
		}
	}

	// Validate UndoList integrity
	seenLSNs := make(map[uint64]bool)
	for _, rec := range recoveryState.UndoList {
		if rec == nil {
			return fmt.Errorf("nil record in UndoList")
		}
		if seenLSNs[rec.LSN] {
			return fmt.Errorf("duplicate LSN %d in UndoList", rec.LSN)
		}
		seenLSNs[rec.LSN] = true

		if rec.LSN == 0 {
			return fmt.Errorf("invalid LSN 0 in UndoList record")
		}
	}

	return nil
}

func (l *LSMTreeBackend) verifyMemtableIntegrity() error {
	if l.memtable == nil {
		return fmt.Errorf("memtable is nil after replay")
	}

	if len(l.memtable.data) == 0 {
		return nil
	}

	// Verify data/tombstone consistency
	for key, value := range l.memtable.data {
		if l.memtable.tombstones[key] && len(value) > 0 {
			return fmt.Errorf("tombstone key %s has non-empty value", key)
		}
	}

	return nil
}

// persistSSTable saves an SSTable to disk in binary format
func (l *LSMTreeBackend) persistSSTable(sstable *SSTable, id uint64, level int) error {
	levelDir := filepath.Join(l.dataDir, fmt.Sprintf("level_%d", level))
	if err := os.MkdirAll(levelDir, 0755); err != nil {
		return fmt.Errorf("failed to create level directory: %w", err)
	}

	// Serialize SSTable to binary
	buf := new(bytes.Buffer)

	// Write SSTable ID (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, id); err != nil {
		return fmt.Errorf("failed to write SSTable ID: %w", err)
	}

	// Write number of entries (8 bytes)
	numEntries := uint64(len(sstable.data))
	if err := binary.Write(buf, binary.LittleEndian, numEntries); err != nil {
		return fmt.Errorf("failed to write entry count: %w", err)
	}

	if len(sstable.sortedRows) != len(sstable.data) {
		sstable.rebuildSortedRows()
	}

	// opt: walk contiguous sorted rows for better write-side locality.
	for i := range sstable.sortedRows {
		row := sstable.sortedRows[i]
		keyBytes := []byte(row.key)
		// Write key length (4 bytes)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
			return fmt.Errorf("failed to write key length: %w", err)
		}
		// Write key
		if _, err := buf.Write(keyBytes); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}
		// Write value length (4 bytes)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(row.value))); err != nil {
			return fmt.Errorf("failed to write value length: %w", err)
		}
		// Write value
		if _, err := buf.Write(row.value); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}
	}

	// Write number of tombstones (8 bytes)
	numTombstones := uint64(len(sstable.tombstones))
	if err := binary.Write(buf, binary.LittleEndian, numTombstones); err != nil {
		return fmt.Errorf("failed to write tombstone count: %w", err)
	}

	// Write each tombstone key
	for key := range sstable.tombstones {
		keyBytes := []byte(key)
		// Write key length (4 bytes)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
			return fmt.Errorf("failed to write tombstone key length: %w", err)
		}
		// Write key
		if _, err := buf.Write(keyBytes); err != nil {
			return fmt.Errorf("failed to write tombstone key: %w", err)
		}
	}

	// Write to file
	filePath := filepath.Join(levelDir, fmt.Sprintf("sstable_%d.bin", id))
	if err := ioutil.WriteFile(filePath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write SSTable file: %w", err)
	}

	applog.Debug("[LSMPersist] SSTable %d persisted to %s (%d bytes)", id, filePath, buf.Len())
	return nil
}

// updateMetadata writes LSM metadata to disk
func (l *LSMTreeBackend) updateMetadata() error {
	// Create directory
	if err := os.MkdirAll(l.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create lsm data directory: %w", err)
	}

	// Collect all SSTables
	var sstables []SSTableMetadata
	l.levelsLock.RLock()
	for levelIdx, level := range l.levels {
		for _, sstable := range level {
			sstables = append(sstables, SSTableMetadata{
				ID:    sstable.id,
				Level: levelIdx,
			})
		}
	}
	l.levelsLock.RUnlock()

	// Create metadata
	metadata := LSMMetadata{
		SSTableID: l.ssTableIdCounter.Load(),
		SSTables:  sstables,
	}

	// Write metadata file
	filePath := filepath.Join(l.dataDir, MetadataFile)
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	applog.Debug("[LSMMetadata] metadata updated: %d SSTables, next ID: %d", len(sstables), metadata.SSTableID)
	return nil
}

// LoadSSTables loads all persisted SSTables from disk
func (l *LSMTreeBackend) LoadSSTables(dir string) error {
	metadataPath := filepath.Join(dir, MetadataFile)

	// Check if metadata file exists
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		applog.Debug("[LSMLoad] no persisted SSTables found (metadata file missing)")
		return nil // No persisted data is OK
	}

	// Read metadata file
	metadataData, err := ioutil.ReadFile(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	var metadata LSMMetadata
	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	applog.Debug("[LSMLoad] found metadata: %d SSTables, next ID: %d", len(metadata.SSTables), metadata.SSTableID)

	// Update SSTable ID counter
	l.ssTableIdCounter.Store(metadata.SSTableID)

	// Prepare levels
	if len(metadata.SSTables) == 0 {
		return nil
	}

	// Find max level
	maxLevel := 0
	for _, st := range metadata.SSTables {
		if st.Level > maxLevel {
			maxLevel = st.Level
		}
	}

	// Initialize levels array
	l.levelsLock.Lock()
	l.levels = make([][]*SSTable, maxLevel+1)
	for i := range l.levels {
		l.levels[i] = make([]*SSTable, 0)
	}
	l.levelsLock.Unlock()

	// Load each SSTable
	loadedCount := 0
	for _, stMeta := range metadata.SSTables {
		if err := l.loadSSTable(dir, stMeta.ID, stMeta.Level); err != nil {
			applog.Warn("[LSMLoad] failed to load SSTable %d from level %d: %v", stMeta.ID, stMeta.Level, err)
			continue
		}
		loadedCount++
	}

	applog.Info("[LSMLoad] successfully loaded %d SSTables", loadedCount)

	// Recalculate total items
	l.recalculateTotalItems()

	return nil
}

// loadSSTable loads a single SSTable from disk in binary format
func (l *LSMTreeBackend) loadSSTable(dir string, id uint64, level int) error {
	levelDir := filepath.Join(dir, fmt.Sprintf("level_%d", level))
	filePath := filepath.Join(levelDir, fmt.Sprintf("sstable_%d.bin", id))

	// Read file
	fileData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read SSTable file: %w", err)
	}

	if len(fileData) < 16 {
		return fmt.Errorf("failed to read SSTable header: file too small")
	}

	offset := 0
	readU32 := func() (uint32, error) {
		if offset+4 > len(fileData) {
			return 0, fmt.Errorf("unexpected EOF while reading uint32")
		}
		v := binary.LittleEndian.Uint32(fileData[offset : offset+4])
		offset += 4
		return v, nil
	}
	readU64 := func() (uint64, error) {
		if offset+8 > len(fileData) {
			return 0, fmt.Errorf("unexpected EOF while reading uint64")
		}
		v := binary.LittleEndian.Uint64(fileData[offset : offset+8])
		offset += 8
		return v, nil
	}

	readID, err := readU64()
	if err != nil {
		return fmt.Errorf("failed to read SSTable ID: %w", err)
	}

	if readID != id {
		return fmt.Errorf("SSTable ID mismatch: expected %d, got %d", id, readID)
	}

	// Read number of entries (8 bytes)
	numEntries, err := readU64()
	if err != nil {
		return fmt.Errorf("failed to read entry count: %w", err)
	}

	// opt: decode rows in one forward pass over bytes for cache-friendly reads.
	data := make(map[string][]byte, int(numEntries))
	for i := uint64(0); i < numEntries; i++ {
		keyLen, readErr := readU32()
		if readErr != nil {
			return fmt.Errorf("failed to read key length: %w", readErr)
		}
		if offset+int(keyLen) > len(fileData) {
			return fmt.Errorf("failed to read key: unexpected EOF")
		}
		key := string(fileData[offset : offset+int(keyLen)])
		offset += int(keyLen)

		valueLen, readErr := readU32()
		if readErr != nil {
			return fmt.Errorf("failed to read value length: %w", readErr)
		}
		if offset+int(valueLen) > len(fileData) {
			return fmt.Errorf("failed to read value: unexpected EOF")
		}
		valueBytes := append([]byte(nil), fileData[offset:offset+int(valueLen)]...)
		offset += int(valueLen)
		data[key] = valueBytes
	}

	numTombstones, err := readU64()
	if err != nil {
		return fmt.Errorf("failed to read tombstone count: %w", err)
	}

	tombstones := make(map[string]bool, int(numTombstones))
	for i := uint64(0); i < numTombstones; i++ {
		keyLen, readErr := readU32()
		if readErr != nil {
			return fmt.Errorf("failed to read tombstone key length: %w", readErr)
		}
		if offset+int(keyLen) > len(fileData) {
			return fmt.Errorf("failed to read tombstone key: unexpected EOF")
		}
		tombstones[string(fileData[offset:offset+int(keyLen)])] = true
		offset += int(keyLen)
	}

	// Create SSTable with rebuilt bloom filter
	bf := NewBloomFilter(uint(len(data)), 0.01)
	for key := range data {
		bf.Add([]byte(key))
	}

	sstable := NewSSTable(readID, data, bf)
	sstable.tombstones = tombstones
	sstable.rebuildSortedRows()

	// Add to levels
	l.levelsLock.Lock()
	l.levels[level] = append(l.levels[level], sstable)
	l.levelsLock.Unlock()

	applog.Debug("[LSMLoad] loaded SSTable %d (level %d) with %d entries, %d tombstones", id, level, len(data), len(tombstones))
	return nil
}

// recalculateTotalItems updates the total items count based on loaded SSTables
func (l *LSMTreeBackend) recalculateTotalItems() {
	l.levelsLock.RLock()
	total := int64(len(l.memtable.data))
	for _, level := range l.levels {
		for _, sstable := range level {
			total += int64(len(sstable.data))
		}
	}
	l.levelsLock.RUnlock()
	l.totalItems.Store(total)
}
