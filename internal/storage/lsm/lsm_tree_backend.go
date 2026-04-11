package lsm

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

const SSTableMaxSize = 65536

type LSMTreeBackend struct {
	logManager    log.ILogManager
	bufferManager buffer.IBufferPoolManager

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

func NewLSMTreeBackend(log log.ILogManager, buf buffer.IBufferPoolManager) *LSMTreeBackend {
	lsm := &LSMTreeBackend{
		logManager:       log,
		bufferManager:    buf,
		memtable:         NewMemTable(),
		levels:           make([][]*SSTable, 0),
		compactionPolicy: NewGarneringCompactionPolicy(10.0, 0.5, 4), // T=10, c=0.5, L0_cap=4
	}
	lsm.ssTableIdCounter.Store(1)
	return lsm
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
			if err == nil {
				return val, nil
			}
		}
	}

	return nil, fmt.Errorf("Key %q not found in LSM tree", string(key))
}

func (l *LSMTreeBackend) Put(key []byte, value []byte) error {
	l.memtableLock.Lock()

	keyStr := string(key) // TODO: this might be expensive to have everything on string
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
	l.ssTableIdCounter.Add(1)

	l.levelsLock.Lock()
	if len(l.levels) == 0 {
		l.levels = append(l.levels, make([]*SSTable, 0))
	}
	l.levels[0] = append(l.levels[0], sstable)
	l.levelsLock.Unlock()

	l.memtable = NewMemTable()
	l.memtableLock.Unlock()

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

// Scan returns all key-value pairs in the LSM tree (excluding tombstoned entries)
func (l *LSMTreeBackend) Scan() ([]storage.ScanEntry, error) {
	var results []storage.ScanEntry
	seenKeys := make(map[string]bool)

	// First, collect all iterators from memtable and SSTables
	l.memtableLock.RLock()
	memtableIter := newSSTableIter(&SSTable{
		data:       l.memtable.data,
		tombstones: l.memtable.tombstones,
	}, 1000000) // highest priority (newest)
	l.memtableLock.RUnlock()

	// Collect SSTables from all levels (newest first)
	l.levelsLock.RLock()
	allTables := make([]*SSTable, 0)
	for lvl := 0; lvl < len(l.levels); lvl++ {
		for _, ss := range l.levels[lvl] {
			allTables = append(allTables, ss)
		}
	}
	l.levelsLock.RUnlock()

	// Create iterators for all SSTables
	iters := make([]*sstableIter, 0, len(allTables)+1)
	iters = append(iters, memtableIter)
	for priority, ss := range allTables {
		iters = append(iters, newSSTableIter(ss, len(allTables)-priority))
	}

	// Use heap to merge all iterators
	h := &iterHeap{}
	for _, it := range iters {
		if it.next() {
			heap.Push(h, it)
		}
	}

	// Process entries in sorted key order
	for h.Len() > 0 {
		it := heap.Pop(h).(*sstableIter)
		key := it.key()

		// Skip if we've already seen this key (newer version already processed)
		if !seenKeys[key] {
			seenKeys[key] = true

			// Skip tombstoned entries
			if !it.IsTombstoned() {
				results = append(results, storage.ScanEntry{
					Key:   []byte(key),
					Value: it.val(),
				})
			}
		}

		// Advance iterator
		if it.next() {
			heap.Push(h, it)
		}
	}

	return results, nil
}
