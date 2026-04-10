package lsm

import (
	"fmt"
	"sort"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

const SSTableMaxSize = 100

type LSMTreeBackend struct {
	logManager    log.ILogManager
	bufferManager buffer.IBufferPoolManager

	memtable *MemTable
	levels   [][]*SSTable

	compactionPolicy CompactionPolicy

	ssTableIdCounter uint64
	totalItems       int  // Track total items for level calculation (used for Garnering)
	lastLevelFull    bool // Track if last level is full (for delayed compaction)
}

func NewLSMTreeBackend(log log.ILogManager, buf buffer.IBufferPoolManager) *LSMTreeBackend {
	return &LSMTreeBackend{
		logManager:       log,
		bufferManager:    buf,
		memtable:         NewMemTable(),
		levels:           make([][]*SSTable, 0),
		compactionPolicy: NewGarneringCompactionPolicy(10.0, 0.5, 4), // T=10, c=0.5, L0_cap=4
		ssTableIdCounter: 1,
		totalItems:       0,
		lastLevelFull:    false,
	}
}

func (l *LSMTreeBackend) Get(key []byte) ([]byte, error) {
	// Garnering optimization: Fewer levels due to flattened tree structure
	// means faster Get operations - reduces from O(log N) to O(√log N)
	if val, ok := l.memtable.Get(key); ok {
		return val, nil
	}

	for lvl := 0; lvl < len(l.levels); lvl++ {
		// Newest files first within the level (append-only behavior simulated)
		for i := len(l.levels[lvl]) - 1; i >= 0; i-- {
			sstable := l.levels[lvl][i]
			val, err := sstable.Get(key)
			if err == nil {
				return val, nil
			}
		}
	}

	return nil, fmt.Errorf("Key %q not found in LSM tree", string(key))
}

func (l *LSMTreeBackend) Put(key []byte, value []byte) error {
	l.memtable.Put(key, value)
	l.totalItems++

	// Flush when memtable reaches SSTableMaxSize
	if len(l.memtable.data) >= SSTableMaxSize {
		l.flush()
	}
	return nil
}

func (l *LSMTreeBackend) flush() {
	if len(l.memtable.data) == 0 {
		return
	}
	sstable := FlushMemTable(l.ssTableIdCounter, l.memtable, 0.01)
	l.ssTableIdCounter++

	if len(l.levels) == 0 {
		l.levels = append(l.levels, make([]*SSTable, 0))
	}
	l.levels[0] = append(l.levels[0], sstable)

	// Reset the MemTable
	l.memtable = NewMemTable()

	l.compactIfNeeded()
}

func (l *LSMTreeBackend) compactIfNeeded() {
	// Carnering policy: dynamically determine max level based on total items
	maxLevelTarget := l.compactionPolicy.GetNumLevels(l.totalItems)

	// First pass: compact non-last levels
	for i := 0; i < len(l.levels)-1; i++ {
		limit := l.compactionPolicy.GetCapacityLimit(i, maxLevelTarget)
		if len(l.levels[i]) > limit {
			l.compactLevel(i)
		}
	}

	// Handle the last level with delayed compaction (from Autumn paper section 3.1)
	if len(l.levels) > 0 {
		lastLevel := len(l.levels) - 1
		limit := l.compactionPolicy.GetCapacityLimit(lastLevel, maxLevelTarget)

		if len(l.levels[lastLevel]) > limit {
			// Delayed last level compaction strategy:
			// Instead of immediately compacting the last level when it's full,
			// we create a new level and delay compaction to the next cycle.
			// This is because in Garnering, the capacity of each level depends on
			// the total number of levels.
			if lastLevel == maxLevelTarget-1 {
				// Last level is full, but if we create a new level, all capacities change
				// Delay actual compaction and let next round handle it
				l.levels = append(l.levels, make([]*SSTable, 0))
				l.lastLevelFull = true
				return
			}

			// Otherwise compact normally
			l.compactLevel(lastLevel)
		}
	}
}

func (l *LSMTreeBackend) compactLevel(level int) {
	// Garnering: Re-evaluate the optimal number of levels
	maxLevelTarget := l.compactionPolicy.GetNumLevels(l.totalItems)

	// Create next level if it doesn't exist
	if level+1 >= len(l.levels) {
		l.levels = append(l.levels, make([]*SSTable, 0))
	}

	// Merge all maps from level and level+1
	mergedData := make(map[string][]byte)

	// Older items are in level+1, so insert them first
	for _, ss := range l.levels[level+1] {
		for k, v := range ss.data {
			mergedData[k] = v
		}
	}

	// Newer items are in level, insert them next (overwrites older keys)
	for _, ss := range l.levels[level] {
		for k, v := range ss.data {
			mergedData[k] = v
		}
	}

	// Dump mergedData into chunks of SSTableMaxSize
	l.levels[level] = make([]*SSTable, 0)   // Clear level
	l.levels[level+1] = make([]*SSTable, 0) // We will overwrite level+1

	// To ensure stable order of keys during split, let's sort keys
	// to approximate sorted string table
	var keys []string
	for k := range mergedData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	currentMap := make(map[string][]byte)
	for _, k := range keys {
		currentMap[k] = mergedData[k]
		if len(currentMap) >= SSTableMaxSize {
			l.createSSTableFromMap(level+1, currentMap)
			currentMap = make(map[string][]byte)
		}
	}
	if len(currentMap) > 0 {
		l.createSSTableFromMap(level+1, currentMap)
	}

	// Recursively compact if needed (Garnering policy handles cascading)
	if level+1 < len(l.levels) {
		limit := l.compactionPolicy.GetCapacityLimit(level+1, maxLevelTarget)
		if len(l.levels[level+1]) > limit {
			l.compactLevel(level + 1)
		}
	}
}

func (l *LSMTreeBackend) createSSTableFromMap(targetLevel int, data map[string][]byte) {
	tempMem := NewMemTable()
	for k, v := range data {
		tempMem.Put([]byte(k), v)
	}
	sstable := FlushMemTable(l.ssTableIdCounter, tempMem, 0.01)
	l.ssTableIdCounter++
	l.levels[targetLevel] = append(l.levels[targetLevel], sstable)
}

func (l *LSMTreeBackend) Delete(key []byte) error {
	return nil
}
