package lsm

import "sort"

type Tombstone struct {
	Deleted bool
}

type SSTable struct {
	id          uint64
	data        map[string][]byte
	tombstones  map[string]bool
	bloomFilter *BloomFilter
	sortedRows  []sstableRow
}

type sstableRow struct {
	key       string
	value     []byte
	tombstone bool
}

func NewSSTable(id uint64, data map[string][]byte, bf *BloomFilter) *SSTable {
	ss := &SSTable{
		id:          id,
		data:        data,
		tombstones:  make(map[string]bool),
		bloomFilter: bf,
	}
	ss.rebuildSortedRows()
	return ss
}

func (ss *SSTable) Get(key []byte) ([]byte, error) {
	keyStr := string(key)
	if ss.tombstones[keyStr] {
		return nil, ErrKeyTombstoned
	}

	if ss.bloomFilter != nil && !ss.bloomFilter.Test(key) {
		return nil, ErrKeyNotFound
	}

	val, ok := ss.data[keyStr]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return val, nil
}

func (ss *SSTable) IsTombstoned(key []byte) bool {
	return ss.tombstones[string(key)]
}

func (ss *SSTable) rebuildSortedRows() {
	if ss == nil {
		return
	}
	keys := make([]string, 0, len(ss.data))
	for k := range ss.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rows := make([]sstableRow, len(keys))
	for i, key := range keys {
		rows[i] = sstableRow{key: key, value: ss.data[key], tombstone: ss.tombstones[key]}
	}
	ss.sortedRows = rows
}

