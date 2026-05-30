package lsm

type MemTable struct {
	data       map[string][]byte
	tombstones map[string]bool
}

func NewMemTable() *MemTable {
	return &MemTable{
		data:       make(map[string][]byte),
		tombstones: make(map[string]bool),
	}
}

func (m *MemTable) Put(key []byte, value []byte) {
	m.data[string(key)] = value
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	val, ok := m.data[string(key)]
	return val, ok
}

func (m *MemTable) Exists(key []byte) bool {
	keyStr := string(key)
	_, inData := m.data[keyStr]
	_, inTombstones := m.tombstones[keyStr]
	return inData || inTombstones
}

func (m *MemTable) MarkTombstone(key []byte) {
	keyStr := string(key)
	m.tombstones[keyStr] = true
	if _, ok := m.data[keyStr]; !ok {
		m.data[keyStr] = nil
	}
}

func (m *MemTable) IsTombstoned(key []byte) bool {
	return m.tombstones[string(key)]
}

func FlushMemTable(id uint64, m *MemTable, falsePositiveRate float64) *SSTable {
	bf := NewBloomFilter(uint(len(m.data)), falsePositiveRate)

	for k := range m.data {
		bf.Add([]byte(k))
	}

	sstable := NewSSTable(id, m.data, bf)
	for k := range m.tombstones {
		sstable.tombstones[k] = true
	}
	// opt: refresh packed rows once so scan iterators use contiguous data.
	sstable.rebuildSortedRows()
	return sstable
}

func FlushMemTableFromMap(id uint64, data map[string][]byte, tombstones map[string]bool, falsePositiveRate float64) *SSTable {
	bf := NewBloomFilter(uint(len(data)), falsePositiveRate)

	for k := range data {
		bf.Add([]byte(k))
	}

	sstable := NewSSTable(id, data, bf)
	for k := range tombstones {
		sstable.tombstones[k] = true
	}
	// opt: refresh packed rows once so scan iterators use contiguous data.
	sstable.rebuildSortedRows()
	return sstable
}
