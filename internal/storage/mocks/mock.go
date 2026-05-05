package mocks

import (
	"bytes"
	"sort"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type MockLogManager struct {
	records []log.ILogRecord
	mu      sync.Mutex
}

func (m *MockLogManager) AppendLogRecord(record log.ILogRecord) (log.LSN, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, record)
	return log.LSN(len(m.records)), nil
}

func (m *MockLogManager) Flush(upToLSN log.LSN) error {
	return nil
}

func (m *MockLogManager) Recover() (*log.RecoveryState, error) {
	return nil, nil
}

func (m *MockLogManager) Checkpoint() error {
	return nil
}

func (m *MockLogManager) CleanupTransactionOperations(txnID uint64) {
}

func (m *MockLogManager) GetTransactionOperations(txnID uint64) []log.RecoveryOperation {
	_ = txnID
	return nil
}

func (m *MockLogManager) GetLastCheckpointLSN() uint64 {
	return 0
}

func (m *MockLogManager) AddTransactionOperation(txnID uint64, tableName string, opType log.RecordType, key []byte, value []byte) {

}

func (m *MockLogManager) Close() error {
	return nil
}

func (m *MockLogManager) ReadAllRecords() ([]log.WALRecord, error) {
	return nil, nil
}

type MockResourcePage struct {
	id    page.ResourcePageID
	data  []byte
	dirty bool
	rmu   sync.RWMutex
	wmu   sync.Mutex
}

func newMockResourcePage(id page.ResourcePageID, size int) *MockResourcePage {
	return &MockResourcePage{
		id:   id,
		data: make([]byte, size),
	}
}

func (p *MockResourcePage) GetID() page.ResourcePageID            { return p.id }
func (p *MockResourcePage) GetData() []byte                       { return p.data }
func (p *MockResourcePage) SetDirty(dirty bool)                   { p.dirty = dirty }
func (p *MockResourcePage) IsDirty() bool                         { return p.dirty }
func (p *MockResourcePage) RLock()                                { p.rmu.RLock() }
func (p *MockResourcePage) RUnlock()                              { p.rmu.RUnlock() }
func (p *MockResourcePage) WLock()                                { p.wmu.Lock() }
func (p *MockResourcePage) WUnlock()                              { p.wmu.Unlock() }
func (p *MockResourcePage) GetLSN() uint64                        { return 0 }
func (p *MockResourcePage) SetLSN(lsn uint64)                     {}
func (p *MockResourcePage) ResetMemory()                          { p.data = make([]byte, len(p.data)) }
func (p *MockResourcePage) GetPinCount() int32                    { return 0 }
func (p *MockResourcePage) SetPinCount(count int32)               {}
func (p *MockResourcePage) ReplacePage(newID page.ResourcePageID) { p.id = newID }
func (p *MockResourcePage) Close()                                {}

type MockBufferManager struct {
	pages      map[page.ResourcePageID]*MockResourcePage
	nextPageID page.ResourcePageID
	mu         sync.Mutex
}

func (m *MockBufferManager) NewPage() (*page.IResourcePage, error) {
	m.mu.Lock()
	pageID := m.nextPageID
	m.nextPageID++
	m.mu.Unlock()

	mockPage := newMockResourcePage(pageID, 4096)
	m.mu.Lock()
	m.pages[pageID] = mockPage
	m.mu.Unlock()

	var iface page.IResourcePage = mockPage
	return &iface, nil
}

func (m *MockBufferManager) PinPage(pageID page.ResourcePageID) (page.IResourcePage, error) {
	m.mu.Lock()
	p, ok := m.pages[pageID]
	m.mu.Unlock()

	if !ok {
		mockPage := newMockResourcePage(pageID, 4096)
		m.mu.Lock()
		m.pages[pageID] = mockPage
		m.mu.Unlock()
		var iface page.IResourcePage = mockPage
		return iface, nil
	}

	var iface page.IResourcePage = p
	return iface, nil
}

func (m *MockBufferManager) UnpinPage(pageID page.ResourcePageID, isDirty bool) error {
	return nil
}

func (m *MockBufferManager) FlushAll() error {
	return nil
}

func (m *MockBufferManager) Close() error {
	return nil
}

// MockStorageEngine simulates a sorted storage backend (B-Tree/LSM)
type MockStorageEngine struct {
      mu   sync.RWMutex // Protects the data map
      data map[string][]byte
}

func NewMockStorage() *MockStorageEngine {
      return &MockStorageEngine{
            data: make(map[string][]byte),
      }
}

func (m *MockStorageEngine) Put(key []byte, value []byte) error {
      m.mu.Lock()
      defer m.mu.Unlock()
      m.data[string(key)] = value
      return nil
}

func (m *MockStorageEngine) Get(key []byte) ([]byte, error) {
      m.mu.RLock()
      defer m.mu.RUnlock()
      val, ok := m.data[string(key)]
      if !ok {
            return nil, nil
      }
      return val, nil
}

func (m *MockStorageEngine) Delete(key []byte) error {
      m.mu.Lock()
      defer m.mu.Unlock()
      delete(m.data, string(key))
      return nil
}

func (m *MockStorageEngine) Scan(opts storage.ScanOptions) (storage.ICursor, error) {
      m.mu.RLock()
      defer m.mu.RUnlock()

      // 1. Extract and sort keys
      keys := make([]string, 0, len(m.data))
      for k := range m.data {
            keys = append(keys, k)
      }

      if opts.Reverse {
            sort.Sort(sort.Reverse(sort.StringSlice(keys)))
      } else {
            sort.Strings(keys)
      }

      // 2. Filter by boundaries
      var entries []storage.ScanEntry
      for _, k := range keys {
            keyBytes := []byte(k)

            if opts.LowerBound != nil {
                  if bytes.Compare(keyBytes, opts.LowerBound) < 0 {
                        continue
                  }
            }

            if opts.UpperBound != nil {
                  cmp := bytes.Compare(keyBytes, opts.UpperBound)
                  if opts.Inclusive {
                        if cmp > 0 { break }
                  } else {
                        if cmp >= 0 { break }
                  }
            }

            entries = append(entries, storage.ScanEntry{
                  Key:   keyBytes,
                  Value: m.data[k],
            })
      }

      return &MockCursor{entries: entries, index: -1}, nil
}

// MockCursor implements storage.ICursor
type MockCursor struct {
	entries []storage.ScanEntry
	index   int
	err     error
}

func (c *MockCursor) Next() bool {
	if c.index+1 < len(c.entries) {
		c.index++
		return true
	}
	return false
}

func (c *MockCursor) Entry() storage.ScanEntry {
	if c.index < 0 || c.index >= len(c.entries) {
		return storage.ScanEntry{}
	}
	return c.entries[c.index]
}

func (c *MockCursor) Error() error {
	return c.err
}

func (c *MockCursor) Close() error {
	c.entries = nil
	return nil
}
