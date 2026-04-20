package mocks

import (
	"sync"

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

func (m*MockLogManager) CleanupTransactionOperations(txnID uint64) {
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
