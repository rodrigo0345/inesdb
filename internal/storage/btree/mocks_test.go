package btree

import (
	"fmt"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage/page"
)


type mockDiskManager struct {
	pages map[page.ResourcePageID][]byte
	mu    sync.Mutex
}

func newMockDiskManager() *mockDiskManager {
	return &mockDiskManager{
		pages: make(map[page.ResourcePageID][]byte),
	}
}

func (m *mockDiskManager) WritePage(pageID page.ResourcePageID, pageData []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data := make([]byte, len(pageData))
	copy(data, pageData)
	m.pages[pageID] = data
	return nil
}

func (m *mockDiskManager) ReadPage(pageID page.ResourcePageID, pageData []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if storedData, ok := m.pages[pageID]; ok {
		copy(pageData, storedData)
		return nil
	}
	return fmt.Errorf("page not found")
}

type mockResourcePage struct {
	id    page.ResourcePageID
	data  []byte
	dirty bool
	rmu   sync.RWMutex
	wmu   sync.Mutex
}

func newMockResourcePage(id page.ResourcePageID, size int) *mockResourcePage {
	return &mockResourcePage{
		id:   id,
		data: make([]byte, size),
	}
}

func (p *mockResourcePage) GetID() page.ResourcePageID            { return p.id }
func (p *mockResourcePage) GetData() []byte                       { return p.data }
func (p *mockResourcePage) SetDirty(dirty bool)                   { p.dirty = dirty }
func (p *mockResourcePage) IsDirty() bool                         { return p.dirty }
func (p *mockResourcePage) RLock()                                { p.rmu.RLock() }
func (p *mockResourcePage) RUnlock()                              { p.rmu.RUnlock() }
func (p *mockResourcePage) WLock()                                { p.wmu.Lock() }
func (p *mockResourcePage) WUnlock()                              { p.wmu.Unlock() }
func (p *mockResourcePage) GetLSN() uint64                        { return 0 }
func (p *mockResourcePage) SetLSN(lsn uint64)                     {}
func (p *mockResourcePage) ResetMemory()                          { p.data = make([]byte, len(p.data)) }
func (p *mockResourcePage) GetPinCount() int32                    { return 0 }
func (p *mockResourcePage) SetPinCount(count int32)               {}
func (p *mockResourcePage) ReplacePage(newID page.ResourcePageID) { p.id = newID }
func (p *mockResourcePage) Close()                                {}

type mockBufferManager struct {
	pages      map[page.ResourcePageID]*mockResourcePage
	nextPageID page.ResourcePageID
	mu         sync.Mutex
}

func newMockBufferManager() *mockBufferManager {
	return &mockBufferManager{
		pages:      make(map[page.ResourcePageID]*mockResourcePage),
		nextPageID: page.ResourcePageID(1),
	}
}

func (m *mockBufferManager) NewPage() (*page.IResourcePage, error) {
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

func (m *mockBufferManager) PinPage(pageID page.ResourcePageID) (page.IResourcePage, error) {
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

func (m *mockBufferManager) UnpinPage(pageID page.ResourcePageID, isDirty bool) error {
	return nil
}

func (m *mockBufferManager) DeletePage(pageID page.ResourcePageID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pages, pageID)
	return nil
}

func (m *mockBufferManager) FlushAllPages() error {
	return nil
}

func (m *mockBufferManager) FlushAll() error {
	return nil
}

func (m *mockBufferManager) Close() error {
	return nil
}
