package page

import "sync"

type ResourcePageID uint64

const PageSize = 4096

type ResourcePage struct {
	id       ResourcePageID
	data     [PageSize]byte
	pinCount int32
	isDirty  bool
	pageLSN  uint64       // LSN of last write (for WAL)
	rwMutex  sync.RWMutex // physical latch
}

func NewResourcePage(id ResourcePageID) IResourcePage {
	return &ResourcePage{
		id:       id,
		pinCount: 0,
		isDirty:  false,
		pageLSN:  0,
	}
}

// RLock acquires a read lock on the page
func (p *ResourcePage) RLock() {
	p.rwMutex.RLock()
}

// RUnlock releases a read lock on the page
func (p *ResourcePage) RUnlock() {
	p.rwMutex.RUnlock()
}

// WLock acquires a write lock on the page
func (p *ResourcePage) WLock() {
	p.rwMutex.Lock()
}

// WUnlock releases a write lock on the page
func (p *ResourcePage) WUnlock() {
	p.rwMutex.Unlock()
}

// GetData returns a slice of the page data
func (p *ResourcePage) GetData() []byte {
	return p.data[:]
}

// GetID returns the page ID
func (p *ResourcePage) GetID() ResourcePageID {
	return p.id
}

// GetPinCount returns the pin count
func (p *ResourcePage) GetPinCount() int32 {
	return p.pinCount
}

// SetPinCount sets the pin count
func (p *ResourcePage) SetPinCount(count int32) {
	p.pinCount = count
}

// IsDirty returns whether the page has been modified
func (p *ResourcePage) IsDirty() bool {
	return p.isDirty
}

// SetDirty marks the page as dirty
func (p *ResourcePage) SetDirty(dirty bool) {
	p.isDirty = dirty
}

// GetLSN returns the page's LSN (Log Sequence Number)
func (p *ResourcePage) GetLSN() uint64 {
	return p.pageLSN
}

// SetLSN sets the page's LSN
func (p *ResourcePage) SetLSN(lsn uint64) {
	p.pageLSN = lsn
}

func (p *ResourcePage) DeepClean() {
	for i := range p.data {
		p.data[i] = 0
	}
}

func (p *ResourcePage) ReplacePage(newID ResourcePageID) {
	p.id = newID
	p.pinCount = 1
	p.isDirty = false
	p.pageLSN = 0
	p.DeepClean()
}
