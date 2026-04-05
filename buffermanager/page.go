package buffermanager

import "sync"

type PageID uint32
type FrameID uint32

const PageSize = 4096

type Page struct {
	id       PageID
	data     [PageSize]byte
	pinCount int32
	isDirty  bool
	pageLSN  uint64       // LSN of last write (for WAL)
	rwMutex  sync.RWMutex // physical latch
}

func NewPage(id PageID) *Page {
	return &Page{
		id:       id,
		pinCount: 0,
		isDirty:  false,
		pageLSN:  0,
	}
}

// RLock acquires a read lock on the page
func (p *Page) RLock() {
	p.rwMutex.RLock()
}

// RUnlock releases a read lock on the page
func (p *Page) RUnlock() {
	p.rwMutex.RUnlock()
}

// WLock acquires a write lock on the page
func (p *Page) WLock() {
	p.rwMutex.Lock()
}

// WUnlock releases a write lock on the page
func (p *Page) WUnlock() {
	p.rwMutex.Unlock()
}

// GetData returns a slice of the page data
func (p *Page) GetData() []byte {
	return p.data[:]
}

// GetID returns the page ID
func (p *Page) GetID() PageID {
	return p.id
}

// GetPinCount returns the pin count
func (p *Page) GetPinCount() int32 {
	return p.pinCount
}

// SetPinCount sets the pin count
func (p *Page) SetPinCount(count int32) {
	p.pinCount = count
}

// IsDirty returns whether the page has been modified
func (p *Page) IsDirty() bool {
	return p.isDirty
}

// SetDirty marks the page as dirty
func (p *Page) SetDirty(dirty bool) {
	p.isDirty = dirty
}

// GetLSN returns the page's LSN (Log Sequence Number)
func (p *Page) GetLSN() uint64 {
	return p.pageLSN
}

// SetLSN sets the page's LSN
func (p *Page) SetLSN(lsn uint64) {
	p.pageLSN = lsn
}

func (p *Page) DeepClean() {
	for i := range p.data {
		p.data[i] = 0
	}
}
