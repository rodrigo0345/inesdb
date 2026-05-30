package page

import "sync"

type ResourcePageID uint64

const PageSize = 4096

type ResourcePage struct {
	id       ResourcePageID
	data     [PageSize]byte
	pinCount int32
	isDirty  bool
	pageLSN  uint64
	rwMutex  sync.RWMutex
}

func NewResourcePage(id ResourcePageID) IResourcePage {
	return &ResourcePage{
		id:       id,
		pinCount: 0,
		isDirty:  false,
		pageLSN:  0,
	}
}

func (p *ResourcePage) RLock() {
	p.rwMutex.RLock()
}

func (p *ResourcePage) RUnlock() {
	p.rwMutex.RUnlock()
}

func (p *ResourcePage) WLock() {
	p.rwMutex.Lock()
}

func (p *ResourcePage) WUnlock() {
	p.rwMutex.Unlock()
}

func (p *ResourcePage) GetData() []byte {
	return p.data[:]
}

func (p *ResourcePage) GetID() ResourcePageID {
	return p.id
}

func (p *ResourcePage) GetPinCount() int32 {
	return p.pinCount
}

func (p *ResourcePage) SetPinCount(count int32) {
	p.pinCount = count
}

func (p *ResourcePage) IsDirty() bool {
	return p.isDirty
}

func (p *ResourcePage) SetDirty(dirty bool) {
	p.isDirty = dirty
}

func (p *ResourcePage) GetLSN() uint64 {
	return p.pageLSN
}

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
