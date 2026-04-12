package page

type IResourcePage interface {
	RLock()
	RUnlock()
	WLock()
	WUnlock()
	GetData() []byte
	GetID() ResourcePageID
	GetPinCount() int32
	IsDirty() bool
	SetDirty(dirty bool)
	SetPinCount(count int32)
	ReplacePage(newID ResourcePageID)
	GetLSN() uint64
}
