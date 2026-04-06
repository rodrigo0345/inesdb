package buffermanager

type IBufferPoolManager interface {
	PinPage(pageID PageID) (*Page, error)
	UnpinPage(pageID PageID, isDirty bool) error
}
