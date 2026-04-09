package buffer

import "github.com/rodrigo0345/omag/internal/storage/page"

type IBufferPoolManager interface {
	NewPage() (*page.IResourcePage, error)
	PinPage(pageID page.ResourcePageID) (page.IResourcePage, error)
	UnpinPage(pageID page.ResourcePageID, isDirty bool) error
}
