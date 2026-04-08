package buffermanager

import "github.com/rodrigo0345/omag/resource_page"

type IBufferPoolManager interface {
	NewPage() (*resource_page.IResourcePage, error)
	PinPage(pageID resource_page.ResourcePageID) (*resource_page.IResourcePage, error)
	UnpinPage(pageID resource_page.ResourcePageID, isDirty bool) error
}
