package btree

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/page"
)

type BPlusTreeBackend struct {
	bufferManager buffer.IBufferPoolManager
	diskManager   interface {
		WritePage(pageID page.ResourcePageID, pageData []byte) error
		ReadPage(pageID page.ResourcePageID, pageData []byte) error
	}
	meta *MetaLogicPage
}

// NewBPlusTreeBackend creates a new B+ tree storage engine with disk manager
func NewBPlusTreeBackend(
	bufferMgr buffer.IBufferPoolManager,
	diskMgr interface {
		WritePage(pageID page.ResourcePageID, pageData []byte) error
		ReadPage(pageID page.ResourcePageID, pageData []byte) error
	},
) (*BPlusTreeBackend, error) {
	// Create in-memory meta page
	meta := NewMetaPage()

	backend := &BPlusTreeBackend{
		bufferManager: bufferMgr,
		diskManager:   diskMgr,
		meta:          meta,
	}

	// Try to load metadata from disk (page 0) if it exists
	if err := backend.LoadMetadataFromDisk(); err != nil {
		// If loading fails, that's OK - we'll use default metadata
		// This happens on first run when page 0 doesn't exist yet
	}

	return backend, nil
}

// LoadMetadataFromDisk loads the metadata from disk page 0 if it exists
func (b *BPlusTreeBackend) LoadMetadataFromDisk() error {
	if b.diskManager == nil {
		return nil // No disk manager, skip loading
	}

	// Try to read page 0 from disk
	pageData := make([]byte, 4096) // Default page size
	err := b.diskManager.ReadPage(page.ResourcePageID(0), pageData)
	if err != nil {
		// Page 0 doesn't exist yet - that's OK for first run
		return nil
	}

	// Validate it's a meta page by checking magic number
	loadedMeta := &MetaLogicPage{data: pageData}
	if loadedMeta.MagicNumber() == MagicNumber {
		b.meta = loadedMeta
	}

	return nil
}

// SaveMetadataToDisk persists the metadata to disk page 0
func (b *BPlusTreeBackend) SaveMetadataToDisk() error {
	if b.diskManager == nil {
		return nil // No disk manager, skip saving
	}

	// Write metadata to page 0
	return b.diskManager.WritePage(page.ResourcePageID(0), b.meta.data)
}

// initializeRoot creates the first root page (a leaf page)
func (b *BPlusTreeBackend) initializeRoot() error {
	// Allocate a new page for the root
	pageRef, err := b.bufferManager.NewPage()
	if err != nil {
		return err
	}

	// pageRef is *page.IResourcePage - use it directly
	rootPage := *pageRef

	// Initialize it as a leaf page
	leaf := NewLeafPage(uint32(len(rootPage.GetData())))
	// Copy the initialized leaf page data into the buffer
	copy(rootPage.GetData(), leaf.data)

	rootPage.SetDirty(true)

	// Get the newly allocated page ID and set it as root in meta
	rootPageID := rootPage.GetID()
	b.meta.SetRootPage(uint64(rootPageID))

	// Unpin the page - this keeps it in the buffer pool
	if err := b.bufferManager.UnpinPage(rootPageID, true); err != nil {
		return err
	}

	return nil
}

// GetRootPageID returns the current root page ID for debugging purposes
func (b *BPlusTreeBackend) GetRootPageID() uint64 {
	return b.meta.RootPage()
}

func (b *BPlusTreeBackend) Get(key []byte) ([]byte, error) {
	rootPage := b.meta.RootPage()
	if rootPage == 0 {
		return nil, nil
	}

	path, err := b.findLeafPage(rootPage, key)
	if err != nil {
		return nil, fmt.Errorf("findLeafPage: %w", err)
	}

	leafPageID := path[len(path)-1]
	leafResourcePage, err := b.bufferManager.PinPage(page.ResourcePageID(leafPageID))
	if err != nil {
		return nil, err
	}
	defer b.bufferManager.UnpinPage(page.ResourcePageID(leafPageID), false)

	leafResourcePage.RLock()
	defer leafResourcePage.RUnlock()

	leafLogicalPage := &LeafLogicPage{data: leafResourcePage.GetData()}
	return leafLogicalPage.Get(key)
}

func (b *BPlusTreeBackend) Put(key []byte, value []byte) error {
	rootID := b.meta.RootPage()

	// Initialize root page if tree is empty
	if rootID == 0 {
		if err := b.initializeRoot(); err != nil {
			return fmt.Errorf("failed to initialize root: %w", err)
		}
		rootID = b.meta.RootPage()
	}

	path, err := b.findLeafPage(rootID, key)
	if err != nil {
		return fmt.Errorf("findLeafPage: %w", err)
	}

	leafID := path[len(path)-1]
	leafPage, err := b.bufferManager.PinPage(page.ResourcePageID(leafID))
	if err != nil {
		return err
	}

	leafPage.WLock()

	leaf := &LeafLogicPage{data: leafPage.GetData()}

	err = leaf.Insert(key, value)
	if err == ErrPageFull {
		// splitLeaf takes ownership of the lock and the pin
		return b.splitLeaf(path, leafPage, leafID, key, value)
	}
	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		return err
	}

	leafPage.SetDirty(true)
	leafPage.WUnlock()

	if err := b.bufferManager.UnpinPage(page.ResourcePageID(leafID), true); err != nil {
		return err
	}

	return nil
}

func (b *BPlusTreeBackend) Delete(key []byte) error {
	rootID := b.meta.RootPage()

	// If tree is empty, nothing to delete
	if rootID == 0 {
		return nil
	}

	path, err := b.findLeafPage(rootID, key)
	if err != nil {
		return err
	}

	leafID := path[len(path)-1]
	leafPage, err := b.bufferManager.PinPage(page.ResourcePageID(leafID))
	if err != nil {
		return err
	}

	// Acquire write latch on page
	leafPage.WLock()

	leaf := &LeafLogicPage{data: leafPage.GetData()}

	err = leaf.Remove(key)

	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		return err
	}

	leafPage.SetDirty(true)
	leafPage.WUnlock()

	if err := b.bufferManager.UnpinPage(page.ResourcePageID(leafID), true); err != nil {
		return err
	}
	return nil
}

// Scan returns all key-value pairs in the tree
func (b *BPlusTreeBackend) Scan() ([]struct{ Key, Value []byte }, error) {
	var results []struct{ Key, Value []byte }

	rootID := b.meta.RootPage()
	if rootID == 0 {
		return results, nil // Empty tree
	}

	// Pin the root page
	rootPage, err := b.bufferManager.PinPage(page.ResourcePageID(rootID))
	if err != nil {
		return nil, err
	}
	defer b.bufferManager.UnpinPage(page.ResourcePageID(rootID), false)

	// Read as leaf page and scan all cells
	rootPage.RLock()
	leaf := &LeafLogicPage{data: rootPage.GetData()}
	cellCount := leaf.CellCount()

	for i := uint16(0); i < cellCount; i++ {
		offset := leaf.GetCellOffset(i)
		cell := leaf.GetCell(offset)
		// Only include non-deleted entries (deleted entries have empty values)
		if len(cell.Value) > 0 {
			results = append(results, struct{ Key, Value []byte }{
				Key:   cell.Key,
				Value: cell.Value,
			})
		}
	}
	rootPage.RUnlock()

	return results, nil
}
