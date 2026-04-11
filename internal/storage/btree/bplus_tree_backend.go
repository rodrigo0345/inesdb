package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/rodrigo0345/omag/internal/storage"
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
	backend.LoadMetadataFromDisk()

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

	leaf := NewLeafPage(uint32(len(rootPage.GetData())))
	copy(rootPage.GetData(), leaf.data)

	rootPage.SetDirty(true)

	// Get the newly allocated page ID and set it as root in meta
	rootPageID := rootPage.GetID()
	b.meta.SetRootPage(uint64(rootPageID))

	// Unpin the page - this keeps it in the buffer pool
	if err := b.bufferManager.UnpinPage(rootPageID, true); err != nil {
		return err
	}

	// Persist metadata to disk
	return b.SaveMetadataToDisk()
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

	// Get cell with overflow support
	inlineValue, overflowID, err := b.getWithOverflow(leafLogicalPage, key)
	if err != nil {
		return nil, err
	}

	// If no overflow, return inline value
	if overflowID == 0 {
		return inlineValue, nil
	}

	// Reconstruct full value from inline + overflow pages
	return b.readValueWithOverflow(inlineValue, overflowID)
}

// getWithOverflow retrieves a value and its overflow ID from a leaf page
func (b *BPlusTreeBackend) getWithOverflow(leaf *LeafLogicPage, key []byte) ([]byte, uint64, error) {
	cellCount := int(leaf.CellCount())

	index := sort.Search(cellCount, func(i int) bool {
		cell := leaf.GetCell(leaf.GetCellOffset(uint16(i)))
		return bytes.Compare(cell.Key, key) >= 0
	})

	if index < cellCount {
		cell := leaf.GetCell(leaf.GetCellOffset(uint16(index)))
		if bytes.Equal(cell.Key, key) {
			return cell.Value, cell.OverflowID, nil
		}
	}

	return nil, 0, ErrKeyNotFound
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

	// Handle overflow for large values
	inlineValue := value
	var overflowID uint64
	if len(value) > OverflowThreshold {
		inlineLen, overflowPageID, err := b.writeValueWithOverflow(value)
		if err != nil {
			return fmt.Errorf("failed to write overflow pages: %w", err)
		}
		overflowID = overflowPageID
		inlineValue = value[:inlineLen] // Truncate to inline threshold
	}

	path, err := b.findLeafPage(rootID, key)
	if err != nil {
		if overflowID != 0 {
			b.deleteValueWithOverflow(overflowID)
		}
		return fmt.Errorf("findLeafPage: %w", err)
	}

	leafID := path[len(path)-1]
	leafPage, err := b.bufferManager.PinPage(page.ResourcePageID(leafID))
	if err != nil {
		if overflowID != 0 {
			b.deleteValueWithOverflow(overflowID)
		}
		return err
	}

	leafPage.WLock()

	leaf := &LeafLogicPage{data: leafPage.GetData()}

	// Try to insert with overflow support
	err = b.insertWithOverflow(leaf, key, inlineValue, overflowID)
	if err == ErrPageFull {
		// splitLeaf takes ownership of the lock and the pin
		return b.splitLeafWithOverflow(path, leafPage, leafID, key, inlineValue, overflowID)
	}
	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		if overflowID != 0 {
			b.deleteValueWithOverflow(overflowID)
		}
		return err
	}

	leafPage.SetDirty(true)
	leafPage.WUnlock()

	if err := b.bufferManager.UnpinPage(page.ResourcePageID(leafID), true); err != nil {
		return err
	}

	return nil
}

// insertWithOverflow inserts a key-value pair, storing the overflow ID in the cell
func (b *BPlusTreeBackend) insertWithOverflow(leaf *LeafLogicPage, key []byte, inlineValue []byte, overflowID uint64) error {
	cellSize := uint16(CellHeaderSize + len(key) + len(inlineValue))
	spaceNeeded := cellSize + SlotSize

	slotArrayEnd := LeafHeaderSize + (leaf.CellCount() * SlotSize)
	availableSpace := leaf.FreeSpacePointer() - slotArrayEnd

	if availableSpace < spaceNeeded {
		absoluteUsedSpace := slotArrayEnd
		for i := uint16(0); i < leaf.CellCount(); i++ {
			c := leaf.GetCell(leaf.GetCellOffset(i))
			absoluteUsedSpace += uint16(CellHeaderSize + len(c.Key) + len(c.Value))
		}
		actualFreeSpace := uint16(len(leaf.data)) - absoluteUsedSpace
		if actualFreeSpace >= spaceNeeded {
			leaf.Vacuum()
		} else {
			return ErrPageFull
		}
	}

	insertIndex := uint16(sort.Search(int(leaf.CellCount()), func(i int) bool {
		cell := leaf.GetCell(leaf.GetCellOffset(uint16(i)))
		return bytes.Compare(cell.Key, key) >= 0
	}))

	newFreeSpace := leaf.FreeSpacePointer() - cellSize
	leaf.SetFreeSpacePointer(newFreeSpace)
	leaf.WriteCellWithOverflow(newFreeSpace, key, inlineValue, overflowID)

	if insertIndex < leaf.CellCount() {
		insertPos := LeafHeaderSize + (insertIndex * SlotSize)
		endPos := LeafHeaderSize + (leaf.CellCount() * SlotSize)
		copy(leaf.data[insertPos+SlotSize:endPos+SlotSize], leaf.data[insertPos:endPos])
	}

	newSlotPos := LeafHeaderSize + (insertIndex * SlotSize)
	binary.LittleEndian.PutUint16(leaf.data[newSlotPos:], newFreeSpace)
	leaf.SetCellCount(leaf.CellCount() + 1)

	return nil
}

// splitLeafWithOverflow splits a full leaf page with overflow support
func (b *BPlusTreeBackend) splitLeafWithOverflow(
	breadcrumbs []uint64,
	leafPage page.IResourcePage,
	leafID uint64,
	key []byte,
	inlineValue []byte,
	overflowID uint64,
) error {
	if len(breadcrumbs) == 0 {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		if overflowID != 0 {
			b.deleteValueWithOverflow(overflowID)
		}
		return fmt.Errorf("breadcrumbs is empty, cannot promote key")
	}

	// Allocate new sibling page
	newPageRef, err := b.bufferManager.NewPage()
	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		if overflowID != 0 {
			b.deleteValueWithOverflow(overflowID)
		}
		return fmt.Errorf("NewPage: %w", err)
	}
	newPage := *newPageRef
	newPageID := newPage.GetID()

	leaf := &LeafLogicPage{data: leafPage.GetData()}
	newPageData := NewLeafPage(uint32(len(newPage.GetData())))

	promotedKey := leaf.Split(newPageData, uint64(newPageID))

	// Insert the new key into whichever half it belongs to
	if bytes.Compare(key, promotedKey) < 0 {
		if err := b.insertWithOverflow(leaf, key, inlineValue, overflowID); err != nil {
			leafPage.WUnlock()
			b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
			b.bufferManager.UnpinPage(page.ResourcePageID(newPageID), false)
			if overflowID != 0 {
				b.deleteValueWithOverflow(overflowID)
			}
			return fmt.Errorf("insert into old leaf: %w", err)
		}
	} else {
		if err := b.insertWithOverflow(newPageData, key, inlineValue, overflowID); err != nil {
			leafPage.WUnlock()
			b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
			b.bufferManager.UnpinPage(page.ResourcePageID(newPageID), false)
			if overflowID != 0 {
				b.deleteValueWithOverflow(overflowID)
			}
			return fmt.Errorf("insert into new leaf: %w", err)
		}
	}

	copy(leafPage.GetData(), leaf.data)
	leafPage.SetDirty(true)
	leafPage.WUnlock()
	b.bufferManager.UnpinPage(page.ResourcePageID(leafID), true)

	copy(newPage.GetData(), newPageData.data)
	newPage.SetDirty(true)
	b.bufferManager.UnpinPage(page.ResourcePageID(newPageID), true)

	return b.promoteKey(breadcrumbs[:len(breadcrumbs)-1], promotedKey, uint64(newPageID))
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
func (b *BPlusTreeBackend) Scan() ([]storage.ScanEntry, error) {
	var results []storage.ScanEntry

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
			results = append(results, storage.ScanEntry{
				Key:   cell.Key,
				Value: cell.Value,
			})
		}
	}
	rootPage.RUnlock()

	return results, nil
}
