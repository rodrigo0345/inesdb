package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/page"
)

// findLeafPage traverses from pageID down to the leaf for key,
// returning the path of page IDs (breadcrumbs) from root to leaf.
func (b *BPlusTreeBackend) findLeafPage(pageID uint64, key []byte) ([]uint64, error) {
	var path []uint64

	for {
		path = append(path, pageID)

		// Pin once; reuse for both type-check and navigation.
		resPagePtr, err := b.bufferManager.PinPage(page.ResourcePageID(pageID))
		if err != nil {
			return nil, err
		}

		logicPageType := getPageType(resPagePtr, 0, 2)

		switch logicPageType {
		case TypeLeaf:
			// Unpin and return — we have the full path.
			b.bufferManager.UnpinPage(page.ResourcePageID(pageID), false)
			return path, nil

		case TypeInternal:
			// Reuse the already-pinned page for the search; unpin after.
			nextPageID := nextInternalPage(resPagePtr, key)
			b.bufferManager.UnpinPage(page.ResourcePageID(pageID), false)
			pageID = nextPageID

		default:
			b.bufferManager.UnpinPage(page.ResourcePageID(pageID), false)
			return nil, fmt.Errorf("invalid page type: %d", logicPageType)
		}
	}
}

// splitLeaf takes ownership of leafPage's write lock and pin.
// It is responsible for unlocking and unpinning leafPage in all code paths.
func (b *BPlusTreeBackend) splitLeaf(
	breadcrumbs []uint64,
	leafPage page.IResourcePage,
	leafID uint64,
	key, value []byte,
) error {
	if len(breadcrumbs) == 0 {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		return fmt.Errorf("breadcrumbs is empty, cannot promote key")
	}

	// Capture before-image of the old leaf BEFORE any mutation.
	beforeImage := make([]byte, len(leafPage.GetData()))
	copy(beforeImage, leafPage.GetData())

	// Allocate the new sibling page — check error before dereferencing.
	newPageRef, err := b.bufferManager.NewPage()
	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		return fmt.Errorf("NewPage: %w", err)
	}
	newPage := *newPageRef
	newPageID := newPage.GetID()

	// Build in-memory logical views.
	leaf := &LeafLogicPage{data: leafPage.GetData()}
	newPageData := NewLeafPage(uint32(len(newPage.GetData())))

	// Split: moves upper half into newPageData, returns the promoted key.
	promotedKey := leaf.Split(newPageData, uint64(newPageID))

	// Insert the new key into whichever half it belongs to.
	if bytes.Compare(key, promotedKey) < 0 {
		if err := leaf.Insert(key, value); err != nil {
			leafPage.WUnlock()
			b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
			b.bufferManager.UnpinPage(page.ResourcePageID(newPageID), false)
			return fmt.Errorf("insert into old leaf: %w", err)
		}
	} else {
		if err := newPageData.Insert(key, value); err != nil {
			leafPage.WUnlock()
			b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
			b.bufferManager.UnpinPage(page.ResourcePageID(newPageID), false)
			return fmt.Errorf("insert into new leaf: %w", err)
		}
	}

	// WAL for the old leaf — before-image captured above, after-image is leaf.data.
	// Both WAL records must be written before any page is marked dirty.

	// WAL for the new sibling — no before-image (newly allocated page).

	// WAL is durable — now safe to write pages.
	copy(leafPage.GetData(), leaf.data)
	leafPage.SetDirty(true)
	leafPage.WUnlock()
	b.bufferManager.UnpinPage(page.ResourcePageID(leafID), true)

	copy(newPage.GetData(), newPageData.data)
	newPage.SetDirty(true)
	b.bufferManager.UnpinPage(page.ResourcePageID(newPageID), true)

	return b.promoteKey(breadcrumbs[:len(breadcrumbs)-1], promotedKey, uint64(newPageID))
}

// promoteKey inserts a key/child-pointer pair into the parent internal node,
// splitting it if necessary.
func (tree *BPlusTreeBackend) promoteKey(breadcrumbs []uint64, key []byte, childID uint64) error {
	if len(breadcrumbs) == 0 {
		return tree.createNewRoot(tree.meta.RootPage(), key, childID)
	}

	parentID := breadcrumbs[len(breadcrumbs)-1]
	parentPage, err := tree.bufferManager.PinPage(page.ResourcePageID(parentID))
	if err != nil {
		return err
	}
	parentPage.WLock()

	// Capture before-image BEFORE any mutation.
	beforeImage := make([]byte, len(parentPage.GetData()))
	copy(beforeImage, parentPage.GetData())

	parent := &InternalLogicPage{data: parentPage.GetData()}
	err = parent.Insert(key, childID)

	if err == ErrPageFull {
		// splitInternal takes ownership of the lock and pin.
		return tree.splitInternal(breadcrumbs, parentPage, parentID, key, childID)
	}
	if err != nil {
		parentPage.WUnlock()
		tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), false)
		return err
	}

	// WAL before dirty.

	parentPage.SetDirty(true)
	parentPage.WUnlock()

	return tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), true)
}

// splitInternal takes ownership of parentPage's write lock and pin.
// It is responsible for unlocking and unpinning parentPage in all code paths.
func (tree *BPlusTreeBackend) splitInternal(
	path []uint64,
	parentPage page.IResourcePage,
	parentID uint64,
	key []byte,
	childID uint64,
) error {

	if len(path) == 0 {
		parentPage.WUnlock()
		tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), false)
		return fmt.Errorf("path is empty, cannot promote key")
	}

	// Capture before-image BEFORE any mutation.
	beforeImage := make([]byte, len(parentPage.GetData()))
	copy(beforeImage, parentPage.GetData())

	// Check error before dereferencing.
	newPageRef, err := tree.bufferManager.NewPage()
	if err != nil {
		parentPage.WUnlock()
		tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), false)
		return fmt.Errorf("NewPage: %w", err)
	}
	newPage := *newPageRef
	newPageID := newPage.GetID()

	newPageData := NewInternalPage(uint32(len(newPage.GetData())))
	parent := &InternalLogicPage{data: parentPage.GetData()}

	promotedKey := parent.Split(newPageData)

	// Insert into the correct half.
	if bytes.Compare(key, promotedKey) < 0 {
		if err := parent.Insert(key, childID); err != nil {
			parentPage.WUnlock()
			tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), false)
			tree.bufferManager.UnpinPage(page.ResourcePageID(newPageID), false)
			return fmt.Errorf("insert into old internal: %w", err)
		}
	} else {
		if err := newPageData.Insert(key, childID); err != nil {
			parentPage.WUnlock()
			tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), false)
			tree.bufferManager.UnpinPage(page.ResourcePageID(newPageID), false)
			return fmt.Errorf("insert into new internal: %w", err)
		}
	}

	// WAL for the old internal page.

	// WAL for the new sibling internal page — no before-image.

	// WAL durable — safe to write pages.
	copy(parentPage.GetData(), parent.data)
	parentPage.SetDirty(true)
	parentPage.WUnlock()
	tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), true)

	copy(newPage.GetData(), newPageData.data)
	newPage.SetDirty(true)
	tree.bufferManager.UnpinPage(page.ResourcePageID(newPageID), true)

	return tree.promoteKey(path[:len(path)-1], promotedKey, uint64(newPageID))
}

// createNewRoot creates a new root internal page with oldRootID as left child
// and rightChildID as right child, separated by key.
func (tree *BPlusTreeBackend) createNewRoot(oldRootID uint64, key []byte, rightChildID uint64) error {

	// Check error before dereferencing.
	newRootPageRef, err := tree.bufferManager.NewPage()
	if err != nil {
		return fmt.Errorf("NewPage for new root: %w", err)
	}
	newRootPage := *newRootPageRef
	newRootID := newRootPage.GetID()

	newRoot := NewInternalPage(uint32(len(newRootPage.GetData())))
	newRoot.SetRightmostPointer(rightChildID)

	if err := newRoot.Insert(key, oldRootID); err != nil {
		tree.bufferManager.UnpinPage(page.ResourcePageID(newRootID), false)
		return fmt.Errorf("insert into new root: %w", err)
	}

	// WAL for the new root page — no before-image.

	// Write new root page — lock around the write.
	newRootPage.WLock()
	copy(newRootPage.GetData(), newRoot.data)
	newRootPage.SetDirty(true)
	newRootPage.WUnlock()
	tree.bufferManager.UnpinPage(page.ResourcePageID(newRootID), true)

	// Update in-memory meta, then persist it.
	tree.meta.SetRootPage(uint64(newRootID))

	metaPage, err := tree.bufferManager.PinPage(page.ResourcePageID(0))
	if err != nil {
		return fmt.Errorf("pin meta page: %w", err)
	}

	// WAL for the meta page change.

	metaPage.WLock()
	copy(metaPage.GetData(), tree.meta.data)
	metaPage.SetDirty(true)
	metaPage.WUnlock()

	return tree.bufferManager.UnpinPage(page.ResourcePageID(0), true)
}

func getPageType(
	pageObj page.IResourcePage,
	typeStartIndex uint8,
	typeEndIndex uint8,
) LogicPageType {
	pageObj.RLock()
	defer pageObj.RUnlock()

	data := pageObj.GetData()
	return LogicPageType(binary.LittleEndian.Uint16(data[typeStartIndex:typeEndIndex]))
}

func nextInternalPage(internalPage page.IResourcePage, key []byte) uint64 {
	internalPage.RLock()
	defer internalPage.RUnlock()

	internal := &InternalLogicPage{data: internalPage.GetData()}
	return internal.Search(key)
}
