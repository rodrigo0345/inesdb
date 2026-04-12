package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/page"
)

func (b *BPlusTreeBackend) findLeafPage(pageID uint64, key []byte) ([]uint64, error) {
	var path []uint64

	for {
		path = append(path, pageID)

		resPagePtr, err := b.bufferManager.PinPage(page.ResourcePageID(pageID))
		if err != nil {
			return nil, err
		}

		logicPageType := getPageType(resPagePtr, 0, 2)

		switch logicPageType {
		case TypeLeaf:
			b.bufferManager.UnpinPage(page.ResourcePageID(pageID), false)
			return path, nil

		case TypeInternal:
			nextPageID := nextInternalPage(resPagePtr, key)
			b.bufferManager.UnpinPage(page.ResourcePageID(pageID), false)
			pageID = nextPageID

		default:
			b.bufferManager.UnpinPage(page.ResourcePageID(pageID), false)
			return nil, fmt.Errorf("invalid page type: %d", logicPageType)
		}
	}
}

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

	beforeImage := make([]byte, len(leafPage.GetData()))
	copy(beforeImage, leafPage.GetData())

	newPageRef, err := b.bufferManager.NewPage()
	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		return fmt.Errorf("NewPage: %w", err)
	}
	newPage := *newPageRef
	newPageID := newPage.GetID()

	leaf := &LeafLogicPage{data: leafPage.GetData()}
	newPageData := NewLeafPage(uint32(len(newPage.GetData())))

	promotedKey := leaf.Split(newPageData, uint64(newPageID))

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



	copy(leafPage.GetData(), leaf.data)
	leafPage.SetDirty(true)
	leafPage.WUnlock()
	b.bufferManager.UnpinPage(page.ResourcePageID(leafID), true)

	copy(newPage.GetData(), newPageData.data)
	newPage.SetDirty(true)
	b.bufferManager.UnpinPage(page.ResourcePageID(newPageID), true)

	return b.promoteKey(breadcrumbs[:len(breadcrumbs)-1], promotedKey, uint64(newPageID))
}

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

	beforeImage := make([]byte, len(parentPage.GetData()))
	copy(beforeImage, parentPage.GetData())

	parent := &InternalLogicPage{data: parentPage.GetData()}
	err = parent.Insert(key, childID)

	if err == ErrPageFull {
		return tree.splitInternal(breadcrumbs, parentPage, parentID, key, childID)
	}
	if err != nil {
		parentPage.WUnlock()
		tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), false)
		return err
	}


	parentPage.SetDirty(true)
	parentPage.WUnlock()

	return tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), true)
}

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

	beforeImage := make([]byte, len(parentPage.GetData()))
	copy(beforeImage, parentPage.GetData())

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



	copy(parentPage.GetData(), parent.data)
	parentPage.SetDirty(true)
	parentPage.WUnlock()
	tree.bufferManager.UnpinPage(page.ResourcePageID(parentID), true)

	copy(newPage.GetData(), newPageData.data)
	newPage.SetDirty(true)
	tree.bufferManager.UnpinPage(page.ResourcePageID(newPageID), true)

	return tree.promoteKey(path[:len(path)-1], promotedKey, uint64(newPageID))
}

func (tree *BPlusTreeBackend) createNewRoot(oldRootID uint64, key []byte, rightChildID uint64) error {

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


	newRootPage.WLock()
	copy(newRootPage.GetData(), newRoot.data)
	newRootPage.SetDirty(true)
	newRootPage.WUnlock()
	tree.bufferManager.UnpinPage(page.ResourcePageID(newRootID), true)

	tree.meta.SetRootPage(uint64(newRootID))

	metaPage, err := tree.bufferManager.PinPage(page.ResourcePageID(0))
	if err != nil {
		return fmt.Errorf("pin meta page: %w", err)
	}

	metaPage.WLock()
	copy(metaPage.GetData(), tree.meta.data)
	metaPage.SetDirty(true)
	metaPage.WUnlock()

	if err := tree.bufferManager.UnpinPage(page.ResourcePageID(0), true); err != nil {
		return err
	}

	return tree.SaveMetadataToDisk()
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
