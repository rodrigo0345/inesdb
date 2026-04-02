package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var (
	ErrInvalidPageType = errors.New("invalid page type encountered")
	ErrSplitNotImpl    = errors.New("page split logic not implemented")
)

type BTree struct {
	pager *Pager
	meta  *MetaPage
}

func NewBTree(pager *Pager) (*BTree, error) {
	tree := &BTree{pager: pager}

	if pager.PageCount() == 0 {
		meta := NewMetaPage()
		root := NewLeafPage(pager.pageSize)

		metaID, _, err := pager.AllocatePage()
		if err != nil {
			return nil, err
		}

		rootID, _, err := pager.AllocatePage()
		if err != nil {
			return nil, err
		}

		meta.SetRootPage(rootID)

		if err := pager.WritePage(metaID, meta.data); err != nil {
			return nil, err
		}
		if err := pager.WritePage(rootID, root.data); err != nil {
			return nil, err
		}

		tree.meta = meta
		return tree, nil
	}

	metaData, err := pager.FetchPage(0)
	if err != nil {
		return nil, err
	}

	tree.meta = &MetaPage{data: metaData}
	return tree, nil
}

func (tree *BTree) Find(key []byte) ([]byte, error) {
	rootID := tree.meta.RootPage()
	path, err := tree.findLeafPage(rootID, key)
	if err != nil {
		return nil, err
	}

	leafID := path[len(path)-1]
	leafData, err := tree.pager.FetchPage(leafID)
	if err != nil {
		return nil, err
	}

	leaf := &LeafPage{data: leafData}
	return leaf.Get(key)
}

func (tree *BTree) Get(key []byte) ([]byte, error) {
	return tree.Find(key)
}

func (tree *BTree) Put(key []byte, value []byte) error {
	return tree.Insert(key, value)
}

func (tree *BTree) Delete(key []byte) error {
	// basic lazy delete
	rootID := tree.meta.RootPage()
	path, err := tree.findLeafPage(rootID, key)
	if err != nil {
		return err
	}

	leafID := path[len(path)-1]
	leafData, err := tree.pager.FetchPage(leafID)
	if err != nil {
		return err
	}

	leaf := &LeafPage{data: leafData}
	err = leaf.Remove(key)
	if err != nil {
		return err
	}
	return tree.pager.WritePage(leafID, leaf.data)
}

func (tree *BTree) Insert(key []byte, value []byte) error {
	rootID := tree.meta.RootPage()
	path, err := tree.findLeafPage(rootID, key)
	if err != nil {
		return err
	}

	leafID := path[len(path)-1]
	leafData, err := tree.pager.FetchPage(leafID)
	if err != nil {
		return err
	}

	leaf := &LeafPage{data: leafData}
	err = leaf.Insert(key, value)

	if err == ErrPageFull {
		return tree.splitLeaf(path, leaf, leafID, key, value)
	}
	if err != nil {
		return err
	}

	return tree.pager.WritePage(leafID, leaf.data)
}

func (tree *BTree) splitLeaf(path []uint64, leaf *LeafPage, leafID uint64, key, value []byte) error {
	newPageID, _, err := tree.pager.AllocatePage()
	if err != nil {
		return err
	}
	newPage := NewLeafPage(tree.pager.pageSize)

	promotedKey := leaf.Split(newPage, newPageID)

	// insert the new key either in the old or new page
	if bytes.Compare(key, promotedKey) < 0 {
		if err := leaf.Insert(key, value); err != nil {
			return err
		}
	} else {
		if err := newPage.Insert(key, value); err != nil {
			return err
		}
	}

	if err := tree.pager.WritePage(leafID, leaf.data); err != nil {
		return err
	}
	if err := tree.pager.WritePage(newPageID, newPage.data); err != nil {
		return err
	}

	return tree.promoteKey(path[:len(path)-1], promotedKey, newPageID)
}

func (tree *BTree) promoteKey(path []uint64, key []byte, childID uint64) error {
	if len(path) == 0 {
		return tree.createNewRoot(tree.meta.RootPage(), key, childID)
	}

	parentID := path[len(path)-1]
	parentData, err := tree.pager.FetchPage(parentID)
	if err != nil {
		return err
	}

	parent := &InternalPage{data: parentData}
	err = parent.Insert(key, childID)

	if err == ErrPageFull {
		return tree.splitInternal(path, parent, parentID, key, childID)
	}
	if err != nil {
		return err
	}

	return tree.pager.WritePage(parentID, parent.data)
}

func (tree *BTree) splitInternal(path []uint64, parent *InternalPage, parentID uint64, key []byte, childID uint64) error {
	newPageID, _, err := tree.pager.AllocatePage()
	if err != nil {
		return err
	}
	newPage := NewInternalPage(tree.pager.pageSize)

	promotedKey := parent.Split(newPage)

	// insert the correct side
	if bytes.Compare(key, promotedKey) < 0 {
		if err := parent.Insert(key, childID); err != nil {
			return err
		}
	} else {
		if err := newPage.Insert(key, childID); err != nil {
			return err
		}
	}

	if err := tree.pager.WritePage(parentID, parent.data); err != nil {
		return err
	}
	if err := tree.pager.WritePage(newPageID, newPage.data); err != nil {
		return err
	}

	return tree.promoteKey(path[:len(path)-1], promotedKey, newPageID)
}

func (tree *BTree) createNewRoot(oldRootID uint64, key []byte, rightChildID uint64) error {
	newRootID, _, err := tree.pager.AllocatePage()
	if err != nil {
		return err
	}

	newRoot := NewInternalPage(tree.pager.pageSize)
	newRoot.SetRightmostPointer(rightChildID)

	if err := newRoot.Insert(key, oldRootID); err != nil { // The left child has the smaller keys
		return err
	}

	if err := tree.pager.WritePage(newRootID, newRoot.data); err != nil {
		return err
	}

	tree.meta.SetRootPage(newRootID)
	return tree.pager.WritePage(0, tree.meta.data)
}

func (tree *BTree) findLeafPage(pageID uint64, key []byte) ([]uint64, error) {
	var path []uint64

	for {
		path = append(path, pageID)

		pageData, err := tree.pager.FetchPage(pageID)
		if err != nil {
			return nil, err
		}

		pageType := PageType(binary.LittleEndian.Uint16(pageData[0:2]))

		switch pageType {
		case TypeLeaf:
			return path, nil
		case TypeInternal:
			internal := &InternalPage{data: pageData}
			pageID = internal.Search(key)
		default:
			return nil, ErrInvalidPageType
		}
	}
}
