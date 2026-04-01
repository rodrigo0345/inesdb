package btree

import (
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
	leafID, err := tree.findLeafPage(rootID, key)
	if err != nil {
		return nil, err
	}

	leafData, err := tree.pager.FetchPage(leafID)
	if err != nil {
		return nil, err
	}

	leaf := &LeafPage{data: leafData}
	return leaf.Get(key)
}

func (tree *BTree) Insert(key []byte, value []byte) error {
	rootID := tree.meta.RootPage()
	leafID, err := tree.findLeafPage(rootID, key)
	if err != nil {
		return err
	}

	leafData, err := tree.pager.FetchPage(leafID)
	if err != nil {
		return err
	}

	leaf := &LeafPage{data: leafData}
	err = leaf.Insert(key, value)

	if err == ErrPageFull {
		return ErrSplitNotImpl
	}
	if err != nil {
		return err
	}

	return tree.pager.WritePage(leafID, leaf.data)
}

func (tree *BTree) findLeafPage(pageID uint64, key []byte) (uint64, error) {
	for {
		pageData, err := tree.pager.FetchPage(pageID)
		if err != nil {
			return 0, err
		}

		pageType := PageType(binary.LittleEndian.Uint16(pageData[0:2]))

		switch pageType {
		case TypeLeaf:
			return pageID, nil
		case TypeInternal:
			internal := &InternalPage{data: pageData}
			pageID = internal.Search(key)
		default:
			return 0, ErrInvalidPageType
		}
	}
}
