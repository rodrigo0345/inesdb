package btree

import (
	"encoding/binary"

	"github.com/rodrigo0345/omag/buffermanager"
)

type TreeCursor struct {
	tree           *BTree
	currentPageObj *buffermanager.Page
	currentPageID  uint64
	currentCell    int
}

func (tree *BTree) Cursor() (Cursor, error) {
	return &TreeCursor{
		tree:        tree,
		currentCell: 0,
	}, nil
}

func (c *TreeCursor) Seek(key []byte) error {
	rootID := c.tree.meta.RootPage()
	path, err := c.tree.findLeafPage(rootID, key)
	if err != nil {
		return err
	}

	// Close previous page if any
	if c.currentPageObj != nil {
		c.tree.bufferPool.UnpinPage(c.currentPageObj.GetID(), false)
	}

	c.currentPageID = path[len(path)-1]
	leafPageObj, err := c.tree.bufferPool.PinPage(buffermanager.PageID(c.currentPageID))
	if err != nil {
		return err
	}

	leafPageObj.RLock()
	c.currentPageObj = leafPageObj
	leaf := &LeafPage{data: leafPageObj.GetData()}

	cellCount := int(leaf.CellCount())
	c.currentCell = cellCount

	// Find the first cell that is >= key
	for i := 0; i < cellCount; i++ {
		cell := leaf.GetCell(leaf.GetCellOffset(uint16(i)))
		if string(cell.Key) >= string(key) {
			c.currentCell = i
			break
		}
	}
	leafPageObj.RUnlock()

	// If we're at the end of the page but there's a right sibling, move to it.
	if c.currentCell >= int(leaf.CellCount()) && leaf.RightSibling() != 0 {
		return c.Next()
	}

	return nil
}

func (c *TreeCursor) First() error {
	// Close previous page if any
	if c.currentPageObj != nil {
		c.tree.bufferPool.UnpinPage(c.currentPageObj.GetID(), false)
	}

	// To find the first node, we traverse down the leftmost child of each internal node
	pageID := c.tree.meta.RootPage()

	for {
		pageObj, err := c.tree.bufferPool.PinPage(buffermanager.PageID(pageID))
		if err != nil {
			return err
		}

		pageObj.RLock()
		pageData := pageObj.GetData()
		pageType := PageType(binary.LittleEndian.Uint16(pageData[LeafHeaderTypeOffset:]))
		pageObj.RUnlock()

		if pageType == TypeLeaf {
			c.currentPageID = pageID
			c.currentPageObj = pageObj
			c.currentCell = 0
			return nil
		}

		if pageType == TypeInternal {
			pageObj.RLock()
			internal := &InternalPage{data: pageData}
			if internal.CellCount() > 0 {
				cell := internal.GetCell(internal.GetCellOffset(0))
				pageID = cell.ChildPointer
			} else {
				pageID = internal.RightmostPointer()
			}
			pageObj.RUnlock()
			c.tree.bufferPool.UnpinPage(pageObj.GetID(), false)
		} else {
			c.tree.bufferPool.UnpinPage(pageObj.GetID(), false)
			return ErrInvalidPageType
		}
	}
}

func (c *TreeCursor) Next() error {
	if c.currentPageObj == nil {
		return nil // Invalid state
	}

	c.currentPageObj.RLock()
	leaf := &LeafPage{data: c.currentPageObj.GetData()}
	cellCount := int(leaf.CellCount())
	rightSib := leaf.RightSibling()
	c.currentPageObj.RUnlock()

	c.currentCell++

	// If we've reached the end of the current page, move to the next page
	if c.currentCell >= cellCount {
		if rightSib == 0 {
			// No more pages
			c.tree.bufferPool.UnpinPage(c.currentPageObj.GetID(), false)
			c.currentPageObj = nil
			return nil
		}

		c.tree.bufferPool.UnpinPage(c.currentPageObj.GetID(), false)

		pageObj, err := c.tree.bufferPool.PinPage(buffermanager.PageID(rightSib))
		if err != nil {
			return err
		}

		c.currentPageID = rightSib
		c.currentPageObj = pageObj
		c.currentCell = 0
	}

	return nil
}

func (c *TreeCursor) Valid() bool {
	if c.currentPageObj == nil {
		return false
	}

	c.currentPageObj.RLock()
	leaf := &LeafPage{data: c.currentPageObj.GetData()}
	result := c.currentCell < int(leaf.CellCount())
	c.currentPageObj.RUnlock()

	return result
}

func (c *TreeCursor) Key() []byte {
	if !c.Valid() {
		return nil
	}

	c.currentPageObj.RLock()
	leaf := &LeafPage{data: c.currentPageObj.GetData()}
	cell := leaf.GetCell(leaf.GetCellOffset(uint16(c.currentCell)))
	key := make([]byte, len(cell.Key))
	copy(key, cell.Key)
	c.currentPageObj.RUnlock()

	return key
}

func (c *TreeCursor) Value() []byte {
	if !c.Valid() {
		return nil
	}

	c.currentPageObj.RLock()
	leaf := &LeafPage{data: c.currentPageObj.GetData()}
	cell := leaf.GetCell(leaf.GetCellOffset(uint16(c.currentCell)))
	value := make([]byte, len(cell.Value))
	copy(value, cell.Value)
	c.currentPageObj.RUnlock()

	return value
}

// Close releases the current page
func (c *TreeCursor) Close() error {
	if c.currentPageObj != nil {
		return c.tree.bufferPool.UnpinPage(c.currentPageObj.GetID(), false)
	}
	return nil
}
