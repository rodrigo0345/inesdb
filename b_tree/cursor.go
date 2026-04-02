package btree

import "encoding/binary"

type TreeCursor struct {
	tree         *BTree
	currentPage  *LeafPage
	currentPageID uint64
	currentCell  int
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

	c.currentPageID = path[len(path)-1]
	leafData, err := c.tree.pager.FetchPage(c.currentPageID)
	if err != nil {
		return err
	}

	c.currentPage = &LeafPage{data: leafData}

	cellCount := int(c.currentPage.CellCount())
	c.currentCell = cellCount

	// Find the first cell that is >= key
	for i := 0; i < cellCount; i++ {
		cell := c.currentPage.GetCell(c.currentPage.GetCellOffset(uint16(i)))
		if string(cell.Key) >= string(key) {
			c.currentCell = i
			break
		}
	}

	// If we're at the end of the page but there's a right sibling, move to it.
	if c.currentCell >= int(c.currentPage.CellCount()) && c.currentPage.RightSibling() != 0 {
		return c.Next()
	}

	return nil
}

func (c *TreeCursor) First() error {
	// To find the first node, we traverse down the leftmost child of each internal node
	pageID := c.tree.meta.RootPage()

	for {
		pageData, err := c.tree.pager.FetchPage(pageID)
		if err != nil {
			return err
		}

		pageType := PageType(binary.LittleEndian.Uint16(pageData[LeafHeaderTypeOffset:]))

		if pageType == TypeLeaf {
			c.currentPageID = pageID
			c.currentPage = &LeafPage{data: pageData}
			c.currentCell = 0
			return nil
		}

		if pageType == TypeInternal {
			internal := &InternalPage{data: pageData}
			if internal.CellCount() > 0 {
				cell := internal.GetCell(internal.GetCellOffset(0))
				pageID = cell.ChildPointer
			} else {
				pageID = internal.RightmostPointer()
			}
		} else {
			return ErrInvalidPageType
		}
	}
}

func (c *TreeCursor) Next() error {
	if c.currentPage == nil {
		return nil // Invalid state
	}

	c.currentCell++

	// If we've reached the end of the current page, move to the next page
	if c.currentCell >= int(c.currentPage.CellCount()) {
		nextPageID := c.currentPage.RightSibling()
		if nextPageID == 0 {
			// No more pages
			c.currentPage = nil
			return nil
		}

		pageData, err := c.tree.pager.FetchPage(nextPageID)
		if err != nil {
			return err
		}

		c.currentPageID = nextPageID
		c.currentPage = &LeafPage{data: pageData}
		c.currentCell = 0
	}

	return nil
}

func (c *TreeCursor) Valid() bool {
	return c.currentPage != nil && c.currentCell < int(c.currentPage.CellCount())
}

func (c *TreeCursor) Key() []byte {
	if !c.Valid() {
		return nil
	}
	cell := c.currentPage.GetCell(c.currentPage.GetCellOffset(uint16(c.currentCell)))
	return cell.Key
}

func (c *TreeCursor) Value() []byte {
	if !c.Valid() {
		return nil
	}
	cell := c.currentPage.GetCell(c.currentPage.GetCellOffset(uint16(c.currentCell)))
	return cell.Value
}
