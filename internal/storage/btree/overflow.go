package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/page"
)


const (
	OverflowThreshold = 100

	OverflowHeaderSize = 8

	OverflowDataSize = DefaultPageSize - OverflowHeaderSize
)

type OverflowPage struct {
	data []byte
}

func NewOverflowPage(size int) *OverflowPage {
	return &OverflowPage{
		data: make([]byte, size),
	}
}

func (op *OverflowPage) SetNextOverflowID(nextID uint64) {
	binary.LittleEndian.PutUint64(op.data[0:8], nextID)
}

func (op *OverflowPage) GetNextOverflowID() uint64 {
	return binary.LittleEndian.Uint64(op.data[0:8])
}

func (op *OverflowPage) WriteData(offset int, data []byte) int {
	availableSpace := len(op.data) - OverflowHeaderSize - offset
	if availableSpace <= 0 {
		return 0
	}

	bytesToWrite := len(data)
	if bytesToWrite > availableSpace {
		bytesToWrite = availableSpace
	}

	writePos := OverflowHeaderSize + offset
	copy(op.data[writePos:writePos+bytesToWrite], data)
	return bytesToWrite
}

func (op *OverflowPage) ReadData(offset int, length int) []byte {
	startPos := OverflowHeaderSize + offset
	endPos := startPos + length

	if startPos < OverflowHeaderSize {
		startPos = OverflowHeaderSize
	}
	if endPos > len(op.data) {
		endPos = len(op.data)
	}

	if startPos >= endPos {
		return []byte{}
	}

	result := make([]byte, endPos-startPos)
	copy(result, op.data[startPos:endPos])
	return result
}

func (op *OverflowPage) GetData() []byte {
	return op.data
}


func (b *BPlusTreeBackend) writeValueWithOverflow(value []byte) (int, uint64, error) {
	if len(value) <= OverflowThreshold {
		return len(value), 0, nil
	}

	var firstOverflowPageID uint64
	remaining := value[:]
	var prevOverflowPageID uint64

	for len(remaining) > 0 {
		pageRef, err := b.bufferManager.NewPage()
		if err != nil {
			return 0, 0, fmt.Errorf("failed to allocate overflow page: %w", err)
		}
		resourcePage := *pageRef
		overflowPageID := resourcePage.GetID()

		overflowPage := NewOverflowPage(len(resourcePage.GetData()))

		bytesWritten := overflowPage.WriteData(0, remaining)

		if prevOverflowPageID != 0 {
			prevPage, err := b.bufferManager.PinPage(page.ResourcePageID(prevOverflowPageID))
			if err != nil {
				return 0, 0, fmt.Errorf("failed to pin previous overflow page: %w", err)
			}
			prevOverflowPage := &OverflowPage{data: prevPage.GetData()}
			prevOverflowPage.SetNextOverflowID(uint64(overflowPageID))
			prevPage.SetDirty(true)
			b.bufferManager.UnpinPage(page.ResourcePageID(prevOverflowPageID), true)
		} else {
			firstOverflowPageID = uint64(overflowPageID)
		}

		copy(resourcePage.GetData(), overflowPage.data)
		resourcePage.SetDirty(true)
		b.bufferManager.UnpinPage(overflowPageID, true)

		remaining = remaining[bytesWritten:]
		prevOverflowPageID = uint64(overflowPageID)
	}

	return OverflowThreshold, firstOverflowPageID, nil
}

func (b *BPlusTreeBackend) readValueWithOverflow(inlineData []byte, firstOverflowPageID uint64) ([]byte, error) {
	result := make([]byte, len(inlineData))
	copy(result, inlineData)

	if firstOverflowPageID == 0 {
		return result, nil
	}

	currentOverflowPageID := firstOverflowPageID
	for currentOverflowPageID != 0 {
		overflowResourcePage, err := b.bufferManager.PinPage(page.ResourcePageID(currentOverflowPageID))
		if err != nil {
			return nil, fmt.Errorf("failed to pin overflow page: %w", err)
		}

		overflowPage := &OverflowPage{data: overflowResourcePage.GetData()}
		overflowData := overflowPage.ReadData(0, OverflowDataSize)
		result = append(result, overflowData...)

		nextOverflowPageID := overflowPage.GetNextOverflowID()
		b.bufferManager.UnpinPage(page.ResourcePageID(currentOverflowPageID), false)
		currentOverflowPageID = nextOverflowPageID
	}

	return result, nil
}

func (b *BPlusTreeBackend) deleteValueWithOverflow(firstOverflowPageID uint64) error {
	currentOverflowPageID := firstOverflowPageID

	for currentOverflowPageID != 0 {
		overflowResourcePage, err := b.bufferManager.PinPage(page.ResourcePageID(currentOverflowPageID))
		if err != nil {
			return fmt.Errorf("failed to pin overflow page for deletion: %w", err)
		}

		overflowPage := &OverflowPage{data: overflowResourcePage.GetData()}
		nextOverflowPageID := overflowPage.GetNextOverflowID()

		b.bufferManager.UnpinPage(page.ResourcePageID(currentOverflowPageID), false)
		currentOverflowPageID = nextOverflowPageID
	}

	return nil
}


func (b *BPlusTreeBackend) shouldMergePage(page page.IResourcePage) bool {
	pageType := getPageType(page, 0, 2)

	if pageType == TypeLeaf {
		leaf := &LeafLogicPage{data: page.GetData()}
		usedSpace := int(leaf.FreeSpacePointer()) - LeafHeaderSize
		totalSpace := len(page.GetData()) - LeafHeaderSize
		if totalSpace == 0 {
			return false
		}
		usagePercent := (len(page.GetData()) - usedSpace) * 100 / len(page.GetData())
		return usagePercent < 25
	}

	return false
}

func (b *BPlusTreeBackend) getRightSibling(leafPage page.IResourcePage) uint64 {
	leaf := &LeafLogicPage{data: leafPage.GetData()}
	return leaf.RightSibling()
}

func (b *BPlusTreeBackend) getLeftSibling(breadcrumbs []uint64, leafID uint64) (uint64, error) {
	if len(breadcrumbs) <= 1 {
		return 0, nil
	}

	parentID := breadcrumbs[len(breadcrumbs)-1]
	parentPage, err := b.bufferManager.PinPage(page.ResourcePageID(parentID))
	if err != nil {
		return 0, err
	}
	defer b.bufferManager.UnpinPage(page.ResourcePageID(parentID), false)

	parent := &InternalLogicPage{data: parentPage.GetData()}

	for i := uint16(0); i < parent.CellCount(); i++ {
		cell := parent.GetCell(parent.GetCellOffset(i))
		if cell.ChildPointer == uint64(leafID) {
			if i == 0 {
				return 0, nil
			}
			prevCell := parent.GetCell(parent.GetCellOffset(i - 1))
			return prevCell.ChildPointer, nil
		}
	}

	if parent.RightmostPointer() == uint64(leafID) {
		cell := parent.GetCell(parent.GetCellOffset(parent.CellCount() - 1))
		return cell.ChildPointer, nil
	}

	return 0, fmt.Errorf("leaf not found in parent")
}

func (b *BPlusTreeBackend) mergeLeafPages(
	breadcrumbs []uint64,
	leafPage page.IResourcePage,
	leafID uint64,
) (bool, error) {
	rightSiblingID := b.getRightSibling(leafPage)
	if rightSiblingID == 0 {
		leftSiblingID, err := b.getLeftSibling(breadcrumbs, uint64(leafID))
		if err != nil || leftSiblingID == 0 {
			return false, err
		}

		leftSiblingPage, err := b.bufferManager.PinPage(page.ResourcePageID(leftSiblingID))
		if err != nil {
			return false, err
		}
		defer b.bufferManager.UnpinPage(page.ResourcePageID(leftSiblingID), false)

		leftLeaf := &LeafLogicPage{data: leftSiblingPage.GetData()}
		currentLeaf := &LeafLogicPage{data: leafPage.GetData()}

		for i := uint16(0); i < currentLeaf.CellCount(); i++ {
			cell := currentLeaf.GetCell(currentLeaf.GetCellOffset(i))
			if err := leftLeaf.Insert(cell.Key, cell.Value); err != nil {
				return false, nil
			}
		}

		leftLeaf.SetRightSibling(currentLeaf.RightSibling())
		leftSiblingPage.SetDirty(true)

		if err := b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false); err != nil {
			return false, err
		}

		return true, nil
	}

	rightSiblingPage, err := b.bufferManager.PinPage(page.ResourcePageID(rightSiblingID))
	if err != nil {
		return false, err
	}
	defer b.bufferManager.UnpinPage(page.ResourcePageID(rightSiblingID), false)

	rightLeaf := &LeafLogicPage{data: rightSiblingPage.GetData()}
	currentLeaf := &LeafLogicPage{data: leafPage.GetData()}

	for i := uint16(0); i < currentLeaf.CellCount(); i++ {
		cell := currentLeaf.GetCell(currentLeaf.GetCellOffset(i))
		if err := rightLeaf.Insert(cell.Key, cell.Value); err != nil {
			return false, nil
		}
	}

	currentLeaf.SetRightSibling(rightLeaf.RightSibling())
	leafPage.SetDirty(true)


	return true, nil
}
