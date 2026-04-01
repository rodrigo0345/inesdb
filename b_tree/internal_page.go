package btree

import (
	"bytes"
	"encoding/binary"
	"sort"
)

const (
	InternalHeaderTypeOffset      = 0  // 2 bytes (PageType)
	InternalHeaderCellsOffset     = 2  // 2 bytes (Cell Count)
	InternalHeaderFreeSpaceOffset = 4  // 2 bytes (Free Space Pointer)
	InternalHeaderRightmostOffset = 6  // 8 bytes (Rightmost Child Page ID)
	InternalHeaderSize            = 14 // Total header size for internal pages

	InternalCellHeaderSize = 10 // 8 bytes (Child Page ID) + 2 bytes (Key Length)
)

type InternalPage struct {
	data []byte
}

func NewInternalPage(pageSize uint32) *InternalPage {
	p := &InternalPage{
		data: make([]byte, pageSize),
	}

	p.SetPageType(TypeInternal)
	p.SetCellCount(0)
	p.SetFreeSpacePointer(uint16(pageSize))
	p.SetRightmostPointer(0)

	return p
}

func (node *InternalPage) PageType() PageType {
	return PageType(binary.LittleEndian.Uint16(node.data[InternalHeaderTypeOffset:]))
}

func (node *InternalPage) CellCount() uint16 {
	return binary.LittleEndian.Uint16(node.data[InternalHeaderCellsOffset:])
}

func (node *InternalPage) FreeSpacePointer() uint16 {
	return binary.LittleEndian.Uint16(node.data[InternalHeaderFreeSpaceOffset:])
}

func (node *InternalPage) RightmostPointer() uint64 {
	return binary.LittleEndian.Uint64(node.data[InternalHeaderRightmostOffset:])
}

func (node *InternalPage) SetPageType(pageType PageType) {
	binary.LittleEndian.PutUint16(node.data[InternalHeaderTypeOffset:], uint16(pageType))
}

func (node *InternalPage) SetCellCount(count uint16) {
	binary.LittleEndian.PutUint16(node.data[InternalHeaderCellsOffset:], count)
}

func (node *InternalPage) SetFreeSpacePointer(pointer uint16) {
	binary.LittleEndian.PutUint16(node.data[InternalHeaderFreeSpaceOffset:], pointer)
}

func (node *InternalPage) SetRightmostPointer(pointer uint64) {
	binary.LittleEndian.PutUint64(node.data[InternalHeaderRightmostOffset:], pointer)
}

func (node *InternalPage) GetCellOffset(cellIndex uint16) uint16 {
	slotOffset := InternalHeaderSize + (cellIndex * SlotSize)
	return binary.LittleEndian.Uint16(node.data[slotOffset:])
}

type InternalCell struct {
	ChildPointer uint64
	Key          []byte
}

func (node *InternalPage) GetCell(offset uint16) InternalCell {
	offset32 := uint32(offset)

	childPtr := binary.LittleEndian.Uint64(node.data[offset32 : offset32+8])
	keyLen := uint32(binary.LittleEndian.Uint16(node.data[offset32+8 : offset32+10]))

	keyStart := offset32 + InternalCellHeaderSize

	return InternalCell{
		ChildPointer: childPtr,
		Key:          node.data[keyStart : keyStart+keyLen],
	}
}

func (node *InternalPage) WriteCell(offset uint16, key []byte, childPointer uint64) uint16 {
	offset32 := uint32(offset)
	keyLen := uint32(len(key))

	binary.LittleEndian.PutUint64(node.data[offset32:offset32+8], childPointer)
	binary.LittleEndian.PutUint16(node.data[offset32+8:offset32+10], uint16(keyLen))

	keyStart := offset32 + InternalCellHeaderSize
	copy(node.data[keyStart:], key)

	return uint16(InternalCellHeaderSize + keyLen)
}

func (node *InternalPage) Insert(key []byte, childPointer uint64) error {
	cellSize := uint16(InternalCellHeaderSize + len(key))
	spaceNeeded := cellSize + SlotSize

	slotArrayEnd := InternalHeaderSize + (node.CellCount() * SlotSize)
	availableSpace := node.FreeSpacePointer() - slotArrayEnd

	if availableSpace < spaceNeeded {
		absoluteUsedSpace := slotArrayEnd
		for i := uint16(0); i < node.CellCount(); i++ {
			c := node.GetCell(node.GetCellOffset(i))
			absoluteUsedSpace += uint16(InternalCellHeaderSize + len(c.Key))
		}
		actualFreeSpace := uint16(len(node.data)) - absoluteUsedSpace
		if actualFreeSpace >= spaceNeeded {
			node.Vacuum()
		} else {
			return ErrPageFull
		}
	}

	insertIndex := uint16(sort.Search(int(node.CellCount()), func(i int) bool {
		cell := node.GetCell(node.GetCellOffset(uint16(i)))
		return bytes.Compare(cell.Key, key) >= 0
	}))

	newFreeSpace := node.FreeSpacePointer() - cellSize
	node.SetFreeSpacePointer(newFreeSpace)
	node.WriteCell(newFreeSpace, key, childPointer)

	if insertIndex < node.CellCount() {
		insertPos := InternalHeaderSize + (insertIndex * SlotSize)
		endPos := InternalHeaderSize + (node.CellCount() * SlotSize)
		copy(node.data[insertPos+SlotSize:endPos+SlotSize], node.data[insertPos:endPos])
	}

	newSlotPos := InternalHeaderSize + (insertIndex * SlotSize)
	binary.LittleEndian.PutUint16(node.data[newSlotPos:], newFreeSpace)

	node.SetCellCount(node.CellCount() + 1)

	return nil
}

func (node *InternalPage) Search(key []byte) uint64 {
	cellCount := int(node.CellCount())

	index := sort.Search(cellCount, func(i int) bool {
		cell := node.GetCell(node.GetCellOffset(uint16(i)))
		return bytes.Compare(cell.Key, key) > 0
	})

	if index >= cellCount {
		return node.RightmostPointer()
	}

	cell := node.GetCell(node.GetCellOffset(uint16(index)))
	return cell.ChildPointer
}

func (node *InternalPage) Vacuum() {
	pageSize := uint32(len(node.data))
	tmp := NewInternalPage(pageSize)
	tmp.SetRightmostPointer(node.RightmostPointer())

	cellCount := node.CellCount()
	tmp.SetCellCount(cellCount)

	for i := uint16(0); i < cellCount; i++ {
		oldOffset := node.GetCellOffset(i)
		cell := node.GetCell(oldOffset)

		cellSize := uint16(InternalCellHeaderSize + len(cell.Key))

		newFreeSpace := tmp.FreeSpacePointer() - cellSize
		tmp.SetFreeSpacePointer(newFreeSpace)

		tmp.WriteCell(newFreeSpace, cell.Key, cell.ChildPointer)

		newSlotPos := InternalHeaderSize + (i * SlotSize)
		binary.LittleEndian.PutUint16(tmp.data[newSlotPos:], newFreeSpace)
	}

	copy(node.data, tmp.data)
}
