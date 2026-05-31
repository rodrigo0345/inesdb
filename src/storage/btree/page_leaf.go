package btree

import (
	"bytes"
	"errors"
	"sort"

	"github.com/rodrigo0345/omag/src/storage/schema"
)

const (
	LeafHeaderTypeOffset      = 0
	LeafHeaderCellsOffset     = 2
	LeafHeaderFreeSpaceOffset = 4
	LeafHeaderSiblingOffset   = 6
	LeafHeaderSize            = 14
)

type LeafLogicPage struct {
	data []byte
}

type Cell struct {
	Key        []byte
	Value      []byte
	OverflowID uint64
}

func (node *LeafLogicPage) Insert(key []byte, value []byte) error {

	cellSize := uint16(CellHeaderSize + len(key) + len(value))
	spaceNeeded := cellSize + SlotSize

	slotArrayEnd := LeafHeaderSize + (node.CellCount() * SlotSize)
	availableSpace := node.FreeSpacePointer() - slotArrayEnd

	if availableSpace < spaceNeeded {
		absoluteUsedSpace := slotArrayEnd
		for i := uint16(0); i < node.CellCount(); i++ {
			c := node.GetCell(node.GetCellOffset(i))
			absoluteUsedSpace += uint16(CellHeaderSize + len(c.Key) + len(c.Value))
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
	node.WriteCell(newFreeSpace, key, value)

	if insertIndex < node.CellCount() {
		insertPos := LeafHeaderSize + (insertIndex * SlotSize)
		endPos := LeafHeaderSize + (node.CellCount() * SlotSize)

		copy(node.data[insertPos+SlotSize:endPos+SlotSize], node.data[insertPos:endPos])
	}

	newSlotPos := LeafHeaderSize + (insertIndex * SlotSize)
	schema.DbEndian.PutUint16(node.data[newSlotPos:], newFreeSpace)

	node.SetCellCount(node.CellCount() + 1)

	return nil
}

func (node *LeafLogicPage) Get(key []byte) ([]byte, error) {
	cellCount := int(node.CellCount())

	index := sort.Search(cellCount, func(i int) bool {
		cell := node.GetCell(node.GetCellOffset(uint16(i)))
		return bytes.Compare(cell.Key, key) >= 0
	})

	if index < cellCount {
		cell := node.GetCell(node.GetCellOffset(uint16(index)))
		if bytes.Equal(cell.Key, key) {
			return cell.Value, nil
		}
	}

	return nil, ErrKeyNotFound
}

func (node *LeafLogicPage) Remove(key []byte) error {
	cellCount := int(node.CellCount())
	index := sort.Search(cellCount, func(i int) bool {
		cell := node.GetCell(node.GetCellOffset(uint16(i)))
		return bytes.Compare(cell.Key, key) >= 0
	})

	if index >= cellCount {
		return ErrKeyNotFound
	}

	slotPos := LeafHeaderSize + (index * SlotSize)
	endPos := LeafHeaderSize + (cellCount * SlotSize)

	if index < cellCount-1 {
		copy(node.data[slotPos:endPos-SlotSize], node.data[slotPos+SlotSize:endPos])
	}
	node.SetCellCount(uint16(cellCount - 1))
	return nil
}

func (node *LeafLogicPage) Vacuum() {
	pageSize := uint32(len(node.data))
	tmp := NewLeafPage(pageSize)
	tmp.SetRightSibling(node.RightSibling())

	cellCount := node.CellCount()
	tmp.SetCellCount(cellCount)

	for i := uint16(0); i < cellCount; i++ {
		oldOffset := node.GetCellOffset(i)
		cell := node.GetCell(oldOffset)

		cellSize := uint16(CellHeaderSize + len(cell.Key) + len(cell.Value))

		newFreeSpace := tmp.FreeSpacePointer() - cellSize
		tmp.SetFreeSpacePointer(newFreeSpace)

		tmp.WriteCell(newFreeSpace, cell.Key, cell.Value)

		newSlotPos := LeafHeaderSize + (i * SlotSize)
		schema.DbEndian.PutUint16(tmp.data[newSlotPos:], newFreeSpace)
	}

	copy(node.data, tmp.data)
}

func (node *LeafLogicPage) Split(newPage *LeafLogicPage, newPageID uint64) []byte {
	cellCount := node.CellCount()
	midIndex := cellCount / 2

	var splitKey []byte

	for i := midIndex; i < cellCount; i++ {
		cell := node.GetCell(node.GetCellOffset(i))
		if i == midIndex {
			splitKey = make([]byte, len(cell.Key))
			copy(splitKey, cell.Key)
		}

		newPage.Insert(cell.Key, cell.Value)
	}

	newPage.SetRightSibling(node.RightSibling())
	node.SetRightSibling(newPageID)

	pageSize := uint32(len(node.data))
	tmp := NewLeafPage(pageSize)
	tmp.SetRightSibling(node.RightSibling())
	tmp.SetCellCount(midIndex)

	for i := uint16(0); i < midIndex; i++ {
		oldOffset := node.GetCellOffset(i)
		cell := node.GetCell(oldOffset)

		cellSize := uint16(CellHeaderSize + len(cell.Key) + len(cell.Value))
		newFreeSpace := tmp.FreeSpacePointer() - cellSize

		tmp.SetFreeSpacePointer(newFreeSpace)
		tmp.WriteCell(newFreeSpace, cell.Key, cell.Value)

		newSlotPos := LeafHeaderSize + (i * SlotSize)
		schema.DbEndian.PutUint16(tmp.data[newSlotPos:], newFreeSpace)
	}

	copy(node.data, tmp.data)

	return splitKey
}

func NewLeafPage(pageSize uint32) *LeafLogicPage {
	p := &LeafLogicPage{
		data: make([]byte, pageSize),
	}

	p.SetPageType(TypeLeaf)
	p.SetCellCount(0)

	p.SetFreeSpacePointer(uint16(pageSize))
	p.SetRightSibling(0)

	return p
}

func (node *LeafLogicPage) GetCellOffset(cellIndex uint16) uint16 {
	slotOffset := LeafHeaderSize + (cellIndex * SlotSize)
	return schema.DbEndian.Uint16(node.data[slotOffset:])
}

func (node *LeafLogicPage) PageType() LogicPageType {
	return LogicPageType(schema.DbEndian.Uint16(node.data[LeafHeaderTypeOffset:]))
}

func (node *LeafLogicPage) CellCount() uint16 {
	return schema.DbEndian.Uint16(node.data[LeafHeaderCellsOffset:])
}

func (node *LeafLogicPage) FreeSpacePointer() uint16 {
	return schema.DbEndian.Uint16(node.data[LeafHeaderFreeSpaceOffset:])
}

func (node *LeafLogicPage) RightSibling() uint64 {
	return schema.DbEndian.Uint64(node.data[LeafHeaderSiblingOffset:])
}

func (node *LeafLogicPage) SetPageType(pageType LogicPageType) {
	schema.DbEndian.PutUint16(node.data[LeafHeaderTypeOffset:], uint16(pageType))
}

func (node *LeafLogicPage) SetCellCount(count uint16) {
	schema.DbEndian.PutUint16(node.data[LeafHeaderCellsOffset:], count)
}

func (node *LeafLogicPage) SetFreeSpacePointer(pointer uint16) {
	schema.DbEndian.PutUint16(node.data[LeafHeaderFreeSpaceOffset:], pointer)
}

func (node *LeafLogicPage) SetRightSibling(siblingID uint64) {
	schema.DbEndian.PutUint64(node.data[LeafHeaderSiblingOffset:], siblingID)
}

func (node *LeafLogicPage) GetCell(offset uint16) Cell {
	offset32 := uint32(offset)

	keyLen := uint32(schema.DbEndian.Uint16(node.data[offset32 : offset32+2]))
	valLen := schema.DbEndian.Uint32(node.data[offset32+2 : offset32+6])
	overflowID := schema.DbEndian.Uint64(node.data[offset32+6 : offset32+14])

	keyStart := offset32 + CellHeaderSize
	valStart := keyStart + keyLen

	return Cell{
		Key:        node.data[keyStart : keyStart+keyLen],
		Value:      node.data[valStart : valStart+valLen],
		OverflowID: overflowID,
	}
}

func (node *LeafLogicPage) WriteCell(offset uint16, key []byte, value []byte) uint16 {
	return node.WriteCellWithOverflow(offset, key, value, 0)
}

func (node *LeafLogicPage) WriteCellWithOverflow(offset uint16, key []byte, value []byte, overflowID uint64) uint16 {
	offset32 := uint32(offset)
	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	schema.DbEndian.PutUint16(node.data[offset32:offset32+2], uint16(keyLen))
	schema.DbEndian.PutUint32(node.data[offset32+2:offset32+6], valLen)
	schema.DbEndian.PutUint64(node.data[offset32+6:offset32+14], overflowID)

	keyStart := offset32 + CellHeaderSize
	copy(node.data[keyStart:], key)

	valStart := keyStart + keyLen
	copy(node.data[valStart:], value)

	return uint16(CellHeaderSize + keyLen + valLen)
}

var ErrPageFull = errors.New("page is full")
var ErrKeyNotFound = errors.New("key not found")
