package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
)

type LeafPage struct {
	data []byte
}

func NewLeafPage(pageSize uint32) *LeafPage {
	p := &LeafPage{
		data: make([]byte, pageSize),
	}

	p.SetPageType(TypeLeaf)
	p.SetCellCount(0)

	// Free space must start at the dynamic page size, not the hardcoded default!
	p.SetFreeSpacePointer(uint16(pageSize))
	p.SetRightSibling(0)

	return p
}

func (node *LeafPage) GetCellOffset(cellIndex uint16) uint16 {
	slotOffset := LeafHeaderSize + (cellIndex * SlotSize)
	return binary.LittleEndian.Uint16(node.data[slotOffset:])
}

func (node *LeafPage) PageType() PageType {
	return PageType(binary.LittleEndian.Uint16(node.data[LeafHeaderTypeOffset:]))
}

func (node *LeafPage) CellCount() uint16 {
	return binary.LittleEndian.Uint16(node.data[LeafHeaderCellsOffset:])
}

func (node *LeafPage) FreeSpacePointer() uint16 {
	return binary.LittleEndian.Uint16(node.data[LeafHeaderFreeSpaceOffset:])
}

func (node *LeafPage) RightSibling() uint64 {
	return binary.LittleEndian.Uint64(node.data[LeafHeaderSiblingOffset:])
}

func (node *LeafPage) SetPageType(pageType PageType) {
	binary.LittleEndian.PutUint16(node.data[LeafHeaderTypeOffset:], uint16(pageType))
}

func (node *LeafPage) SetCellCount(count uint16) {
	binary.LittleEndian.PutUint16(node.data[LeafHeaderCellsOffset:], count)
}

func (node *LeafPage) SetFreeSpacePointer(pointer uint16) {
	binary.LittleEndian.PutUint16(node.data[LeafHeaderFreeSpaceOffset:], pointer)
}

func (node *LeafPage) SetRightSibling(siblingID uint64) {
	binary.LittleEndian.PutUint64(node.data[LeafHeaderSiblingOffset:], siblingID)
}

// pointers to the key and value, no copy used
type Cell struct {
	Key   []byte
	Value []byte
}

func (node *LeafPage) GetCell(offset uint16) Cell {
	offset32 := uint32(offset)

	keyLen := uint32(binary.LittleEndian.Uint16(node.data[offset32 : offset32+2]))
	valLen := binary.LittleEndian.Uint32(node.data[offset32+2 : offset32+6])

	keyStart := offset32 + CellHeaderSize
	valStart := keyStart + keyLen

	return Cell{
		Key:   node.data[keyStart : keyStart+keyLen],
		Value: node.data[valStart : valStart+valLen],
	}
}

func (node *LeafPage) WriteCell(offset uint16, key []byte, value []byte) uint16 {
	offset32 := uint32(offset)
	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	binary.LittleEndian.PutUint16(node.data[offset32:offset32+2], uint16(keyLen))
	binary.LittleEndian.PutUint32(node.data[offset32+2:offset32+6], valLen)

	keyStart := offset32 + CellHeaderSize
	copy(node.data[keyStart:], key)

	valStart := keyStart + keyLen
	copy(node.data[valStart:], value)

	return uint16(CellHeaderSize + keyLen + valLen)
}

var ErrPageFull = errors.New("page is full")
var ErrKeyNotFound = errors.New("key not found")

// insert a key-value pair to the leaf page.
// keeps the slot array sorted by key.
func (node *LeafPage) Insert(key []byte, value []byte) error {

	cellSize := uint16(CellHeaderSize + len(key) + len(value))
	spaceNeeded := cellSize + SlotSize

	slotArrayEnd := LeafHeaderSize + (node.CellCount() * SlotSize)
	availableSpace := node.FreeSpacePointer() - slotArrayEnd

	if availableSpace < spaceNeeded {
		// We might have fragmentation. Let's calculate the absolute used space.
		// Used space = Header + (Slots) + (Sum of all active cell payloads)
		absoluteUsedSpace := slotArrayEnd
		for i := uint16(0); i < node.CellCount(); i++ {
			c := node.GetCell(node.GetCellOffset(i))
			absoluteUsedSpace += uint16(CellHeaderSize + len(c.Key) + len(c.Value))
		}
		actualFreeSpace := uint16(len(node.data)) - absoluteUsedSpace
		if actualFreeSpace >= spaceNeeded {
			// There is enough space, it's just fragmented
			node.Vacuum()
		} else {
			return ErrPageFull
		}
	}

	// sort.Search returns the smallest index where the function evaluates to true
	insertIndex := uint16(sort.Search(int(node.CellCount()), func(i int) bool {
		cell := node.GetCell(node.GetCellOffset(uint16(i)))
		// bytes.Compare returns >= 0 if cell.Key is greater than or equal to our insert key
		return bytes.Compare(cell.Key, key) >= 0
	}))

	newFreeSpace := node.FreeSpacePointer() - cellSize
	node.SetFreeSpacePointer(newFreeSpace)
	node.WriteCell(newFreeSpace, key, value)

	// If we are not inserting at the very end, we need to shift slots to the right
	if insertIndex < node.CellCount() {
		insertPos := LeafHeaderSize + (insertIndex * SlotSize)
		endPos := LeafHeaderSize + (node.CellCount() * SlotSize)

		// built-in copy to safely shift overlapping byte slices right by 2 bytes
		copy(node.data[insertPos+SlotSize:endPos+SlotSize], node.data[insertPos:endPos])
	}

	newSlotPos := LeafHeaderSize + (insertIndex * SlotSize)
	binary.LittleEndian.PutUint16(node.data[newSlotPos:], newFreeSpace)

	node.SetCellCount(node.CellCount() + 1)

	return nil
}

func (node *LeafPage) Get(key []byte) ([]byte, error) {
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

// lazely done, vacuum does the rest of the job
func (node *LeafPage) Remove(key []byte) error {
	cellCount := int(node.CellCount())
	index := sort.Search(cellCount, func(i int) bool {
		cell := node.GetCell(node.GetCellOffset(uint16(i)))
		return bytes.Compare(cell.Key, key) >= 0
	})

	if index >= cellCount {
		return ErrKeyNotFound
	}

	// cell := node.GetCell(node.GetCellOffset(uint16(index)))
	// if !bytes.Equal(cell.Key, key) {
	//  return ErrKeyNotFound
	// }

	// shift the slot array to the left to erase the 2-byte pointer
	slotPos := LeafHeaderSize + (index * SlotSize)
	endPos := LeafHeaderSize + (cellCount * SlotSize)

	// shift everything after the deleted slot 2 bytes left
	if index < cellCount-1 {
		copy(node.data[slotPos:endPos-SlotSize], node.data[slotPos+SlotSize:endPos])
	}
	node.SetCellCount(uint16(cellCount - 1))
	return nil
}

func (node *LeafPage) Vacuum() {
	pageSize := uint32(len(node.data))
	tmp := NewLeafPage(pageSize)
	tmp.SetRightSibling(node.RightSibling())

	cellCount := node.CellCount()
	tmp.SetCellCount(cellCount)

	// Iterate through the active slots in the current page
	for i := uint16(0); i < cellCount; i++ {
		oldOffset := node.GetCellOffset(i)
		cell := node.GetCell(oldOffset)

		cellSize := uint16(CellHeaderSize + len(cell.Key) + len(cell.Value))

		newFreeSpace := tmp.FreeSpacePointer() - cellSize
		tmp.SetFreeSpacePointer(newFreeSpace)

		tmp.WriteCell(newFreeSpace, cell.Key, cell.Value)

		newSlotPos := LeafHeaderSize + (i * SlotSize)
		binary.LittleEndian.PutUint16(tmp.data[newSlotPos:], newFreeSpace)
	}

	// 4. Overwrite the current page's underlying byte array with the defragmented one
	copy(node.data, tmp.data)
}
