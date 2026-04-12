package btree

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestLeafPage_Initialization(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	if leaf.PageType() != TypeLeaf {
		t.Fatalf("expected PageType %d, got %d", TypeLeaf, leaf.PageType())
	}

	if leaf.CellCount() != 0 {
		t.Fatalf("expected 0 cells in new leaf, got %d", leaf.CellCount())
	}

	if leaf.FreeSpacePointer() != uint16(pageSize) {
		t.Fatalf("expected FreeSpacePointer %d, got %d", pageSize, leaf.FreeSpacePointer())
	}

	if leaf.RightSibling() != 0 {
		t.Fatalf("expected RightSibling 0, got %d", leaf.RightSibling())
	}
}

func TestLeafPage_SettersAndGetters(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	leaf.SetCellCount(5)
	leaf.SetFreeSpacePointer(3000)
	leaf.SetRightSibling(99)

	if leaf.CellCount() != 5 {
		t.Fatalf("expected 5 cells, got %d", leaf.CellCount())
	}
	if leaf.FreeSpacePointer() != 3000 {
		t.Fatalf("expected FreeSpacePointer 3000, got %d", leaf.FreeSpacePointer())
	}
	if leaf.RightSibling() != 99 {
		t.Fatalf("expected RightSibling 99, got %d", leaf.RightSibling())
	}
}

func TestLeafPage_SlotArrayMath(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)


	cell0Offset := uint16(4000)

	slot0Position := LeafHeaderSize + (0 * SlotSize)

	binary.LittleEndian.PutUint16(leaf.data[slot0Position:], cell0Offset)

	retrievedOffset := leaf.GetCellOffset(0)

	if retrievedOffset != cell0Offset {
		t.Fatalf("expected Cell 0 offset to be %d, got %d", cell0Offset, retrievedOffset)
	}
}

func TestLeafPage_WriteAndGetCell(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	key := []byte("hello")
	value := []byte("world")

	expectedSize := uint16(CellHeaderSize + len(key) + len(value))

	offset := uint16(4000)

	writtenSize := leaf.WriteCell(offset, key, value)
	if writtenSize != expectedSize {
		t.Fatalf("expected written size %d, got %d", expectedSize, writtenSize)
	}

	cell := leaf.GetCell(offset)
	if string(cell.Key) != string(key) {
		t.Fatalf("expected key '%s', got '%s'", string(key), string(cell.Key))
	}
	if string(cell.Value) != string(value) {
		t.Fatalf("expected value '%s', got '%s'", string(value), string(cell.Value))
	}
}

func TestLeafPage_ManualInsertSimulation(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	key := []byte("user_1")
	value := []byte("some_json_data")

	cellSize := uint16(CellHeaderSize + len(key) + len(value))

	newFreeSpace := leaf.FreeSpacePointer() - cellSize
	leaf.SetFreeSpacePointer(newFreeSpace)

	leaf.WriteCell(newFreeSpace, key, value)

	cellIndex := leaf.CellCount()
	slotPosition := LeafHeaderSize + (cellIndex * SlotSize)
	binary.LittleEndian.PutUint16(leaf.data[slotPosition:], newFreeSpace)

	leaf.SetCellCount(cellIndex + 1)
	if leaf.CellCount() != 1 {
		t.Fatalf("expected 1 cell, got %d", leaf.CellCount())
	}

	retrievedOffset := leaf.GetCellOffset(0)
	if retrievedOffset != newFreeSpace {
		t.Fatalf("slot array mismatch: expected offset %d, got %d", newFreeSpace, retrievedOffset)
	}

	cell := leaf.GetCell(retrievedOffset)
	if string(cell.Key) != string(key) {
		t.Fatalf("expected key '%s', got '%s'", string(key), string(cell.Key))
	}
	if string(cell.Value) != string(value) {
		t.Fatalf("expected value '%s', got '%s'", string(value), string(cell.Value))
	}
}

func BenchmarkLeafPage_WriteCell(b *testing.B) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	key := []byte("user_12345")
	value := []byte("this_is_some_user_data_payload")
	offset := uint16(100)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		leaf.WriteCell(offset, key, value)
	}
}

func BenchmarkLeafPage_GetCell(b *testing.B) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	key := []byte("user_12345")
	value := []byte("this_is_some_user_data_payload")
	offset := uint16(100)

	leaf.WriteCell(offset, key, value)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = leaf.GetCell(offset)
	}
}

func TestLeafPage_RemoveAndVacuum(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	key1 := []byte("A_key")
	val1 := []byte("A_value")
	key2 := []byte("B_key")
	val2 := []byte("B_value")

	leaf.Insert(key1, val1)
	leaf.Insert(key2, val2)

	if leaf.CellCount() != 2 {
		t.Fatalf("expected 2 cells, got %d", leaf.CellCount())
	}

	spaceBeforeDelete := leaf.FreeSpacePointer()

	err := leaf.Remove(key1)
	if err != nil {
		t.Fatalf("failed to remove key: %v", err)
	}

	if leaf.CellCount() != 1 {
		t.Fatalf("expected 1 cell after removal, got %d", leaf.CellCount())
	}

	cell := leaf.GetCell(leaf.GetCellOffset(0))
	if string(cell.Key) != "B_key" {
		t.Fatalf("expected remaining key to be B_key, got %s", cell.Key)
	}

	if leaf.FreeSpacePointer() != spaceBeforeDelete {
		t.Fatalf("free space pointer shifted prematurely")
	}

	leaf.Vacuum()

	if leaf.FreeSpacePointer() <= spaceBeforeDelete {
		t.Fatalf("vacuum failed to reclaim space. pointer is %d", leaf.FreeSpacePointer())
	}

	cellAfterVacuum := leaf.GetCell(leaf.GetCellOffset(0))
	if string(cellAfterVacuum.Key) != "B_key" {
		t.Fatalf("data corrupted after vacuum, got %s", cellAfterVacuum.Key)
	}
}

func TestLeafPage_Insert_PageFull(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	massiveValue := make([]byte, 4050)

	err := leaf.Insert([]byte("giant_key"), massiveValue)
	if err != nil {
		t.Fatalf("failed to insert initial massive payload: %v", err)
	}

	err = leaf.Insert([]byte("tiny_key"), []byte("tiny_value"))

	if err == nil {
		t.Fatalf("expected an error, but insert succeeded!")
	}
	if err != ErrPageFull {
		t.Fatalf("expected ErrPageFull, got %v", err)
	}
}

func TestLeafPage_Insert_VacuumLimits(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	val1 := make([]byte, 2000)
	leaf.Insert([]byte("key1"), val1)

	val2 := make([]byte, 1000)
	leaf.Insert([]byte("key2"), val2)
	leaf.Remove([]byte("key1"))

	val3 := make([]byte, 2500)
	err := leaf.Insert([]byte("key3"), val3)
	if err != nil {
		t.Fatalf("expected insert to succeed by automatically vacuuming, but failed: %v", err)
	}

	val4 := make([]byte, 1000)
	err = leaf.Insert([]byte("key4"), val4)

	if err != ErrPageFull {
		t.Fatalf("expected ErrPageFull after vacuum exhaustion, got %v", err)
	}
}

func TestLeafPage_Get(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	leaf.Insert([]byte("zebra"), []byte("stripes"))
	leaf.Insert([]byte("apple"), []byte("red"))
	leaf.Insert([]byte("mango"), []byte("yellow"))

	val, err := leaf.Get([]byte("apple"))
	if err != nil {
		t.Fatalf("expected to find apple, got error: %v", err)
	}
	if string(val) != "red" {
		t.Fatalf("expected 'red', got '%s'", val)
	}

	val, err = leaf.Get([]byte("zebra"))
	if err != nil {
		t.Fatalf("expected to find zebra, got error: %v", err)
	}
	if string(val) != "stripes" {
		t.Fatalf("expected 'stripes', got '%s'", val)
	}

	_, err = leaf.Get([]byte("banana"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for missing key, got %v", err)
	}
}

func TestLeafPage_GetAfterRemove(t *testing.T) {
	pageSize := uint32(4096)
	leaf := NewLeafPage(pageSize)

	leaf.Insert([]byte("ghost"), []byte("boo"))
	leaf.Insert([]byte("goblin"), []byte("trick"))
	leaf.Remove([]byte("ghost"))

	_, err := leaf.Get([]byte("ghost"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after deletion, got %v", err)
	}

	val, err := leaf.Get([]byte("goblin"))
	if err != nil {
		t.Fatalf("failed to get remaining record: %v", err)
	}
	if string(val) != "trick" {
		t.Fatalf("expected 'trick', got '%s'", val)
	}
}

func TestLeafPage_Split(t *testing.T) {
	pageSize := uint32(4096)
	page := NewLeafPage(pageSize)
	page.SetRightSibling(99)

	keys := []string{"A", "B", "C", "D", "E", "F"}
	for _, k := range keys {
		err := page.Insert([]byte(k), []byte(k+"_val"))
		if err != nil {
			t.Fatalf("failed to insert %s: %v", k, err)
		}
	}

	newPage := NewLeafPage(pageSize)
	newPageID := uint64(2)

	splitKey := page.Split(newPage, newPageID)

	if string(splitKey) != "D" {
		t.Fatalf("expected split key 'D', got '%s'", splitKey)
	}

	if page.CellCount() != 3 {
		t.Fatalf("expected left page 3 cells, got %d", page.CellCount())
	}
	if newPage.CellCount() != 3 {
		t.Fatalf("expected right page 3 cells, got %d", newPage.CellCount())
	}

	expectedLeft := []string{"A", "B", "C"}
	for i := uint16(0); i < page.CellCount(); i++ {
		cell := page.GetCell(page.GetCellOffset(i))
		if string(cell.Key) != expectedLeft[i] {
			t.Errorf("left page cell %d: expected %s, got %s", i, expectedLeft[i], cell.Key)
		}
	}

	expectedRight := []string{"D", "E", "F"}
	for i := uint16(0); i < newPage.CellCount(); i++ {
		cell := newPage.GetCell(newPage.GetCellOffset(i))
		if string(cell.Key) != expectedRight[i] {
			t.Errorf("right page cell %d: expected %s, got %s", i, expectedRight[i], cell.Key)
		}
	}

	if page.RightSibling() != newPageID {
		t.Fatalf("expected left sibling %d, got %d", newPageID, page.RightSibling())
	}
	if newPage.RightSibling() != 99 {
		t.Fatalf("expected right sibling 99, got %d", newPage.RightSibling())
	}
}

func BenchmarkLeafPage_Split(b *testing.B) {
	pageSize := uint32(4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		page := NewLeafPage(pageSize)
		for j := 0; j < 100; j++ {
			key := []byte(fmt.Sprintf("key_%04d", j))
			val := []byte(fmt.Sprintf("val_%04d", j))
			page.Insert(key, val)
		}
		newPage := NewLeafPage(pageSize)
		b.StartTimer()

		page.Split(newPage, uint64(i+1))
	}
}
