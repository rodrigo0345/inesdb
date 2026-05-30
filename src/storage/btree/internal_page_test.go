package btree

import (
	"bytes"
	"testing"
)

func TestNewInternalPage(t *testing.T) {
	pageSize := uint32(4096)
	page := NewInternalPage(pageSize)

	if page.PageType() != TypeInternal {
		t.Fatalf("expected PageType %d, got %d", TypeInternal, page.PageType())
	}
	if page.CellCount() != 0 {
		t.Fatalf("expected CellCount 0, got %d", page.CellCount())
	}
	if page.FreeSpacePointer() != uint16(pageSize) {
		t.Fatalf("expected FreeSpacePointer %d, got %d", pageSize, page.FreeSpacePointer())
	}
	if page.RightmostPointer() != 0 {
		t.Fatalf("expected RightmostPointer 0, got %d", page.RightmostPointer())
	}
}

func TestInternalPage_InsertAndSort(t *testing.T) {
	page := NewInternalPage(4096)

	err := page.Insert([]byte("C"), 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	page.Insert([]byte("A"), 1)
	page.Insert([]byte("B"), 2)

	if page.CellCount() != 3 {
		t.Fatalf("expected 3 cells, got %d", page.CellCount())
	}

	expectedKeys := []string{"A", "B", "C"}
	expectedPointers := []uint64{1, 2, 3}

	for i := uint16(0); i < page.CellCount(); i++ {
		offset := page.GetCellOffset(i)
		cell := page.GetCell(offset)

		if string(cell.Key) != expectedKeys[i] {
			t.Errorf("cell %d: expected key %s, got %s", i, expectedKeys[i], cell.Key)
		}
		if cell.ChildPointer != expectedPointers[i] {
			t.Errorf("cell %d: expected pointer %d, got %d", i, expectedPointers[i], cell.ChildPointer)
		}
	}
}

func TestInternalPage_Search(t *testing.T) {
	page := NewInternalPage(4096)
	page.SetRightmostPointer(99)

	page.Insert([]byte("apple"), 10)
	page.Insert([]byte("cherry"), 20)
	page.Insert([]byte("banana"), 15)

	tests := []struct {
		searchKey       []byte
		expectedPointer uint64
	}{
		{[]byte("aardvark"), 10},
		{[]byte("apple"), 15},
		{[]byte("apricot"), 15},
		{[]byte("banana"), 20},
		{[]byte("blueberry"), 20},
		{[]byte("cherry"), 99},
		{[]byte("zebra"), 99},
	}

	for _, tt := range tests {
		ptr := page.Search(tt.searchKey)
		if ptr != tt.expectedPointer {
			t.Errorf("Search(%s): expected pointer %d, got %d", tt.searchKey, tt.expectedPointer, ptr)
		}
	}
}

func TestInternalPage_Insert_PageFull(t *testing.T) {
	page := NewInternalPage(128)

	for i := 0; i < 8; i++ {
		key := []byte{byte('A' + i)}
		err := page.Insert(key, uint64(i))
		if err != nil {
			t.Fatalf("unexpected error on insertion %d: %v", i, err)
		}
	}

	err := page.Insert([]byte("Z"), 99)
	if err != ErrPageFull {
		t.Fatalf("expected ErrPageFull, got %v", err)
	}
}

func TestInternalPage_Vacuum(t *testing.T) {
	page := NewInternalPage(256)

	page.Insert([]byte("key1"), 1)
	page.Insert([]byte("key2"), 2)

	expectedFreeSpace := page.FreeSpacePointer()

	fragmentedSpace := expectedFreeSpace - 50
	page.SetFreeSpacePointer(fragmentedSpace)

	if page.FreeSpacePointer() != fragmentedSpace {
		t.Fatalf("failed to artificially fragment page")
	}

	page.Vacuum()

	if page.FreeSpacePointer() != expectedFreeSpace {
		t.Errorf("Vacuum failed to reclaim space. Expected FreeSpacePointer %d, got %d", expectedFreeSpace, page.FreeSpacePointer())
	}

	if page.CellCount() != 2 {
		t.Fatalf("expected 2 cells after vacuum, got %d", page.CellCount())
	}

	cell0 := page.GetCell(page.GetCellOffset(0))
	if !bytes.Equal(cell0.Key, []byte("key1")) {
		t.Errorf("expected cell 0 key 'key1', got '%s'", cell0.Key)
	}

	cell1 := page.GetCell(page.GetCellOffset(1))
	if !bytes.Equal(cell1.Key, []byte("key2")) {
		t.Errorf("expected cell 1 key 'key2', got '%s'", cell1.Key)
	}
}

func TestInternalPage_Split(t *testing.T) {
	pageSize := uint32(4096)
	page := NewInternalPage(pageSize)
	page.SetRightmostPointer(99)

	keys := []string{"A", "B", "C", "D", "E", "F"}
	for i, k := range keys {
		err := page.Insert([]byte(k), uint64(i+1))
		if err != nil {
			t.Fatalf("failed to insert %s: %v", k, err)
		}
	}

	newPage := NewInternalPage(pageSize)

	promotedKey := page.Split(newPage)

	if string(promotedKey) != "D" {
		t.Fatalf("expected promoted key 'D', got '%s'", promotedKey)
	}

	if page.CellCount() != 3 {
		t.Fatalf("expected left page 3 cells, got %d", page.CellCount())
	}
	if newPage.CellCount() != 2 {
		t.Fatalf("expected right page 2 cells, got %d", newPage.CellCount())
	}

	if page.RightmostPointer() != 4 {
		t.Fatalf("expected left rightmost pointer 4, got %d", page.RightmostPointer())
	}

	if newPage.RightmostPointer() != 99 {
		t.Fatalf("expected right rightmost pointer 99, got %d", newPage.RightmostPointer())
	}

	expectedLeftKeys := []string{"A", "B", "C"}
	expectedLeftPtrs := []uint64{1, 2, 3}
	for i := uint16(0); i < page.CellCount(); i++ {
		cell := page.GetCell(page.GetCellOffset(i))
		if string(cell.Key) != expectedLeftKeys[i] {
			t.Errorf("left page cell %d: expected %s, got %s", i, expectedLeftKeys[i], cell.Key)
		}
		if cell.ChildPointer != expectedLeftPtrs[i] {
			t.Errorf("left page cell %d: expected ptr %d, got %d", i, expectedLeftPtrs[i], cell.ChildPointer)
		}
	}

	expectedRightKeys := []string{"E", "F"}
	expectedRightPtrs := []uint64{5, 6}
	for i := uint16(0); i < newPage.CellCount(); i++ {
		cell := newPage.GetCell(newPage.GetCellOffset(i))
		if string(cell.Key) != expectedRightKeys[i] {
			t.Errorf("right page cell %d: expected %s, got %s", i, expectedRightKeys[i], cell.Key)
		}
		if cell.ChildPointer != expectedRightPtrs[i] {
			t.Errorf("right page cell %d: expected ptr %d, got %d", i, expectedRightPtrs[i], cell.ChildPointer)
		}
	}
}
