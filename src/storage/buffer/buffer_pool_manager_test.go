package buffer

import (
	"path/filepath"
	"testing"
)

func TestNewBufferPoolManager(t *testing.T) {
	dm := createTestDiskManager(t)
	defer dm.Close()

	bpm := NewBufferPoolManager(10, dm)

	switch v := bpm.(type) {
	case *BufferPoolManager:
		if v.poolSize != 10 {
			t.Fatalf("expected poolSize 10, got %d", v.poolSize)
		}
		if len(v.frames) != 10 {
			t.Fatalf("expected 10 frames, got %d", len(v.frames))
		}
		if len(v.freeList) != 10 {
			t.Fatalf("expected 10 free frames, got %d", len(v.freeList))
		}
		if len(v.pageTable) != 0 {
			t.Fatalf("expected empty pageTable, got %d entries", len(v.pageTable))
		}
	default:
		t.Fatalf("expected BufferPoolManager type, got %T", bpm)
	}

}

func TestNewPage_Standard(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	pageRef, err := bpm.NewPage()
	page := *pageRef
	if err != nil {
		t.Fatalf("failed to create new page: %v", err)
	}

	if page == nil {
		t.Fatal("expected page to be non-nil")
	}
	if page.GetPinCount() != 1 {
		t.Fatalf("expected pin count 1, got %d", page.GetPinCount())
	}
	if page.IsDirty() {
		t.Fatal("expected new page to not be dirty")
	}

	if len(bpm.freeList) != 9 {
		t.Fatalf("expected 9 remaining free frames, got %d", len(bpm.freeList))
	}

	bpm.UnpinPage(page.GetID(), false)
}

func TestNewPage_BufferFull(t *testing.T) {
	bpm := createTestBufferPool(t, 2)

	page1Ref, _ := bpm.NewPage()
	page1 := *page1Ref
	page2Ref, _ := bpm.NewPage()
	page2 := *page2Ref

	page3, err := bpm.NewPage()
	if err != ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v or page %v", err, page3)
	}

	bpm.UnpinPage(page1.GetID(), false)
	bpm.UnpinPage(page2.GetID(), false)
}

func TestFetchPage_NewPage(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	page, err := bpm.PinPage(99)
	if err != nil {
		t.Fatalf("failed to fetch page: %v", err)
	}

	if page == nil {
		t.Fatal("expected page to be non-nil")
	}
	if page.GetPinCount() != 1 {
		t.Fatalf("expected pin count 1, got %d", page.GetPinCount())
	}

	bpm.UnpinPage(99, false)
}

func TestFetchPage_AlreadyCached(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	page1, _ := bpm.PinPage(50)
	page1Pin1 := page1.GetPinCount()

	page2, _ := bpm.PinPage(50)
	if page1 != page2 {
		t.Fatal("expected same page object")
	}

	page2Pin := page2.GetPinCount()
	if page2Pin != page1Pin1+1 {
		t.Fatalf("expected pin count %d, got %d", page1Pin1+1, page2Pin)
	}

	bpm.UnpinPage(50, false)
	bpm.UnpinPage(50, false)
}

func TestFetchPage_BufferFull(t *testing.T) {
	bpm := createTestBufferPool(t, 1)

	page1Ref, _ := bpm.NewPage()
	page1 := *page1Ref
	page1.SetDirty(true)

	_, err := bpm.PinPage(100)
	if err != ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}

	bpm.UnpinPage(page1.GetID(), true)
}

func TestFetchPage_EvictsAndFlushes(t *testing.T) {
	bpm := createTestBufferPool(t, 2)

	page1Ref, _ := bpm.NewPage()
	page1 := *page1Ref
	page1.SetDirty(true)
	id1 := page1.GetID()

	bpm.UnpinPage(id1, true)

	page2Ref, _ := bpm.NewPage()
	page2 := *page2Ref
	id2 := page2.GetID()
	bpm.UnpinPage(id2, true)

	page3, _ := bpm.PinPage(300)
	page3.SetDirty(true)

	if _, exists := bpm.pageTable[id1]; exists {
		t.Fatal("expected page1 to be evicted from page table")
	}

	bpm.UnpinPage(page3.GetID(), true)
}

func TestUnpinPage_Success(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	page, _ := bpm.PinPage(1)
	if page.GetPinCount() != 1 {
		t.Fatalf("expected pin count 1, got %d", page.GetPinCount())
	}

	err := bpm.UnpinPage(1, false)
	if err != nil {
		t.Fatalf("failed to unpin: %v", err)
	}

	if page.GetPinCount() != 0 {
		t.Fatalf("expected pin count 0, got %d", page.GetPinCount())
	}
	if page.IsDirty() {
		t.Fatal("expected page to not be dirty after unpin with isDirty=false")
	}
}

func TestUnpinPage_MarkDirty(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	page, _ := bpm.PinPage(2)
	err := bpm.UnpinPage(2, true)
	if err != nil {
		t.Fatalf("failed to unpin: %v", err)
	}

	if !page.IsDirty() {
		t.Fatal("expected page to be dirty after unpin with isDirty=true")
	}
}

func TestUnpinPage_NotFound(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	err := bpm.UnpinPage(999, false)
	if err != ErrPageNotFound {
		t.Fatalf("expected ErrPageNotFound, got %v", err)
	}
}

func TestUnpinPage_ZeroPinCount(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	page, _ := bpm.PinPage(3)
	bpm.UnpinPage(3, false)

	err := bpm.UnpinPage(3, false)
	if err != nil {
		t.Fatalf("failed to unpin second time: %v", err)
	}

	if page.GetPinCount() < 0 {
		t.Fatalf("expected non-negative pin count, got %d", page.GetPinCount())
	}
}

func TestFlushPage_Success(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	pageRef, _ := bpm.NewPage()
	page := *pageRef
	pageID := page.GetID()
	page.SetDirty(true)
	pageData := page.GetData()
	pageData[0] = 42

	err := bpm.FlushPage(pageID)
	if err != nil {
		t.Fatalf("failed to flush page: %v", err)
	}

	if page.IsDirty() {
		t.Fatal("expected page to not be dirty after flush")
	}

	bpm.UnpinPage(pageID, false)
}

func TestFlushPage_NotFound(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	err := bpm.FlushPage(999)
	if err != ErrPageNotFound {
		t.Fatalf("expected ErrPageNotFound, got %v", err)
	}
}

func TestFlushAll_Success(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	page1Ref, _ := bpm.NewPage()
	page1 := *page1Ref
	page1.SetDirty(true)
	id1 := page1.GetID()

	page2Ref, _ := bpm.NewPage()
	page2 := *page2Ref
	page2.SetDirty(true)
	id2 := page2.GetID()

	page3Ref, _ := bpm.NewPage()
	page3 := *page3Ref
	page3.SetDirty(true)
	id3 := page3.GetID()

	err := bpm.FlushAll()
	if err != nil {
		t.Fatalf("failed to flush all: %v", err)
	}

	if page1.IsDirty() {
		t.Fatal("expected page1 to not be dirty")
	}
	if page2.IsDirty() {
		t.Fatal("expected page2 to not be dirty")
	}
	if page3.IsDirty() {
		t.Fatal("expected page3 to not be dirty")
	}

	bpm.UnpinPage(id1, false)
	bpm.UnpinPage(id2, false)
	bpm.UnpinPage(id3, false)
}

func TestFlushAll_Empty(t *testing.T) {
	bpm := createTestBufferPool(t, 10)

	err := bpm.FlushAll()
	if err != nil {
		t.Fatalf("expected no error on empty buffer, got %v", err)
	}
}

func TestClose_AllPagesFlushed(t *testing.T) {
	dm := createTestDiskManager(t)
	bpm := NewBufferPoolManager(10, dm)

	page1Ref, _ := bpm.NewPage()
	page1 := *page1Ref
	page1.SetDirty(true)

	page2Ref, _ := bpm.NewPage()
	page2 := *page2Ref
	page2.SetDirty(true)

	if !page1.IsDirty() || !page2.IsDirty() {
		t.Fatal("expected pages to be dirty before close")
	}

	err := bpm.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

}

func TestPageReuse(t *testing.T) {
	bpm := createTestBufferPool(t, 2)

	page1Ref, _ := bpm.NewPage()
	page1 := *page1Ref
	id1 := page1.GetID()
	bpm.UnpinPage(id1, false)

	page2Ref, _ := bpm.NewPage()
	page2 := *page2Ref
	id2 := page2.GetID()
	bpm.UnpinPage(id2, false)

	page3Ref, _ := bpm.NewPage()
	page3 := *page3Ref
	id3 := page3.GetID()

	if _, exists := bpm.pageTable[id1]; exists {
		t.Fatal("expected page1 to be evicted")
	}

	if _, exists := bpm.pageTable[id3]; !exists {
		t.Fatal("expected page3 to be in pageTable")
	}

	bpm.UnpinPage(id3, false)
}

func TestGetPoolSize(t *testing.T) {
	testCases := []int{1, 5, 10, 20, 50, 100}

	for _, size := range testCases {
		bpm := createTestBufferPool(t, size)
		if bpm.GetPoolSize() != size {
			t.Fatalf("expected pool size %d, got %d", size, bpm.GetPoolSize())
		}
	}
}

func createTestDiskManager(t *testing.T) *DiskManager {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	dm, err := NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	return dm
}

func createTestBufferPool(t *testing.T, poolSize int) *BufferPoolManager {
	dm := createTestDiskManager(t)
	bfpoll := NewBufferPoolManager(poolSize, dm)
	switch v := bfpoll.(type) {
	case *BufferPoolManager:
		return v
	default:
		t.Fatalf("expected BufferPoolManager type, got %T", bfpoll)
		return nil
	}
}
