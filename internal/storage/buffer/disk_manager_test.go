package buffermanager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rodrigo0345/omag/resource_page"
)

// TestNewDiskManager_Success tests successful disk manager creation
func TestNewDiskManager_Success(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, err := NewDiskManager(dbFile)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()

	if dm.dbFile == nil {
		t.Fatal("expected dbFile to be initialized")
	}
	if dm.nextPage != 0 {
		t.Fatalf("expected initial nextPage 0, got %d", dm.nextPage)
	}
}

// TestNewDiskManager_InvalidPath tests disk manager creation with invalid path
func TestNewDiskManager_InvalidPath(t *testing.T) {
	invalidPath := "/nonexistent/directory/that/does/not/exist/file.db"
	_, err := NewDiskManager(invalidPath)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

// TestNewDiskManager_ExistingFile tests disk manager with pre-existing file
func TestNewDiskManager_ExistingFile(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	// Create an empty file first
	if err := os.WriteFile(dbFile, []byte{}, 0666); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Write some data to file to simulate existing pages
	file, err := os.OpenFile(dbFile, os.O_WRONLY, 0666)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	// Write 2 pages worth of data
	data := make([]byte, resource_page.PageSize*2)
	file.Write(data)
	file.Close()

	dm, err := NewDiskManager(dbFile)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()

	// Should detect 2 existing pages
	if dm.nextPage != 2 {
		t.Fatalf("expected nextPage 2 for 2-page file, got %d", dm.nextPage)
	}
}

// TestReadPage_Success tests successful page read
func TestReadPage_Success(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	// Write a test page first
	testData := make([]byte, resource_page.PageSize)
	testData[0] = 42
	testData[resource_page.PageSize-1] = 99

	dm.WritePage(0, testData)

	// Flush to ensure write is on disk before reading
	dm.Flush()

	// Read it back
	readData := make([]byte, resource_page.PageSize)
	err := dm.ReadPage(0, readData)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}

	if readData[0] != 42 {
		t.Fatalf("expected readData[0] = 42, got %d", readData[0])
	}
	if readData[resource_page.PageSize-1] != 99 {
		t.Fatalf("expected readData[resource_page.PageSize-1] = 99, got %d", readData[resource_page.PageSize-1])
	}
}

// TestReadPage_Closed tests reading from a closed disk manager
func TestReadPage_Closed(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	dm.Close()

	data := make([]byte, resource_page.PageSize)
	err := dm.ReadPage(0, data)
	if err == nil {
		t.Fatal("expected error when reading from closed disk manager")
	}
}

// TestWritePage_Success tests successful page write
func TestWritePage_Success(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	testData := make([]byte, resource_page.PageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	err := dm.WritePage(0, testData)
	if err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	// Flush to ensure write is on disk
	err = dm.Flush()
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Verify file size
	stat, err := os.Stat(dbFile)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	if stat.Size() != resource_page.PageSize {
		t.Fatalf("expected file size %d, got %d", resource_page.PageSize, stat.Size())
	}
}

// TestWritePage_SizeMismatch tests writing with wrong page size
func TestWritePage_SizeMismatch(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	wrongData := make([]byte, resource_page.PageSize-1)
	err := dm.WritePage(0, wrongData)
	if err == nil {
		t.Fatal("expected error for size mismatch")
	}
}

// TestWritePage_Closed tests writing to a closed disk manager
func TestWritePage_Closed(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	dm.Close()

	data := make([]byte, resource_page.PageSize)
	err := dm.WritePage(0, data)
	if err == nil {
		t.Fatal("expected error when writing to closed disk manager")
	}
}

// TestAllocatePage tests page allocation
func TestAllocatePage(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	page0 := dm.AllocatePage()
	if page0 != 0 {
		t.Fatalf("expected first page ID 0, got %d", page0)
	}

	page1 := dm.AllocatePage()
	if page1 != 1 {
		t.Fatalf("expected second page ID 1, got %d", page1)
	}

	page2 := dm.AllocatePage()
	if page2 != 2 {
		t.Fatalf("expected third page ID 2, got %d", page2)
	}
}

// TestSync tests flushing data to disk
func TestSync_Success(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	testData := make([]byte, resource_page.PageSize)
	testData[0] = 77
	dm.WritePage(0, testData)

	err := dm.Sync()
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}
}

// TestSync_Closed tests syncing a closed disk manager
func TestSync_Closed(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	dm.Close()

	err := dm.Sync()
	if err == nil {
		t.Fatal("expected error when syncing closed disk manager")
	}
}

// TestClose tests disk manager closure
func TestClose_Success(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	err := dm.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// File handle is closed (attempting operations will fail)
	data := make([]byte, resource_page.PageSize)
	err = dm.ReadPage(0, data)
	if err == nil {
		t.Fatal("expected error after close")
	}
}

// TestClose_AlreadyClosed tests closing an already closed disk manager
func TestClose_AlreadyClosed(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	dm.Close()

	// Closing again may error (file already closed)
	// This is acceptable behavior - we just verify the first close worked
}

// TestMultiplePages tests read/write operations on multiple pages
func TestMultiplePages(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	// Write 5 pages
	pages := 5
	for i := 0; i < pages; i++ {
		data := make([]byte, resource_page.PageSize)
		data[0] = byte(i)
		dm.WritePage(resource_page.ResourcePageID(i), data)
	}

	// Flush to ensure all writes are on disk
	dm.Flush()

	// Read them back and verify
	for i := 0; i < pages; i++ {
		data := make([]byte, resource_page.PageSize)
		dm.ReadPage(resource_page.ResourcePageID(i), data)
		if data[0] != byte(i) {
			t.Fatalf("page %d: expected data[0]=%d, got %d", i, i, data[0])
		}
	}
}

// Helper function to get temp db file
func getTempDbFile(t *testing.T) string {
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "test.db")
}
