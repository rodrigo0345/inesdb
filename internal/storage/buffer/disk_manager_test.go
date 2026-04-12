package buffer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/page"
)

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

func TestNewDiskManager_InvalidPath(t *testing.T) {
	invalidPath := "/nonexistent/directory/that/does/not/exist/file.db"
	_, err := NewDiskManager(invalidPath)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestNewDiskManager_ExistingFile(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	if err := os.WriteFile(dbFile, []byte{}, 0666); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	file, err := os.OpenFile(dbFile, os.O_WRONLY, 0666)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	data := make([]byte, page.PageSize*2)
	file.Write(data)
	file.Close()

	dm, err := NewDiskManager(dbFile)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()

	if dm.nextPage != 2 {
		t.Fatalf("expected nextPage 2 for 2-page file, got %d", dm.nextPage)
	}
}

func TestReadPage_Success(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	testData := make([]byte, page.PageSize)
	testData[0] = 42
	testData[page.PageSize-1] = 99

	dm.WritePage(0, testData)

	dm.Flush()

	readData := make([]byte, page.PageSize)
	err := dm.ReadPage(0, readData)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}

	if readData[0] != 42 {
		t.Fatalf("expected readData[0] = 42, got %d", readData[0])
	}
	if readData[page.PageSize-1] != 99 {
		t.Fatalf("expected readData[page.PageSize-1] = 99, got %d", readData[page.PageSize-1])
	}
}

func TestReadPage_Closed(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	dm.Close()

	data := make([]byte, page.PageSize)
	err := dm.ReadPage(0, data)
	if err == nil {
		t.Fatal("expected error when reading from closed disk manager")
	}
}

func TestWritePage_Success(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	testData := make([]byte, page.PageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	err := dm.WritePage(0, testData)
	if err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	err = dm.Flush()
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	stat, err := os.Stat(dbFile)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	if stat.Size() != page.PageSize {
		t.Fatalf("expected file size %d, got %d", page.PageSize, stat.Size())
	}
}

func TestWritePage_SizeMismatch(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	wrongData := make([]byte, page.PageSize-1)
	err := dm.WritePage(0, wrongData)
	if err == nil {
		t.Fatal("expected error for size mismatch")
	}
}

func TestWritePage_Closed(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	dm.Close()

	data := make([]byte, page.PageSize)
	err := dm.WritePage(0, data)
	if err == nil {
		t.Fatal("expected error when writing to closed disk manager")
	}
}

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

func TestSync_Success(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	testData := make([]byte, page.PageSize)
	testData[0] = 77
	dm.WritePage(0, testData)

	err := dm.Sync()
	if err != nil {
		t.Fatalf("failed to sync: %v", err)
	}
}

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

func TestClose_Success(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	err := dm.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	data := make([]byte, page.PageSize)
	err = dm.ReadPage(0, data)
	if err == nil {
		t.Fatal("expected error after close")
	}
}

func TestClose_AlreadyClosed(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	dm.Close()

}

func TestMultiplePages(t *testing.T) {
	dbFile := getTempDbFile(t)
	defer os.Remove(dbFile)

	dm, _ := NewDiskManager(dbFile)
	defer dm.Close()

	pages := 5
	for i := 0; i < pages; i++ {
		data := make([]byte, page.PageSize)
		data[0] = byte(i)
		dm.WritePage(page.ResourcePageID(i), data)
	}

	dm.Flush()

	for i := 0; i < pages; i++ {
		data := make([]byte, page.PageSize)
		dm.ReadPage(page.ResourcePageID(i), data)
		if data[0] != byte(i) {
			t.Fatalf("page %d: expected data[0]=%d, got %d", i, i, data[0])
		}
	}
}

func getTempDbFile(t *testing.T) string {
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "test.db")
}
