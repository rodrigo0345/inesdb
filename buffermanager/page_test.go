package buffermanager

import (
	"sync"
	"testing"
)

// TestNewPage tests page creation
func TestNewPage(t *testing.T) {
	pageID := PageID(1)
	page := NewPage(pageID)

	if page == nil {
		t.Fatal("expected page to be non-nil")
	}
	if page.GetID() != pageID {
		t.Fatalf("expected page ID %d, got %d", pageID, page.GetID())
	}
	if page.GetPinCount() != 0 {
		t.Fatalf("expected pin count 0, got %d", page.GetPinCount())
	}
	if page.IsDirty() {
		t.Fatal("expected page to not be dirty initially")
	}
}

// TestPage_RLock_RUnlock tests read lock operations
func TestPage_RLock_RUnlock(t *testing.T) {
	page := NewPage(1)

	// Multiple readers should be able to acquire read locks simultaneously
	var wg sync.WaitGroup
	errors := make(chan error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			page.RLock()
			defer page.RUnlock()
			// Simulate some read operation
			_ = page.GetID()
		}()
	}

	wg.Wait()
	close(errors)

	// Should complete without deadlock
	if len(errors) > 0 {
		t.Fatalf("expected no errors, got %v", <-errors)
	}
}

// TestPage_WLock_WUnlock tests write lock operations
func TestPage_WLock_WUnlock(t *testing.T) {
	page := NewPage(2)

	page.WLock()
	page.SetDirty(true)
	page.WUnlock()

	if !page.IsDirty() {
		t.Fatal("expected page to be dirty after write lock operation")
	}
}

// TestPage_ExclusiveWriteLock tests that write lock works correctly
func TestPage_ExclusiveWriteLock(t *testing.T) {
	page := NewPage(3)

	// Simple test: acquire and release write lock
	page.WLock()
	page.SetDirty(true)
	page.WUnlock()

	if !page.IsDirty() {
		t.Fatal("expected page to be dirty after write lock operation")
	}
}

// TestPage_GetData tests data access
func TestPage_GetData(t *testing.T) {
	page := NewPage(4)
	data := page.GetData()

	if len(data) != PageSize {
		t.Fatalf("expected data size %d, got %d", PageSize, len(data))
	}

	// Modify data
	data[0] = 42
	retrievedData := page.GetData()
	if retrievedData[0] != 42 {
		t.Fatalf("expected data[0] = 42, got %d", retrievedData[0])
	}
}

// TestPage_PinCount tests pin count operations
func TestPage_PinCount(t *testing.T) {
	page := NewPage(5)

	page.SetPinCount(1)
	if page.GetPinCount() != 1 {
		t.Fatalf("expected pin count 1, got %d", page.GetPinCount())
	}

	page.SetPinCount(5)
	if page.GetPinCount() != 5 {
		t.Fatalf("expected pin count 5, got %d", page.GetPinCount())
	}

	page.SetPinCount(0)
	if page.GetPinCount() != 0 {
		t.Fatalf("expected pin count 0, got %d", page.GetPinCount())
	}
}

// TestPage_DirtyFlag tests dirty flag operations
func TestPage_DirtyFlag(t *testing.T) {
	page := NewPage(6)

	if page.IsDirty() {
		t.Fatal("expected page to not be dirty initially")
	}

	page.SetDirty(true)
	if !page.IsDirty() {
		t.Fatal("expected page to be dirty after SetDirty(true)")
	}

	page.SetDirty(false)
	if page.IsDirty() {
		t.Fatal("expected page to not be dirty after SetDirty(false)")
	}
}

// TestPage_LSN tests LSN (Log Sequence Number) operations
func TestPage_LSN(t *testing.T) {
	page := NewPage(7)

	// Initial LSN should be 0
	if page.GetLSN() != 0 {
		t.Fatalf("expected initial LSN 0, got %d", page.GetLSN())
	}

	// Set LSN
	page.SetLSN(12345)
	if page.GetLSN() != 12345 {
		t.Fatalf("expected LSN 12345, got %d", page.GetLSN())
	}

	// Update LSN
	page.SetLSN(67890)
	if page.GetLSN() != 67890 {
		t.Fatalf("expected LSN 67890, got %d", page.GetLSN())
	}
}

// TestPage_ConcurrentReadAndWrite tests concurrent read/write operations
func TestPage_ConcurrentReadAndWrite(t *testing.T) {
	page := NewPage(8)
	page.SetDirty(false)

	// Write some data
	data := page.GetData()
	data[0] = 100

	var wg sync.WaitGroup

	// Spawn multiple readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			page.RLock()
			defer page.RUnlock()
			// Read the data
			if page.GetData()[0] != 100 {
				t.Errorf("expected data[0] = 100, got %d", page.GetData()[0])
			}
		}()
	}

	wg.Wait()
}

// TestPage_MultipleStateChanges tests multiple state changes
func TestPage_MultipleStateChanges(t *testing.T) {
	page := NewPage(9)

	// Sequence of operations
	page.SetPinCount(1)
	page.SetDirty(true)
	page.SetLSN(100)

	if page.GetPinCount() != 1 {
		t.Fatalf("expected pin count 1, got %d", page.GetPinCount())
	}
	if !page.IsDirty() {
		t.Fatal("expected page to be dirty")
	}
	if page.GetLSN() != 100 {
		t.Fatalf("expected LSN 100, got %d", page.GetLSN())
	}

	// Reset
	page.SetPinCount(0)
	page.SetDirty(false)
	page.SetLSN(0)

	if page.GetPinCount() != 0 {
		t.Fatalf("expected pin count 0, got %d", page.GetPinCount())
	}
	if page.IsDirty() {
		t.Fatal("expected page to not be dirty")
	}
	if page.GetLSN() != 0 {
		t.Fatalf("expected LSN 0, got %d", page.GetLSN())
	}
}
