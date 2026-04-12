package page

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewPage(t *testing.T) {
	pageID := ResourcePageID(1)
	page := NewResourcePage(pageID)

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

func TestPage_RLock_RUnlock(t *testing.T) {
	page := NewResourcePage(1)

	var wg sync.WaitGroup
	errors := make(chan error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			page.RLock()
			defer page.RUnlock()
			_ = page.GetID()
		}()
	}

	wg.Wait()
	close(errors)

	if len(errors) > 0 {
		t.Fatalf("expected no errors, got %v", <-errors)
	}
}

func TestPage_WLock_WUnlock(t *testing.T) {
	page := NewResourcePage(2)

	page.WLock()
	page.SetDirty(true)
	page.WUnlock()

	if !page.IsDirty() {
		t.Fatal("expected page to be dirty after write lock operation")
	}
}

func TestPage_ExclusiveWriteLock(t *testing.T) {
	page := NewResourcePage(3)

	page.WLock()
	page.SetDirty(true)
	page.WUnlock()

	if !page.IsDirty() {
		t.Fatal("expected page to be dirty after write lock operation")
	}
}

func TestPage_GetData(t *testing.T) {
	page := NewResourcePage(4)
	data := page.GetData()

	if len(data) != PageSize {
		t.Fatalf("expected data size %d, got %d", PageSize, len(data))
	}

	data[0] = 42
	retrievedData := page.GetData()
	if retrievedData[0] != 42 {
		t.Fatalf("expected data[0] = 42, got %d", retrievedData[0])
	}
}

func TestPage_PinCount(t *testing.T) {
	page := NewResourcePage(5)

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

func TestPage_DirtyFlag(t *testing.T) {
	page := NewResourcePage(6)

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

func TestPage_LSN(t *testing.T) {
	page := NewResourcePage(7)

	switch v := page.(type) {
	case *ResourcePage:

		if v.GetLSN() != 0 {
			t.Fatalf("expected initial LSN 0, got %d", v.GetLSN())
		}

		v.SetLSN(12345)
		if v.GetLSN() != 12345 {
			t.Fatalf("expected LSN 12345, got %d", v.GetLSN())
		}

		v.SetLSN(67890)
		if v.GetLSN() != 67890 {
			t.Fatalf("expected LSN 67890, got %d", v.GetLSN())
		}

	default:
		panic(fmt.Sprintf("unexpected type %T for IResourcePage", v))
	}

}

func TestPage_ConcurrentReadAndWrite(t *testing.T) {
	page := NewResourcePage(8)
	page.SetDirty(false)

	data := page.GetData()
	data[0] = 100

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			page.RLock()
			defer page.RUnlock()
			if page.GetData()[0] != 100 {
				t.Errorf("expected data[0] = 100, got %d", page.GetData()[0])
			}
		}()
	}

	wg.Wait()
}

func TestPage_MultipleStateChanges(t *testing.T) {
	page := NewResourcePage(9)

	page.SetPinCount(1)
	page.SetDirty(true)

	switch v := page.(type) {
	case *ResourcePage:
		v.SetLSN(555)
	default:
		panic(fmt.Sprintf("unexpected type %T for IResourcePage", v))
	}

	if page.GetPinCount() != 1 {
		t.Fatalf("expected pin count 1, got %d", page.GetPinCount())
	}
	if !page.IsDirty() {
		t.Fatal("expected page to be dirty")
	}
	if page.GetLSN() != 555 {
		t.Fatalf("expected LSN 555, got %d", page.GetLSN())
	}

	page.SetPinCount(0)
	page.SetDirty(false)

	switch v := page.(type) {
	case *ResourcePage:
		v.SetLSN(0)
	default:
		panic(fmt.Sprintf("unexpected type %T for IResourcePage", v))
	}

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
