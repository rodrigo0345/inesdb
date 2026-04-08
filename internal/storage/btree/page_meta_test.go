package bplus_tree_backend

import (
	"testing"
)

func TestMetaPage_Initialization(t *testing.T) {
	meta := NewMetaPage()

	if meta.PageType() != TypeMeta {
		t.Fatalf("expected PageType %d, got %d", TypeMeta, meta.PageType())
	}

	if meta.MagicNumber() != MagicNumber {
		t.Fatalf("expected MagicNumber %x, got %x", MagicNumber, meta.MagicNumber())
	}

	if meta.Version() != 1 {
		t.Fatalf("expected Version 1, got %d", meta.Version())
	}
	if meta.RootPage() != 1 {
		t.Fatalf("expected RootPage 1, got %d", meta.RootPage())
	}
}

func TestMetaPage_SettersAndGetters(t *testing.T) {
	meta := NewMetaPage()

	expectedRoot := uint64(42)
	meta.SetRootPage(expectedRoot)

	if meta.RootPage() != expectedRoot {
		t.Fatalf("expected RootPage %d, got %d", expectedRoot, meta.RootPage())
	}

	if meta.PageSize() == 0 {
		t.Fatalf("PageSize was corrupted by SetRootPage!")
	}
}
