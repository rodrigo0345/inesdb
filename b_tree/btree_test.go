package btree

import (
	"bytes"
	"testing"
)

func setupMemPager(t *testing.T, pageSize uint32) *Pager {
	pager, err := NewPager("", pageSize, true)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	return pager
}

func TestNewBTree_Empty(t *testing.T) {
	pager := setupMemPager(t, 4096)
	tree, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tree.meta.RootPage() != 1 {
		t.Fatalf("expected root page ID 1, got %d", tree.meta.RootPage())
	}
	if pager.PageCount() != 2 {
		t.Fatalf("expected 2 pages (meta + root leaf), got %d", pager.PageCount())
	}
}

func TestNewBTree_Existing(t *testing.T) {
	pager := setupMemPager(t, 4096)
	_, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("unexpected error initializing: %v", err)
	}

	tree2, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("unexpected error reopening: %v", err)
	}
	if tree2.meta.RootPage() != 1 {
		t.Fatalf("expected root page ID 1, got %d", tree2.meta.RootPage())
	}
}

func TestBTree_InsertAndFind(t *testing.T) {
	pager := setupMemPager(t, 4096)
	tree, _ := NewBTree(pager)

	err := tree.Insert([]byte("keyA"), []byte("valA"))
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	err = tree.Insert([]byte("keyB"), []byte("valB"))
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	val, err := tree.Find([]byte("keyA"))
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if !bytes.Equal(val, []byte("valA")) {
		t.Fatalf("expected 'valA', got '%s'", val)
	}

	val, err = tree.Find([]byte("keyB"))
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if !bytes.Equal(val, []byte("valB")) {
		t.Fatalf("expected 'valB', got '%s'", val)
	}
}

func TestBTree_Find_NotFound(t *testing.T) {
	pager := setupMemPager(t, 4096)
	tree, _ := NewBTree(pager)

	tree.Insert([]byte("key1"), []byte("val1"))

	_, err := tree.Find([]byte("key2"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestBTree_InternalNodeRouting(t *testing.T) {
	pager := setupMemPager(t, 4096)
	tree, _ := NewBTree(pager)

	rootInternal := NewInternalPage(4096)
	rootInternal.Insert([]byte("M"), 2)
	rootInternal.SetRightmostPointer(3)
	pager.WritePage(1, rootInternal.data)

	leftLeaf := NewLeafPage(4096)
	leftLeaf.Insert([]byte("A"), []byte("apple"))
	leftLeaf.Insert([]byte("B"), []byte("banana"))
	pager.AllocatePage()
	pager.WritePage(2, leftLeaf.data)

	rightLeaf := NewLeafPage(4096)
	rightLeaf.Insert([]byte("M"), []byte("mango"))
	rightLeaf.Insert([]byte("Z"), []byte("zebra"))
	pager.AllocatePage()
	pager.WritePage(3, rightLeaf.data)

	val, err := tree.Find([]byte("A"))
	if err != nil || !bytes.Equal(val, []byte("apple")) {
		t.Fatalf("expected 'apple', got %s (err: %v)", val, err)
	}

	val, err = tree.Find([]byte("Z"))
	if err != nil || !bytes.Equal(val, []byte("zebra")) {
		t.Fatalf("expected 'zebra', got %s (err: %v)", val, err)
	}

	_, err = tree.Find([]byte("Q"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for Q, got %v", err)
	}
}
