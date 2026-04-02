package btree

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestCursor_NextEmpty(t *testing.T) {
	pager := setupMemPager(t, 4096)
	tree, _ := NewBTree(pager)

	cursor, _ := tree.Cursor()
	err := cursor.First()
	if err != nil {
		t.Fatalf("first failed: %v", err)
	}

	if cursor.Valid() {
		t.Fatalf("cursor should not be valid on empty tree")
	}
}

func TestCursor_FirstNextScan(t *testing.T) {
	pager := setupMemPager(t, 4096)
	tree, _ := NewBTree(pager)

	numEntries := 50
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		val := []byte(fmt.Sprintf("val%03d", i))
		tree.Insert(key, val)
	}

	cursor, _ := tree.Cursor()
	err := cursor.First()
	if err != nil {
		t.Fatalf("first failed: %v", err)
	}

	count := 0
	for ; cursor.Valid(); cursor.Next() {
		expectedKey := []byte(fmt.Sprintf("key%03d", count))
		if !bytes.Equal(cursor.Key(), expectedKey) {
			t.Fatalf("expected key %s, got %s", expectedKey, cursor.Key())
		}
		count++
	}

	if count != numEntries {
		t.Fatalf("expected %d entries, found %d", numEntries, count)
	}
}

func TestCursor_Seek(t *testing.T) {
	pager := setupMemPager(t, 4096)
	tree, _ := NewBTree(pager)

	// we want multiple leaves to test seeking across pages, making the page size smaller
	pager = setupMemPager(t, 512)
	tree, _ = NewBTree(pager)

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%02d", i*2)) // key00, key02, key04
		val := []byte(fmt.Sprintf("val%02d", i*2))
		tree.Insert(key, val)
	}

	cursor, _ := tree.Cursor()

	// Seek exact match
	err := cursor.Seek([]byte("key10"))
	if err != nil {
		t.Fatalf("seek failed: %v", err)
	}
	if !cursor.Valid() || string(cursor.Key()) != "key10" {
		t.Fatalf("seek key10 failed, found %s", cursor.Key())
	}

	// Seek immediately after
	err = cursor.Seek([]byte("key11"))
	if err != nil {
		t.Fatalf("seek failed: %v", err)
	}
	if !cursor.Valid() || string(cursor.Key()) != "key12" {
		t.Fatalf("seek key11 failed, found %s", cursor.Key())
	}

	// Seek beyond end
	err = cursor.Seek([]byte("key99"))
	if err != nil {
		t.Fatalf("seek failed: %v", err)
	}
	if cursor.Valid() {
		t.Fatalf("seek key99 should be invalid, but found %s", cursor.Key())
	}
}

func TestPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "omag-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	pager, err := NewPager(dbPath, 1024, false) // 1kb pages to force splits easily
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	tree, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Insert enough data to cause internal node splits
	numRecords := 500
	for i := 0; i < numRecords; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		val := []byte(fmt.Sprintf("v%05d", i))
		err = tree.Put(key, val)
		if err != nil {
			t.Fatalf("put failed on i=%d: %v", i, err)
		}
	}

	// Sync and close
	pager.Sync()
	pager.Close()

	// Reopen
	pager2, err := NewPager(dbPath, 1024, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager2.Close()

	tree2, err := NewBTree(pager2)
	if err != nil {
		t.Fatalf("failed to open btree: %v", err)
	}

	// Verify with Cursor
	cursor, _ := tree2.Cursor()
	cursor.First()
	count := 0
	for cursor.Valid() {
		expectedKey := fmt.Sprintf("k%05d", count)
		if string(cursor.Key()) != expectedKey {
			t.Fatalf("expected key %s, got %s", expectedKey, cursor.Key())
		}
		count++
		cursor.Next()
	}

	if count != numRecords {
		t.Fatalf("expected %d records after reopen, found %d", numRecords, count)
	}

	// Test Seek on reopened db
	cursor.Seek([]byte("k00250"))
	if !cursor.Valid() || string(cursor.Key()) != "k00250" || string(cursor.Value()) != "v00250" {
		t.Fatalf("seek failed on reopened db: %s/%s", cursor.Key(), cursor.Value())
	}

	// Delete a record
	err = tree2.Delete([]byte("k00250"))
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	val, err := tree2.Get([]byte("k00250"))
	if err == nil {
		t.Fatalf("expected ErrKeyNotFound, got %v", string(val))
	}

	// Add some data and sync again
	for i := numRecords; i < numRecords+50; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		val := []byte(fmt.Sprintf("v%05d", i))
		tree2.Put(key, val)
	}

	val, err = tree2.Get([]byte(fmt.Sprintf("k%05d", numRecords+10)))
	if err != nil || string(val) != fmt.Sprintf("v%05d", numRecords+10) {
		t.Fatalf("get newly added record failed")
	}

	pager2.Sync()

	// Verify all functionality of tests
	cursor, _ = tree2.Cursor()
	cursor.Seek([]byte("k00510"))
	if !cursor.Valid() {
		t.Fatalf("seek on newly added valid record failed")
	}
	if string(cursor.Key()) != "k00510" {
		t.Fatalf("expected skip matching, got %s", cursor.Key())
	}
}

func TestCursor_NextAcrossPages(t *testing.T) {
	pager := setupMemPager(t, 256) // small page to easily fill it
	tree, _ := NewBTree(pager)

	for i := 0; i < 20; i++ {
		// making keys big so it splits
		key := []byte(strconv.Itoa(i) + "padpadpadpadpadpad")
		val := []byte(strconv.Itoa(i) + "valvalvalvalvalval")
		tree.Insert(key, val)
	}

	cursor, _ := tree.Cursor()
	cursor.First()

	validCount := 0
	for cursor.Valid() {
		validCount++
		cursor.Next()
	}

	if validCount != 20 {
		t.Fatalf("expected 20 valid items, got %d", validCount)
	}
}
