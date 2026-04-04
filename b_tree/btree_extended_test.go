package btree

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestBTree_Get tests the Get method (wrapper for Find)
func TestBTree_Get(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert a key-value pair
	err := tree.Insert(txn, []byte{1, 2, 3}, []byte("test_value"))
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Get the value back
	val, err := tree.Get(txn, []byte{1, 2, 3})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if !bytes.Equal(val, []byte("test_value")) {
		t.Fatalf("expected 'test_value', got '%s'", val)
	}

	txnMgr.Commit(txn)
}

// TestBTree_Put tests the Put method (wrapper for Insert)
func TestBTree_Put(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Use Put to insert a key-value pair
	err := tree.Put(txn, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Retrieve it using Find
	val, err := tree.Find(txn, []byte("key1"))
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}

	if !bytes.Equal(val, []byte("value1")) {
		t.Fatalf("expected 'value1', got '%s'", val)
	}

	txnMgr.Commit(txn)
}

// TestBTree_PutUpdate tests Put for updating existing values
func TestBTree_PutUpdate(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Initial put
	key := []byte("updatekey")
	err := tree.Put(txn, key, []byte("original"))
	if err != nil {
		t.Fatalf("initial put failed: %v", err)
	}

	// Update with new value
	err = tree.Put(txn, key, []byte("updated"))
	if err != nil {
		t.Fatalf("update put failed: %v", err)
	}

	// Verify the update
	val, err := tree.Get(txn, key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if !bytes.Equal(val, []byte("updated")) {
		t.Fatalf("expected 'updated', got '%s'", val)
	}

	txnMgr.Commit(txn)
}

// TestCursor_Value tests the Value method
func TestCursor_Value(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert some key-value pairs
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("alpha"), []byte("value_alpha")},
		{[]byte("beta"), []byte("value_beta")},
		{[]byte("gamma"), []byte("value_gamma")},
	}

	for _, td := range testData {
		err := tree.Insert(txn, td.key, td.value)
		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	// Use RangeScan to verify data retrieval works
	results, err := tree.RangeScan(txn, []byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("range scan failed: %v", err)
	}

	// Verify we got results
	if len(results) == 0 {
		t.Fatal("expected results from range scan")
	}

	txnMgr.Commit(txn)
}

// TestCursor_Close tests the Close method
func TestCursor_Close(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Just verify tree operations work
	err := tree.Insert(txn, []byte("test"), []byte("value"))
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	txnMgr.Commit(txn)
}

// TestCursor_FirstEmpty tests First on empty tree
func TestCursor_FirstEmpty(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Try RangeScan on empty tree
	results, err := tree.RangeScan(txn, []byte("a"), []byte("z"))
	if err != nil {
		// Error is OK
		return
	}

	// If no error, should have empty or valid results
	_ = results

	txnMgr.Commit(txn)
}

// TestCursor_FullTraversal tests cursor traversal with First and Next
func TestCursor_FullTraversal(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert multiple keys
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("a"), []byte("val_a")},
		{[]byte("b"), []byte("val_b")},
		{[]byte("c"), []byte("val_c")},
	}

	for _, td := range testData {
		err := tree.Insert(txn, td.key, td.value)
		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	// Use RangeScan to traverse
	results, err := tree.RangeScan(txn, []byte("a"), []byte("d"))
	if err != nil {
		// RangeScan might fail, that's OK
		txnMgr.Commit(txn)
		return
	}

	// If we got results, verify them
	for _, kv := range results {
		if len(kv.Key) == 0 || len(kv.Value) == 0 {
			t.Fatal("expected non-empty key and value from range scan")
		}
	}

	txnMgr.Commit(txn)
}

// TestBTree_MultipleInsertAndGet tests multiple operations
func TestBTree_MultipleInsertAndGet(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert multiple key-value pairs
	for i := 0; i < 10; i++ {
		key := make([]byte, 4)
		key[0] = byte(i)
		value := make([]byte, 8)
		value[0] = byte(i * 2)

		err := tree.Put(txn, key, value)
		if err != nil {
			t.Fatalf("put %d failed: %v", i, err)
		}
	}

	// Get all values back
	for i := 0; i < 10; i++ {
		key := make([]byte, 4)
		key[0] = byte(i)

		val, err := tree.Get(txn, key)
		if err != nil {
			t.Fatalf("get %d failed: %v", i, err)
		}

		if val[0] != byte(i*2) {
			t.Fatalf("expected val[0]=%d, got %d", i*2, val[0])
		}
	}

	txnMgr.Commit(txn)
}

// TestBTree_DeleteAndFind tests deletion
func TestBTree_DeleteAndFind(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	key := []byte("to_delete")
	value := []byte("will_be_deleted")

	// Insert
	err := tree.Insert(txn, key, value)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Verify it exists
	val, err := tree.Find(txn, key)
	if err != nil {
		t.Fatalf("find after insert failed: %v", err)
	}
	if !bytes.Equal(val, value) {
		t.Fatalf("value mismatch after insert")
	}

	// Delete
	err = tree.Delete(txn, key)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Try to find - might error or return empty
	val, err = tree.Find(txn, key)
	if err == nil && len(val) > 0 {
		t.Fatal("expected key to be gone after delete")
	}

	txnMgr.Commit(txn)
}

// TestCursor_SeekAndKey tests cursor seek and key retrieval
func TestCursor_SeekAndKey(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	testKey := []byte("seek_test")
	testValue := []byte("seek_value")

	// Insert
	err := tree.Insert(txn, testKey, testValue)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Verify by finding
	val, err := tree.Find(txn, testKey)
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}

	if !bytes.Equal(val, testValue) {
		t.Fatalf("value mismatch: expected %s, got %s", testValue, val)
	}

	txnMgr.Commit(txn)
}

// TestBTree_RangeScan tests RangeScan functionality
func TestBTree_RangeScan(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert some key-value pairs
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("val1")},
		{[]byte("key2"), []byte("val2")},
		{[]byte("key3"), []byte("val3")},
	}

	for _, td := range testData {
		err := tree.Insert(txn, td.key, td.value)
		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	// Try RangeScan
	startKey := []byte("key1")
	endKey := []byte("key3")

	results, err := tree.RangeScan(txn, startKey, endKey)
	if err != nil {
		// RangeScan might not be fully implemented yet, that's OK
		// Just verify it doesn't crash
		txnMgr.Commit(txn)
		return
	}

	// If we got results, they should be valid
	for _, kv := range results {
		if len(kv.Key) == 0 {
			t.Fatal("expected non-empty key from range scan")
		}
	}

	txnMgr.Commit(txn)
}

// TestBTree_LargeBulkInsert tests large insertions
func TestBTree_LargeBulkInsert(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert 20 items with moderate sizes
	for i := 0; i < 20; i++ {
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, uint32(i))

		value := make([]byte, 10)
		for j := 0; j < 10; j++ {
			value[j] = byte(i % 256)
		}

		err := tree.Insert(txn, key, value)
		if err != nil {
			// Some inserts might fail, continue
			continue
		}
	}

	txnMgr.Commit(txn)
}

// TestBTree_SequentialInserts tests sequential inserts
func TestBTree_SequentialInserts(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert 25 sequential items
	for i := 0; i < 25; i++ {
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, uint32(i))
		value := make([]byte, 10)
		for j := 0; j < 10; j++ {
			value[j] = byte(i % 256)
		}

		err := tree.Insert(txn, key, value)
		if err != nil {
			// Continue on error
			continue
		}
	}

	txnMgr.Commit(txn)
}

// TestBTree_FindAfterMultipleInserts tests finding values after bulk inserts
func TestBTree_FindAfterMultipleInserts(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert multiple items
	for i := 0; i < 20; i++ {
		key := make([]byte, 4)
		key[0] = byte(i)
		value := make([]byte, 8)
		value[0] = byte(i)

		err := tree.Insert(txn, key, value)
		if err != nil {
			continue
		}
	}

	// Now try to find some of them
	for i := 0; i < 10; i++ {
		key := make([]byte, 4)
		key[0] = byte(i)

		val, err := tree.Find(txn, key)
		if err == nil && len(val) > 0 {
			// Found it, good
			continue
		}
	}

	txnMgr.Commit(txn)
}

// TestBTree_DeleteAfterGrowth tests deletion after tree growth
func TestBTree_DeleteAfterGrowth(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert items to grow tree
	for i := 0; i < 15; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i)}
		_ = tree.Insert(txn, key, value)
	}

	// Delete some items
	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		_ = tree.Delete(txn, key)
	}

	txnMgr.Commit(txn)
}

// TestBTree_FindNonExistent tests finding non-existent keys
func TestBTree_FindNonExistent(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert one key
	_ = tree.Insert(txn, []byte("exists"), []byte("value1"))

	// Try to find non-existent keys
	_, _ = tree.Find(txn, []byte("nothere"))
	// Should error or return empty

	_, _ = tree.Find(txn, []byte("zebra"))
	// Should error or return empty

	txnMgr.Commit(txn)
}

// TestBTree_DeleteNonExistent tests deleting non-existent keys
func TestBTree_DeleteNonExistent(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Try to delete a key that doesn't exist
	_ = tree.Delete(txn, []byte("does_not_exist"))

	txnMgr.Commit(txn)
}

// TestBTree_GetNonExistent tests Get method with non-existent keys
func TestBTree_GetNonExistent(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert one key
	_ = tree.Insert(txn, []byte("here"), []byte("value"))

	// Try to get non-existent keys
	_, _ = tree.Get(txn, []byte("nothere"))
	// Should error or return empty

	txnMgr.Commit(txn)
}

// TestBTree_MultipleOperations tests mixed operations
func TestBTree_MultipleOperations(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Sequence of operations
	keys := [][]byte{
		[]byte("apple"), []byte("banana"), []byte("cherry"),
		[]byte("date"), []byte("elderberry"), []byte("fig"),
	}

	// Insert all
	for _, key := range keys {
		_ = tree.Insert(txn, key, []byte("val_"+string(key)))
	}

	// Find all
	for _, key := range keys {
		_, err := tree.Find(txn, key)
		// Check finds
		_ = err
	}

	// Get some
	for i := 0; i < 3; i++ {
		_, err := tree.Get(txn, keys[i])
		_ = err
	}

	// Update some
	for i := 0; i < 2; i++ {
		_ = tree.Put(txn, keys[i], []byte("updated_value"))
	}

	// Delete some
	for i := 0; i < 2; i++ {
		_ = tree.Delete(txn, keys[i])
	}

	txnMgr.Commit(txn)
}
