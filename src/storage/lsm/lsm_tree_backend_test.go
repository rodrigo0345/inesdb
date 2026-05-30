package lsm

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/buffer"
	"github.com/rodrigo0345/omag/src/storage/schema"
	log "github.com/rodrigo0345/omag/src/engine/wal"
)

func setupLSMWithRealStack(t *testing.T) (*LSMTreeBackend, *schema.TableManager) {
	t.Helper()

	// Use t.TempDir() for automatic cleanup and isolation between tests
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal.db")
	diskPath := filepath.Join(tempDir, "disk_manager")
	dataPath := filepath.Join(tempDir, "dataDir")

	tm := schema.NewTableManager()

	logMgr, err := log.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to create real WAL manager: %v", err)
	}

	diskManager, err := buffer.NewDiskManager(diskPath)
	if err != nil {
		t.Fatalf("failed to create real disk manager: %v", err)
	}
	bufferMgr := buffer.NewBufferPoolManager(10, diskManager)

	storageEngine := NewLSMTreeBackendWithDataDir(logMgr, bufferMgr, dataPath)

	cols := []schema.Column{
		{Name: "id", Type: schema.TypeInt32},
		{Name: "val", Type: schema.TypeInt32},
	}
	ts := schema.NewTableSchema("test_table", cols)

	ts.AddIndex("PRIMARY", []string{"id"}, storageEngine)
	tm.CreateTable(ts, true)

	t.Cleanup(func() {
		storageEngine.Close()
		logMgr.Close()
		diskManager.Close()
	})

	return storageEngine, tm
}

func scanCursorToMap(t *testing.T, cursor storage.ICursor) map[string]string {
	t.Helper()
	defer cursor.Close()
	result := make(map[string]string)

	for cursor.Next() {
		entry := cursor.Entry()
		// Deep copy key and value
		k := make([]byte, len(entry.Key))
		copy(k, entry.Key)
		v := make([]byte, len(entry.Value))
		copy(v, entry.Value)
		result[string(k)] = string(v)
	}

	if err := cursor.Error(); err != nil {
		t.Fatalf("cursor error: %v", err)
	}
	return result
}

func TestLSMTreeBackend_BasicPutGet(t *testing.T) {
	lsm, _ := setupLSMWithRealStack(t)
	key, value := []byte("testkey"), []byte("testvalue")

	if err := lsm.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := lsm.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(retrieved, value) {
		t.Fatalf("expected value %q, got %q", value, retrieved)
	}
}

func TestLSMTreeBackend_ScanReturnsMergedView(t *testing.T) {
	lsm, _ := setupLSMWithRealStack(t)

	lsm.Put([]byte("alpha"), []byte("one"))
	lsm.Put([]byte("bravo"), []byte("two"))
	lsm.Put([]byte("charlie"), []byte("three"))
	lsm.flush() // Persists to SSTable

	lsm.Put([]byte("bravo"), []byte("two-updated"))
	lsm.Delete([]byte("alpha"))
	lsm.Put([]byte("delta"), []byte("four"))

	// Range Scan
	cursor, err := lsm.Scan(storage.ScanOptions{
		LowerBound: []byte("a"),
		UpperBound: []byte("z"),
		Inclusive:  true,
	})
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}

	got := scanCursorToMap(t, cursor)
	want := map[string]string{
		"bravo":   "two-updated",
		"charlie": "three",
		"delta":   "four",
	}

	if len(got) != len(want) {
		t.Fatalf("Count mismatch. Got %d, Want %d. Data: %v", len(got), len(want), got)
	}

	for k, v := range want {
		if got[k] != v {
			t.Errorf("Key %q mismatch: got %q, want %q", k, got[k], v)
		}
	}

	if _, exists := got["alpha"]; exists {
		t.Error("Tombstoned key 'alpha' should not be in scan results")
	}
}

func TestLSMTreeBackend_UpdateValue(t *testing.T) {
	lsm, _ := setupLSMWithRealStack(t)
	key := []byte("key")

	lsm.Put(key, []byte("v1"))
	lsm.Put(key, []byte("v2"))

	res, _ := lsm.Get(key)
	if string(res) != "v2" {
		t.Fatalf("Expected v2, got %s", string(res))
	}
}

func TestLSMTreeBackend_MemtableFlush(t *testing.T) {
	lsm, _ := setupLSMWithRealStack(t)
	count := SSTableMaxSize + 5

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key_%05d", i))
		val := []byte(fmt.Sprintf("val_%05d", i))
		lsm.Put(key, val)
	}

	// Verify all data is accessible across Memtable and SSTables
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key_%05d", i))
		want := fmt.Sprintf("val_%05d", i)
		got, err := lsm.Get(key)
		if err != nil || string(got) != want {
			t.Fatalf("Failed at key %s: got %s, want %s, err: %v", key, got, want, err)
		}
	}
}

func TestLSMTreeBackend_ConcurrentPuts(t *testing.T) {
	lsm, _ := setupLSMWithRealStack(t)
	numG, itemsPerG := 8, 100
	var wg sync.WaitGroup

	for g := 0; g < numG; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < itemsPerG; i++ {
				key := []byte(fmt.Sprintf("g%d_i%d", gid, i))
				val := []byte(fmt.Sprintf("v%d_%d", gid, i))
				lsm.Put(key, val)
			}
		}(g)
	}
	wg.Wait()

	// Verify integrity
	for g := 0; g < numG; g++ {
		for i := 0; i < itemsPerG; i++ {
			key := []byte(fmt.Sprintf("g%d_i%d", g, i))
			want := fmt.Sprintf("v%d_%d", g, i)
			got, _ := lsm.Get(key)
			if string(got) != want {
				t.Errorf("Concurrent write lost for key %s", key)
			}
		}
	}
}

func TestLSMTreeBackend_SequentialPattern(t *testing.T) {
	lsm, _ := setupLSMWithRealStack(t)
	itemCount := 1000

	// Put sequential keys
	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("seq_%06d", i))
		lsm.Put(key, []byte(fmt.Sprintf("val_%d", i)))
	}

	// Perform updates on subset
	for i := 0; i < itemCount; i += 2 {
		key := []byte(fmt.Sprintf("seq_%06d", i))
		lsm.Put(key, []byte("updated"))
	}

	// Verify
	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("seq_%06d", i))
		got, _ := lsm.Get(key)
		if i%2 == 0 {
			if string(got) != "updated" {
				t.Errorf("Key %d should be updated", i)
			}
		} else {
			want := fmt.Sprintf("val_%d", i)
			if string(got) != want {
				t.Errorf("Key %d mismatch: got %s, want %s", i, got, want)
			}
		}
	}
}

func TestLSMTreeBackend_EdgeCases(t *testing.T) {
	lsm, _ := setupLSMWithRealStack(t)

	lsm.Put([]byte("empty"), []byte(""))
	got, _ := lsm.Get([]byte("empty"))
	if len(got) != 0 || got == nil {
		t.Fatalf("Expected empty slice, got %v", got)
	}

	binKey := []byte{0x00, 0xFF, 0x01, 0x02}
	lsm.Put(binKey, []byte("binary_success"))
	got, _ = lsm.Get(binKey)
	if string(got) != "binary_success" {
		t.Fatal("Failed to handle binary key")
	}
}
