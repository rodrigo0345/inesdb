package lsm

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/lsm/mocks"
	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

func createTestLSM(t *testing.T) *LSMTreeBackend {
	t.Helper()
	// Keep tests isolated from repository-level lsm_data to avoid state bleed.
	return createIsolatedTestLSM(t)
}

func createIsolatedTestLSM(t *testing.T) *LSMTreeBackend {
	t.Helper()

	mockLogMgr := &mocks.MockLogManager{}
	mockBufMgr := &mocks.MockBufferManager{}

	lsm := NewLSMTreeBackendWithDataDir(mockLogMgr, mockBufMgr, t.TempDir())
	if lsm == nil {
		t.Fatal("failed to create isolated LSM tree backend")
	}
	return lsm
}

func scanEntriesToMap(entries []storage.ScanEntry) map[string]string {
	result := make(map[string]string, len(entries))
	for _, entry := range entries {
		result[string(entry.Key)] = string(entry.Value)
	}
	return result
}

func sortedScanKeys(entries []storage.ScanEntry) []string {
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, string(entry.Key))
	}
	sort.Strings(keys)
	return keys
}

type mockLogManager struct {
	records []log.ILogRecord
	mu      sync.Mutex
}

func (m *mockLogManager) AppendLogRecord(record log.ILogRecord) (log.LSN, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, record)
	return log.LSN(len(m.records)), nil
}

func (m *mockLogManager) Flush(upToLSN log.LSN) error {
	return nil
}

func (m *mockLogManager) Recover() (*log.RecoveryState, error) {
	return nil, nil
}

func (m *mockLogManager) Checkpoint() error {
	return nil
}

func (m *mockLogManager) GetLastCheckpointLSN() uint64 {
	return 0
}

func (m *mockLogManager) AddTransactionOperation(txnID uint64, tableName string, opType log.RecordType, key []byte, value []byte) {

}

func (m *mockLogManager) Close() error {
	return nil
}

func (m *mockLogManager) ReadAllRecords() ([]log.WALRecord, error) {
	return nil, nil
}

type mockResourcePage struct {
	id    page.ResourcePageID
	data  []byte
	dirty bool
	rmu   sync.RWMutex
	wmu   sync.Mutex
}

func newMockResourcePage(id page.ResourcePageID, size int) *mockResourcePage {
	return &mockResourcePage{
		id:   id,
		data: make([]byte, size),
	}
}

func (p *mockResourcePage) GetID() page.ResourcePageID            { return p.id }
func (p *mockResourcePage) GetData() []byte                       { return p.data }
func (p *mockResourcePage) SetDirty(dirty bool)                   { p.dirty = dirty }
func (p *mockResourcePage) IsDirty() bool                         { return p.dirty }
func (p *mockResourcePage) RLock()                                { p.rmu.RLock() }
func (p *mockResourcePage) RUnlock()                              { p.rmu.RUnlock() }
func (p *mockResourcePage) WLock()                                { p.wmu.Lock() }
func (p *mockResourcePage) WUnlock()                              { p.wmu.Unlock() }
func (p *mockResourcePage) GetLSN() uint64                        { return 0 }
func (p *mockResourcePage) SetLSN(lsn uint64)                     {}
func (p *mockResourcePage) ResetMemory()                          { p.data = make([]byte, len(p.data)) }
func (p *mockResourcePage) GetPinCount() int32                    { return 0 }
func (p *mockResourcePage) SetPinCount(count int32)               {}
func (p *mockResourcePage) ReplacePage(newID page.ResourcePageID) { p.id = newID }
func (p *mockResourcePage) Close()                                {}

type mockBufferManager struct {
	pages      map[page.ResourcePageID]*mockResourcePage
	nextPageID page.ResourcePageID
	mu         sync.Mutex
}

func (m *mockBufferManager) NewPage() (*page.IResourcePage, error) {
	m.mu.Lock()
	pageID := m.nextPageID
	m.nextPageID++
	m.mu.Unlock()

	mockPage := newMockResourcePage(pageID, 4096)
	m.mu.Lock()
	m.pages[pageID] = mockPage
	m.mu.Unlock()

	var iface page.IResourcePage = mockPage
	return &iface, nil
}

func (m *mockBufferManager) PinPage(pageID page.ResourcePageID) (page.IResourcePage, error) {
	m.mu.Lock()
	p, ok := m.pages[pageID]
	m.mu.Unlock()

	if !ok {
		mockPage := newMockResourcePage(pageID, 4096)
		m.mu.Lock()
		m.pages[pageID] = mockPage
		m.mu.Unlock()
		var iface page.IResourcePage = mockPage
		return iface, nil
	}

	var iface page.IResourcePage = p
	return iface, nil
}

func (m *mockBufferManager) UnpinPage(pageID page.ResourcePageID, isDirty bool) error {
	return nil
}

func (m *mockBufferManager) FlushAll() error {
	return nil
}

func (m *mockBufferManager) Close() error {
	return nil
}

func TestLSMTreeBackend_BasicPutGet(t *testing.T) {
	lsm := createTestLSM(t)

	key := []byte("testkey")
	value := []byte("testvalue")

	err := lsm.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := lsm.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(value) {
		t.Fatalf("expected value %q, got %q", value, retrieved)
	}
}

func TestLSMTreeBackend_GetNonExistentKey(t *testing.T) {
	lsm := createTestLSM(t)

	_, err := lsm.Get([]byte("nonexistent"))
	if err == nil {
		t.Fatal("expected error for non-existent key, got nil")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found' error, got: %v", err)
	}
}

func TestLSMTreeBackend_UpdateValue(t *testing.T) {
	lsm := createTestLSM(t)

	key := []byte("key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	lsm.Put(key, value1)
	retrieved1, _ := lsm.Get(key)
	if string(retrieved1) != "value1" {
		t.Fatalf("expected initial value 'value1', got %q", retrieved1)
	}

	lsm.Put(key, value2)
	retrieved2, _ := lsm.Get(key)
	if string(retrieved2) != "value2" {
		t.Fatalf("expected updated value 'value2', got %q", retrieved2)
	}
}

func TestLSMTreeBackend_MultipleKV(t *testing.T) {
	lsm := createTestLSM(t)

	testCases := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
		{[]byte("abc"), []byte("xyz")},
	}

	for _, tc := range testCases {
		if err := lsm.Put(tc.key, tc.value); err != nil {
			t.Fatalf("Put failed for key %q: %v", tc.key, err)
		}
	}

	for _, tc := range testCases {
		retrieved, err := lsm.Get(tc.key)
		if err != nil {
			t.Fatalf("Get failed for key %q: %v", tc.key, err)
		}
		if string(retrieved) != string(tc.value) {
			t.Fatalf("key %q: expected %q, got %q", tc.key, tc.value, retrieved)
		}
	}
}

func TestLSMTreeBackend_ScanReturnsMergedView(t *testing.T) {
	lsm := createIsolatedTestLSM(t)

	if err := lsm.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put(alpha) error = %v", err)
	}
	if err := lsm.Put([]byte("bravo"), []byte("two")); err != nil {
		t.Fatalf("Put(bravo) error = %v", err)
	}
	if err := lsm.Put([]byte("charlie"), []byte("three")); err != nil {
		t.Fatalf("Put(charlie) error = %v", err)
	}

	lsm.flush()

	if err := lsm.Put([]byte("bravo"), []byte("two-updated")); err != nil {
		t.Fatalf("Put(bravo updated) error = %v", err)
	}
	if err := lsm.Delete([]byte("alpha")); err != nil {
		t.Fatalf("Delete(alpha) error = %v", err)
	}
	if err := lsm.Put([]byte("delta"), []byte("four")); err != nil {
		t.Fatalf("Put(delta) error = %v", err)
	}

	entries, err := lsm.Scan([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	got := scanEntriesToMap(entries)
	want := map[string]string{
		"bravo":   "two-updated",
		"charlie": "three",
		"delta":   "four",
	}

	if len(got) != len(want) {
		t.Fatalf("Scan() entry count = %d, want %d (keys=%v)", len(got), len(want), sortedScanKeys(entries))
	}
	for key, wantValue := range want {
		if got[key] != wantValue {
			t.Fatalf("Scan() key %q = %q, want %q", key, got[key], wantValue)
		}
	}
	if _, exists := got["alpha"]; exists {
		t.Fatalf("Scan() unexpectedly returned tombstoned key alpha")
	}
}

func TestLSMTreeBackend_MemtableFlush(t *testing.T) {
	lsm := createTestLSM(t)

	initialLevel0Tables := 0
	if len(lsm.levels) > 0 {
		initialLevel0Tables = len(lsm.levels[0])
	}

	for i := 0; i < SSTableMaxSize+10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := lsm.Put(key, value); err != nil {
			t.Fatalf("Put failed at iteration %d: %v", i, err)
		}
	}

	if len(lsm.levels) == 0 {
		t.Fatal("expected at least one level after flush")
	}
	if len(lsm.levels[0]) <= initialLevel0Tables {
		t.Fatalf("expected level-0 table count to increase after flush, before=%d after=%d", initialLevel0Tables, len(lsm.levels[0]))
	}

	for i := 0; i < SSTableMaxSize+10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expected := fmt.Sprintf("value%d", i)
		retrieved, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("Get failed for key %q: %v", key, err)
		}
		if string(retrieved) != expected {
			t.Fatalf("key %q: expected %q, got %q", key, expected, retrieved)
		}
	}
}

func TestLSMTreeBackend_MemtableResetAfterFlush(t *testing.T) {
	lsm := createTestLSM(t)

	for i := 0; i < SSTableMaxSize-1; i++ {
		key := []byte(fmt.Sprintf("fillkey%d", i))
		lsm.Put(key, []byte("fillvalue"))
	}

	memtableBeforeTrigger := len(lsm.memtable.data)
	if memtableBeforeTrigger != SSTableMaxSize-1 {
		t.Fatalf("expected memtable size %d before trigger, got %d", SSTableMaxSize-1, memtableBeforeTrigger)
	}

	lsm.Put([]byte("triggerkey"), []byte("triggervalue"))

	levelsAfterFlush := len(lsm.levels)
	if levelsAfterFlush == 0 {
		t.Fatalf("expected levels to exist after flush, but got none")
	}

	for i := 0; i < SSTableMaxSize-1; i++ {
		key := []byte(fmt.Sprintf("fillkey%d", i))
		retrieved, _ := lsm.Get(key)
		if string(retrieved) != "fillvalue" {
			t.Fatalf("post-flush original key fillkey%d not found or incorrect", i)
		}
	}

	retrieved, _ := lsm.Get([]byte("triggerkey"))
	if string(retrieved) != "triggervalue" {
		t.Fatalf("post-flush trigger key not found or incorrect")
	}
}

func TestLSMTreeBackend_LargeValues(t *testing.T) {
	lsm := createTestLSM(t)

	largeValue := make([]byte, 10000)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	key := []byte("largekey")
	lsm.Put(key, largeValue)

	retrieved, err := lsm.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(retrieved) != len(largeValue) {
		t.Fatalf("expected value length %d, got %d", len(largeValue), len(retrieved))
	}

	for i := range largeValue {
		if retrieved[i] != largeValue[i] {
			t.Fatalf("byte mismatch at index %d", i)
		}
	}
}

func TestLSMTreeBackend_EdgeCases(t *testing.T) {
	lsm := createTestLSM(t)

	lsm.Put([]byte("emptykey"), []byte(""))
	retrieved, _ := lsm.Get([]byte("emptykey"))
	if len(retrieved) != 0 {
		t.Fatalf("expected empty value, got %v", retrieved)
	}

	specialKey := []byte("key\x00\xFF\x01")
	lsm.Put(specialKey, []byte("specialvalue"))
	retrieved, _ = lsm.Get(specialKey)
	if string(retrieved) != "specialvalue" {
		t.Fatalf("special character key failed")
	}
}

func TestLSMTreeBackend_TotalItemsTracking(t *testing.T) {
	lsm := createTestLSM(t)

	if lsm.totalItems.Load() != 0 {
		t.Fatalf("expected totalItems 0 at start, got %d", lsm.totalItems.Load())
	}

	for i := 0; i < 50; i++ {
		lsm.Put([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
	}

	if lsm.totalItems.Load() != 50 {
		t.Fatalf("expected totalItems 50, got %d", lsm.totalItems.Load())
	}
}

func TestLSMTreeBackend_CascadingCompaction(t *testing.T) {
	lsm := createTestLSM(t)

	itemCount := SSTableMaxSize * 5
	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("cascadekey%d", i))
		lsm.Put(key, []byte("cascadevalue"))
	}

	if len(lsm.levels) < 2 {
		t.Fatalf("expected at least 2 levels after cascading compaction, got %d", len(lsm.levels))
	}

	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("cascadekey%d", i))
		_, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("key %q not found after cascading compaction: %v", key, err)
		}
	}
}

func TestLSMTreeBackend_ConcurrentPuts_ThreadSafety(t *testing.T) {
	lsm := createTestLSM(t)
	numGoroutines := 4
	itemsPerGoroutine := 50

	var wg sync.WaitGroup
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("concurrent_g%d_i%d", goroutineID, i))
				value := []byte(fmt.Sprintf("value_g%d_i%d", goroutineID, i))
				if err := lsm.Put(key, value); err != nil {
					t.Errorf("concurrent put failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()
	duration := time.Since(start)
	totalOps := numGoroutines * itemsPerGoroutine

	t.Logf("True Concurrent Puts (%d goroutines, %d items each): %d ops in %v",
		numGoroutines, itemsPerGoroutine, totalOps, duration)

	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < itemsPerGoroutine; i++ {
			key := []byte(fmt.Sprintf("concurrent_g%d_i%d", g, i))
			expected := fmt.Sprintf("value_g%d_i%d", g, i)
			retrieved, err := lsm.Get(key)
			if err != nil {
				t.Errorf("failed to retrieve concurrent key g%d_i%d: %v", g, i, err)
			}
			if string(retrieved) != expected {
				t.Errorf("concurrent key g%d_i%d: expected %q, got %q", g, i, expected, string(retrieved))
			}
		}
	}
}

func TestLSMTreeBackend_ConcurrentReadsWrites(t *testing.T) {
	lsm := createTestLSM(t)

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("initial_key_%d", i))
		lsm.Put(key, []byte(fmt.Sprintf("initial_value_%d", i)))
	}

	var wg sync.WaitGroup
	numReaders := 5
	numWriters := 5
	start := time.Now()

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for i := 0; i < 50; i++ {
				keyNum := i % 100
				key := []byte(fmt.Sprintf("initial_key_%d", keyNum%50))
				if _, err := lsm.Get(key); err != nil {
				}

				newKey := []byte(fmt.Sprintf("new_key_%d", keyNum))
				if _, err := lsm.Get(newKey); err != nil {
				}
			}
		}(r)
	}

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < 50; i++ {
				key := []byte(fmt.Sprintf("new_key_%d_w%d_i%d", writerID, writerID, i))
				value := []byte(fmt.Sprintf("new_value_%d_%d", writerID, i))
				if err := lsm.Put(key, value); err != nil {
					t.Errorf("concurrent write failed: %v", err)
				}
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(start)
	t.Logf("True Concurrent Reads+Writes (%d readers, %d writers): completed in %v",
		numReaders, numWriters, duration)

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("initial_key_%d", i))
		retrieved, err := lsm.Get(key)
		if err != nil {
			continue
		}

		expected := fmt.Sprintf("initial_value_%d", i)
		if string(retrieved) != expected {
			t.Fatalf("initial key %d: expected %q, got %q", i, expected, retrieved)
		}
	}

	for w := 0; w < numWriters; w++ {
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("new_key_%d_w%d_i%d", w, w, i))
			expected := fmt.Sprintf("new_value_%d_%d", w, i)
			retrieved, err := lsm.Get(key)
			if err != nil {
				t.Errorf("writer %d key %d not found: %v", w, i, err)
				continue
			}
			if string(retrieved) != expected {
				t.Errorf("writer %d key %d: expected %q, got %q", w, i, expected, string(retrieved))
			}
		}
	}
}

func TestLSMTreeBackend_SequentialPattern(t *testing.T) {
	lsm := createTestLSM(t)

	itemCount := 500
	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("seq_key_%06d", i))
		value := []byte(fmt.Sprintf("seq_value_%d", i))
		if err := lsm.Put(key, value); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("seq_key_%06d", i))
		expected := fmt.Sprintf("seq_value_%d", i)
		retrieved, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("Get failed at %d: %v", i, err)
		}
		if string(retrieved) != expected {
			t.Fatalf("at %d: expected %q, got %q", i, expected, retrieved)
		}
	}

	for i := 0; i < itemCount; i += 2 {
		key := []byte(fmt.Sprintf("seq_key_%06d", i))
		newValue := []byte(fmt.Sprintf("updated_value_%d", i))
		lsm.Put(key, newValue)
	}

	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("seq_key_%06d", i))
		retrieved, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("Get failed at %d: %v", i, err)
		}

		var expected string
		if i%2 == 0 {
			expected = fmt.Sprintf("updated_value_%d", i)
		} else {
			expected = fmt.Sprintf("seq_value_%d", i)
		}

		if string(retrieved) != expected {
			t.Fatalf("at %d: expected %q, got %q", i, expected, retrieved)
		}
	}
}

func TestLSMTreeBackend_HotColdWorkload(t *testing.T) {
	lsm := createTestLSM(t)

	hotKeyCount := 10
	for i := 0; i < hotKeyCount; i++ {
		key := []byte(fmt.Sprintf("hot_key_%d", i))
		lsm.Put(key, []byte(fmt.Sprintf("hot_value_%d", i)))
	}

	coldKeyCount := 490
	for i := 0; i < coldKeyCount; i++ {
		key := []byte(fmt.Sprintf("cold_key_%d", i))
		lsm.Put(key, []byte(fmt.Sprintf("cold_value_%d", i)))
	}

	for iteration := 0; iteration < 100; iteration++ {
		for i := 0; i < hotKeyCount; i++ {
			key := []byte(fmt.Sprintf("hot_key_%d", i))
			retrieved, err := lsm.Get(key)
			if err != nil {
				t.Fatalf("hot key access failed: %v", err)
			}
			if len(retrieved) == 0 {
				t.Fatal("hot key returned empty value")
			}
		}
	}

	for i := 0; i < hotKeyCount; i++ {
		key := []byte(fmt.Sprintf("hot_key_%d", i))
		_, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("hot key %d not found", i)
		}
	}

	for i := 0; i < coldKeyCount; i++ {
		key := []byte(fmt.Sprintf("cold_key_%d", i))
		_, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("cold key %d not found", i)
		}
	}
}
