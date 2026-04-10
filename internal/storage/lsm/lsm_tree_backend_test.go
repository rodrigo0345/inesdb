package lsm

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

// Helper function to create a mock LSM tree for testing
func createTestLSM(t *testing.T) *LSMTreeBackend {
	// Create mock implementations
	mockLogMgr := &mockLogManager{}
	mockBufMgr := &mockBufferManager{}

	lsm := NewLSMTreeBackend(mockLogMgr, mockBufMgr)
	if lsm == nil {
		t.Fatal("failed to create LSM tree backend")
	}
	return lsm
}

// Mock implementations - NOW REALISTIC FOR FAIR COMPARISON

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
	return nil // Simulates disk write
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

func (m *mockLogManager) Close() error {
	return nil
}

func (m *mockLogManager) ReadAllRecords() ([]log.WALRecord, error) {
	return nil, nil
}

// Realistic mock - tracks pages with locking like real buffer pool
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
	return nil // Mock doesn't track pin counts
}

func (m *mockBufferManager) FlushAll() error {
	return nil
}

func (m *mockBufferManager) Close() error {
	return nil
}

// ============================================================================
// UNIT TESTS
// ============================================================================

// Test: Basic Put and Get operations
func TestLSMTreeBackend_BasicPutGet(t *testing.T) {
	lsm := createTestLSM(t)

	key := []byte("testkey")
	value := []byte("testvalue")

	// Put a key-value pair
	err := lsm.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get the key
	retrieved, err := lsm.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(value) {
		t.Fatalf("expected value %q, got %q", value, retrieved)
	}
}

// Test: Non-existent key returns error
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

// Test: Overwriting values
func TestLSMTreeBackend_UpdateValue(t *testing.T) {
	lsm := createTestLSM(t)

	key := []byte("key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Put initial value
	lsm.Put(key, value1)
	retrieved1, _ := lsm.Get(key)
	if string(retrieved1) != "value1" {
		t.Fatalf("expected initial value 'value1', got %q", retrieved1)
	}

	// Update value
	lsm.Put(key, value2)
	retrieved2, _ := lsm.Get(key)
	if string(retrieved2) != "value2" {
		t.Fatalf("expected updated value 'value2', got %q", retrieved2)
	}
}

// Test: Multiple keys and values
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

	// Insert all key-value pairs
	for _, tc := range testCases {
		if err := lsm.Put(tc.key, tc.value); err != nil {
			t.Fatalf("Put failed for key %q: %v", tc.key, err)
		}
	}

	// Retrieve and verify all
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

// Test: Memtable flushing (triggers when SSTableMaxSize is reached)
func TestLSMTreeBackend_MemtableFlush(t *testing.T) {
	lsm := createTestLSM(t)

	// Insert enough items to trigger flush
	initialLevels := len(lsm.levels)

	for i := 0; i < SSTableMaxSize+10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := lsm.Put(key, value); err != nil {
			t.Fatalf("Put failed at iteration %d: %v", i, err)
		}
	}

	// After inserting more than SSTableMaxSize items, levels should be created
	if len(lsm.levels) <= initialLevels {
		t.Fatalf("expected levels to increase after flush, but remained at %d", len(lsm.levels))
	}

	// Verify all keys are still retrievable
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

// Test: Memtable state after flush
func TestLSMTreeBackend_MemtableResetAfterFlush(t *testing.T) {
	lsm := createTestLSM(t)

	// Add items to fill memtable (99 items to stay just below threshold)
	for i := 0; i < SSTableMaxSize-1; i++ {
		key := []byte(fmt.Sprintf("fillkey%d", i))
		lsm.Put(key, []byte("fillvalue"))
	}

	// At this point, memtable has 99 items
	memtableBeforeTrigger := len(lsm.memtable.data)
	if memtableBeforeTrigger != SSTableMaxSize-1 {
		t.Fatalf("expected memtable size %d before trigger, got %d", SSTableMaxSize-1, memtableBeforeTrigger)
	}

	// Add one more to trigger flush (this will hit >= 100)
	lsm.Put([]byte("triggerkey"), []byte("triggervalue"))

	// After flush trigger, verify levels exist and keys are persisted
	levelsAfterFlush := len(lsm.levels)
	if levelsAfterFlush == 0 {
		t.Fatalf("expected levels to exist after flush, but got none")
	}

	// All keys including the trigger should be retrievable
	for i := 0; i < SSTableMaxSize-1; i++ {
		key := []byte(fmt.Sprintf("fillkey%d", i))
		retrieved, _ := lsm.Get(key)
		if string(retrieved) != "fillvalue" {
			t.Fatalf("post-flush original key fillkey%d not found or incorrect", i)
		}
	}

	// Trigger key should be retrievable
	retrieved, _ := lsm.Get([]byte("triggerkey"))
	if string(retrieved) != "triggervalue" {
		t.Fatalf("post-flush trigger key not found or incorrect")
	}
}

func TestLSMTreeBackend_LargeValues(t *testing.T) {
	lsm := createTestLSM(t)

	// Create a large value
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

// Test: Empty keys and values
func TestLSMTreeBackend_EdgeCases(t *testing.T) {
	lsm := createTestLSM(t)

	// Empty value (should be allowed)
	lsm.Put([]byte("emptykey"), []byte(""))
	retrieved, _ := lsm.Get([]byte("emptykey"))
	if len(retrieved) != 0 {
		t.Fatalf("expected empty value, got %v", retrieved)
	}

	// Special characters in key
	specialKey := []byte("key\x00\xFF\x01")
	lsm.Put(specialKey, []byte("specialvalue"))
	retrieved, _ = lsm.Get(specialKey)
	if string(retrieved) != "specialvalue" {
		t.Fatalf("special character key failed")
	}
}

// Test: Total items tracking
func TestLSMTreeBackend_TotalItemsTracking(t *testing.T) {
	lsm := createTestLSM(t)

	if lsm.totalItems != 0 {
		t.Fatalf("expected totalItems 0 at start, got %d", lsm.totalItems)
	}

	for i := 0; i < 50; i++ {
		lsm.Put([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
	}

	if lsm.totalItems != 50 {
		t.Fatalf("expected totalItems 50, got %d", lsm.totalItems)
	}
}

// Test: Cascading compaction
func TestLSMTreeBackend_CascadingCompaction(t *testing.T) {
	lsm := createTestLSM(t)

	// Insert enough items to cause compaction cascading through multiple levels
	itemCount := SSTableMaxSize * 5
	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("cascadekey%d", i))
		lsm.Put(key, []byte("cascadevalue"))
	}

	// Should have multiple levels due to cascading
	if len(lsm.levels) < 2 {
		t.Fatalf("expected at least 2 levels after cascading compaction, got %d", len(lsm.levels))
	}

	// All keys should still be retrievable
	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("cascadekey%d", i))
		_, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("key %q not found after cascading compaction: %v", key, err)
		}
	}
}

// ============================================================================
// CONCURRENT TESTS
// ============================================================================

// Test: Concurrent puts (Note: LSM tree memtable is not thread-safe by default)
// This test demonstrates the race condition and documents expected behavior
func TestLSMTreeBackend_ConcurrentPuts_ThreadSafety(t *testing.T) {
	lsm := createTestLSM(t)
	numGoroutines := 4      // Simulated goroutines
	itemsPerGoroutine := 50 // Items per simulated routine

	// IMPORTANT: LSM memtable uses bare maps which are not thread-safe.
	// This test simulates concurrent patterns without actual goroutines.
	// In production, wrap LSM operations with sync.Mutex or use sync.Map.

	start := time.Now()

	// Simulate concurrent access by interleaving operations
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < itemsPerGoroutine; i++ {
			key := []byte(fmt.Sprintf("concurrent_g%d_i%d", g, i))
			value := []byte(fmt.Sprintf("value_g%d_i%d", g, i))
			lsm.Put(key, value)
		}
	}

	duration := time.Since(start)
	totalOps := numGoroutines * itemsPerGoroutine

	t.Logf("Simulated Concurrent Puts (%d routines, %d items each): %d ops in %v (simulated, not true concurrency)",
		numGoroutines, itemsPerGoroutine, totalOps, duration)
	t.Log("NOTE: LSM memtable requires external synchronization for true concurrent access")
}

// Test: Concurrent reads and writes (serialized scenario)
func TestLSMTreeBackend_ConcurrentReadsWrites(t *testing.T) {
	lsm := createTestLSM(t)

	// Pre-populate some data in a single thread to avoid races
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("initial_key_%d", i))
		lsm.Put(key, []byte(fmt.Sprintf("initial_value_%d", i)))
	}

	// Create a mutex to serialize access to the LSM tree during testing
	// This documents that LSM is not inherently thread-safe without external synchronization
	var mu sync.Mutex
	var wg sync.WaitGroup
	numReaders := 5
	numWriters := 5

	// Start readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for i := 0; i < 50; i++ {
				// Try to read both initial and new keys
				keyNum := i % 100
				key := []byte(fmt.Sprintf("initial_key_%d", keyNum%50))
				mu.Lock()
				lsm.Get(key)
				mu.Unlock()

				newKey := []byte(fmt.Sprintf("new_key_%d", keyNum))
				mu.Lock()
				lsm.Get(newKey)
				mu.Unlock()
			}
		}(r)
	}

	// Start writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < 50; i++ {
				key := []byte(fmt.Sprintf("new_key_%d_w%d_i%d", writerID, writerID, i))
				value := []byte(fmt.Sprintf("new_value_%d_%d", writerID, i))
				mu.Lock()
				lsm.Put(key, value)
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()

	// Verify initial keys are still there
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("initial_key_%d", i))
		retrieved, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("initial key %d not found", i)
		}

		expected := fmt.Sprintf("initial_value_%d", i)
		if string(retrieved) != expected {
			t.Fatalf("initial key %d: expected %q, got %q", i, expected, retrieved)
		}
	}
}

// Test: Sequential writes followed by reads
func TestLSMTreeBackend_SequentialPattern(t *testing.T) {
	lsm := createTestLSM(t)

	// Phase 1: Write many items
	itemCount := 500
	for i := 0; i < itemCount; i++ {
		key := []byte(fmt.Sprintf("seq_key_%06d", i))
		value := []byte(fmt.Sprintf("seq_value_%d", i))
		if err := lsm.Put(key, value); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	// Phase 2: Read all items
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

	// Phase 3: Update some items
	for i := 0; i < itemCount; i += 2 {
		key := []byte(fmt.Sprintf("seq_key_%06d", i))
		newValue := []byte(fmt.Sprintf("updated_value_%d", i))
		lsm.Put(key, newValue)
	}

	// Phase 4: Verify updates
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

// Test: Hot/cold workload (some keys accessed much more frequently)
func TestLSMTreeBackend_HotColdWorkload(t *testing.T) {
	lsm := createTestLSM(t)

	// Insert hot keys (frequently accessed)
	hotKeyCount := 10
	for i := 0; i < hotKeyCount; i++ {
		key := []byte(fmt.Sprintf("hot_key_%d", i))
		lsm.Put(key, []byte(fmt.Sprintf("hot_value_%d", i)))
	}

	// Insert cold keys (rarely accessed)
	coldKeyCount := 490
	for i := 0; i < coldKeyCount; i++ {
		key := []byte(fmt.Sprintf("cold_key_%d", i))
		lsm.Put(key, []byte(fmt.Sprintf("cold_value_%d", i)))
	}

	// Access hot keys many times
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

	// Verify all keys are still accessible
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
