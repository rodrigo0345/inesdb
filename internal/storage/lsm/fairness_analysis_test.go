package lsm

import (
	"fmt"
	"testing"
	"time"
)

// ============================================================================
// FAIRNESS ANALYSIS: Investigate Test Conditions
// ============================================================================

func TestFairnessAnalysis_CompactionOverhead(t *testing.T) {
	lsm := createTestLSM(t)

	// Track state before
	beforeFlushes := 0

	// Put items that trigger flushes
	numItems := 600 // 6 flushes expected (100 items per flush)
	start := time.Now()

	for i := 0; i < numItems; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		lsm.Put(key, value)

		// Check if flush occurred
		if len(lsm.memtable.data) == 0 {
			beforeFlushes++
		}
	}

	duration := time.Since(start)

	t.Logf(`
	
FAIRNESS ANALYSIS - LSM Compaction Overhead:
============================================

Items Inserted: %d
Memtable Size: %d
Number of Levels: %d
Expected Flushes: %d (approx)
Actual Duration: %v
Calculated Throughput: %.0f ops/sec

Level Distribution:
`, numItems, len(lsm.memtable.data), len(lsm.levels), numItems/100, duration, float64(numItems)/duration.Seconds())

	for i, level := range lsm.levels {
		t.Logf("  Level %d: %d SSTables", i, len(level))
	}

	t.Logf(`
Note: Above throughput includes:
  - Memtable inserts (fast, map operations)
  - Flush operations (creating SSTables, bloom filters)
  - Compaction operations (merging levels)
  
This is a HIDDEN cost not directly measured in "Put" operations.
`)
}

func TestFairnessAnalysis_PutLatency(t *testing.T) {
	lsm := createTestLSM(t)

	// Measure individual Put latencies to see the distribution
	latencies := make([]time.Duration, 0)

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("latency_test_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))

		start := time.Now()
		lsm.Put(key, value)
		latency := time.Since(start)

		latencies = append(latencies, latency)
	}

	// Find min, max, avg
	minLat := latencies[0]
	maxLat := latencies[0]
	totalLat := time.Duration(0)

	for _, lat := range latencies {
		if lat < minLat {
			minLat = lat
		}
		if lat > maxLat {
			maxLat = lat
		}
		totalLat += lat
	}

	avgLat := totalLat / time.Duration(len(latencies))

	t.Logf(`
FAIRNESS ANALYSIS - LSM Put Latency Distribution:
=================================================

Number of Operations: %d
Min Latency: %v
Max Latency: %v
Avg Latency: %v
Total Time: %v

Analysis:
- Most operations are fast (memtable inserts)
- Occasional spikes when flush/compaction occurs
- This variance is NOT captured in throughput measurements
`, len(latencies), minLat, maxLat, avgLat, totalLat)
}

func TestFairnessAnalysis_MockImplementation(t *testing.T) {
	t.Logf(`

FAIRNESS ANALYSIS - Mock Implementation Differences:
====================================================

LSM Mock (from lsm_tree_backend_test.go):
  type mockBufferManager struct{}
  
  func (m *mockBufferManager) NewPage() (*page.IResourcePage, error) {
    return nil, nil  // <-- Returns nil, no actual page allocation!
  }
  
  func (m *mockBufferManager) PinPage(pageID page.ResourcePageID) (page.IResourcePage, error) {
    return nil, nil  // <-- Returns nil, no page locking/management!
  }
  
  Analysis: STUB IMPLEMENTATION
  - No actual page management
  - No allocationoverhead
  - No locking overhead
  - No persistence overhead

B+Tree Mock (from btree/performance_comparison_test.go):
  type mockBufferManager struct {
    pages      map[page.ResourcePageID]*mockResourcePage
    nextPageID page.ResourcePageID
    mu         sync.Mutex
  }
  
  func (m *mockBufferManager) NewPage() (*page.IResourcePage, error) {
    m.mu.Lock()
    // ... actual page allocation with locking
    mockPage := newMockResourcePage(pageID, 4096)
    m.mu.Lock()
    m.pages[pageID] = mockPage  // <-- Stores page in map
    m.mu.Unlock()
    // ... returns actual page reference
  }
  
  func (m *mockBufferManager) PinPage(pageID page.ResourcePageID) (page.IResourcePage, error) {
    m.mu.Lock()
    p, ok := m.pages[pageID]  // <-- Locks, retrieves from map
    m.mu.Unlock()
    if !ok {
      // ... create and store new page
    }
    // ... returns actual page reference
  }
  
  Analysis: REALISTIC IMPLEMENTATION
  - Actual page allocation and storage
  - Mutex locking on all operations
  - Memory overhead tracking
  - State management
  - Simulates real buffer pool behavior

CONCLUSION:
===========
The LSM mocks are STUBS (do nothing), while B+Tree mocks are REALISTIC.
This creates MASSIVE UNFAIRNESS:

1. LSM Put is measured as: Just memtable.Put(key, value)
   - Plus occasional flush and compaction (sometimes in Put!)
   
2. B+Tree Put is measured as: Full tree operation
   - PinPage (with locking, state management)
   - Tree traversal and search
   - Page updates with dirty marking
   - UnpinPage (with locking, state management)

LSM appears slower on reads because:
- It must search multiple levels
- Each level search calls sstable.Get which checks bloom filter

B+Tree is faster on reads because:
- It does a tree traversal (which is bypassed in mock)
- Returns first result found

For fair comparison, both need equivalent mock implementations.
`)
}
