package btree

import (
	"fmt"
	"math/rand"
	"testing"
)

// ============================================================================
// B+TREE BENCHMARKS - Using Go's built-in benchmark tool
// ============================================================================
//
// Run benchmarks with:
//   go test -bench=. -benchmem -benchtime=5s ./internal/storage/btree
//
// Compare LSM and B+Tree with:
//   go test -bench=. -benchmem -benchtime=5s ./internal/storage/lsm ./internal/storage/btree
//
// This uses Go's internal benchmark infrastructure which:
// - Automatically scales b.N to find meaningful run times
// - Calculates statistics (min, max, avg, stddev)
// - Reports allocation statistics (-benchmem)
// - Provides reproducible, accurate measurements
// ============================================================================

// Helper to create test B+Tree
func createBenchmarkBTree(b *testing.B) *BPlusTreeBackend {
	bufferMgr := newMockBufferManager()
	diskMgr := newMockDiskManager()

	btree, err := NewBPlusTreeBackend(bufferMgr, diskMgr)
	if err != nil {
		b.Fatalf("failed to create B+ tree: %v", err)
	}

	return btree
}

// ============================================================================
// BENCHMARK 1: SEQUENTIAL WRITES
// ============================================================================

func BenchmarkBTree_SequentialWrites(b *testing.B) {
	btree := createBenchmarkBTree(b)
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("seq_write_%010d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		if err := btree.Put(key, value); err != nil {
			b.Logf("Put error: %v", err)
		}
	}
}

// ============================================================================
// BENCHMARK 2: RANDOM WRITES
// ============================================================================

func BenchmarkBTree_RandomWrites(b *testing.B) {
	btree := createBenchmarkBTree(b)
	rng := rand.New(rand.NewSource(42))
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randomID := rng.Intn(b.N * 10)
		key := []byte(fmt.Sprintf("rand_write_%010d", randomID))
		value := []byte(fmt.Sprintf("value_%d", i))
		if err := btree.Put(key, value); err != nil {
			b.Logf("Put error: %v", err)
		}
	}
}

// ============================================================================
// BENCHMARK 3: SEQUENTIAL READS
// ============================================================================

func BenchmarkBTree_SequentialReads(b *testing.B) {
	btree := createBenchmarkBTree(b)

	// Pre-populate with sequential keys
	populateCount := 10000
	for i := 0; i < populateCount; i++ {
		key := []byte(fmt.Sprintf("seq_read_%010d", i))
		btree.Put(key, []byte("value"))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("seq_read_%010d", i%populateCount))
		btree.Get(key)
	}
}

// ============================================================================
// BENCHMARK 4: RANDOM READS
// ============================================================================

func BenchmarkBTree_RandomReads(b *testing.B) {
	btree := createBenchmarkBTree(b)
	rng := rand.New(rand.NewSource(123))

	// Pre-populate with random keys
	populateCount := 10000
	for i := 0; i < populateCount; i++ {
		randomID := rng.Intn(100000)
		key := []byte(fmt.Sprintf("rand_read_%010d", randomID))
		btree.Put(key, []byte("value"))
	}

	rng = rand.New(rand.NewSource(456))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		randomID := rng.Intn(100000)
		key := []byte(fmt.Sprintf("rand_read_%010d", randomID))
		btree.Get(key)
	}
}

// ============================================================================
// BENCHMARK 5: MIXED READ/WRITE (70% read, 30% write)
// ============================================================================

func BenchmarkBTree_MixedReadWrite(b *testing.B) {
	btree := createBenchmarkBTree(b)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("mixed_%010d", i))
		btree.Put(key, []byte("value"))
	}

	rng := rand.New(rand.NewSource(789))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.7 {
			// Read operation
			keyID := rng.Intn(1000)
			key := []byte(fmt.Sprintf("mixed_%010d", keyID))
			btree.Get(key)
		} else {
			// Write operation
			keyID := rng.Intn(2000)
			key := []byte(fmt.Sprintf("mixed_%010d", keyID))
			value := []byte(fmt.Sprintf("value_%d", keyID))
			btree.Put(key, value)
		}
	}
}

// ============================================================================
// BENCHMARK 6: HOT/COLD KEY DISTRIBUTION (80% hot, 20% cold)
// ============================================================================

func BenchmarkBTree_HotColdDistribution(b *testing.B) {
	btree := createBenchmarkBTree(b)

	totalKeys := 500
	hotKeyCount := 100 // 20% are hot
	rng := rand.New(rand.NewSource(999))

	// Pre-populate all keys
	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("hc_%010d", i))
		btree.Put(key, []byte("value"))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var keyID int
		if rng.Float64() < 0.8 {
			// Access hot keys
			keyID = rng.Intn(hotKeyCount)
		} else {
			// Access cold keys
			keyID = hotKeyCount + rng.Intn(totalKeys-hotKeyCount)
		}

		key := []byte(fmt.Sprintf("hc_%010d", keyID))
		if rng.Float64() < 0.5 {
			btree.Get(key)
		} else {
			btree.Put(key, []byte(fmt.Sprintf("updated_%d", i)))
		}
	}
}

// ============================================================================
// BENCHMARK 7: LARGE VALUES (1KB, 10KB, 100KB)
// ============================================================================

func BenchmarkBTree_LargeValues_1KB(b *testing.B) {
	btree := createBenchmarkBTree(b)
	largeValue := make([]byte, 1024) // 1KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("large_1k_%010d", i))
		if err := btree.Put(key, largeValue); err != nil {
			b.Logf("Put error: %v", err)
		}
	}
}

func BenchmarkBTree_LargeValues_10KB(b *testing.B) {
	btree := createBenchmarkBTree(b)
	largeValue := make([]byte, 10240) // 10KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("large_10k_%010d", i))
		if err := btree.Put(key, largeValue); err != nil {
			b.Logf("Put error: %v", err)
		}
	}
}

func BenchmarkBTree_LargeValues_100KB(b *testing.B) {
	btree := createBenchmarkBTree(b)
	largeValue := make([]byte, 102400) // 100KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("large_100k_%010d", i))
		if err := btree.Put(key, largeValue); err != nil {
			b.Logf("Put error: %v", err)
		}
	}
}

// ============================================================================
// BENCHMARK 8: UPDATE-HEAVY (many updates to same keys)
// ============================================================================

func BenchmarkBTree_UpdateHeavy(b *testing.B) {
	btree := createBenchmarkBTree(b)

	numKeys := 100
	keysToUpdate := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keysToUpdate[i] = []byte(fmt.Sprintf("update_%010d", i))
		btree.Put(keysToUpdate[i], []byte("initial"))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		keyIdx := i % numKeys
		value := []byte(fmt.Sprintf("update_%d", i))
		btree.Put(keysToUpdate[keyIdx], value)
	}
}

// ============================================================================
// BENCHMARK 9: TREE BALANCE STRESS (many inserts forcing rebalancing)
// ============================================================================

func BenchmarkBTree_TreeRebalanceStress(b *testing.B) {
	btree := createBenchmarkBTree(b)
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("rebalance_%010d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		if err := btree.Put(key, value); err != nil {
			b.Logf("Put error: %v", err)
		}
	}
}
