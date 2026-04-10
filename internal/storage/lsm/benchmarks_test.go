package lsm

import (
	"fmt"
	"math/rand"
	"testing"
)

// ============================================================================
// LSM TREE BENCHMARKS - Using Go's built-in benchmark tool
// ============================================================================
//
// Run benchmarks with:
//   go test -bench=. -benchmem -benchtime=5s ./internal/storage/lsm
//
// This uses Go's internal benchmark infrastructure which:
// - Automatically scales b.N to find meaningful run times
// - Calculates statistics (min, max, avg, stddev)
// - Reports allocation statistics (-benchmem)
// - Provides reproducible, accurate measurements
// ============================================================================

// ============================================================================
// BENCHMARK 1: SEQUENTIAL WRITES
// ============================================================================

func BenchmarkLSM_SequentialWrites(b *testing.B) {
	lsm := createTestLSM(&testing.T{})
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("seq_write_%010d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		lsm.Put(key, value)
	}
}

// ============================================================================
// BENCHMARK 2: RANDOM WRITES
// ============================================================================

func BenchmarkLSM_RandomWrites(b *testing.B) {
	lsm := createTestLSM(&testing.T{})
	rng := rand.New(rand.NewSource(42))
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		randomID := rng.Intn(b.N * 10)
		key := []byte(fmt.Sprintf("rand_write_%010d", randomID))
		value := []byte(fmt.Sprintf("value_%d", i))
		lsm.Put(key, value)
	}
}

// ============================================================================
// BENCHMARK 3: SEQUENTIAL READS
// ============================================================================

func BenchmarkLSM_SequentialReads(b *testing.B) {
	lsm := createTestLSM(&testing.T{})

	// Pre-populate with sequential keys
	populateCount := 10000
	for i := 0; i < populateCount; i++ {
		key := []byte(fmt.Sprintf("seq_read_%010d", i))
		lsm.Put(key, []byte("value"))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("seq_read_%010d", i%populateCount))
		lsm.Get(key)
	}
}

// ============================================================================
// BENCHMARK 4: RANDOM READS
// ============================================================================

func BenchmarkLSM_RandomReads(b *testing.B) {
	lsm := createTestLSM(&testing.T{})
	rng := rand.New(rand.NewSource(123))

	// Pre-populate with random keys
	populateCount := 10000
	for i := 0; i < populateCount; i++ {
		randomID := rng.Intn(100000)
		key := []byte(fmt.Sprintf("rand_read_%010d", randomID))
		lsm.Put(key, []byte("value"))
	}

	rng = rand.New(rand.NewSource(456))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		randomID := rng.Intn(100000)
		key := []byte(fmt.Sprintf("rand_read_%010d", randomID))
		lsm.Get(key)
	}
}

// ============================================================================
// BENCHMARK 5: MIXED READ/WRITE (70% read, 30% write)
// ============================================================================

func BenchmarkLSM_MixedReadWrite(b *testing.B) {
	lsm := createTestLSM(&testing.T{})

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("mixed_%010d", i))
		lsm.Put(key, []byte("value"))
	}

	rng := rand.New(rand.NewSource(789))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.7 {
			// Read operation
			keyID := rng.Intn(1000)
			key := []byte(fmt.Sprintf("mixed_%010d", keyID))
			lsm.Get(key)
		} else {
			// Write operation
			keyID := rng.Intn(2000)
			key := []byte(fmt.Sprintf("mixed_%010d", keyID))
			value := []byte(fmt.Sprintf("value_%d", keyID))
			lsm.Put(key, value)
		}
	}
}

// ============================================================================
// BENCHMARK 6: HOT/COLD KEY DISTRIBUTION (80% hot, 20% cold)
// ============================================================================

func BenchmarkLSM_HotColdDistribution(b *testing.B) {
	lsm := createTestLSM(&testing.T{})

	totalKeys := 500
	hotKeyCount := 100 // 20% are hot
	rng := rand.New(rand.NewSource(999))

	// Pre-populate all keys
	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("hc_%010d", i))
		lsm.Put(key, []byte("value"))
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
			lsm.Get(key)
		} else {
			lsm.Put(key, []byte(fmt.Sprintf("updated_%d", i)))
		}
	}
}

// ============================================================================
// BENCHMARK 7: LARGE VALUES (1KB, 10KB, 100KB)
// ============================================================================

func BenchmarkLSM_LargeValues_1KB(b *testing.B) {
	lsm := createTestLSM(&testing.T{})
	largeValue := make([]byte, 1024) // 1KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("large_1k_%010d", i))
		lsm.Put(key, largeValue)
	}
}

func BenchmarkLSM_LargeValues_10KB(b *testing.B) {
	lsm := createTestLSM(&testing.T{})
	largeValue := make([]byte, 10240) // 10KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("large_10k_%010d", i))
		lsm.Put(key, largeValue)
	}
}

func BenchmarkLSM_LargeValues_100KB(b *testing.B) {
	lsm := createTestLSM(&testing.T{})
	largeValue := make([]byte, 102400) // 100KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("large_100k_%010d", i))
		lsm.Put(key, largeValue)
	}
}

// ============================================================================
// BENCHMARK 8: UPDATE-HEAVY (many updates to same keys)
// ============================================================================

func BenchmarkLSM_UpdateHeavy(b *testing.B) {
	lsm := createTestLSM(&testing.T{})

	numKeys := 100
	keysToUpdate := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keysToUpdate[i] = []byte(fmt.Sprintf("update_%010d", i))
		lsm.Put(keysToUpdate[i], []byte("initial"))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		keyIdx := i % numKeys
		value := []byte(fmt.Sprintf("update_%d", i))
		lsm.Put(keysToUpdate[keyIdx], value)
	}
}

// ============================================================================
// BENCHMARK 9: COMPACTION STRESS TEST (force multiple flushes)
// ============================================================================

func BenchmarkLSM_CompactionStress(b *testing.B) {
	lsm := createTestLSM(&testing.T{})
	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("comp_stress_%010d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		lsm.Put(key, value)
	}
}
