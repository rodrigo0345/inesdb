package concurrency

import (
	"math/rand"
	"sync"
	"testing"
)

// ============================================================================
// Benchmark: Basic Operations
// ============================================================================

// BenchmarkLRU_Unpin measures Unpin operation performance for LRU replacer
func BenchmarkLRU_Unpin(b *testing.B) {
	replacer := NewLRUReplacer(10000)
	frameID := FrameID(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacer.Unpin(frameID)
		frameID = (frameID + 1) % 10000
	}
}

// BenchmarkClock_Unpin measures Unpin operation performance for Clock replacer
func BenchmarkClock_Unpin(b *testing.B) {
	replacer := NewClockReplacer(10000)
	frameID := FrameID(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacer.Unpin(frameID)
		frameID = (frameID + 1) % 10000
	}
}

// BenchmarkLRU_Pin measures Pin operation performance for LRU replacer
func BenchmarkLRU_Pin(b *testing.B) {
	replacer := NewLRUReplacer(10000)
	// Pre-load frames
	for i := 0; i < 10000; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	frameID := FrameID(0)
	for i := 0; i < b.N; i++ {
		replacer.Pin(frameID)
		frameID = (frameID + 1) % 10000
	}
}

// BenchmarkClock_Pin measures Pin operation performance for Clock replacer
func BenchmarkClock_Pin(b *testing.B) {
	replacer := NewClockReplacer(10000)
	// Pre-load frames
	for i := 0; i < 10000; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	frameID := FrameID(0)
	for i := 0; i < b.N; i++ {
		replacer.Pin(frameID)
		frameID = (frameID + 1) % 10000
	}
}

// BenchmarkLRU_Victim measures Victim operation performance for LRU replacer
func BenchmarkLRU_Victim(b *testing.B) {
	replacer := NewLRUReplacer(10000)
	// Pre-load frames
	for i := 0; i < 10000; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameid, ok := replacer.Victim()
		if !ok {
			// Reload when empty
			for j := 0; j < 10000; j++ {
				replacer.Unpin(FrameID(j))
			}
		} else {
			_ = frameid
		}
	}
}

// BenchmarkClock_Victim measures Victim operation performance for Clock replacer
func BenchmarkClock_Victim(b *testing.B) {
	replacer := NewClockReplacer(10000)
	// Pre-load frames
	for i := 0; i < 10000; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameid, ok := replacer.Victim()
		if !ok {
			// Reload when empty
			for j := 0; j < 10000; j++ {
				replacer.Unpin(FrameID(j))
			}
		} else {
			_ = frameid
		}
	}
}

// BenchmarkLRU_Size measures Size operation performance for LRU replacer
func BenchmarkLRU_Size(b *testing.B) {
	replacer := NewLRUReplacer(10000)
	for i := 0; i < 1000; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = replacer.Size()
	}
}

// BenchmarkClock_Size measures Size operation performance for Clock replacer
func BenchmarkClock_Size(b *testing.B) {
	replacer := NewClockReplacer(10000)
	for i := 0; i < 1000; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = replacer.Size()
	}
}

// ============================================================================
// Benchmark: Sequential Access Patterns
// ============================================================================

// BenchmarkLRU_SequentialAccess simulates linear access to frames
func BenchmarkLRU_SequentialAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

	// Pre-load all frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(i % poolSize)
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// BenchmarkClock_SequentialAccess simulates linear access to frames
func BenchmarkClock_SequentialAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

	// Pre-load all frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(i % poolSize)
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// ============================================================================
// Benchmark: Random Access Patterns
// ============================================================================

// BenchmarkLRU_RandomAccess simulates random access to frames
func BenchmarkLRU_RandomAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

	// Pre-load all frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// BenchmarkClock_RandomAccess simulates random access to frames
func BenchmarkClock_RandomAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

	// Pre-load all frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// ============================================================================
// Benchmark: Working Set Simulation
// ============================================================================

// BenchmarkLRU_WorkingSet simulates a hot working set of 20% of frames
func BenchmarkLRU_WorkingSet(b *testing.B) {
	poolSize := 10000
	workingSetSize := poolSize / 5 // 20% of frames are hot
	replacer := NewLRUReplacer(poolSize)

	// Pre-load all frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 80% of the time, access from working set
		var frameID FrameID
		if rng.Float64() < 0.8 {
			frameID = FrameID(rng.Intn(workingSetSize))
		} else {
			frameID = FrameID(rng.Intn(poolSize))
		}
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// BenchmarkClock_WorkingSet simulates a hot working set of 20% of frames
func BenchmarkClock_WorkingSet(b *testing.B) {
	poolSize := 10000
	workingSetSize := poolSize / 5 // 20% of frames are hot
	replacer := NewClockReplacer(poolSize)

	// Pre-load all frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 80% of the time, access from working set
		var frameID FrameID
		if rng.Float64() < 0.8 {
			frameID = FrameID(rng.Intn(workingSetSize))
		} else {
			frameID = FrameID(rng.Intn(poolSize))
		}
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// ============================================================================
// Benchmark: Victim Selection Frequency
// ============================================================================

// BenchmarkLRU_HighEvictionRate tests performance with frequent victim selections
func BenchmarkLRU_HighEvictionRate(b *testing.B) {
	poolSize := 1000
	replacer := NewLRUReplacer(poolSize)

	// Pre-load frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	nextFrame := FrameID(poolSize)
	for i := 0; i < b.N; i++ {
		// Evict a frame
		_, ok := replacer.Victim()
		if !ok {
			// Reload if needed
			for j := 0; j < poolSize; j++ {
				replacer.Unpin(FrameID(j))
			}
			nextFrame = FrameID(poolSize)
		} else {
			// Add new frame
			replacer.Unpin(nextFrame)
			nextFrame++
		}
	}
}

// BenchmarkClock_HighEvictionRate tests performance with frequent victim selections
func BenchmarkClock_HighEvictionRate(b *testing.B) {
	poolSize := 1000
	replacer := NewClockReplacer(poolSize)

	// Pre-load frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	nextFrame := FrameID(poolSize)
	for i := 0; i < b.N; i++ {
		// Evict a frame
		_, ok := replacer.Victim()
		if !ok {
			// Reload if needed
			for j := 0; j < poolSize; j++ {
				replacer.Unpin(FrameID(j))
			}
			nextFrame = FrameID(poolSize)
		} else {
			// Add new frame
			replacer.Unpin(nextFrame)
			nextFrame++
		}
	}
}

// ============================================================================
// Benchmark: Scalability with Different Pool Sizes
// ============================================================================

// BenchmarkLRU_SmallPool tests performance with a small pool (100 frames)
func BenchmarkLRU_SmallPool(b *testing.B) {
	poolSize := 100
	replacer := NewLRUReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// BenchmarkClock_SmallPool tests performance with a small pool (100 frames)
func BenchmarkClock_SmallPool(b *testing.B) {
	poolSize := 100
	replacer := NewClockReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// BenchmarkLRU_MediumPool tests performance with a medium pool (10000 frames)
func BenchmarkLRU_MediumPool(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// BenchmarkClock_MediumPool tests performance with a medium pool (10000 frames)
func BenchmarkClock_MediumPool(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// BenchmarkLRU_LargePool tests performance with a large pool (100000 frames)
func BenchmarkLRU_LargePool(b *testing.B) {
	poolSize := 100000
	replacer := NewLRUReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// BenchmarkClock_LargePool tests performance with a large pool (100000 frames)
func BenchmarkClock_LargePool(b *testing.B) {
	poolSize := 100000
	replacer := NewClockReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// ============================================================================
// Benchmark: Mixed Workload (Realistic scenario)
// ============================================================================

// BenchmarkLRU_MixedWorkload simulates a realistic workload with pin, unpin, and victim
func BenchmarkLRU_MixedWorkload(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

	// Pre-load frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := rng.Intn(100)

		if op < 40 {
			// 40% unpin operations (mark frame as available)
			frameID := FrameID(rng.Intn(poolSize))
			replacer.Unpin(frameID)
		} else if op < 70 {
			// 30% pin operations (mark frame as in-use)
			frameID := FrameID(rng.Intn(poolSize))
			replacer.Pin(frameID)
		} else if op < 95 {
			// 25% victim selections
			_, ok := replacer.Victim()
			if !ok {
				// Reload if needed
				for j := 0; j < poolSize; j++ {
					replacer.Unpin(FrameID(j))
				}
			}
		} else {
			// 5% size queries
			_ = replacer.Size()
		}
	}
}

// BenchmarkClock_MixedWorkload simulates a realistic workload with pin, unpin, and victim
func BenchmarkClock_MixedWorkload(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

	// Pre-load frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := rng.Intn(100)

		if op < 40 {
			// 40% unpin operations (mark frame as available)
			frameID := FrameID(rng.Intn(poolSize))
			replacer.Unpin(frameID)
		} else if op < 70 {
			// 30% pin operations (mark frame as in-use)
			frameID := FrameID(rng.Intn(poolSize))
			replacer.Pin(frameID)
		} else if op < 95 {
			// 25% victim selections
			_, ok := replacer.Victim()
			if !ok {
				// Reload if needed
				for j := 0; j < poolSize; j++ {
					replacer.Unpin(FrameID(j))
				}
			}
		} else {
			// 5% size queries
			_ = replacer.Size()
		}
	}
}

// ============================================================================
// Benchmark: Concurrent Access
// ============================================================================

// BenchmarkLRU_Concurrent tests performance with concurrent goroutines
func BenchmarkLRU_Concurrent(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

	// Pre-load frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	numGoroutines := 8
	operationsPerGoroutine := b.N / numGoroutines

	b.ResetTimer()
	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(goroutineID)))

			for i := 0; i < operationsPerGoroutine; i++ {
				frameID := FrameID(rng.Intn(poolSize))
				if rng.Float64() < 0.5 {
					replacer.Pin(frameID)
				} else {
					replacer.Unpin(frameID)
				}
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkClock_Concurrent tests performance with concurrent goroutines
func BenchmarkClock_Concurrent(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

	// Pre-load frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	numGoroutines := 8
	operationsPerGoroutine := b.N / numGoroutines

	b.ResetTimer()
	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(goroutineID)))

			for i := 0; i < operationsPerGoroutine; i++ {
				frameID := FrameID(rng.Intn(poolSize))
				if rng.Float64() < 0.5 {
					replacer.Pin(frameID)
				} else {
					replacer.Unpin(frameID)
				}
			}
		}(g)
	}
	wg.Wait()
}

// ============================================================================
// Benchmark: Pin-Heavy Workload (many pinned frames)
func BenchmarkLRU_PinHeavy(b *testing.B) {
	poolSize := 10000
	pinnedFrames := poolSize / 2 // 50% of frames are pinned
	replacer := NewLRUReplacer(poolSize)

	// Pre-load and pin half the frames
	for i := 0; i < poolSize; i++ {
		if i < pinnedFrames {
			replacer.Unpin(FrameID(i))
			replacer.Pin(FrameID(i))
		} else {
			replacer.Unpin(FrameID(i))
		}
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		// Try to pin already pinned frames
		if frameID < FrameID(pinnedFrames) {
			replacer.Pin(frameID)
		} else {
			replacer.Unpin(frameID)
		}
	}
}

// BenchmarkClock_PinHeavy tests Clock replacer with heavy pinning
func BenchmarkClock_PinHeavy(b *testing.B) {
	poolSize := 10000
	pinnedFrames := poolSize / 2 // 50% of frames are pinned
	replacer := NewClockReplacer(poolSize)

	// Pre-load and pin half the frames
	for i := 0; i < poolSize; i++ {
		if i < pinnedFrames {
			replacer.Unpin(FrameID(i))
			replacer.Pin(FrameID(i))
		} else {
			replacer.Unpin(FrameID(i))
		}
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameID := FrameID(rng.Intn(poolSize))
		// Try to pin already pinned frames
		if frameID < FrameID(pinnedFrames) {
			replacer.Pin(frameID)
		} else {
			replacer.Unpin(frameID)
		}
	}
}

// ============================================================================
// Benchmark: Zipfian Distribution (realistic pagecache workload)
// ============================================================================

// BenchmarkLRU_ZipfianAccess tests with Zipfian distribution (few frames accessed frequently)
func BenchmarkLRU_ZipfianAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

	// Pre-load all frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	// Simplified Zipfian: bias towards lower frame IDs
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Generate Zipfian-like distribution (exponential bias towards lower IDs)
		u := rng.Float64()
		frameID := FrameID(int(float64(poolSize) * (1.0 - u*u)))

		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

// BenchmarkClock_ZipfianAccess tests with Zipfian distribution (few frames accessed frequently)
func BenchmarkClock_ZipfianAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

	// Pre-load all frames
	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	// Simplified Zipfian: bias towards lower frame IDs
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Generate Zipfian-like distribution (exponential bias towards lower IDs)
		u := rng.Float64()
		frameID := FrameID(int(float64(poolSize) * (1.0 - u*u)))

		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}
