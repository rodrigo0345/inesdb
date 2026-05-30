package concurrency

import (
	"math/rand"
	"sync"
	"testing"
)

func BenchmarkLRU_Unpin(b *testing.B) {
	replacer := NewLRUReplacer(10000)
	frameID := FrameID(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacer.Unpin(frameID)
		frameID = (frameID + 1) % 10000
	}
}

func BenchmarkClock_Unpin(b *testing.B) {
	replacer := NewClockReplacer(10000)
	frameID := FrameID(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacer.Unpin(frameID)
		frameID = (frameID + 1) % 10000
	}
}

func BenchmarkLRU_Pin(b *testing.B) {
	replacer := NewLRUReplacer(10000)
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

func BenchmarkClock_Pin(b *testing.B) {
	replacer := NewClockReplacer(10000)
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

func BenchmarkLRU_Victim(b *testing.B) {
	replacer := NewLRUReplacer(10000)
	for i := 0; i < 10000; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameid, ok := replacer.Victim()
		if !ok {
			for j := 0; j < 10000; j++ {
				replacer.Unpin(FrameID(j))
			}
		} else {
			_ = frameid
		}
	}
}

func BenchmarkClock_Victim(b *testing.B) {
	replacer := NewClockReplacer(10000)
	for i := 0; i < 10000; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frameid, ok := replacer.Victim()
		if !ok {
			for j := 0; j < 10000; j++ {
				replacer.Unpin(FrameID(j))
			}
		} else {
			_ = frameid
		}
	}
}

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

func BenchmarkLRU_SequentialAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

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

func BenchmarkClock_SequentialAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

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

func BenchmarkLRU_RandomAccess(b *testing.B) {
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

func BenchmarkClock_RandomAccess(b *testing.B) {
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

func BenchmarkLRU_WorkingSet(b *testing.B) {
	poolSize := 10000
	workingSetSize := poolSize / 5
	replacer := NewLRUReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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

func BenchmarkClock_WorkingSet(b *testing.B) {
	poolSize := 10000
	workingSetSize := poolSize / 5
	replacer := NewClockReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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

func BenchmarkLRU_HighEvictionRate(b *testing.B) {
	poolSize := 1000
	replacer := NewLRUReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	nextFrame := FrameID(poolSize)
	for i := 0; i < b.N; i++ {
		_, ok := replacer.Victim()
		if !ok {
			for j := 0; j < poolSize; j++ {
				replacer.Unpin(FrameID(j))
			}
			nextFrame = FrameID(poolSize)
		} else {
			replacer.Unpin(nextFrame)
			nextFrame++
		}
	}
}

func BenchmarkClock_HighEvictionRate(b *testing.B) {
	poolSize := 1000
	replacer := NewClockReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	b.ResetTimer()
	nextFrame := FrameID(poolSize)
	for i := 0; i < b.N; i++ {
		_, ok := replacer.Victim()
		if !ok {
			for j := 0; j < poolSize; j++ {
				replacer.Unpin(FrameID(j))
			}
			nextFrame = FrameID(poolSize)
		} else {
			replacer.Unpin(nextFrame)
			nextFrame++
		}
	}
}

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

func BenchmarkLRU_MixedWorkload(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := rng.Intn(100)

		if op < 40 {
			frameID := FrameID(rng.Intn(poolSize))
			replacer.Unpin(frameID)
		} else if op < 70 {
			frameID := FrameID(rng.Intn(poolSize))
			replacer.Pin(frameID)
		} else if op < 95 {
			_, ok := replacer.Victim()
			if !ok {
				for j := 0; j < poolSize; j++ {
					replacer.Unpin(FrameID(j))
				}
			}
		} else {
			_ = replacer.Size()
		}
	}
}

func BenchmarkClock_MixedWorkload(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := rng.Intn(100)

		if op < 40 {
			frameID := FrameID(rng.Intn(poolSize))
			replacer.Unpin(frameID)
		} else if op < 70 {
			frameID := FrameID(rng.Intn(poolSize))
			replacer.Pin(frameID)
		} else if op < 95 {
			_, ok := replacer.Victim()
			if !ok {
				for j := 0; j < poolSize; j++ {
					replacer.Unpin(FrameID(j))
				}
			}
		} else {
			_ = replacer.Size()
		}
	}
}

func BenchmarkLRU_Concurrent(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

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

func BenchmarkClock_Concurrent(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

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

func BenchmarkLRU_PinHeavy(b *testing.B) {
	poolSize := 10000
	pinnedFrames := poolSize / 2
	replacer := NewLRUReplacer(poolSize)

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
		if frameID < FrameID(pinnedFrames) {
			replacer.Pin(frameID)
		} else {
			replacer.Unpin(frameID)
		}
	}
}

func BenchmarkClock_PinHeavy(b *testing.B) {
	poolSize := 10000
	pinnedFrames := poolSize / 2
	replacer := NewClockReplacer(poolSize)

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
		if frameID < FrameID(pinnedFrames) {
			replacer.Pin(frameID)
		} else {
			replacer.Unpin(frameID)
		}
	}
}


func BenchmarkLRU_ZipfianAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewLRUReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := rng.Float64()
		frameID := FrameID(int(float64(poolSize) * (1.0 - u*u)))

		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}

func BenchmarkClock_ZipfianAccess(b *testing.B) {
	poolSize := 10000
	replacer := NewClockReplacer(poolSize)

	for i := 0; i < poolSize; i++ {
		replacer.Unpin(FrameID(i))
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := rng.Float64()
		frameID := FrameID(int(float64(poolSize) * (1.0 - u*u)))

		replacer.Pin(frameID)
		replacer.Unpin(frameID)
	}
}
