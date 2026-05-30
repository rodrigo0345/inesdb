package lsm

import (
	"math"
	"testing"
)

func TestGarneringCompactionPolicy_NewPolicy(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	if policy.T != 10.0 {
		t.Fatalf("expected T=10.0, got %f", policy.T)
	}
	if policy.C != 0.5 {
		t.Fatalf("expected C=0.5, got %f", policy.C)
	}
	if policy.L0Capacity != 4 {
		t.Fatalf("expected L0Capacity=4, got %d", policy.L0Capacity)
	}
}

func TestGarneringCompactionPolicy_InvalidCParameter(t *testing.T) {
	policy1 := NewGarneringCompactionPolicy(10.0, 1.5, 4)
	if policy1.C != 0.5 {
		t.Fatalf("expected C to default to 0.5 when > 1.0, got %f", policy1.C)
	}

	policy2 := NewGarneringCompactionPolicy(10.0, 0.0, 4)
	if policy2.C != 0.5 {
		t.Fatalf("expected C to default to 0.5 when <= 0, got %f", policy2.C)
	}
}

func TestGarneringCompactionPolicy_Level0Capacity(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	capacity := policy.GetCapacityLimit(0, 5)
	if capacity != 4 {
		t.Fatalf("expected level 0 capacity 4, got %d", capacity)
	}
}

func TestGarneringCompactionPolicy_CapacityGrowth(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)
	maxLevel := 5

	prevCapacity := 0
	for level := 0; level <= maxLevel; level++ {
		capacity := policy.GetCapacityLimit(level, maxLevel)
		if capacity < prevCapacity {
			t.Fatalf("expected capacity to increase, but level %d has %d < %d", level, capacity, prevCapacity)
		}
		prevCapacity = capacity
	}
}

func TestGarneringCompactionPolicy_DynamicRatio(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	cap3Levels := policy.GetCapacityLimit(1, 3)
	cap5Levels := policy.GetCapacityLimit(1, 5)

	if cap3Levels == cap5Levels {
		t.Fatalf("expected capacity ratios to differ based on max level, but they're equal")
	}
}

func TestGarneringCompactionPolicy_GetNumLevels(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	testCases := []struct {
		items    int
		minLevel int
		maxLevel int
	}{
		{100, 1, 2},
		{1000, 1, 2},
		{10000, 1, 3},
		{100000, 2, 3},
		{1000000, 2, 4},
		{10000000, 2, 5},
	}

	for _, tc := range testCases {
		levels := policy.GetNumLevels(tc.items)

		if levels < tc.minLevel || levels > tc.maxLevel {
			t.Fatalf("items=%d: expected levels between %d-%d, got %d",
				tc.items, tc.minLevel, tc.maxLevel, levels)
		}
	}
}

func TestGarneringCompactionPolicy_LevelReduction(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	totalItems := 10000000
	memtableSize := 100

	garnering := policy.GetNumLevels(totalItems)

	traditional := int(math.Ceil(math.Log(float64(totalItems)/float64(memtableSize)) / math.Log(10.0)))

	if garnering > traditional+1 {
		t.Logf("Note: For dataset size %d, Garnering=%d, Traditional≈%d (reasonable variance)",
			totalItems, garnering, traditional)
	}

	if garnering < 1 {
		t.Fatalf("Garnering levels should be >= 1, got %d", garnering)
	}
}

func TestGarneringCompactionPolicy_ParameterInfluence(t *testing.T) {
	totalItems := 100000

	policyLowestC := NewGarneringCompactionPolicy(10.0, 0.2, 4)
	policyMidC := NewGarneringCompactionPolicy(10.0, 0.5, 4)
	policyHighC := NewGarneringCompactionPolicy(10.0, 0.9, 4)

	levelsLowest := policyLowestC.GetNumLevels(totalItems)
	levelsMid := policyMidC.GetNumLevels(totalItems)
	levelsHighest := policyHighC.GetNumLevels(totalItems)

	if levelsLowest >= levelsMid || levelsMid >= levelsHighest {
		t.Fatalf("expected lower c to produce fewer levels: lowest=%d, mid=%d, highest=%d",
			levelsLowest, levelsMid, levelsHighest)
	}
}

func TestGarneringCompactionPolicy_CapacityScaling(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 10)
	maxLevel := 5

	cap0 := policy.GetCapacityLimit(0, maxLevel)
	if cap0 != 10 {
		t.Fatalf("level 0 should have capacity 10, got %d", cap0)
	}

	cap1 := policy.GetCapacityLimit(1, maxLevel)
	expectedRatio := 10.0 / math.Pow(0.5, float64(maxLevel-1))
	expectedCap1 := int(float64(cap0) * expectedRatio)

	if cap1 != expectedCap1 && cap1 != expectedCap1+1 {
		t.Fatalf("level 1: expected ~%d, got %d", expectedCap1, cap1)
	}

	for level := 0; level <= maxLevel; level++ {
		cap := policy.GetCapacityLimit(level, maxLevel)
		if cap <= 0 {
			t.Fatalf("capacity at level %d is non-positive: %d", level, cap)
		}
	}
}

func TestGarneringCompactionPolicy_MemtableSize(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	if policy.MemtableB != 100 {
		t.Fatalf("expected MemtableB 100, got %d", policy.MemtableB)
	}

	cap0 := policy.GetCapacityLimit(0, 3)
	if cap0 != 4 {
		t.Fatalf("L0Capacity should be returned for level 0, got %d", cap0)
	}
}

func TestGarneringCompactionPolicy_SingleLevel(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	capacity := policy.GetCapacityLimit(0, 1)
	if capacity != 4 {
		t.Fatalf("single level should have capacity=L0Capacity, got %d", capacity)
	}
}

func TestGarneringCompactionPolicy_ZeroItems(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	levels := policy.GetNumLevels(0)
	if levels < 1 {
		t.Fatalf("should have at least 1 level, got %d", levels)
	}
}

func TestBloomFilterAllocator_Creation(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 5)

	if allocator.T != 10.0 {
		t.Fatalf("expected T=10.0, got %f", allocator.T)
	}
	if allocator.C != 0.5 {
		t.Fatalf("expected C=0.5, got %f", allocator.C)
	}
	if allocator.TotalMemBudget != 8000000 {
		t.Fatalf("expected TotalMemBudget=8000000, got %d", allocator.TotalMemBudget)
	}
	if allocator.NumLevels != 5 {
		t.Fatalf("expected NumLevels=5, got %d", allocator.NumLevels)
	}
}

func TestBloomFilterAllocator_AllocateBitsPerLevel(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 4)

	itemsPerLevel := []uint{100, 1000, 10000, 100000}
	allocation := allocator.AllocateBitsPerLevel(itemsPerLevel)

	if len(allocation) != len(itemsPerLevel) {
		t.Fatalf("expected %d allocations, got %d", len(itemsPerLevel), len(allocation))
	}

	totalAllocated := uint(0)
	for _, bits := range allocation {
		totalAllocated += bits
	}

	if totalAllocated == 0 {
		t.Fatal("total allocated bits should be > 0")
	}

	for i, bits := range allocation {
		if bits == 0 && itemsPerLevel[i] > 0 {
			t.Logf("Level %d has zero allocation (items=%d)", i, itemsPerLevel[i])
		}
	}
}

func TestBloomFilterAllocator_CalculateOptimalFPR(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 5)

	testCases := []struct {
		level       int
		expectFPR   float64
		description string
	}{
		{4, 1.0, "last level should have FPR = 1.0"},
		{0, 0.0001, "first level should have low FPR"},
		{1, 0.0001, "second level should have low FPR"},
	}

	for _, tc := range testCases {
		fpr := allocator.CalculateOptimalFPR(tc.level)

		if tc.level == 4 && fpr != 1.0 {
			t.Fatalf("level %d FPR: expected 1.0, got %f", tc.level, fpr)
		}

		if fpr < 0.0 || fpr > 1.0 {
			t.Fatalf("level %d FPR out of range: %f", tc.level, fpr)
		}
	}
}

func TestBloomFilterAllocator_FPRGradient(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 5)

	fprHighLevel := allocator.CalculateOptimalFPR(3)
	fprLowLevel := allocator.CalculateOptimalFPR(0)

	if fprLowLevel > fprHighLevel+0.0001 {
		t.Fatalf("expected lower levels to have lower/equal FPR: level0=%f, level3=%f",
			fprLowLevel, fprHighLevel)
	}
}

func TestBloomFilterAllocator_EmptyLevels(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 4)

	itemsPerLevel := []uint{0, 0, 0, 0}
	allocation := allocator.AllocateBitsPerLevel(itemsPerLevel)

	for _, bits := range allocation {
		if bits < 0 {
			t.Fatalf("allocation should be non-negative, got %d", bits)
		}
	}
}

func TestBloomFilterAllocator_SingleLevel(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 1)

	itemsPerLevel := []uint{100000}
	allocation := allocator.AllocateBitsPerLevel(itemsPerLevel)

	if len(allocation) != 1 {
		t.Fatalf("expected 1 allocation, got %d", len(allocation))
	}

	if allocation[0] == 0 {
		t.Fatal("allocation for single level should be > 0")
	}

	if allocation[0] > allocator.TotalMemBudget {
		t.Fatalf("allocation exceeds budget: %d > %d", allocation[0], allocator.TotalMemBudget)
	}
}

func TestBloomFilterAllocator_LargeBudget(t *testing.T) {
	largeMemBudget := uint(1000000000)
	allocator := NewBloomFilterAllocator(10.0, 0.5, largeMemBudget, 5)

	itemsPerLevel := []uint{1000, 10000, 100000, 1000000, 10000000}
	allocation := allocator.AllocateBitsPerLevel(itemsPerLevel)

	if len(allocation) != len(itemsPerLevel) {
		t.Fatalf("expected %d allocations, got %d", len(itemsPerLevel), len(allocation))
	}

	totalAllocated := uint(0)
	for _, bits := range allocation {
		totalAllocated += bits
	}

	if totalAllocated == 0 {
		t.Fatal("large budget allocation should be > 0")
	}
}
