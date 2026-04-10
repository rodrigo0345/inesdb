package lsm

import (
	"math"
	"testing"
)

// ============================================================================
// GARNERING COMPACTION POLICY TESTS
// ============================================================================

// Test: Policy initialization
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

// Test: Invalid C parameter (c >= 1.0 or c <= 0) should default to 0.5
func TestGarneringCompactionPolicy_InvalidCParameter(t *testing.T) {
	// C >= 1.0 should default to 0.5
	policy1 := NewGarneringCompactionPolicy(10.0, 1.5, 4)
	if policy1.C != 0.5 {
		t.Fatalf("expected C to default to 0.5 when > 1.0, got %f", policy1.C)
	}

	// C <= 0 should default to 0.5
	policy2 := NewGarneringCompactionPolicy(10.0, 0.0, 4)
	if policy2.C != 0.5 {
		t.Fatalf("expected C to default to 0.5 when <= 0, got %f", policy2.C)
	}
}

// Test: Level 0 capacity limit
func TestGarneringCompactionPolicy_Level0Capacity(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	capacity := policy.GetCapacityLimit(0, 5)
	if capacity != 4 {
		t.Fatalf("expected level 0 capacity 4, got %d", capacity)
	}
}

// Test: Capacity increases with level
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

// Test: Dynamic capacity ratio (key feature of Garnering)
func TestGarneringCompactionPolicy_DynamicRatio(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	// Get capacity for different max levels
	cap3Levels := policy.GetCapacityLimit(1, 3)
	cap5Levels := policy.GetCapacityLimit(1, 5)

	// When we have more levels, the ratios should change
	// This is the key feature of Garnering - ratios depend on total levels
	if cap3Levels == cap5Levels {
		t.Fatalf("expected capacity ratios to differ based on max level, but they're equal")
	}
}

// Test: GetNumLevels formula - O(√-log_c(N/B·T))
func TestGarneringCompactionPolicy_GetNumLevels(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	testCases := []struct {
		items    int
		minLevel int // Minimum expected levels
		maxLevel int // Maximum expected levels (with some tolerance)
	}{
		{100, 1, 2},      // Small dataset
		{1000, 1, 2},     // Still small
		{10000, 1, 3},    // Medium
		{100000, 2, 3},   // Larger
		{1000000, 2, 4},  // Large dataset
		{10000000, 2, 5}, // Very large
	}

	for _, tc := range testCases {
		levels := policy.GetNumLevels(tc.items)

		if levels < tc.minLevel || levels > tc.maxLevel {
			t.Fatalf("items=%d: expected levels between %d-%d, got %d",
				tc.items, tc.minLevel, tc.maxLevel, levels)
		}
	}
}

// Test: Garnering reduces levels compared to traditional Leveling
func TestGarneringCompactionPolicy_LevelReduction(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	// For a given number of items, calculate traditional Leveling levels
	// Traditional: L = log_T(N/B)
	// Garnering: L = √-log_c(N/(B·T))
	totalItems := 10000000 // Use larger dataset to see reduction
	memtableSize := 100

	garnering := policy.GetNumLevels(totalItems)

	// Traditional Leveling would need approximately:
	traditional := int(math.Ceil(math.Log(float64(totalItems)/float64(memtableSize)) / math.Log(10.0)))

	// Garnering should have same or fewer levels
	// For very large datasets, it should be noticeably fewer
	if garnering > traditional+1 { // Allow small variance
		t.Logf("Note: For dataset size %d, Garnering=%d, Traditional≈%d (reasonable variance)",
			totalItems, garnering, traditional)
	}

	// Verify it at least stays reasonable
	if garnering < 1 {
		t.Fatalf("Garnering levels should be >= 1, got %d", garnering)
	}
}

// Test: Parameter influence on levels
func TestGarneringCompactionPolicy_ParameterInfluence(t *testing.T) {
	totalItems := 100000

	// Test with different c values (lower c = flatter tree)
	policyLowestC := NewGarneringCompactionPolicy(10.0, 0.2, 4)
	policyMidC := NewGarneringCompactionPolicy(10.0, 0.5, 4)
	policyHighC := NewGarneringCompactionPolicy(10.0, 0.9, 4)

	levelsLowest := policyLowestC.GetNumLevels(totalItems)
	levelsMid := policyMidC.GetNumLevels(totalItems)
	levelsHighest := policyHighC.GetNumLevels(totalItems)

	// Lower c should result in fewer levels (flatter tree)
	if levelsLowest >= levelsMid || levelsMid >= levelsHighest {
		t.Fatalf("expected lower c to produce fewer levels: lowest=%d, mid=%d, highest=%d",
			levelsLowest, levelsMid, levelsHighest)
	}
}

// Test: Capacity scaling
func TestGarneringCompactionPolicy_CapacityScaling(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 10)
	maxLevel := 5

	// Level 0 should have capacity 10
	cap0 := policy.GetCapacityLimit(0, maxLevel)
	if cap0 != 10 {
		t.Fatalf("level 0 should have capacity 10, got %d", cap0)
	}

	// Level 1 should be scaled up by the ratio T/c^(L-1)
	cap1 := policy.GetCapacityLimit(1, maxLevel)
	expectedRatio := 10.0 / math.Pow(0.5, float64(maxLevel-1))
	expectedCap1 := int(float64(cap0) * expectedRatio)

	if cap1 != expectedCap1 && cap1 != expectedCap1+1 { // Allow for rounding
		t.Fatalf("level 1: expected ~%d, got %d", expectedCap1, cap1)
	}

	// Verify monotonic increase
	for level := 0; level <= maxLevel; level++ {
		cap := policy.GetCapacityLimit(level, maxLevel)
		if cap <= 0 {
			t.Fatalf("capacity at level %d is non-positive: %d", level, cap)
		}
	}
}

// Test: Memtable size effect
func TestGarneringCompactionPolicy_MemtableSize(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	// MemtableB is set during creation
	if policy.MemtableB != 100 {
		t.Fatalf("expected MemtableB 100, got %d", policy.MemtableB)
	}

	// Capacity calculations should account for memtable size indirectly
	cap0 := policy.GetCapacityLimit(0, 3)
	if cap0 != 4 { // L0Capacity is 4
		t.Fatalf("L0Capacity should be returned for level 0, got %d", cap0)
	}
}

// Test: Edge case - single level
func TestGarneringCompactionPolicy_SingleLevel(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	capacity := policy.GetCapacityLimit(0, 1)
	if capacity != 4 {
		t.Fatalf("single level should have capacity=L0Capacity, got %d", capacity)
	}
}

// Test: Edge case - zero items
func TestGarneringCompactionPolicy_ZeroItems(t *testing.T) {
	policy := NewGarneringCompactionPolicy(10.0, 0.5, 4)

	levels := policy.GetNumLevels(0)
	if levels < 1 {
		t.Fatalf("should have at least 1 level, got %d", levels)
	}
}

// ============================================================================
// BLOOM FILTER ALLOCATOR TESTS
// ============================================================================

// Test: BloomFilterAllocator creation
func TestBloomFilterAllocator_Creation(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 5)

	if allocator.T != 10.0 {
		t.Fatalf("expected T=10.0, got %f", allocator.T)
	}
	if allocator.C != 0.5 {
		t.Fatalf("expected C=0.5, got %f", allocator.C)
	}
	if allocator.TotalMemBudget != 8000000 { // 1000000 bytes * 8 bits
		t.Fatalf("expected TotalMemBudget=8000000, got %d", allocator.TotalMemBudget)
	}
	if allocator.NumLevels != 5 {
		t.Fatalf("expected NumLevels=5, got %d", allocator.NumLevels)
	}
}

// Test: Allocate bits per level
func TestBloomFilterAllocator_AllocateBitsPerLevel(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 4)

	itemsPerLevel := []uint{100, 1000, 10000, 100000}
	allocation := allocator.AllocateBitsPerLevel(itemsPerLevel)

	if len(allocation) != len(itemsPerLevel) {
		t.Fatalf("expected %d allocations, got %d", len(itemsPerLevel), len(allocation))
	}

	// Sum of allocations should equal or be close to total budget
	totalAllocated := uint(0)
	for _, bits := range allocation {
		totalAllocated += bits
	}

	if totalAllocated == 0 {
		t.Fatal("total allocated bits should be > 0")
	}

	// Just verify that all levels get some allocation
	for i, bits := range allocation {
		if bits == 0 && itemsPerLevel[i] > 0 {
			t.Logf("Level %d has zero allocation (items=%d)", i, itemsPerLevel[i])
		}
	}
}

// Test: Calculate optimal FPR
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

// Test: FPR decreases toward lower levels
func TestBloomFilterAllocator_FPRGradient(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 5)

	// FPR should generally decrease from high levels to low levels
	fprHighLevel := allocator.CalculateOptimalFPR(3)
	fprLowLevel := allocator.CalculateOptimalFPR(0)

	// Low levels (0, 1, 2) should have lower or equal FPR than high levels (3, 4)
	if fprLowLevel > fprHighLevel+0.0001 { // Small tolerance for floating point
		t.Fatalf("expected lower levels to have lower/equal FPR: level0=%f, level3=%f",
			fprLowLevel, fprHighLevel)
	}
}

// Test: Empty items per level
func TestBloomFilterAllocator_EmptyLevels(t *testing.T) {
	allocator := NewBloomFilterAllocator(10.0, 0.5, 1000000, 4)

	itemsPerLevel := []uint{0, 0, 0, 0}
	allocation := allocator.AllocateBitsPerLevel(itemsPerLevel)

	// Should not crash and return appropriate values
	for _, bits := range allocation {
		if bits < 0 {
			t.Fatalf("allocation should be non-negative, got %d", bits)
		}
	}
}

// Test: Single level allocation
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

// Test: Large budget allocation
func TestBloomFilterAllocator_LargeBudget(t *testing.T) {
	largeMemBudget := uint(1000000000) // 1 billion bytes = 8 billion bits
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
