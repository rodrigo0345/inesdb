package lsm

import (
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure that is used to
// test whether an element is a member of a set.
type BloomFilter struct {
	m      uint   // Number of bits in the filter
	k      uint   // Number of hash functions
	bitset []byte // The bit array
}

// NewBloomFilter creates a new BloomFilter given the expected number of items
// and the desired false positive rate.
func NewBloomFilter(expectedItems uint, falsePositiveRate float64) *BloomFilter {
	if expectedItems == 0 {
		expectedItems = 1 // Prevent division by zero
	}

	// Calculate optimal size of bit array (m) and optimal number of hash functions (k)
	m := uint(math.Ceil(float64(expectedItems) * math.Log(falsePositiveRate) / math.Log(1.0/math.Pow(2, math.Log(2)))))
	k := uint(math.Round((float64(m) / float64(expectedItems)) * math.Log(2)))

	// Ensure at least 1 hash function and at least 1 byte
	if k == 0 {
		k = 1
	}
	if m == 0 {
		m = 8
	}

	return &BloomFilter{
		m:      m,
		k:      k,
		bitset: make([]byte, (m+7)/8),
	}
}

// Add inserts an element into the Bloom filter.
func (bf *BloomFilter) Add(data []byte) {
	h1, h2 := hash(data)
	for i := uint(0); i < bf.k; i++ {
		bitIndex := (uint(h1) + i*uint(h2)) % bf.m
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8
		bf.bitset[byteIndex] |= (1 << bitOffset)
	}
}

// Test checks if an element is likely in the Bloom filter.
func (bf *BloomFilter) Test(data []byte) bool {
	h1, h2 := hash(data)
	for i := uint(0); i < bf.k; i++ {
		bitIndex := (uint(h1) + i*uint(h2)) % bf.m
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8
		if bf.bitset[byteIndex]&(1<<bitOffset) == 0 {
			return false // Definitely not in the set
		}
	}
	return true // Probably in the set
}

// hash uses FNV-1a to generate two 32-bit hash values from the data.
// This allows us to simulate k hash functions using the formula: h_i(x) = h1(x) + i * h2(x).
func hash(data []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(data)
	val := h.Sum64()
	return uint32(val), uint32(val >> 32)
}

// BloomFilterAllocator handles optimal bloom filter memory allocation across LSM levels
// based on the Autumn paper's optimization strategy (Section 3, Bloom Filter Optimization).
//
// The key insight: For point queries that don't find the key, the cost is the sum of FPRs
// across all levels. Since lower levels have fewer entries but each miss costs the same,
// allocating more bits to lower levels is memory-efficient.
//
// Optimal FPR at level i:
// p_i = O(R · c^((L-i)*(L-i-1)/2) / T^(L-i))
// where R is the desired read cost and c, T are from the Garnering policy.
type BloomFilterAllocator struct {
	T              float64 // Base capacity ratio
	C              float64 // Scaling factor
	TotalMemBudget uint    // Total bits available for all bloom filters
	NumLevels      int     // Total number of levels
}

// NewBloomFilterAllocator creates an allocator for optimal bloom filter distribution
func NewBloomFilterAllocator(t float64, c float64, memBudgetBytes uint, numLevels int) *BloomFilterAllocator {
	return &BloomFilterAllocator{
		T:              t,
		C:              c,
		TotalMemBudget: memBudgetBytes * 8, // Convert bytes to bits
		NumLevels:      numLevels,
	}
}

// AllocateBitsPerLevel returns the optimal number of bits to allocate to each level's bloom filter.
// This implements the Autumn paper's Monkey optimization strategy.
//
// The allocation prioritizes lower levels where each bit saves more disk I/Os.
func (bfa *BloomFilterAllocator) AllocateBitsPerLevel(itemsPerLevel []uint) []uint {
	if len(itemsPerLevel) == 0 {
		return []uint{}
	}

	bitsPerLevel := make([]uint, len(itemsPerLevel))

	// Calculate the weight for each level based on Garnering structure
	// Lower levels get exponentially more weight per item
	weights := make([]float64, len(itemsPerLevel))
	totalWeight := 0.0

	for i := 0; i < len(itemsPerLevel); i++ {
		if itemsPerLevel[i] == 0 {
			weights[i] = 0
			continue
		}

		// Weight is inversely proportional to level size
		// Lower levels (smaller indices) get higher weights
		levelIndex := len(itemsPerLevel) - 1 - i
		exponent := float64(levelIndex * (levelIndex + 1) / 2)
		weight := math.Pow(bfa.C, exponent) / math.Pow(bfa.T, float64(levelIndex))
		weights[i] = weight * float64(itemsPerLevel[i])
		totalWeight += weights[i]
	}

	// Distribute memory budget proportionally
	if totalWeight > 0 {
		for i := 0; i < len(itemsPerLevel); i++ {
			allocation := uint(math.Round((weights[i] / totalWeight) * float64(bfa.TotalMemBudget)))
			bitsPerLevel[i] = allocation
		}
	}

	return bitsPerLevel
}

// CalculateOptimalFPR computes the optimal false positive rate for a given level
// based on the Garnering policy parameters.
//
// Formula from Autumn paper (Equation 9):
// p_{L-i} = p_L · c^(i(i-1)/2) / T^i
func (bfa *BloomFilterAllocator) CalculateOptimalFPR(levelIndex int) float64 {
	if levelIndex >= bfa.NumLevels {
		return 1.0 // Last level has FPR = 1 (no filtering)
	}

	if levelIndex == bfa.NumLevels-1 {
		return 1.0 // Last level (largest) has FPR = 1.0
	}

	distanceFromEnd := bfa.NumLevels - 1 - levelIndex
	exponent := float64(distanceFromEnd * (distanceFromEnd - 1) / 2)

	// Base FPR for last level is 1.0
	fpr := 1.0 * math.Pow(bfa.C, exponent) / math.Pow(bfa.T, float64(distanceFromEnd))

	// Clamp to valid range
	if fpr < 0.0001 {
		return 0.0001
	}
	if fpr > 1.0 {
		return 1.0
	}

	return fpr
}
