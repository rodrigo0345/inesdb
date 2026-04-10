package lsm

import (
	"fmt"
	"math"
)

type CompactionPolicy interface {
	GetCapacityLimit(level int, maxLevel int) int
	GetNumLevels(totalItems int) int
}

// GarneringCompactionPolicy implements the Autumn paper's Garnering policy.
//
// The Garnering merge policy dynamically adjusts capacity ratios between adjacent levels
// to optimize read performance while maintaining scalable writes.
//
// Key formula: C_i / C_{i-1} = T / c^(L-i) where c < 1 is a scaling factor
//
// This flattens the LSM-tree compared to traditional policies:
// - Traditional Leveling: Number of levels = O(log_T(N/B))
// - Garnering: Number of levels = O(√-log_c(N/B·T))
//
// Benefits:
// - Point read cost improves from O(log N) to O(√log N)
// - Range read performance also improves by the same factor
// - Range queries benefit from fewer levels to scan
// - Write amplification remains sub-linear: O(T/c^√-log_c(N))
type GarneringCompactionPolicy struct {
	T          float64 // Base capacity ratio between last two levels (typically 10)
	C          float64 // Scaling factor (0 < c < 1, typically 0.5). Controls level expansion rate
	L0Capacity int     // Base capacity for Level 0 (number of SSTables before compaction)
	MemtableB  int     // Size of memtable in entries
}

func NewGarneringCompactionPolicy(t, c float64, l0Cap int) *GarneringCompactionPolicy {
	if c >= 1.0 || c <= 0 {
		fmt.Println("Warning: c should be between 0 and 1, defaulting to 0.5")
		c = 0.5
	}
	return &GarneringCompactionPolicy{
		T:          t,
		C:          c,
		L0Capacity: l0Cap,
		MemtableB:  100, // Default memtable size in entries
	}
}

// GetCapacityLimit returns the number of SSTables allowed at a given level
// before compaction is triggered.
//
// Calculation based on Equation 5 from the paper:
// C_i = T^i / c^((2L-1-i)*i/2) * B
func (g *GarneringCompactionPolicy) GetCapacityLimit(level int, maxLevel int) int {
	if level == 0 {
		return g.L0Capacity
	}

	// For level i (i > 0), compute the capacity ratio from level i-1 to level i
	// Ratio = T / c^(L-i)
	if maxLevel <= 0 {
		return g.L0Capacity
	}

	// Accumulate ratios from level 0 to current level
	capacity := float64(g.L0Capacity)
	for i := 1; i <= level; i++ {
		exp := float64(maxLevel - i)
		r := g.T / math.Pow(g.C, exp)
		capacity *= r
	}

	return int(math.Round(capacity))
}

// GetNumLevels returns the optimal number of levels needed to store n items.
//
// Based on Equation 6 from the paper:
// L = O(√-log_c(N/(B·T)))
//
// This shows that Garnering reduces the number of levels with a square root function,
// compared to Leveling's logarithmic function. This is the key to improved read performance.
func (g *GarneringCompactionPolicy) GetNumLevels(totalItems int) int {
	if totalItems <= g.L0Capacity*g.MemtableB {
		return 1
	}

	// Calculate: √-log_c(N/(B·T))
	// Note: -log_c(x) = ln(x) / -ln(c) = ln(x) / ln(1/c)
	ratio := float64(totalItems) / (float64(g.MemtableB) * g.T)

	// Avoid negative log
	if ratio <= 0 {
		return 1
	}

	// -log_c(ratio) where c < 1
	logValue := math.Log(ratio) / math.Log(1/g.C)
	if logValue < 0 {
		logValue = -logValue
	}

	// √logValue
	numLevels := int(math.Ceil(math.Sqrt(logValue)))

	// Ensure at least 1 level
	if numLevels < 1 {
		numLevels = 1
	}

	return numLevels
}
