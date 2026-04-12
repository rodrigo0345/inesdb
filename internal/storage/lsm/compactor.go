package lsm

import (
	"fmt"
	"math"
)

type CompactionPolicy interface {
	GetCapacityLimit(level int, maxLevel int) int
	GetNumLevels(totalItems int) int
}

type GarneringCompactionPolicy struct {
	T          float64
	C          float64
	L0Capacity int
	MemtableB  int
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
		MemtableB:  100,
	}
}

func (g *GarneringCompactionPolicy) GetCapacityLimit(level int, maxLevel int) int {
	if level == 0 {
		return g.L0Capacity
	}

	if maxLevel <= 0 {
		return g.L0Capacity
	}

	capacity := float64(g.L0Capacity)
	for i := 1; i <= level; i++ {
		exp := float64(maxLevel - i)
		r := g.T / math.Pow(g.C, exp)
		capacity *= r
	}

	return int(math.Round(capacity))
}

func (g *GarneringCompactionPolicy) GetNumLevels(totalItems int) int {
	if totalItems <= g.L0Capacity*g.MemtableB {
		return 1
	}

	ratio := float64(totalItems) / (float64(g.MemtableB) * g.T)

	if ratio <= 0 {
		return 1
	}

	logValue := math.Log(ratio) / math.Log(1/g.C)
	if logValue < 0 {
		logValue = -logValue
	}

	numLevels := int(math.Ceil(math.Sqrt(logValue)))

	if numLevels < 1 {
		numLevels = 1
	}

	return numLevels
}
