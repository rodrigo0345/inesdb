package lsm

import (
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	m      uint
	k      uint
	bitset []byte
}

func NewBloomFilter(expectedItems uint, falsePositiveRate float64) *BloomFilter {
	if expectedItems == 0 {
		expectedItems = 1
	}

	m := uint(math.Ceil(float64(expectedItems) * math.Log(falsePositiveRate) / math.Log(1.0/math.Pow(2, math.Log(2)))))
	k := uint(math.Round((float64(m) / float64(expectedItems)) * math.Log(2)))

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

func (bf *BloomFilter) Add(data []byte) {
	h1, h2 := hash(data)
	for i := uint(0); i < bf.k; i++ {
		bitIndex := (uint(h1) + i*uint(h2)) % bf.m
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8
		bf.bitset[byteIndex] |= (1 << bitOffset)
	}
}

func (bf *BloomFilter) Test(data []byte) bool {
	h1, h2 := hash(data)
	for i := uint(0); i < bf.k; i++ {
		bitIndex := (uint(h1) + i*uint(h2)) % bf.m
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8
		if bf.bitset[byteIndex]&(1<<bitOffset) == 0 {
			return false
		}
	}
	return true
}

func hash(data []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(data)
	val := h.Sum64()
	return uint32(val), uint32(val >> 32)
}

type BloomFilterAllocator struct {
	T              float64
	C              float64
	TotalMemBudget uint
	NumLevels      int
}

func NewBloomFilterAllocator(t float64, c float64, memBudgetBytes uint, numLevels int) *BloomFilterAllocator {
	return &BloomFilterAllocator{
		T:              t,
		C:              c,
		TotalMemBudget: memBudgetBytes * 8,
		NumLevels:      numLevels,
	}
}

func (bfa *BloomFilterAllocator) AllocateBitsPerLevel(itemsPerLevel []uint) []uint {
	if len(itemsPerLevel) == 0 {
		return []uint{}
	}

	bitsPerLevel := make([]uint, len(itemsPerLevel))

	weights := make([]float64, len(itemsPerLevel))
	totalWeight := 0.0

	for i := 0; i < len(itemsPerLevel); i++ {
		if itemsPerLevel[i] == 0 {
			weights[i] = 0
			continue
		}

		levelIndex := len(itemsPerLevel) - 1 - i
		exponent := float64(levelIndex * (levelIndex + 1) / 2)
		weight := math.Pow(bfa.C, exponent) / math.Pow(bfa.T, float64(levelIndex))
		weights[i] = weight * float64(itemsPerLevel[i])
		totalWeight += weights[i]
	}

	if totalWeight > 0 {
		for i := 0; i < len(itemsPerLevel); i++ {
			allocation := uint(math.Round((weights[i] / totalWeight) * float64(bfa.TotalMemBudget)))
			bitsPerLevel[i] = allocation
		}
	}

	return bitsPerLevel
}

func (bfa *BloomFilterAllocator) CalculateOptimalFPR(levelIndex int) float64 {
	if levelIndex >= bfa.NumLevels {
		return 1.0
	}

	if levelIndex == bfa.NumLevels-1 {
		return 1.0
	}

	distanceFromEnd := bfa.NumLevels - 1 - levelIndex
	exponent := float64(distanceFromEnd * (distanceFromEnd - 1) / 2)

	fpr := 1.0 * math.Pow(bfa.C, exponent) / math.Pow(bfa.T, float64(distanceFromEnd))

	if fpr < 0.0001 {
		return 0.0001
	}
	if fpr > 1.0 {
		return 1.0
	}

	return fpr
}
