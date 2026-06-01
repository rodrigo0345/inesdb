package vector

import (
	"container/heap"
	"sync"
)

// HNSWConfig holds the tuning parameters for an HNSW index.
// These correspond to the original paper's notation (Malkov & Yashunin, 2018).
type HNSWConfig struct {
	M              int // max bidirectional links per node at layers > 0 (default 16)
	M0             int // max links at layer 0; typically 2*M (default 32)
	EfConstruction int // beam width during graph construction (default 200)
	EfSearch       int // beam width during similarity search (default 50)
}

// DefaultHNSWConfig returns production-quality defaults matching Qdrant's baseline.
func DefaultHNSWConfig() HNSWConfig {
	return HNSWConfig{M: 16, M0: 32, EfConstruction: 200, EfSearch: 50}
}

// HNSWNode is one vertex in the HNSW graph.
type HNSWNode struct {
	id         int32
	vector     []float32
	storageKey []byte
	maxLevel   int
	deleted    bool
	neighbours [][]int32 // neighbours[layer] → sorted neighbour IDs
	mu         sync.RWMutex
}

func newHNSWNode(id int32, vec []float32, key []byte, maxLevel int) *HNSWNode {
	return &HNSWNode{
		id:         id,
		vector:     vec,
		storageKey: key,
		maxLevel:   maxLevel,
		neighbours: make([][]int32, maxLevel+1),
	}
}

func (n *HNSWNode) getNeighbours(layer int) []int32 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if layer >= len(n.neighbours) {
		return nil
	}
	nb := n.neighbours[layer]
	result := make([]int32, len(nb))
	copy(result, nb)
	return result
}

func (n *HNSWNode) setNeighbours(layer int, nb []int32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for len(n.neighbours) <= layer {
		n.neighbours = append(n.neighbours, nil)
	}
	n.neighbours[layer] = nb
}

// --- Heap types for beam search ---

// candidateItem is a (node-id, distance) pair used in the beam-search heaps.
type candidateItem struct {
	id   int32
	dist float32
}

// minHeap keeps the smallest distance at the top — used to process nearest
// candidates first during beam search.
type minHeap []candidateItem

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(candidateItem)) }
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// maxHeap keeps the largest distance at the top — used to evict the worst
// result when the result set W exceeds ef.
type maxHeap []candidateItem

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return h[i].dist > h[j].dist }
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)        { *h = append(*h, x.(candidateItem)) }
func (h *maxHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

var _ heap.Interface = (*minHeap)(nil)
var _ heap.Interface = (*maxHeap)(nil)
