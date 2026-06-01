package vector

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// SearchResult is one approximate nearest-neighbor match returned by SearchKNN.
type SearchResult struct {
	StorageKey []byte
	Distance   float32
}

// HNSW is a Hierarchical Navigable Small World graph index providing
// O(log n) approximate nearest-neighbor search. It is safe for concurrent
// reads; writes are serialised via an internal lock.
type HNSW struct {
	mu         sync.RWMutex
	nodes      map[int32]*HNSWNode
	keyToID    map[string]int32 // string(storageKey) → internal node id
	nextID     int32
	entryPoint int32 // -1 = empty graph
	maxLevel   int
	dim        int // vector dimension; -1 until first Insert

	cfg    HNSWConfig
	metric Metric
	distFn DistanceFunc
	mL     float64    // level-generation normaliser = 1/ln(M)
	rng    *rand.Rand // private RNG; access only under write lock

	deletedCount int32      // atomic counter of soft-deleted nodes
	totalCount   int32      // atomic counter of all nodes (live + deleted)
	rebuildMu    sync.Mutex // serialises concurrent rebuild attempts
}

// NewHNSW creates an empty HNSW index with the given configuration and metric.
func NewHNSW(cfg HNSWConfig, metric Metric) *HNSW {
	if cfg.M <= 0 {
		cfg.M = 16
	}
	if cfg.M0 <= 0 {
		cfg.M0 = 2 * cfg.M
	}
	if cfg.EfConstruction <= 0 {
		cfg.EfConstruction = 200
	}
	if cfg.EfSearch <= 0 {
		cfg.EfSearch = 50
	}
	return &HNSW{
		nodes:      make(map[int32]*HNSWNode),
		keyToID:    make(map[string]int32),
		nextID:     0,
		entryPoint: -1,
		maxLevel:   -1,
		dim:        -1,
		cfg:        cfg,
		metric:     metric,
		distFn:     MetricToFunc(metric),
		mL:         1.0 / math.Log(float64(max32(cfg.M, 2))),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Insert adds storageKey→vec to the graph. If the key already exists its
// vector is updated in-place without structural changes to the graph.
func (h *HNSW) Insert(storageKey []byte, vec []float32) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.dim == -1 {
		h.dim = len(vec)
	} else if len(vec) != h.dim {
		return fmt.Errorf("dimension mismatch: index has %d, got %d", h.dim, len(vec))
	}

	sk := string(storageKey)
	if existingID, ok := h.keyToID[sk]; ok {
		node := h.nodes[existingID]
		node.mu.Lock()
		node.vector = vec
		node.deleted = false
		node.mu.Unlock()
		return nil
	}

	h.insertLocked(storageKey, vec)
	return nil
}

// Delete soft-deletes the node with the given storage key. The node remains
// in the graph so its neighbours stay connected; it is filtered from search
// results. A background rebuild is triggered when the tombstone ratio exceeds 20%.
func (h *HNSW) Delete(storageKey []byte) {
	h.mu.Lock()
	id, ok := h.keyToID[string(storageKey)]
	if ok {
		node := h.nodes[id]
		if !node.deleted {
			node.deleted = true
			atomic.AddInt32(&h.deletedCount, 1)
		}
	}
	total := atomic.LoadInt32(&h.totalCount)
	deleted := atomic.LoadInt32(&h.deletedCount)
	h.mu.Unlock()

	if total > 0 && deleted*5 > total {
		go h.maybeRebuild()
	}
}

// SearchKNN returns at most k approximate nearest neighbours of query,
// sorted by ascending distance.
func (h *HNSW) SearchKNN(query []float32, k int) ([]SearchResult, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.entryPoint == -1 {
		return []SearchResult{}, nil
	}
	if h.dim != -1 && len(query) != h.dim {
		return nil, fmt.Errorf("dimension mismatch: index has %d, query has %d", h.dim, len(query))
	}

	ep := h.entryPoint

	// Greedy single-hop descent from the top layer down to layer 1.
	for lc := h.maxLevel; lc >= 1; lc-- {
		ep = h.greedySearch1(query, ep, lc)
	}

	// Full beam search at layer 0.
	ef := h.cfg.EfSearch
	if ef < k {
		ef = k
	}
	W := h.searchLayer(query, ep, ef, 0)

	results := make([]SearchResult, 0, k)
	for _, item := range W {
		if len(results) >= k {
			break
		}
		node, ok := h.nodes[item.id]
		if !ok || node.deleted {
			continue
		}
		dist := item.dist
		if h.metric == MetricL2 {
			dist = float32(math.Sqrt(float64(dist)))
		}
		keyCopy := make([]byte, len(node.storageKey))
		copy(keyCopy, node.storageKey)
		results = append(results, SearchResult{StorageKey: keyCopy, Distance: dist})
	}
	return results, nil
}

// insertLocked performs an insert without acquiring h.mu — the caller must hold it.
func (h *HNSW) insertLocked(storageKey []byte, vec []float32) {
	level := h.generateLevel()
	id := h.nextID
	h.nextID++

	node := newHNSWNode(id, vec, storageKey, level)
	h.nodes[id] = node
	h.keyToID[string(storageKey)] = id
	atomic.AddInt32(&h.totalCount, 1)

	if h.entryPoint == -1 {
		h.entryPoint = id
		h.maxLevel = level
		return
	}

	ep := h.entryPoint
	curMaxLevel := h.maxLevel

	// Phase 1: greedy single-hop descent from curMaxLevel to level+1.
	for lc := curMaxLevel; lc > level; lc-- {
		ep = h.greedySearch1(vec, ep, lc)
	}

	// Phase 2: beam search + bidirectional connect from min(level,curMaxLevel) to 0.
	for lc := min32(level, curMaxLevel); lc >= 0; lc-- {
		W := h.searchLayer(vec, ep, h.cfg.EfConstruction, lc)
		maxM := h.mForLayer(lc)
		neighbours := h.selectNeighbours(W, maxM)
		node.setNeighbours(lc, neighbours)

		for _, nbID := range neighbours {
			nb, ok := h.nodes[nbID]
			if !ok {
				continue
			}
			cur := nb.getNeighbours(lc)
			updated := append(cur, id)
			if len(updated) > maxM {
				cands := make([]candidateItem, len(updated))
				for i, nid := range updated {
					nn, ok := h.nodes[nid]
					if !ok {
						cands[i] = candidateItem{id: nid, dist: math.MaxFloat32}
						continue
					}
					cands[i] = candidateItem{id: nid, dist: h.distFn(nb.vector, nn.vector)}
				}
				nb.setNeighbours(lc, h.selectNeighbours(cands, maxM))
			} else {
				nb.setNeighbours(lc, updated)
			}
		}

		if len(W) > 0 {
			ep = W[0].id
		}
	}

	if level > curMaxLevel {
		h.entryPoint = id
		h.maxLevel = level
	}
}

// greedySearch1 descends greedily within a single layer, returning the id of
// the node closest to query at that layer.
func (h *HNSW) greedySearch1(query []float32, ep int32, layer int) int32 {
	best := ep
	bestDist := h.distToNode(query, ep)

	for {
		improved := false
		node, ok := h.nodes[best]
		if !ok {
			break
		}
		for _, nbID := range node.getNeighbours(layer) {
			d := h.distToNode(query, nbID)
			if d < bestDist {
				bestDist = d
				best = nbID
				improved = true
			}
		}
		if !improved {
			break
		}
	}
	return best
}

// searchLayer runs a beam search at the given layer and returns candidates
// sorted ascending by distance (closest first). Caller must hold at least
// h.mu.RLock.
func (h *HNSW) searchLayer(query []float32, ep int32, ef, layer int) []candidateItem {
	visited := make(map[int32]struct{}, ef*2)
	visited[ep] = struct{}{}

	epDist := h.distToNode(query, ep)

	cands := &minHeap{{id: ep, dist: epDist}}
	heap.Init(cands)

	W := &maxHeap{{id: ep, dist: epDist}}
	heap.Init(W)

	for cands.Len() > 0 {
		c := heap.Pop(cands).(candidateItem)
		if W.Len() >= ef && c.dist > (*W)[0].dist {
			break
		}

		node, ok := h.nodes[c.id]
		if !ok {
			continue
		}
		for _, nbID := range node.getNeighbours(layer) {
			if _, seen := visited[nbID]; seen {
				continue
			}
			visited[nbID] = struct{}{}

			d := h.distToNode(query, nbID)
			if W.Len() < ef || d < (*W)[0].dist {
				heap.Push(cands, candidateItem{id: nbID, dist: d})
				heap.Push(W, candidateItem{id: nbID, dist: d})
				if W.Len() > ef {
					heap.Pop(W)
				}
			}
		}
	}

	// Drain max-heap into ascending-distance slice.
	result := make([]candidateItem, W.Len())
	for i := len(result) - 1; i >= 0; i-- {
		result[i] = heap.Pop(W).(candidateItem)
	}
	return result
}

// selectNeighbours returns the IDs of the maxM closest candidates.
func (h *HNSW) selectNeighbours(candidates []candidateItem, maxM int) []int32 {
	n := len(candidates)
	if n > maxM {
		n = maxM
	}
	result := make([]int32, n)
	for i := range result {
		result[i] = candidates[i].id
	}
	return result
}

func (h *HNSW) distToNode(query []float32, id int32) float32 {
	node, ok := h.nodes[id]
	if !ok || len(query) == 0 {
		return math.MaxFloat32
	}
	return h.distFn(query, node.vector)
}

func (h *HNSW) mForLayer(layer int) int {
	if layer == 0 {
		return h.cfg.M0
	}
	return h.cfg.M
}

func (h *HNSW) generateLevel() int {
	l := int(math.Floor(-math.Log(h.rng.Float64()) * h.mL))
	if l < 0 {
		l = 0
	}
	return l
}

func (h *HNSW) maybeRebuild() {
	h.rebuildMu.Lock()
	defer h.rebuildMu.Unlock()

	total := atomic.LoadInt32(&h.totalCount)
	deleted := atomic.LoadInt32(&h.deletedCount)
	if total == 0 || deleted*5 <= total {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rebuildUnderLock()
}

// rebuildUnderLock rebuilds the graph from live nodes. Caller must hold h.mu.Lock.
func (h *HNSW) rebuildUnderLock() {
	type entry struct {
		key []byte
		vec []float32
	}
	live := make([]entry, 0, int(atomic.LoadInt32(&h.totalCount)))
	for _, node := range h.nodes {
		if !node.deleted {
			k := make([]byte, len(node.storageKey))
			copy(k, node.storageKey)
			v := make([]float32, len(node.vector))
			copy(v, node.vector)
			live = append(live, entry{key: k, vec: v})
		}
	}

	savedDim := h.dim
	h.nodes = make(map[int32]*HNSWNode)
	h.keyToID = make(map[string]int32)
	h.nextID = 0
	h.entryPoint = -1
	h.maxLevel = -1
	h.dim = savedDim
	atomic.StoreInt32(&h.deletedCount, 0)
	atomic.StoreInt32(&h.totalCount, 0)

	for _, e := range live {
		h.insertLocked(e.key, e.vec)
	}
}

func min32(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max32(a, b int) int {
	if a > b {
		return a
	}
	return b
}
