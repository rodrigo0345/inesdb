package lsm

type iterHeap struct {
	iters   []*sstableIter
	reverse bool // Controls heap direction
}

func (h *iterHeap) Len() int      { return len(h.iters) }
func (h *iterHeap) Swap(i, j int) { h.iters[i], h.iters[j] = h.iters[j], h.iters[i] }

func (h *iterHeap) Less(i, j int) bool {
	if h.iters[i].priority != h.iters[j].priority {
		return h.iters[i].priority > h.iters[j].priority
	}
	// If priorities are equal, compare keys to maintain sort order
	if h.reverse {
		return h.iters[i].key() > h.iters[j].key()
	}
	return h.iters[i].key() < h.iters[j].key()
}

func (h *iterHeap) Push(x any) {
	h.iters = append(h.iters, x.(*sstableIter))
}

func (h *iterHeap) Pop() any {
	old := h.iters
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	h.iters = old[:n-1]
	return it
}
