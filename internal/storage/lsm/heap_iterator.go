package lsm

type iterHeap []*sstableIter

func (h iterHeap) Len() int      { return len(h) }
func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h iterHeap) Less(i, j int) bool {
	ki, kj := h[i].key(), h[j].key()
	if ki != kj {
		return ki < kj
	}
	return h[i].priority > h[j].priority
}

func (h *iterHeap) Push(x any) { *h = append(*h, x.(*sstableIter)) }

func (h *iterHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}
