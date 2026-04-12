package lock

import "sync"

type WaitForGraph struct {
	mu        sync.Mutex
	waitEdges map[uint64][]uint64
}

func NewWaitForGraph() *WaitForGraph {
	return &WaitForGraph{
		waitEdges: make(map[uint64][]uint64),
	}
}

func (wfg *WaitForGraph) AddWait(waiter, holder uint64) {
	if waiter == holder {
		return
	}
	wfg.mu.Lock()
	defer wfg.mu.Unlock()
	wfg.waitEdges[waiter] = append(wfg.waitEdges[waiter], holder)
}

func (wfg *WaitForGraph) RemoveWait(waiter, holder uint64) {
	wfg.mu.Lock()
	defer wfg.mu.Unlock()
	edges, exists := wfg.waitEdges[waiter]
	if !exists {
		return
	}
	for i, id := range edges {
		if id == holder {
			wfg.waitEdges[waiter] = append(edges[:i], edges[i+1:]...)
			if len(wfg.waitEdges[waiter]) == 0 {
				delete(wfg.waitEdges, waiter)
			}
			return
		}
	}
}

func (wfg *WaitForGraph) HasCycle(txnID uint64) bool {
	wfg.mu.Lock()
	defer wfg.mu.Unlock()

	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)

	return wfg.dfs(txnID, txnID, visited, recStack)
}

func (wfg *WaitForGraph) dfs(node, startNode uint64, visited, recStack map[uint64]bool) bool {
	visited[node] = true
	recStack[node] = true

	for _, neighbor := range wfg.waitEdges[node] {
		if neighbor == startNode {
			return true
		}

		if !visited[neighbor] {
			if wfg.dfs(neighbor, startNode, visited, recStack) {
				return true
			}
		} else if recStack[neighbor] {
		}
	}

	recStack[node] = false
	return false
}

func (wfg *WaitForGraph) ClearTxn(txnID uint64) {
	wfg.mu.Lock()
	defer wfg.mu.Unlock()
	delete(wfg.waitEdges, txnID)

	for waiter := range wfg.waitEdges {
		edges := wfg.waitEdges[waiter]
		for i := len(edges) - 1; i >= 0; i-- {
			if edges[i] == txnID {
				wfg.waitEdges[waiter] = append(edges[:i], edges[i+1:]...)
			}
		}
		if len(wfg.waitEdges[waiter]) == 0 {
			delete(wfg.waitEdges, waiter)
		}
	}
}
