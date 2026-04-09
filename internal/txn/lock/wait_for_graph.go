package lock

import "sync"

// WaitForGraph tracks transaction dependencies for deadlock detection
// Edge: waiterTxn -> holderTxn means waiterTxn waits for a lock held by holderTxn
type WaitForGraph struct {
	mu        sync.Mutex
	waitEdges map[uint64][]uint64 // txnID -> list of txnIDs it waits for
}

func NewWaitForGraph() *WaitForGraph {
	return &WaitForGraph{
		waitEdges: make(map[uint64][]uint64),
	}
}

// AddWait registers that waiter transaction is waiting for holder transaction
func (wfg *WaitForGraph) AddWait(waiter, holder uint64) {
	if waiter == holder {
		return // transaction cannot wait for itself
	}
	wfg.mu.Lock()
	defer wfg.mu.Unlock()
	wfg.waitEdges[waiter] = append(wfg.waitEdges[waiter], holder)
}

// RemoveWait removes a wait edge between waiter and holder
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

// HasCycle checks if there's a cycle that includes txnID (deadlock detection)
func (wfg *WaitForGraph) HasCycle(txnID uint64) bool {
	wfg.mu.Lock()
	defer wfg.mu.Unlock()

	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)

	// Check if txnID is part of any cycle
	// We need to check: does txnID have outgoing edges that eventually lead back to txnID?
	return wfg.dfs(txnID, txnID, visited, recStack)
}

// dfs checks if startNode is part of a cycle
// We look for: startNode -> ... -> startNode (a back edge to the starting node)
func (wfg *WaitForGraph) dfs(node, startNode uint64, visited, recStack map[uint64]bool) bool {
	visited[node] = true
	recStack[node] = true

	for _, neighbor := range wfg.waitEdges[node] {
		// If we find an edge back to the starting node, we have a cycle
		if neighbor == startNode {
			return true
		}

		// If we haven't visited this neighbor, continue DFS
		if !visited[neighbor] {
			if wfg.dfs(neighbor, startNode, visited, recStack) {
				return true
			}
		} else if recStack[neighbor] {
			// Found a cycle, but not necessarily involving startNode
			// Continue checking other paths from startNode
		}
	}

	recStack[node] = false
	return false
}

// ClearTxn removes all wait edges for a transaction (call on commit/abort)
func (wfg *WaitForGraph) ClearTxn(txnID uint64) {
	wfg.mu.Lock()
	defer wfg.mu.Unlock()
	delete(wfg.waitEdges, txnID)

	for waiter := range wfg.waitEdges {
		edges := wfg.waitEdges[waiter]
		for i := len(edges) - 1; i >= 0; i-- {
			if edges[i] == txnID {
				wfg.waitEdges[waiter] = append(edges[:i], edges[i+1:]...) // remove this transaction from wait edges
			}
		}
		if len(wfg.waitEdges[waiter]) == 0 {
			delete(wfg.waitEdges, waiter)
		}
	}
}
