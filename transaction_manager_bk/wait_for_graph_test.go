package transaction_manager

import (
	"testing"
)

// TestWaitForGraphAddWait tests adding a basic wait edge
func TestWaitForGraphAddWait(t *testing.T) {
	wfg := NewWaitForGraph()

	// Add a wait edge
	wfg.AddWait(1, 2) // Txn1 waits for Txn2

	if len(wfg.waitEdges[1]) != 1 || wfg.waitEdges[1][0] != 2 {
		t.Errorf("Expected wait edge 1->2, got %v", wfg.waitEdges[1])
	}
}

// TestWaitForGraphAddWaitSelfWait tests that self-waits are rejected
func TestWaitForGraphAddWaitSelfWait(t *testing.T) {
	wfg := NewWaitForGraph()

	// Try to add a self-wait (should be ignored)
	wfg.AddWait(1, 1)

	if len(wfg.waitEdges[1]) != 0 {
		t.Errorf("Expected no self-wait edge, but got one: %v", wfg.waitEdges[1])
	}
}

// TestWaitForGraphRemoveWait tests removing wait edges
func TestWaitForGraphRemoveWait(t *testing.T) {
	wfg := NewWaitForGraph()

	wfg.AddWait(1, 2)
	wfg.AddWait(1, 3)

	// Remove one edge
	wfg.RemoveWait(1, 2)

	if len(wfg.waitEdges[1]) != 1 || wfg.waitEdges[1][0] != 3 {
		t.Errorf("Expected wait edge 1->3, got %v", wfg.waitEdges[1])
	}

	// Remove last edge
	wfg.RemoveWait(1, 3)

	if _, exists := wfg.waitEdges[1]; exists {
		t.Errorf("Expected Txn1 to be completely removed from wait edges")
	}
}

func TestWaitForGraphRemoveWaitNonexistent(t *testing.T) {
	wfg := NewWaitForGraph()

	wfg.AddWait(1, 2)

	// Try to remove non-existent edge (should be safe)
	wfg.RemoveWait(1, 3)

	// Should still have the 1->2 edge
	if len(wfg.waitEdges[1]) != 1 || wfg.waitEdges[1][0] != 2 {
		t.Errorf("Expected wait edge 1->2 to remain, got %v", wfg.waitEdges[1])
	}
}

// TestWaitForGraphHasCycleNoCycle tests that linear chains are not detected as cycles
func TestWaitForGraphHasCycleNoCycle(t *testing.T) {
	wfg := NewWaitForGraph()

	wfg.AddWait(1, 2)
	wfg.AddWait(2, 3)

	// No cycle: 1->2->3
	if wfg.HasCycle(1) {
		t.Errorf("Expected no cycle for linear wait chain 1->2->3")
	}
	if wfg.HasCycle(2) {
		t.Errorf("Expected no cycle for linear wait chain starting at 2")
	}
}

// TestWaitForGraphHasCycleSimple tests detection of a simple 2-transaction cycle
func TestWaitForGraphHasCycleSimple(t *testing.T) {
	wfg := NewWaitForGraph()

	wfg.AddWait(1, 2)
	wfg.AddWait(2, 1) // Creates cycle: 1->2->1

	if !wfg.HasCycle(1) {
		t.Errorf("Expected cycle detection for 1->2->1")
	}
	if !wfg.HasCycle(2) {
		t.Errorf("Expected cycle detection starting from 2")
	}
}

// TestWaitForGraphHasCycleComplex tests detection of a complex multi-transaction cycle
func TestWaitForGraphHasCycleComplex(t *testing.T) {
	wfg := NewWaitForGraph()

	// Create: 1->2->3->4, then 4->2 creates cycle 2->3->4->2
	wfg.AddWait(1, 2)
	wfg.AddWait(2, 3)
	wfg.AddWait(3, 4)
	wfg.AddWait(4, 2) // Cycle: 2->3->4->2

	if !wfg.HasCycle(2) {
		t.Errorf("Expected cycle detection for 2->3->4->2")
	}
	if !wfg.HasCycle(3) {
		t.Errorf("Expected cycle detection starting from 3")
	}
	if !wfg.HasCycle(4) {
		t.Errorf("Expected cycle detection from Txn4 perspective")
	}
	// Txn1 is not part of the cycle
	if wfg.HasCycle(1) {
		t.Errorf("Expected no cycle for Txn1 which is not in the cycle")
	}
}

// TestWaitForGraphHasCycle3Way tests a 3-way circular dependency
func TestWaitForGraphHasCycle3Way(t *testing.T) {
	wfg := NewWaitForGraph()

	wfg.AddWait(1, 2)
	wfg.AddWait(2, 3)
	wfg.AddWait(3, 1) // Cycle: 1->2->3->1

	if !wfg.HasCycle(1) {
		t.Errorf("Expected cycle detection for 1->2->3->1")
	}
	if !wfg.HasCycle(2) {
		t.Errorf("Expected cycle detection starting from 2")
	}
	if !wfg.HasCycle(3) {
		t.Errorf("Expected cycle detection starting from 3")
	}
}

// TestWaitForGraphClearTxn tests complete cleanup of a transaction
func TestWaitForGraphClearTxn(t *testing.T) {
	wfg := NewWaitForGraph()

	wfg.AddWait(1, 2)
	wfg.AddWait(2, 3)
	wfg.AddWait(3, 1)

	// Clear Txn2
	wfg.ClearTxn(2)

	if _, exists := wfg.waitEdges[2]; exists {
		t.Errorf("Expected Txn2 edges to be cleared")
	}

	// Verify Txn2 removed as target from Txn1
	if len(wfg.waitEdges[1]) > 0 && wfg.waitEdges[1][0] == 2 {
		t.Errorf("Expected Txn2 to be removed from all wait edges")
	}

	// Verify the cycle is broken
	if wfg.HasCycle(3) {
		t.Errorf("Expected no cycle after clearing Txn2 from 3->1->2 chain")
	}
}

// TestWaitForGraphClearTxnMultipleEdges tests clearing a transaction with multiple wait edges
func TestWaitForGraphClearTxnMultipleEdges(t *testing.T) {
	wfg := NewWaitForGraph()

	// Txn1 waits for multiple transactions
	wfg.AddWait(1, 2)
	wfg.AddWait(1, 3)
	wfg.AddWait(1, 4)

	// Clear Txn1
	wfg.ClearTxn(1)

	if _, exists := wfg.waitEdges[1]; exists {
		t.Errorf("Expected all Txn1 wait edges to be cleared")
	}
}

// TestWaitForGraphClearTxnAsTarget tests clearing a transaction that is waited upon
func TestWaitForGraphClearTxnAsTarget(t *testing.T) {
	wfg := NewWaitForGraph()

	wfg.AddWait(1, 5)
	wfg.AddWait(2, 5)
	wfg.AddWait(3, 5)

	// Clear Txn5 (target of waits)
	wfg.ClearTxn(5)

	// All references to Txn5 should be removed
	for txn := 1; txn <= 3; txn++ {
		if edges, exists := wfg.waitEdges[uint64(txn)]; exists && len(edges) > 0 {
			for _, edge := range edges {
				if edge == 5 {
					t.Errorf("Expected Txn5 to be removed from Txn%d's wait edges", txn)
				}
			}
		}
	}
}

// TestWaitForGraphMultipleWaitsPerTransaction tests one txn waiting for multiple others
func TestWaitForGraphMultipleWaitsPerTransaction(t *testing.T) {
	wfg := NewWaitForGraph()

	// Txn1 waits for Txn2 and Txn3
	wfg.AddWait(1, 2)
	wfg.AddWait(1, 3)

	if len(wfg.waitEdges[1]) != 2 {
		t.Errorf("Expected Txn1 to have 2 wait edges, got %d", len(wfg.waitEdges[1]))
	}
}

// TestWaitForGraphEmptyGraph tests operations on empty graph
func TestWaitForGraphEmptyGraph(t *testing.T) {
	wfg := NewWaitForGraph()

	// Should not panic and should return false
	if wfg.HasCycle(1) {
		t.Errorf("Expected no cycle in empty graph")
	}

	// Should not panic
	wfg.RemoveWait(1, 2)

	// Should not panic
	wfg.ClearTxn(1)
}

// TestWaitForGraphLargeGraph tests with many transactions
func TestWaitForGraphLargeGraph(t *testing.T) {
	wfg := NewWaitForGraph()

	// Create chain: 1->2->3->...->100
	for i := 1; i < 100; i++ {
		wfg.AddWait(uint64(i), uint64(i+1))
	}

	// No cycle should be detected
	for i := 1; i <= 100; i++ {
		if wfg.HasCycle(uint64(i)) {
			t.Errorf("Expected no cycle in linear chain for Txn%d", i)
		}
	}

	// Create cycle at the end
	wfg.AddWait(100, 50) // 100 -> 50, creates cycle 50->...->100->50

	// Now cycle should be detected for affected transactions
	if !wfg.HasCycle(50) {
		t.Errorf("Expected cycle detection after creating 100->50 edge")
	}
}

// TestWaitForGraphConcurrentAccess tests thread safety (basic version)
func TestWaitForGraphConcurrentAccess(t *testing.T) {
	wfg := NewWaitForGraph()

	// Add and check concurrently
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 100; i++ {
			wfg.AddWait(uint64(i), uint64(i+1))
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			wfg.HasCycle(uint64(i))
		}
		done <- true
	}()

	<-done
	<-done
}

// TestWaitForGraphDFSFromUnconnectedNode tests DFS on disconnected nodes
func TestWaitForGraphDFSFromUnconnectedNode(t *testing.T) {
	wfg := NewWaitForGraph()

	// Create: 1->2, and separate 5->6
	wfg.AddWait(1, 2)
	wfg.AddWait(5, 6)

	// Txn3 has no connections
	if wfg.HasCycle(3) {
		t.Errorf("Expected no cycle for disconnected Txn3")
	}

	// Txn1 and Txn5 have no connection between them
	if wfg.HasCycle(1) {
		t.Errorf("Expected no cycle in disconnected component 1->2")
	}
	if wfg.HasCycle(5) {
		t.Errorf("Expected no cycle in disconnected component 5->6")
	}
}
