package lock

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrDeadlock     = errors.New("deadlock detected")
	ErrTxnAborted   = errors.New("transaction aborted")
	ErrLockConflict = errors.New("lock conflict")
)

// ITransaction is the interface that all transaction types must implement for lock management
type ITransaction interface {
	GetID() uint64
	Abort()
	AddSharedLock(key []byte)
	AddExclusiveLock(key []byte)
	RemoveSharedLock(key []byte)
	RemoveExclusiveLock(key []byte)
}

type LockMode int

const (
	SHARED LockMode = iota
	EXCLUSIVE
)

type LockState int

const (
	GRANTED LockState = iota
	WAITING
)

type LockRequest struct {
	txnID uint64
	mode  LockMode
	state LockState
	ch    chan struct{} // channel to block until lock is granted
}

type LockQueue struct {
	requests []*LockRequest
	// condition variable could also be used, but keeping it simple using channels
	// mu is not needed if LockManager has a global lock, but fine-grained locking is better
	mu sync.Mutex
}

type LockManager struct {
	lockTable    map[string]*LockQueue
	mu           sync.Mutex
	waitForGraph *WaitForGraph
	deadlockWait time.Duration // configurable timeout for deadlock detection
}

func NewLockManager() *LockManager {
	return NewLockManagerWithTimeout(5 * time.Second)
}

// NewLockManagerWithTimeout creates a LockManager with a custom deadlock detection timeout
func NewLockManagerWithTimeout(timeout time.Duration) *LockManager {
	return &LockManager{
		lockTable:    make(map[string]*LockQueue),
		waitForGraph: NewWaitForGraph(),
		deadlockWait: timeout,
	}
}

// SetDeadlockTimeout updates the deadlock detection timeout
func (lm *LockManager) SetDeadlockTimeout(timeout time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.deadlockWait = timeout
}

// LockShared acquires a shared lock on the key.
func (lm *LockManager) LockShared(txn ITransaction, key []byte) error {
	if txn == nil {
		return ErrTxnAborted
	}

	keyStr := string(key)

	lm.mu.Lock()

	// each key can have multiple pending requests, that is why we need a queue. The queue also tracks the order of requests for fairness and deadlock detection
	queue, exists := lm.lockTable[keyStr]
	if !exists {
		queue = &LockQueue{requests: make([]*LockRequest, 0)}
		lm.lockTable[keyStr] = queue
	}
	lm.mu.Unlock()

	req := &LockRequest{
		txnID: txn.GetID(),
		mode:  SHARED,
		state: WAITING,
		ch:    make(chan struct{}),
	}

	queue.mu.Lock()

	// Check if already holding the lock if so upgrade
	for _, r := range queue.requests {
		if r.txnID == txn.GetID() && r.state == GRANTED {
			if r.mode == EXCLUSIVE {
				queue.mu.Unlock()
				return nil // this txn is already exclusive, shared is implicitly granted
			}
			if r.mode == SHARED {
				queue.mu.Unlock()
				return nil // this transaction already holds a shared lock
			}
		}
	}

	canGrant := true
	var holderTxnID uint64
	for _, expected := range queue.requests {
		if expected.mode == EXCLUSIVE && expected.state == GRANTED {
			canGrant = false
			holderTxnID = expected.txnID // Track who holds the exclusive lock
			break
		}
	}

	if canGrant {
		req.state = GRANTED
		txn.AddSharedLock(key) // this is tracked to make unlocking easier
	}

	queue.requests = append(queue.requests, req)
	queue.mu.Unlock()

	if canGrant {
		return nil
	}

	// Register the wait dependency in the wait-for graph
	lm.waitForGraph.AddWait(txn.GetID(), holderTxnID)
	defer lm.waitForGraph.RemoveWait(txn.GetID(), holderTxnID)

	// Check for deadlock cycles in the wait-for graph
	if lm.waitForGraph.HasCycle(txn.GetID()) {
		txn.Abort()
		return ErrDeadlock
	}

	// Wait logic with timeout and periodic deadlock detection
	for {
		select {
		case <-req.ch: // notifyNext() is responsible for calling this branch
			// Granted, add to txn
			txn.AddSharedLock(key)
		case <-time.After(lm.deadlockWait):
			// Timeout triggered - check if there's a real cycle
			if lm.waitForGraph.HasCycle(txn.GetID()) {
				// Deadlock detected - remove from queue and abort
				queue.mu.Lock()
				for i, r := range queue.requests {
					if r.txnID == txn.GetID() && r == req {
						queue.requests = append(queue.requests[:i], queue.requests[i+1:]...)
						lm.notifyNext(queue)
						break
					}
				}
				queue.mu.Unlock()
				txn.Abort()
				return ErrDeadlock
			}
			// No deadlock detected, continue waiting for the lock
			// Loop again and wait for the next timeout period
		}
	}
}

// LockExclusive acquires an exclusive lock on the key.
func (lm *LockManager) LockExclusive(txn ITransaction, key []byte) error {
	keyStr := string(key)

	lm.mu.Lock()
	queue, exists := lm.lockTable[keyStr]
	if !exists {
		queue = &LockQueue{requests: make([]*LockRequest, 0)}
		lm.lockTable[keyStr] = queue
	}
	lm.mu.Unlock()

	req := &LockRequest{
		txnID: txn.GetID(),
		mode:  EXCLUSIVE,
		state: WAITING,
		ch:    make(chan struct{}),
	}

	queue.mu.Lock()

	// Check if already holding the lock
	isUpgrade := false
	var holderTxnID uint64
	for _, r := range queue.requests {
		if r.txnID == txn.GetID() && r.state == GRANTED {
			if r.mode == EXCLUSIVE {
				queue.mu.Unlock()
				return nil // Already hold exclusive
			}
			if r.mode == SHARED { // upgrade the lock access
				isUpgrade = true
				req.state = WAITING
				break
			}
		}
	}

	// Find who is currently holding the lock
	for _, r := range queue.requests {
		if r.state == GRANTED && r.txnID != txn.GetID() {
			holderTxnID = r.txnID
			break
		}
	}

	canGrant := len(queue.requests) == 0 || (isUpgrade && len(queue.requests) == 1 && queue.requests[0].txnID == txn.GetID())

	if canGrant {
		req.state = GRANTED
		txn.AddExclusiveLock(key)
		// if upgrade, replace the shared request
		if isUpgrade {
			queue.requests[0] = req
		} else {
			queue.requests = append(queue.requests, req)
		}
	} else {
		queue.requests = append(queue.requests, req)
	}
	queue.mu.Unlock()

	if canGrant {
		return nil
	}

	// Register the wait dependency in the wait-for graph
	if holderTxnID > 0 {
		lm.waitForGraph.AddWait(txn.GetID(), holderTxnID)
		defer lm.waitForGraph.RemoveWait(txn.GetID(), holderTxnID)
	}

	// Check for deadlock cycles in the wait-for graph
	if holderTxnID > 0 && lm.waitForGraph.HasCycle(txn.GetID()) {
		txn.Abort()
		return ErrDeadlock
	}

	// Wait logic with timeout and periodic deadlock detection
	for {
		select {
		case <-req.ch: // notifyNext() is responsible for calling this branch
			txn.AddExclusiveLock(key)

		case <-time.After(lm.deadlockWait):
			// Timeout triggered - check if there's a real cycle
			if lm.waitForGraph.HasCycle(txn.GetID()) {
				// Deadlock detected - remove from queue and abort
				queue.mu.Lock()
				for i, r := range queue.requests {
					if r.txnID == txn.GetID() && r == req {
						queue.requests = append(queue.requests[:i], queue.requests[i+1:]...)
						lm.notifyNext(queue)
						break
					}
				}
				queue.mu.Unlock()
				txn.Abort()
				return ErrDeadlock
			}
			// No deadlock detected, continue waiting for the lock
			// Loop again and wait for the next timeout period
		}
	}
}

// Unlock releases the lock held by the transaction on the key.
func (lm *LockManager) Unlock(txn ITransaction, key []byte) error {
	keyStr := string(key)

	lm.mu.Lock()
	queue, exists := lm.lockTable[keyStr]
	lm.mu.Unlock()

	if !exists {
		return nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	txn.RemoveSharedLock(key)
	txn.RemoveExclusiveLock(key)

	foundIndex := -1
	for i, r := range queue.requests {
		if r.txnID == txn.GetID() && r.state == GRANTED {
			foundIndex = i
			break
		}
	}

	if foundIndex != -1 {
		queue.requests = append(queue.requests[:foundIndex], queue.requests[foundIndex+1:]...)
	}

	// Calls NotifyNext BEFORE cleaning up graph edges so waiting txns get notified
	lm.notifyNext(queue)

	lm.waitForGraph.mu.Lock()
	for waiter := range lm.waitForGraph.waitEdges {
		if edges, exists := lm.waitForGraph.waitEdges[waiter]; exists {
			newEdges := make([]uint64, 0)
			for _, holder := range edges {
				if holder != txn.GetID() {
					newEdges = append(newEdges, holder)
				}
			}
			if len(newEdges) == 0 {
				delete(lm.waitForGraph.waitEdges, waiter)
			} else {
				lm.waitForGraph.waitEdges[waiter] = newEdges
			}
		}
	}
	lm.waitForGraph.mu.Unlock()

	return nil
}

func (lm *LockManager) notifyNext(queue *LockQueue) {
	if len(queue.requests) == 0 {
		return
	}

	firstWaiting := -1
	for i, r := range queue.requests {
		if !(r.state == GRANTED) {
			firstWaiting = i
			break
		}
	}

	if firstWaiting == -1 {
		return // All are potentially granted (e.g. multiple shared)
	}

	if queue.requests[firstWaiting].mode == EXCLUSIVE {
		// Only grant if no one else (different transaction) is currently holding it
		anyoneElseHolding := false
		txnIDWaiting := queue.requests[firstWaiting].txnID
		for i := 0; i < firstWaiting; i++ {
			if queue.requests[i].state == GRANTED && queue.requests[i].txnID != txnIDWaiting {
				anyoneElseHolding = true
				break
			}
		}
		if !anyoneElseHolding {
			queue.requests[firstWaiting].state = GRANTED
			close(queue.requests[firstWaiting].ch)
		}
	} else if queue.requests[firstWaiting].mode == SHARED {
		// Grant to all subsequent consecutive shared requests
		// unless there's an exclusive lock held by a different transaction
		anyoneElseHoldingExclusive := false
		txnIDWaiting := queue.requests[firstWaiting].txnID
		for i := 0; i < firstWaiting; i++ {
			if queue.requests[i].state == GRANTED && queue.requests[i].mode == EXCLUSIVE && queue.requests[i].txnID != txnIDWaiting {
				anyoneElseHoldingExclusive = true
				break
			}
		}

		if !anyoneElseHoldingExclusive {
			for i := firstWaiting; i < len(queue.requests); i++ {
				if queue.requests[i].mode == SHARED {
					if queue.requests[i].state != GRANTED {
						queue.requests[i].state = GRANTED
						close(queue.requests[i].ch)
					}
				} else {
					break // Stop if we hit an exclusive request
				}
			}
		}
	}
}
