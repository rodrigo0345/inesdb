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
	ch    chan struct{}
}

type LockQueue struct {
	requests []*LockRequest
	mu sync.Mutex
}

type LockManager struct {
	lockTable    map[string]*LockQueue
	mu           sync.Mutex
	waitForGraph *WaitForGraph
	deadlockWait time.Duration
}

func NewLockManager() *LockManager {
	return NewLockManagerWithTimeout(5 * time.Second)
}

func NewLockManagerWithTimeout(timeout time.Duration) *LockManager {
	return &LockManager{
		lockTable:    make(map[string]*LockQueue),
		waitForGraph: NewWaitForGraph(),
		deadlockWait: timeout,
	}
}

func (lm *LockManager) SetDeadlockTimeout(timeout time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.deadlockWait = timeout
}

func (lm *LockManager) LockShared(txn ITransaction, key []byte) error {
	if txn == nil {
		return ErrTxnAborted
	}

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
		mode:  SHARED,
		state: WAITING,
		ch:    make(chan struct{}),
	}

	queue.mu.Lock()

	for _, r := range queue.requests {
		if r.txnID == txn.GetID() && r.state == GRANTED {
			if r.mode == EXCLUSIVE {
				queue.mu.Unlock()
				return nil
			}
			if r.mode == SHARED {
				queue.mu.Unlock()
				return nil
			}
		}
	}

	canGrant := true
	var holderTxnID uint64
	for _, expected := range queue.requests {
		if expected.mode == EXCLUSIVE && expected.state == GRANTED {
			canGrant = false
			holderTxnID = expected.txnID
			break
		}
	}

	if canGrant {
		req.state = GRANTED
		txn.AddSharedLock(key)
	}

	queue.requests = append(queue.requests, req)
	queue.mu.Unlock()

	if canGrant {
		return nil
	}

	lm.waitForGraph.AddWait(txn.GetID(), holderTxnID)
	defer lm.waitForGraph.RemoveWait(txn.GetID(), holderTxnID)

	if lm.waitForGraph.HasCycle(txn.GetID()) {
		txn.Abort()
		return ErrDeadlock
	}

	for {
		select {
		case <-req.ch:
			txn.AddSharedLock(key)
		case <-time.After(lm.deadlockWait):
			if lm.waitForGraph.HasCycle(txn.GetID()) {
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
		}
	}
}

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

	isUpgrade := false
	var holderTxnID uint64
	for _, r := range queue.requests {
		if r.txnID == txn.GetID() && r.state == GRANTED {
			if r.mode == EXCLUSIVE {
				queue.mu.Unlock()
				return nil
			}
			if r.mode == SHARED {
				isUpgrade = true
				req.state = WAITING
				break
			}
		}
	}

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

	if holderTxnID > 0 {
		lm.waitForGraph.AddWait(txn.GetID(), holderTxnID)
		defer lm.waitForGraph.RemoveWait(txn.GetID(), holderTxnID)
	}

	if holderTxnID > 0 && lm.waitForGraph.HasCycle(txn.GetID()) {
		txn.Abort()
		return ErrDeadlock
	}

	for {
		select {
		case <-req.ch:
			txn.AddExclusiveLock(key)

		case <-time.After(lm.deadlockWait):
			if lm.waitForGraph.HasCycle(txn.GetID()) {
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
		}
	}
}

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
		return
	}

	if queue.requests[firstWaiting].mode == EXCLUSIVE {
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
					break
				}
			}
		}
	}
}
