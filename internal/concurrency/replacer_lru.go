package concurrency

import (
	"container/list"
	"sync"
)

// LRUReplacer implements Replacer using a Least Recently Used eviction policy
type LRUReplacer struct {
	mu       sync.Mutex
	list     *list.List                // Doubly linked list of frame IDs
	frameMap map[FrameID]*list.Element // Map of frame ID to list element
}

// NewLRUReplacer creates a new LRU replacer with pool_size capacity
func NewLRUReplacer(pool_size int) *LRUReplacer {
	return &LRUReplacer{
		list:     list.New(),
		frameMap: make(map[FrameID]*list.Element),
	}
}

// Victim returns the LRU frame to evict
func (lru *LRUReplacer) Victim() (FrameID, bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	// The front of the list is the least recently used
	if lru.list.Len() == 0 {
		return 0, false
	}

	// Get the front element (LRU)
	elem := lru.list.Front()
	frameID := elem.Value.(FrameID)

	// Remove from list and map
	lru.list.Remove(elem)
	delete(lru.frameMap, frameID)

	return frameID, true
}

// Pin removes a frame from consideration for eviction
func (lru *LRUReplacer) Pin(frameID FrameID) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	// If the frame is in the list, remove it
	if elem, exists := lru.frameMap[frameID]; exists {
		lru.list.Remove(elem)
		delete(lru.frameMap, frameID)
	}
}

// Unpin adds/moves a frame to the end of the list (most recently used)
func (lru *LRUReplacer) Unpin(frameID FrameID) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	// If already in the list, move it to the back (most recently used)
	if elem, exists := lru.frameMap[frameID]; exists {
		lru.list.MoveToBack(elem)
		return
	}

	// Otherwise, add it to the back
	elem := lru.list.PushBack(frameID)
	lru.frameMap[frameID] = elem
}

// Size returns the number of frames available for eviction
func (lru *LRUReplacer) Size() int {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.list.Len()
}
