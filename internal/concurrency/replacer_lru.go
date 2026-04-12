package concurrency

import (
	"container/list"
	"sync"
)

type LRUReplacer struct {
	mu       sync.Mutex
	list     *list.List
	frameMap map[FrameID]*list.Element
}

func NewLRUReplacer(pool_size int) *LRUReplacer {
	return &LRUReplacer{
		list:     list.New(),
		frameMap: make(map[FrameID]*list.Element),
	}
}

func (lru *LRUReplacer) Victim() (FrameID, bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if lru.list.Len() == 0 {
		return 0, false
	}

	elem := lru.list.Front()
	frameID := elem.Value.(FrameID)

	lru.list.Remove(elem)
	delete(lru.frameMap, frameID)

	return frameID, true
}

func (lru *LRUReplacer) Pin(frameID FrameID) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.frameMap[frameID]; exists {
		lru.list.Remove(elem)
		delete(lru.frameMap, frameID)
	}
}

func (lru *LRUReplacer) Unpin(frameID FrameID) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.frameMap[frameID]; exists {
		lru.list.MoveToBack(elem)
		return
	}

	elem := lru.list.PushBack(frameID)
	lru.frameMap[frameID] = elem
}

func (lru *LRUReplacer) Size() int {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.list.Len()
}
