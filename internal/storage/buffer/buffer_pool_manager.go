package buffer

import (
	"errors"
	"sync"

	"github.com/rodrigo0345/omag/internal/concurrency"
	"github.com/rodrigo0345/omag/internal/storage/page"
)

var (
	ErrBufferFull    = errors.New("buffer pool is full")
	ErrPageNotFound  = errors.New("page not found in buffer pool")
	ErrInvalidPageID = errors.New("invalid page ID")
)

type ReplacerType int

const (
	ReplacerLRU   ReplacerType = iota
	ReplacerClock
)

type WALFlusher interface {
	Flush(upToLSN uint64) error
}

type BufferPoolManager struct {
	poolSize        int
	frames          []page.IResourcePage
	pageTable       map[page.ResourcePageID]concurrency.FrameID
	freeList        []concurrency.FrameID
	diskManager     *DiskManager
	replacerManager concurrency.IReplacer
	walMgr          interface{}
	mu              sync.Mutex
}

func NewBufferPoolManager(poolSize int, diskManager *DiskManager) IBufferPoolManager {
	defaultReplacerPolicy := concurrency.NewClockReplacer(poolSize)
	return NewBufferPoolManagerWithReplacer(poolSize, diskManager, defaultReplacerPolicy)
}

func NewBufferPoolManagerWithReplacer(
	poolSize int,
	diskManager *DiskManager,
	replacerManager concurrency.IReplacer) IBufferPoolManager {
	frames := make([]page.IResourcePage, poolSize)
	freeList := make([]concurrency.FrameID, poolSize)

	for i := 0; i < poolSize; i++ {
		frames[i] = page.NewResourcePage(page.ResourcePageID(i))
		freeList[i] = concurrency.FrameID(i)
	}

	return &BufferPoolManager{
		poolSize:        poolSize,
		frames:          frames,
		pageTable:       make(map[page.ResourcePageID]concurrency.FrameID),
		freeList:        freeList,
		diskManager:     diskManager,
		replacerManager: replacerManager,
	}
}

func (bpm *BufferPoolManager) SetWALManager(walMgr interface{}) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	bpm.walMgr = walMgr
}

func (bpm *BufferPoolManager) flushPageInternal(page page.IResourcePage) error {

	if !page.IsDirty() {
		return nil
	}

	if bpm.walMgr != nil {
		if flusher, ok := bpm.walMgr.(WALFlusher); ok {
			if err := flusher.Flush(page.GetLSN()); err != nil {
				return err
			}
		}
	}

	if err := bpm.diskManager.WritePage(page.GetID(), page.GetData()); err != nil {
		return err
	}

	page.SetDirty(false)
	return nil
}

func (bpm *BufferPoolManager) PinPage(pageID page.ResourcePageID) (page.IResourcePage, error) {
	bpm.mu.Lock()

	if frameID, exists := bpm.pageTable[pageID]; exists {
		frame := bpm.frames[frameID]
		frame.SetPinCount(frame.GetPinCount() + 1)
		bpm.replacerManager.Pin(concurrency.FrameID(frameID))
		bpm.mu.Unlock()
		return frame, nil
	}

	frameID, err := bpm.getAvailableFrameIDLocked()
	if err != nil {
		bpm.mu.Unlock()
		return nil, err
	}

	frame := bpm.frames[frameID]
	frame.ReplacePage(pageID)

	if err := bpm.diskManager.ReadPage(pageID, frame.GetData()); err != nil {
		bpm.mu.Unlock()
		return nil, err
	}

	bpm.pageTable[pageID] = frameID
	bpm.replacerManager.Pin(concurrency.FrameID(frameID))

	bpm.mu.Unlock()
	return frame, nil
}

func (bpm *BufferPoolManager) UnpinPage(pageID page.ResourcePageID, isDirty bool) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	frameID, exists := bpm.pageTable[pageID]
	if !exists {
		return ErrPageNotFound
	}

	frame := bpm.frames[frameID]
	if frame.GetPinCount() > 0 {
		frame.SetPinCount(frame.GetPinCount() - 1)
	}

	if isDirty {
		frame.SetDirty(true)
	}

	if frame.GetPinCount() == 0 {
		bpm.replacerManager.Unpin(concurrency.FrameID(frameID))
	}

	return nil
}

func (bpm *BufferPoolManager) NewPage() (*page.IResourcePage, error) {
	bpm.mu.Lock()

	pageID := bpm.diskManager.AllocatePage()

	frameID, err := bpm.getAvailableFrameIDLocked()
	if err != nil {
		bpm.mu.Unlock()
		return nil, err
	}

	frame := bpm.frames[frameID]
	frame.ReplacePage(pageID)

	bpm.pageTable[pageID] = frameID
	bpm.replacerManager.Pin(frameID)

	bpm.mu.Unlock()
	return &frame, nil
}

func (bpm *BufferPoolManager) FlushPage(pageID page.ResourcePageID) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	frameID, exists := bpm.pageTable[pageID]
	if !exists {
		return ErrPageNotFound
	}

	return bpm.flushPageInternal(bpm.frames[frameID])
}

func (bpm *BufferPoolManager) FlushAll() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	for _, frameID := range bpm.pageTable {
		if err := bpm.flushPageInternal(bpm.frames[frameID]); err != nil {
			return err
		}
	}
	return nil
}

func (bpm *BufferPoolManager) Close() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	for _, frameID := range bpm.pageTable {
		if err := bpm.flushPageInternal(bpm.frames[frameID]); err != nil {
			return err
		}
	}

	return bpm.diskManager.Close()
}

func (bpm *BufferPoolManager) getAvailableFrameIDLocked() (concurrency.FrameID, error) {

	if len(bpm.freeList) > 0 {
		frameID := bpm.freeList[0]
		bpm.freeList = bpm.freeList[1:]
		return frameID, nil
	}

	frameID, ok := bpm.replacerManager.Victim()
	if !ok {
		return 0, ErrBufferFull
	}

	victimPage := bpm.frames[frameID]
	if err := bpm.flushPageInternal(victimPage); err != nil {
		return 0, err
	}

	delete(bpm.pageTable, victimPage.GetID())
	return frameID, nil
}

func (bpm *BufferPoolManager) GetPoolSize() int {
	return bpm.poolSize
}

func (bpm *BufferPoolManager) GetReplacerSize() int {
	return bpm.replacerManager.Size()
}
