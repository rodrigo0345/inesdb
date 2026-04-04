package buffermanager

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

var (
	ErrDiskManagerClosed = errors.New("disk manager is closed")
)

const (
	BatchSizeThreshold = 512 * 1024 // 512KB
	FlushInterval      = 2 * time.Second
)

type writeRequest struct {
	pageID   PageID
	pageData []byte
}

type DiskManager struct {
	dbFile     *os.File
	nextPage   PageID
	mu         sync.RWMutex
	writeQueue chan writeRequest
	wg         sync.WaitGroup
	quit       chan struct{}
}

func NewDiskManager(dbPath string) (*DiskManager, error) {
	file, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	dm := &DiskManager{
		dbFile:     file,
		nextPage:   PageID(stat.Size() / int64(PageSize)),
		writeQueue: make(chan writeRequest, 2048), // Larger queue for batching
		quit:       make(chan struct{}),
	}

	dm.wg.Add(1)
	go dm.runBatchWorker()

	return dm, nil
}

func (dm *DiskManager) runBatchWorker() {
	defer dm.wg.Done()

	// Internal buffer to hold pages before flushing
	var buffer []writeRequest
	var currentBufferSize int

	// Timer for the 2-second interval
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	// Helper function to execute the batch write
	flush := func() {
		if len(buffer) == 0 {
			return
		}

		// Optimization: You could sort the buffer by PageID here
		// to make the disk I/O more sequential (LBA ordering)
		for _, req := range buffer {
			offset := int64(req.pageID) * int64(PageSize)
			dm.dbFile.WriteAt(req.pageData, offset)
		}

		// Reset tracking
		buffer = nil
		currentBufferSize = 0
	}

	for {
		select {
		case req, ok := <-dm.writeQueue:
			if !ok {
				flush() // Final flush on channel close
				return
			}

			buffer = append(buffer, req)
			currentBufferSize += len(req.pageData)

			// Trigger 1: Size-based (512KB)
			if currentBufferSize >= BatchSizeThreshold {
				flush()
				ticker.Reset(FlushInterval) // Reset timer after manual flush
			}

		case <-ticker.C:
			// Trigger 2: Time-based (2 seconds)
			flush()

		case <-dm.quit:
			flush()
			return
		}
	}
}

// WritePage remains mostly the same, but now it feeds the batcher
func (dm *DiskManager) WritePage(pageID PageID, pageData []byte) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	// Still cloning to ensure memory safety while the page sits in the batch buffer
	dataCopy := make([]byte, PageSize)
	copy(dataCopy, pageData)

	dm.writeQueue <- writeRequest{
		pageID:   pageID,
		pageData: dataCopy,
	}

	return nil
}

// ReadPage uses ReadAt to remain compatible with the stateless model
func (dm *DiskManager) ReadPage(pageID PageID, pageData []byte) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	offset := int64(pageID) * int64(PageSize)
	_, err := dm.dbFile.ReadAt(pageData, offset)
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (dm *DiskManager) AllocatePage() PageID {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	pageID := dm.nextPage
	dm.nextPage++
	return pageID
}

func (dm *DiskManager) Close() error {
	// 1. Signal worker to stop
	close(dm.quit)
	// 2. Wait for pending writes to finish
	dm.wg.Wait()

	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.dbFile != nil {
		dm.dbFile.Sync()
		err := dm.dbFile.Close()
		dm.dbFile = nil
		return err
	}
	return nil
}

func (dm *DiskManager) Sync() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.dbFile.Sync()
}
