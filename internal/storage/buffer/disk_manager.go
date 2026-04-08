package buffermanager

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rodrigo0345/omag/resource_page"
)

var (
	ErrDiskManagerClosed = errors.New("disk manager is closed")
)

const (
	BatchSizeThreshold = 512 * 1024 // 512KB
	FlushInterval      = 2 * time.Second
)

type writeRequest struct {
	pageID   resource_page.ResourcePageID
	pageData []byte
	done     chan struct{} // Used for synchronization in Flush
	isMarker bool          // True if this is a flush marker, not a real write
}

type DiskManager struct {
	dbFile     *os.File
	nextPage   resource_page.ResourcePageID
	mu         sync.RWMutex
	writeQueue chan writeRequest
	wg         sync.WaitGroup
	quit       chan struct{}
	closed     bool // Track if already closed
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
		nextPage:   resource_page.ResourcePageID(stat.Size() / int64(resource_page.PageSize)),
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
		var doneChans []chan struct{}
		for _, req := range buffer {
			// Skip writing for marker requests (used by Flush)
			if !req.isMarker {
				offset := int64(req.pageID) * int64(resource_page.PageSize)
				dm.dbFile.WriteAt(req.pageData, offset)
			}
			if req.done != nil {
				doneChans = append(doneChans, req.done)
			}
		}

		// Signal all done channels
		for _, ch := range doneChans {
			close(ch)
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

// WritePage validates size and queues the write
func (dm *DiskManager) WritePage(pageID resource_page.ResourcePageID, pageData []byte) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	// Validate page size
	if len(pageData) != resource_page.PageSize {
		return errors.New("invalid page size")
	}

	// Still cloning to ensure memory safety while the page sits in the batch buffer
	dataCopy := make([]byte, resource_page.PageSize)
	copy(dataCopy, pageData)

	dm.writeQueue <- writeRequest{
		pageID:   pageID,
		pageData: dataCopy,
		done:     nil, // Normal writes don't wait
	}

	return nil
}

// ReadPage uses ReadAt to remain compatible with the stateless model
func (dm *DiskManager) ReadPage(pageID resource_page.ResourcePageID, pageData []byte) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	// calculate the offset and read the page data directly into the provided pageData buffer
	offset := int64(pageID) * int64(resource_page.PageSize)

	// read up until len(pageData)
	_, err := dm.dbFile.ReadAt(pageData, offset)
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (dm *DiskManager) AllocatePage() resource_page.ResourcePageID {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	pageID := dm.nextPage
	dm.nextPage++
	return pageID
}

func (dm *DiskManager) Close() error {
	dm.mu.Lock()
	// Prevent double-close
	if dm.closed {
		dm.mu.Unlock()
		return nil
	}
	dm.closed = true
	dm.mu.Unlock()

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

// Flush waits for all pending writes to be written to disk
func (dm *DiskManager) Flush() error {
	dm.mu.RLock()
	if dm.dbFile == nil {
		dm.mu.RUnlock()
		return ErrDiskManagerClosed
	}
	dm.mu.RUnlock()

	// Send a marker write with a done channel to ensure all pending writes get flushed
	done := make(chan struct{})
	dm.writeQueue <- writeRequest{
		pageID:   0,
		pageData: make([]byte, resource_page.PageSize), // Empty/dummy data
		done:     done,
		isMarker: true, // This is a flush marker, don't actually write it
	}

	// Wait for the marker to be processed
	<-done

	// Now sync to ensure data is on disk
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.dbFile != nil {
		return dm.dbFile.Sync()
	}
	return nil
}

func (dm *DiskManager) Sync() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}
	return dm.dbFile.Sync()
}
