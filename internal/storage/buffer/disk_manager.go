package buffer

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rodrigo0345/omag/internal/storage/page"
)

var (
	ErrDiskManagerClosed = errors.New("disk manager is closed")
)

const (
	BatchSizeThreshold = 512 * 1024 // 512KB
	FlushInterval      = 2 * time.Second
)

type writeRequest struct {
	pageID   page.ResourcePageID
	pageData []byte
	done     chan struct{} // Used for synchronization in Flush
	isMarker bool          // True if this is a flush marker, not a real write
}

type DiskManager struct {
	dbFile     *os.File
	nextPage   page.ResourcePageID
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
		nextPage:   page.ResourcePageID(stat.Size() / int64(page.PageSize)),
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

		// TODO: sort the buffer by PageID here
		// to make the disk I/O more sequential (LBA ordering)
		var doneChans []chan struct{}
		for _, req := range buffer {
			// Skip writing for marker requests (used by Flush)
			if !req.isMarker {
				offset := int64(req.pageID) * int64(page.PageSize)
				_, err := dm.dbFile.WriteAt(req.pageData, offset)
				if err != nil {
					// TODO: handle this condition
					panic("error writing page:" + err.Error())
				}
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
func (dm *DiskManager) WritePage(pageID page.ResourcePageID, pageData []byte) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	if dm.writeQueue == nil || dm.quit == nil {
		return ErrDiskManagerClosed
	}

	// Validate page size
	if len(pageData) != page.PageSize {
		return errors.New("invalid page size")
	}

	// Still cloning to ensure memory safety while the page sits in the batch buffer
	dataCopy := make([]byte, page.PageSize)
	copy(dataCopy, pageData)

	dm.writeQueue <- writeRequest{
		pageID:   pageID,
		pageData: dataCopy,
		done:     nil, // Normal writes don't wait
	}

	return nil
}

// ReadPage uses ReadAt to remain compatible with the stateless model
func (dm *DiskManager) ReadPage(pageID page.ResourcePageID, pageData []byte) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	// calculate the offset and read the page data directly into the provided pageData buffer
	offset := int64(pageID) * int64(page.PageSize)

	// read up until len(pageData)
	_, err := dm.dbFile.ReadAt(pageData, offset)
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (dm *DiskManager) AllocatePage() page.ResourcePageID {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	pageID := dm.nextPage
	dm.nextPage++
	return pageID
}

func (dm *DiskManager) Close() error {
	dm.mu.Lock()
	if dm.closed {
		dm.mu.Unlock()
		return nil
	}
	dm.closed = true
	dm.mu.Unlock()

	close(dm.quit)
	close(dm.writeQueue)

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

// GetFileSize returns the current size of the database file in bytes
func (dm *DiskManager) GetFileSize() (int64, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dbFile == nil {
		return 0, ErrDiskManagerClosed
	}

	stat, err := dm.dbFile.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

// Flush waits for all pending writes to be written to disk
func (dm *DiskManager) Flush() error {
	dm.mu.RLock()
	if dm.dbFile == nil {
		dm.mu.RUnlock()
		return ErrDiskManagerClosed
	}
	if dm.writeQueue == nil || dm.quit == nil {
		dm.mu.RUnlock()
		return ErrDiskManagerClosed
	}
	dm.mu.RUnlock()

	// Send a marker write with a done channel to ensure all pending writes get flushed
	done := make(chan struct{})
	dm.writeQueue <- writeRequest{
		pageID:   0,
		pageData: make([]byte, page.PageSize), // Empty/dummy data
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
