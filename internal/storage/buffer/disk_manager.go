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
	BatchSizeThreshold = 512 * 1024
	FlushInterval      = 2 * time.Second
)

type writeRequest struct {
	pageID   page.ResourcePageID
	pageData []byte
	done     chan struct{}
	isMarker bool
}

type DiskManager struct {
	dbFile     *os.File
	nextPage   page.ResourcePageID
	mu         sync.RWMutex
	writeQueue chan writeRequest
	wg         sync.WaitGroup
	quit       chan struct{}
	closed     bool
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
		writeQueue: make(chan writeRequest, 2048),
		quit:       make(chan struct{}),
	}

	dm.wg.Add(1)
	go dm.runBatchWorker()

	return dm, nil
}

func (dm *DiskManager) runBatchWorker() {
	defer dm.wg.Done()

	var buffer []writeRequest
	var currentBufferSize int

	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		var doneChans []chan struct{}
		for _, req := range buffer {
			if !req.isMarker {
				offset := int64(req.pageID) * int64(page.PageSize)
				_, err := dm.dbFile.WriteAt(req.pageData, offset)
				if err != nil {
					panic("error writing page:" + err.Error())
				}
			}
			if req.done != nil {
				doneChans = append(doneChans, req.done)
			}
		}

		for _, ch := range doneChans {
			close(ch)
		}

		buffer = nil
		currentBufferSize = 0
	}

	for {
		select {
		case req, ok := <-dm.writeQueue:
			if !ok {
				flush()
				return
			}

			buffer = append(buffer, req)
			currentBufferSize += len(req.pageData)

			if currentBufferSize >= BatchSizeThreshold {
				flush()
				ticker.Reset(FlushInterval)
			}

		case <-ticker.C:
			flush()

		case <-dm.quit:
			flush()
			return
		}
	}
}

func (dm *DiskManager) WritePage(pageID page.ResourcePageID, pageData []byte) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	if dm.writeQueue == nil || dm.quit == nil {
		return ErrDiskManagerClosed
	}

	if len(pageData) != page.PageSize {
		return errors.New("invalid page size")
	}

	dataCopy := make([]byte, page.PageSize)
	copy(dataCopy, pageData)

	dm.writeQueue <- writeRequest{
		pageID:   pageID,
		pageData: dataCopy,
		done:     nil,
	}

	return nil
}

func (dm *DiskManager) ReadPage(pageID page.ResourcePageID, pageData []byte) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	offset := int64(pageID) * int64(page.PageSize)

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

	done := make(chan struct{})
	dm.writeQueue <- writeRequest{
		pageID:   0,
		pageData: make([]byte, page.PageSize),
		done:     done,
		isMarker: true,
	}

	<-done

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
