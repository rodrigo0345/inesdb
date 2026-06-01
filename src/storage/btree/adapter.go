package btree

import (
	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/buffer"
	"github.com/rodrigo0345/omag/src/storage/page"
)

// DiskMgr is the disk I/O interface required by BPlusTreeBackend.
type DiskMgr interface {
	WritePage(pageID page.ResourcePageID, pageData []byte) error
	ReadPage(pageID page.ResourcePageID, pageData []byte) error
}

// BTreeAdapter wraps BPlusTreeBackend to satisfy storage.IStorageEngine.
type BTreeAdapter struct {
	backend *BPlusTreeBackend
}

// NewBTreeAdapter creates a BTreeAdapter backed by BPlusTreeBackend.
func NewBTreeAdapter(bufMgr buffer.IBufferPoolManager, diskMgr DiskMgr) (*BTreeAdapter, error) {
	backend, err := NewBPlusTreeBackend(bufMgr, diskMgr)
	if err != nil {
		return nil, err
	}
	return &BTreeAdapter{backend: backend}, nil
}

func (a *BTreeAdapter) Put(key, value []byte) error {
	return a.backend.Put(key, value)
}

func (a *BTreeAdapter) Get(key []byte) ([]byte, error) {
	return a.backend.Get(key)
}

func (a *BTreeAdapter) Delete(key []byte) error {
	return a.backend.Delete(key)
}

// Scan adapts the BPlusTreeBackend's slice-returning Scan to the ICursor interface.
func (a *BTreeAdapter) Scan(opts storage.ScanOptions) (storage.ICursor, error) {
	entries, err := a.backend.Scan(opts.LowerBound, opts.UpperBound, opts.ComplexFilter)
	if err != nil {
		return nil, err
	}

	// Apply offset and limit to the slice before wrapping.
	if opts.Offset > 0 && opts.Offset < len(entries) {
		entries = entries[opts.Offset:]
	} else if opts.Offset >= len(entries) {
		entries = nil
	}
	if opts.Limit > 0 && opts.Limit < len(entries) {
		entries = entries[:opts.Limit]
	}

	return &sliceCursor{entries: entries, pos: -1}, nil
}

// sliceCursor adapts []ScanEntry to the ICursor interface.
type sliceCursor struct {
	entries []storage.ScanEntry
	pos     int
}

func (c *sliceCursor) Next() bool {
	c.pos++
	return c.pos < len(c.entries)
}

func (c *sliceCursor) Entry() storage.ScanEntry {
	if c.pos < 0 || c.pos >= len(c.entries) {
		return storage.ScanEntry{}
	}
	return c.entries[c.pos]
}

func (c *sliceCursor) Error() error { return nil }
func (c *sliceCursor) Close() error { return nil }
