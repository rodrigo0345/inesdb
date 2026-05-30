package lsm

import (
	"bytes"
	"container/heap"

	"github.com/rodrigo0345/omag/src/storage"
)

type LSMCursor struct {
	h            *iterHeap
	opts         storage.ScanOptions
	seenKeys     map[string]bool
	currentEntry storage.ScanEntry
	count        int // For Limit
	skipped      int // For Offset
	err          error
	closed       bool
}

func (c *LSMCursor) Next() bool {
	if c.closed || c.err != nil {
		return false
	}

	for c.h.Len() > 0 {
		it := heap.Pop(c.h).(*sstableIter)
		key := it.key()

		// 1. Boundary Checks
		if c.opts.Reverse {
			if len(c.opts.LowerBound) > 0 && bytes.Compare([]byte(key), c.opts.LowerBound) < 0 {
				c.Close()
				return false
			}
		} else {
			if len(c.opts.UpperBound) > 0 {
				cmp := bytes.Compare([]byte(key), c.opts.UpperBound)
				if cmp > 0 || (cmp == 0 && !c.opts.Inclusive) {
					c.Close()
					return false
				}
			}
		}

		// 2. Deduplication: Skip if we already saw a newer version of this key
		if c.seenKeys[key] {
			c.advance(it)
			continue
		}

		// Mark it as seen so we ignore older versions in deeper SSTables
		c.seenKeys[key] = true

		// 3. Tombstone Check: Did the newest version delete this key?
		isDeleted := it.IsTombstoned()
		val := it.val()

		c.advance(it)

		if isDeleted {
			continue // Skip returning this to the user
		}

		// 4. Pagination
		if c.skipped < c.opts.Offset {
			c.skipped++
			continue
		}

		c.count++
		if c.opts.Limit > 0 && c.count > c.opts.Limit {
			c.Close()
			return false
		}

		// 5. Success! Return the active data
		c.currentEntry = storage.ScanEntry{
			Key:   []byte(key),
			Value: append([]byte(nil), val...),
		}
		return true
	}

	return false
}

func (c *LSMCursor) advance(it *sstableIter) {
	var advanced bool
	if c.opts.Reverse {
		advanced = it.prev()
	} else {
		advanced = it.next()
	}
	if advanced {
		heap.Push(c.h, it)
	}
}

func (c *LSMCursor) Entry() storage.ScanEntry {
	return c.currentEntry
}

func (c *LSMCursor) Error() error {
	return c.err
}

func (c *LSMCursor) Close() error {
	c.closed = true
	c.seenKeys = nil // Release memory
	return nil
}
