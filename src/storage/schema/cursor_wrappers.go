package schema

import (
	"sort"

	"github.com/rodrigo0345/omag/src/storage"
)

// FilterCursor yields only entries that satisfy the predicate.
type FilterCursor struct {
	base   storage.ICursor
	filter storage.RowFilterFunction
}

func NewFilterCursor(base storage.ICursor, filter storage.RowFilterFunction) *FilterCursor {
	return &FilterCursor{base: base, filter: filter}
}

func (c *FilterCursor) Next() bool {
	for c.base.Next() {
		entry := c.base.Entry()
		if c.filter(entry) {
			return true
		}
	}
	return false
}

func (c *FilterCursor) Entry() storage.ScanEntry {
	return c.base.Entry()
}

func (c *FilterCursor) Close() error {
	return c.base.Close()
}

func (c *FilterCursor) Error() error {
	return c.base.Error()
}

// LimitCursor terminates the stream after yielding N entries.
type LimitCursor struct {
	base  storage.ICursor
	limit int
	count int
}

func NewLimitCursor(base storage.ICursor, limit int) *LimitCursor {
	return &LimitCursor{base: base, limit: limit, count: 0}
}

func (c *LimitCursor) Next() bool {
	if c.count >= c.limit {
		return false
	}
	if c.base.Next() {
		c.count++
		return true
	}
	return false
}

func (c *LimitCursor) Entry() storage.ScanEntry {
	return c.base.Entry()
}

func (c *LimitCursor) Close() error {
	return c.base.Close()
}

func (c *LimitCursor) Error() error {
	return c.base.Error()
}

// OffsetCursor discards the first N entries before yielding.
type OffsetCursor struct {
	base    storage.ICursor
	offset  int
	skipped bool
}

func NewOffsetCursor(base storage.ICursor, offset int) *OffsetCursor {
	return &OffsetCursor{base: base, offset: offset, skipped: false}
}

func (c *OffsetCursor) Next() bool {
	if !c.skipped {
		for i := 0; i < c.offset; i++ {
			if !c.base.Next() {
				return false
			}
		}
		c.skipped = true
	}
	return c.base.Next()
}

func (c *OffsetCursor) Entry() storage.ScanEntry {
	return c.base.Entry()
}

func (c *OffsetCursor) Close() error {
	return c.base.Close()
}

func (c *OffsetCursor) Error() error {
	return c.base.Error()
}

// ProjectCursor transforms the entry payload.
type ProjectCursor struct {
	base      storage.ICursor
	projectFn func(key, value []byte) ([]byte, []byte)
	curr      storage.ScanEntry
}

func NewProjectCursor(base storage.ICursor, projectFn func(key, value []byte) ([]byte, []byte)) *ProjectCursor {
	return &ProjectCursor{base: base, projectFn: projectFn}
}

func (c *ProjectCursor) Next() bool {
	if c.base.Next() {
		entry := c.base.Entry()
		newKey, newValue := c.projectFn(entry.Key, entry.Value)
		c.curr = storage.ScanEntry{Key: newKey, Value: newValue}
		return true
	}
	return false
}

func (c *ProjectCursor) Entry() storage.ScanEntry {
	return c.curr
}

func (c *ProjectCursor) Close() error {
	return c.base.Close()
}

func (c *ProjectCursor) Error() error {
	return c.base.Error()
}

// SortCursor buffers, sorts, and yields.
type SortCursor struct {
	base    storage.ICursor
	lessFn  func(a, b storage.ScanEntry) bool
	entries []storage.ScanEntry
	idx     int
	loaded  bool
}

func NewSortCursor(base storage.ICursor, lessFn func(a, b storage.ScanEntry) bool) *SortCursor {
	return &SortCursor{base: base, lessFn: lessFn, idx: -1}
}

func (c *SortCursor) loadAndSort() {
	for c.base.Next() {
		entry := c.base.Entry()
		k := make([]byte, len(entry.Key))
		copy(k, entry.Key)
		v := make([]byte, len(entry.Value))
		copy(v, entry.Value)

		c.entries = append(c.entries, storage.ScanEntry{Key: k, Value: v})
	}
	sort.Slice(c.entries, func(i, j int) bool {
		return c.lessFn(c.entries[i], c.entries[j])
	})
	c.loaded = true
}

func (c *SortCursor) Next() bool {
	if !c.loaded {
		c.loadAndSort()
	}
	c.idx++
	return c.idx < len(c.entries)
}

func (c *SortCursor) Entry() storage.ScanEntry {
	if c.idx >= 0 && c.idx < len(c.entries) {
		return c.entries[c.idx]
	}
	return storage.ScanEntry{} // Returns an empty struct if called out of bounds
}

func (c *SortCursor) Close() error {
	c.entries = nil
	return c.base.Close()
}

func (c *SortCursor) Error() error {
	return c.base.Error()
}
