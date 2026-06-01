package vector

import "github.com/rodrigo0345/omag/src/storage"

// vectorCursor implements storage.ICursor over a pre-built slice of entries.
// It is used for both full-table Scan and KNN search results.
type vectorCursor struct {
	entries []storage.ScanEntry
	pos     int
}

func newSliceCursor(entries []storage.ScanEntry) *vectorCursor {
	return &vectorCursor{entries: entries, pos: -1}
}

func (c *vectorCursor) Next() bool {
	c.pos++
	return c.pos < len(c.entries)
}

func (c *vectorCursor) Entry() storage.ScanEntry {
	if c.pos < 0 || c.pos >= len(c.entries) {
		return storage.ScanEntry{}
	}
	return c.entries[c.pos]
}

func (c *vectorCursor) Error() error { return nil }
func (c *vectorCursor) Close() error { return nil }
