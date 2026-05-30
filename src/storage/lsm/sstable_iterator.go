package lsm

import (
	"bytes"
	"sort"
)

type sstableIter struct {
	rows     []sstableRow
	pos      int
	priority int
}

func newSSTableIter(ss *SSTable, priority int) *sstableIter {
	if len(ss.sortedRows) != len(ss.data) {
		// opt: build the contiguous row view once; scans then walk linear memory.
		ss.rebuildSortedRows()
	}

	return &sstableIter{
		rows:     ss.sortedRows,
		pos:      -1,
		priority: priority,
	}
}

func (it *sstableIter) next() bool {
	it.pos++
	return it.pos < len(it.rows)
}

func (it *sstableIter) prev() bool {
	it.pos--
	return it.pos >= 0
}

func (it *sstableIter) key() string { return it.rows[it.pos].key }
func (it *sstableIter) val() []byte { return it.rows[it.pos].value }

func (it *sstableIter) IsTombstoned() bool {
	if it.pos < 0 || it.pos >= len(it.rows) {
		return false
	}
	return it.rows[it.pos].tombstone
}

func (it *sstableIter) last() bool {
	it.pos = len(it.rows) - 1
	return it.pos >= 0
}

func (it *sstableIter) seek(bound []byte, reverse bool) bool {
	if len(bound) == 0 {
		if reverse {
			return it.last()
		}
		it.pos = -1 // Reset for next()
		return it.next()
	}

	// Binary search for the first key >= bound
	// Note: Comparison must be binary-safe. Convert it.rows[i].key to []byte.
	idx := sort.Search(len(it.rows), func(i int) bool {
		return bytes.Compare([]byte(it.rows[i].key), bound) >= 0
	})

	if reverse {
		// Reverse seek: find the largest key <= bound
		if idx >= len(it.rows) || (idx < len(it.rows) && bytes.Compare([]byte(it.rows[idx].key), bound) > 0) {
			idx--
		}
		it.pos = idx
		return it.pos >= 0
	}

	// Forward seek: find the smallest key >= bound
	it.pos = idx
	return it.pos < len(it.rows)
}
