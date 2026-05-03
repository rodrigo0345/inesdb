package isolation

import (
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

type MVCCCursor struct {
	raw          storage.ICursor
	manager      *MVCCManager
	txn          *txn_unit.Transaction
	seenKeys     map[string]bool
	currentEntry storage.ScanEntry
}

func (c *MVCCCursor) Next() bool {
	for c.raw.Next() {
		entry := c.raw.Entry()
		userKey, txnID := c.manager.decodeKey(entry.Key)
		keyStr := string(userKey)

		// If we've already returned a visible version for this key, skip all older ones
		if c.seenKeys[keyStr] {
			continue
		}

		// Check visibility
		if !c.manager.isVisible(c.txn, txn.TransactionID(txnID)) {
			continue
		}

		// We found the latest visible version. Mark it so we don't look at older ones.
		c.seenKeys[keyStr] = true

		// Check if it's a tombstone
		if entry.Value[0] == OpDelete {
			continue // This version is a delete, move to the next unique UserKey
		}

		return true
	}
	return false
}

func (c *MVCCCursor) Entry() storage.ScanEntry { return c.currentEntry }
func (c *MVCCCursor) Close() error             { return c.raw.Close() }
func (c *MVCCCursor) Error() error             { return c.raw.Error() }
