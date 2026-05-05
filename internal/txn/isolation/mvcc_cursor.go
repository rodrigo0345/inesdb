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
            userKey, xmin := c.manager.decodeKey(entry.Key)

            // 1. Is this version committed/visible to us?
            if !c.manager.isVisible(c.txn, txn.TransactionID(xmin)) {
                  continue
            }

            // 2. Only return the newest visible version of a specific key
            keyStr := string(userKey)
            if c.seenKeys[keyStr] {
                  continue
            }
            c.seenKeys[keyStr] = true

            // 3. Check for Tombstone
            if len(entry.Value) > 0 && entry.Value[0] == OpDelete {
                  continue
            }

            // FIX: Must capture the current entry to be returned by Entry()
            c.currentEntry = entry
            return true
      }
      return false
}

func (c *MVCCCursor) Entry() storage.ScanEntry { return c.currentEntry }
func (c *MVCCCursor) Close() error             { return c.raw.Close() }
func (c *MVCCCursor) Error() error             { return c.raw.Error() }
