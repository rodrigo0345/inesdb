package mvcc

import (
	"fmt"

	"github.com/rodrigo0345/omag/src/storage"
	txn_unit "github.com/rodrigo0345/omag/src/engine/txn"
	"github.com/rodrigo0345/omag/pkg/pkglog"
)

type MVCCCursor struct {
	raw          storage.ICursor
	manager      *MVCCManager
	opts         storage.ScanOptions
	txn          *txn_unit.Transaction
	seenKeys     map[string]bool
	currentEntry storage.ScanEntry
	skipped      int
	count        int
	tracer       *pkglog.Tracer // Injected tracer
}

func (c *MVCCCursor) Next() bool {
	if c.tracer != nil {
		c.tracer.Add("Cursor.Next: Awaiting next entry from physical storage engine...")
	}

	for c.raw.Next() {
		entry := c.raw.Entry()
		userKey, writerTxnID, ok := c.manager.decodeKey(entry.Key)
		if !ok {
			if c.tracer != nil {
				c.tracer.Add("Cursor.Next: Warning - Failed to decode a raw key from storage, skipping.")
			}
			continue
		}

		ukStr := humanReadableBytes(userKey)

		if c.tracer != nil {
			c.tracer.Add(fmt.Sprintf("Cursor.Next: Evaluating Key [%s] written by TxnID [%d]", ukStr, writerTxnID))
		}

		uk := string(userKey)

		// 1. Have we already resolved the latest visible state for this key?
		if c.seenKeys[uk] {
			if c.tracer != nil {
				c.tracer.Add(fmt.Sprintf("Cursor.Next: Key [%s] already resolved in this scan. Skipping this older version.", ukStr))
			}
			continue
		}

		// 2. Is this specific version visible to our transaction?
		isSelf := txn_unit.TransactionID(writerTxnID) == c.txn.GetID()
		if !isSelf && !c.manager.isVisible(c.txn, txn_unit.TransactionID(writerTxnID)) {
			if c.tracer != nil {
				c.tracer.Add(fmt.Sprintf("Cursor.Next: Key [%s] (TxnID %d) is NOT visible to our TxnID %d. Continuing search down version chain.", ukStr, writerTxnID, c.txn.GetID()))
			}
			continue
		}

		// 3. We found the most recent visible version! Mark it.
		c.seenKeys[uk] = true
		if c.tracer != nil {
			c.tracer.Add(fmt.Sprintf("Cursor.Next: Key [%s] is the latest visible version for our snapshot. Marked as seen.", ukStr))
		}

		// 4. Tombstone Check
		if len(entry.Value) == 0 {
			if c.tracer != nil {
				c.tracer.Add(fmt.Sprintf("Cursor.Next: Key [%s] has an empty value. Skipping.", ukStr))
			}
			continue
		}
		if entry.Value[0] == OpDelete {
			if c.tracer != nil {
				c.tracer.Add(fmt.Sprintf("Cursor.Next: Key [%s] is a Tombstone (OpDelete). Effectively deleted for this txn, skipping.", ukStr))
			}
			continue
		}

		// 5. Pagination: We apply offset and limit AFTER visibility
		if c.skipped < c.opts.Offset {
			c.skipped++
			if c.tracer != nil {
				c.tracer.Add(fmt.Sprintf("Cursor.Next: Key [%s] skipped due to pagination Offset (%d/%d).", ukStr, c.skipped, c.opts.Offset))
			}
			continue
		}

		c.count++
		if c.opts.Limit > 0 && c.count > c.opts.Limit {
			if c.tracer != nil {
				c.tracer.Add(fmt.Sprintf("Cursor.Next: Scan reached Limit of %d items. Terminating scan early.", c.opts.Limit))
			}
			c.Close()
			return false
		}

		// 6. It's a valid Insert/Update. Copy it safely and return.
		k := make([]byte, len(userKey))
		copy(k, userKey)
		v := make([]byte, len(entry.Value)-1)
		copy(v, entry.Value[1:])

		valStr := humanReadableBytes(v)

		if c.tracer != nil {
			c.tracer.Add(fmt.Sprintf("Cursor.Next: Yielding valid record -> Key: [%s], Value: [%s]", ukStr, valStr))
		}

		c.currentEntry = storage.ScanEntry{
			Key:   k,
			Value: v,
		}
		return true
	}

	if c.tracer != nil {
		c.tracer.Add("Cursor.Next: Exhausted all physical records in range. Ending scan.")
	}
	return false
}

func (c *MVCCCursor) Entry() storage.ScanEntry { return c.currentEntry }
func (c *MVCCCursor) Close() error             { return c.raw.Close() }
func (c *MVCCCursor) Error() error             { return c.raw.Error() }

// humanReadableBytes attempts to convert a byte slice into a readable string.
// If it encounters unprintable binary characters, it falls back to a hex representation.
func humanReadableBytes(data []byte) string {
	if len(data) == 0 {
		return "<empty>"
	}

	// Quick scan for non-printable ASCII characters (excluding standard whitespace)
	isPrintable := true
	for _, b := range data {
		if b < 32 && b != '\n' && b != '\r' && b != '\t' {
			isPrintable = false
			break
		}
	}

	if isPrintable {
		return string(data)
	}

	// Fallback for raw binary data like encoded integers or specialized structs
	return fmt.Sprintf("<binary: 0x%x>", data)
}
