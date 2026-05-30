package schema

import (
	"fmt"

	"github.com/rodrigo0345/omag/src/storage"
)

type TwoHopCursor struct {
	secondaryCursor storage.ICursor
	primaryEngine   storage.IStorageEngine
	currentEntry    storage.ScanEntry
	err             error
}

func (c *TwoHopCursor) Next() bool {
	for {
		ok := c.secondaryCursor.Next()
		if !ok {
			return false
		}

		secEntry := c.secondaryCursor.Entry()
		primaryKey := secEntry.Value

		// Log the mapping found in the secondary index
		fmt.Printf("TwoHop: [Secondary Key: %x] mapped to [Primary Key: %x]\n", secEntry.Key, primaryKey)

		// Transparent Hop 2
		primaryVal, err := c.primaryEngine.Get(primaryKey)
		if err != nil {
			c.err = err
			fmt.Printf("TwoHop: Error fetching Primary Key %x: %v\n", primaryKey, err)
			return false
		}

		// Logic Check: primaryVal == nil usually means the key was not found.
		// If your MVCC Delete returns an OpDelete byte, primaryVal will NOT be nil.
		if primaryVal == nil {
			fmt.Printf("TwoHop: Skipping ghost record (Primary Key %x not found)\n", primaryKey)
			continue
		}

		// Check for MVCC Tombstones if primaryEngine returns them instead of nil
		if len(primaryVal) > 0 && primaryVal[0] == 0x02 { // Assuming OpDelete is 0x02
			fmt.Printf("TwoHop: Found Tombstone for Primary Key %x, skipping\n", primaryKey)
			continue
		}

		c.currentEntry = storage.ScanEntry{
			Key:   secEntry.Key,
			Value: primaryVal,
		}
		return true
	}
}

func (c *TwoHopCursor) Entry() storage.ScanEntry {
	return c.currentEntry
}

func (c *TwoHopCursor) Close() error {
	return c.secondaryCursor.Close()
}

func (c *TwoHopCursor) Error() error {
	if c.err != nil {
		return c.err
	}
	return nil
}
