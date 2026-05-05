package schema

import (
	"github.com/rodrigo0345/omag/internal/storage"
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

		// Transparent Hop 2
		primaryVal, err := c.primaryEngine.Get(primaryKey)
		if err != nil {
			c.err = err
			return false
		}

		// Skip "ghosts": secondary index entry exists but primary row was deleted
		if primaryVal == nil {
			continue
		}

		c.currentEntry = storage.ScanEntry{
			Key:   secEntry.Key, // The user's requested index key
			Value: primaryVal,   // The resolved full row payload
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
