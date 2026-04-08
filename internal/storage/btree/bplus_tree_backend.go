package bplus_tree_backend

import (
	"fmt"

	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/logmanager"
	"github.com/rodrigo0345/omag/resource_page"
	"github.com/rodrigo0345/omag/transaction_manager"
)

type BPlusTreeBackend struct {
	bufferManager  buffermanager.IBufferPoolManager
	logManager     logmanager.ILogManager
	meta           *MetaLogicPage
	getActiveTxnID func() *transaction_manager.TransactionID
}

func (b *BPlusTreeBackend) Get(key []byte) ([]byte, error) {
	rootPage := b.meta.RootPage()
	if rootPage == 0 {
		return nil, nil
	}

	path, err := b.findLeafPage(rootPage, key)
	if err != nil {
		return nil, fmt.Errorf("findLeafPage: %w", err)
	}

	leafPageID := path[len(path)-1]
	leafResourcePage, err := b.bufferManager.PinPage(resource_page.ResourcePageID(leafPageID))
	if err != nil {
		return nil, err
	}
	defer b.bufferManager.UnpinPage(resource_page.ResourcePageID(leafPageID), false)

	(*leafResourcePage).RLock()
	defer (*leafResourcePage).RUnlock()

	leafLogicalPage := &LeafLogicPage{data: (*leafResourcePage).GetData()}
	return leafLogicalPage.Get(key)
}

func (b *BPlusTreeBackend) Put(key []byte, value []byte) error {
	txnID := b.getActiveTxnID()
	if txnID == nil {
		return fmt.Errorf("no active transaction")
	}

	rootID := b.meta.RootPage()
	path, err := b.findLeafPage(rootID, key)
	if err != nil {
		return fmt.Errorf("findLeafPage: %w", err)
	}

	leafID := path[len(path)-1]
	leafPageRef, err := b.bufferManager.PinPage(resource_page.ResourcePageID(leafID))
	if err != nil {
		return err
	}
	leafPage := *leafPageRef

	leafPage.WLock()

	leaf := &LeafLogicPage{data: leafPage.GetData()}

	before, err := leaf.Get(key)
	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(resource_page.ResourcePageID(leafID), false)
		return err
	}

	err = leaf.Insert(key, value)
	if err == ErrPageFull {
		// splitLeaf takes ownership of the lock and the pin
		return b.splitLeaf(path, leafPage, leafID, key, value)
	}
	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(resource_page.ResourcePageID(leafID), false)
		return err
	}

	// WAL before marking dirty — write-ahead
	if err := appendLogUsingWAL(b.logManager, logmanager.UPDATE, *txnID, resource_page.ResourcePageID(leafID), value, before); err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(resource_page.ResourcePageID(leafID), false)
		return fmt.Errorf("WAL append failed: %w", err)
	}

	leafPage.SetDirty(true)
	leafPage.WUnlock()

	if err := b.bufferManager.UnpinPage(resource_page.ResourcePageID(leafID), true); err != nil {
		return err
	}

	return nil
}

func (b *BPlusTreeBackend) Delete(key []byte) error {
	txnID := b.getActiveTxnID()
	if txnID == nil {
		return fmt.Errorf("no active transaction")
	}

	rootID := b.meta.RootPage()
	path, err := b.findLeafPage(rootID, key)
	if err != nil {
		return err
	}

	leafID := path[len(path)-1]
	leafPageRef, err := b.bufferManager.PinPage(resource_page.ResourcePageID(leafID))
	if err != nil {
		return err
	}
	leafPage := *leafPageRef

	// Acquire write latch on page
	leafPage.WLock()

	leaf := &LeafLogicPage{data: leafPage.GetData()}
	err = appendLogUsingWAL(
		b.logManager,
		logmanager.UPDATE,
		*txnID,
		resource_page.ResourcePageID(leafID),
		nil, // No new value for delete
		leaf.data,
	)

	err = leaf.Remove(key)

	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(resource_page.ResourcePageID(leafID), false)
		return err
	}

	leafPage.SetDirty(true)
	leafPage.WUnlock()

	if err := b.bufferManager.UnpinPage(resource_page.ResourcePageID(leafID), true); err != nil {
		return err
	}
	return nil
}
