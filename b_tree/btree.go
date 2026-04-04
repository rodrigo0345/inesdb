package btree

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/transaction_manager"
	"github.com/rodrigo0345/omag/wal"
)

var (
	ErrInvalidPageType = errors.New("invalid page type encountered")
	ErrSplitNotImpl    = errors.New("page split logic not implemented")
)

type BTree struct {
	bufferPool  *buffermanager.BufferPoolManager
	lockManager *transaction_manager.LockManager
	walMgr      *wal.WALManager
	meta        *MetaPage
}

func NewBTree(
	bufferPool *buffermanager.BufferPoolManager,
	lockManager *transaction_manager.LockManager,
	walMgr *wal.WALManager,
	pageSize uint32,
) (*BTree, error) {
	tree := &BTree{
		bufferPool:  bufferPool,
		lockManager: lockManager,
		walMgr:      walMgr,
	}

	// Try to fetch the meta page
	metaPage, err := bufferPool.FetchPage(0)
	if err != nil {
		return nil, err
	}

	// Check if this is a new database by looking at the magic number
	if len(metaPage.GetData()) > 0 && binary.LittleEndian.Uint32(metaPage.GetData()[MetaMagicOffset:MetaMagicOffset+4]) == MagicNumber {
		// Existing database
		tree.meta = &MetaPage{data: metaPage.GetData()}
		return tree, nil
	}

	// New database - initialize meta and root pages
	meta := NewMetaPageWithSize(pageSize)
	root := NewLeafPage(pageSize)

	// Write meta page (page 0)
	copy(metaPage.GetData(), meta.data)
	bufferPool.UnpinPage(0, true)

	// Allocate and write root page (page 1)
	rootPage, err := bufferPool.NewPage()
	if err != nil {
		return nil, err
	}
	copy(rootPage.GetData(), root.data)
	bufferPool.UnpinPage(rootPage.GetID(), true)

	tree.meta = meta
	return tree, nil
}

func (tree *BTree) Find(txn *transaction_manager.Transaction, key []byte) ([]byte, error) {

	// shared locks are used for reads
	if err := tree.lockManager.LockShared(txn, key); err != nil {
		return nil, err
	}

	rootID := tree.meta.RootPage()
	path, err := tree.findLeafPage(rootID, key)
	if err != nil {
		return nil, err
	}

	leafID := path[len(path)-1]
	leafPage, err := tree.bufferPool.FetchPage(buffermanager.PageID(leafID))
	if err != nil {
		return nil, err
	}

	// Acquire read latch on page
	leafPage.RLock()
	defer func() {
		leafPage.RUnlock()
		tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), false)
	}()

	leaf := &LeafPage{data: leafPage.GetData()}
	return leaf.Get(key)
}

// Get is an alias for Find
func (tree *BTree) Get(txn *transaction_manager.Transaction, key []byte) ([]byte, error) {
	return tree.Find(txn, key)
}

// Delete removes a key-value pair within a transaction context
func (tree *BTree) Delete(txn *transaction_manager.Transaction, key []byte) error {
	// Acquire exclusive lock on the key
	if err := tree.lockManager.LockExclusive(txn, key); err != nil {
		return err
	}

	rootID := tree.meta.RootPage()
	path, err := tree.findLeafPage(rootID, key)
	if err != nil {
		return err
	}

	leafID := path[len(path)-1]
	leafPage, err := tree.bufferPool.FetchPage(buffermanager.PageID(leafID))
	if err != nil {
		return err
	}

	// Acquire write latch on page
	leafPage.WLock()

	leaf := &LeafPage{data: leafPage.GetData()}
	err = leaf.Remove(key)

	if err != nil {
		leafPage.WUnlock()
		tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), false)
		return err
	}

	// Append DELETE to WAL before writing
	walRec := wal.WALRecord{
		TxnID:  txn.GetID(),
		Type:   wal.UPDATE,
		PageID: wal.PageID(leafID),
	}
	tree.walMgr.AppendLog(walRec)

	leafPage.SetDirty(true)
	leafPage.WUnlock()

	if err := tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), true); err != nil {
		return err
	}

	if err := tree.lockManager.Unlock(txn, key); err != nil {
		return err
	}

	return nil
}

// Put is an alias for Insert
func (tree *BTree) Put(txn *transaction_manager.Transaction, key []byte, value []byte) error {
	return tree.Insert(txn, key, value)
}

// Insert adds or updates a key-value pair within a transaction context
func (tree *BTree) Insert(txn *transaction_manager.Transaction, key []byte, value []byte) error {
	// Acquire exclusive lock on the key
	if err := tree.lockManager.LockExclusive(txn, key); err != nil {
		return err
	}

	rootID := tree.meta.RootPage()
	path, err := tree.findLeafPage(rootID, key)
	if err != nil {
		return err
	}

	leafID := path[len(path)-1]
	leafPage, err := tree.bufferPool.FetchPage(buffermanager.PageID(leafID))
	if err != nil {
		return err
	}

	leafPage.WLock()

	leaf := &LeafPage{data: leafPage.GetData()}
	err = leaf.Insert(key, value)

	if err == ErrPageFull {
		// Don't unlock here - splitLeaf expects the lock to be held
		return tree.splitLeaf(txn, path, leafPage, leafID, key, value)
	}

	if err != nil {
		leafPage.WUnlock()
		tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), false)
		return err
	}

	// Append INSERT to WAL before writing
	walRec := wal.WALRecord{
		TxnID:  txn.GetID(),
		Type:   wal.UPDATE,
		PageID: wal.PageID(leafID),
		After:  value,
	}
	tree.walMgr.AppendLog(walRec)

	leafPage.SetDirty(true)
	leafPage.WUnlock()

	if err := tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), true); err != nil {
		return err
	}

	if err := tree.lockManager.Unlock(txn, key); err != nil {
		return err
	}

	return nil
}

// RangeScan performs a range query within a transaction context
func (tree *BTree) RangeScan(txn *transaction_manager.Transaction, lo, hi []byte) ([]KVPair, error) {
	// Acquire shared locks on the range
	if err := tree.lockManager.LockShared(txn, lo); err != nil {
		return nil, err
	}
	if err := tree.lockManager.LockShared(txn, hi); err != nil {
		return nil, err
	}

	var results []KVPair

	// Find the starting leaf page
	path, err := tree.findLeafPage(tree.meta.RootPage(), lo)
	if err != nil {
		return nil, err
	}

	leafID := path[len(path)-1]

	// Scan through leaf pages until we exceed hi
	for leafID != 0 {
		leafPage, err := tree.bufferPool.FetchPage(buffermanager.PageID(leafID))
		if err != nil {
			return nil, err
		}

		leafPage.RLock()
		leaf := &LeafPage{data: leafPage.GetData()}

		// Scan cells in this leaf
		for i := uint16(0); i < leaf.CellCount(); i++ {
			cell := leaf.GetCell(leaf.GetCellOffset(i))
			if bytes.Compare(cell.Key, lo) >= 0 && bytes.Compare(cell.Key, hi) <= 0 {
				results = append(results, KVPair{Key: cell.Key, Value: cell.Value})
			} else if bytes.Compare(cell.Key, hi) > 0 {
				leafPage.RUnlock()
				tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), false)
				return results, nil
			}
		}

		// Move to next leaf
		nextLeafID := leaf.RightSibling()
		leafPage.RUnlock()
		tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), false)

		leafID = nextLeafID
	}

	return results, nil
}

type KVPair struct {
	Key   []byte
	Value []byte
}

func (tree *BTree) splitLeaf(txn *transaction_manager.Transaction, path []uint64, leafPage *buffermanager.Page, leafID uint64, key, value []byte) error {
	leaf := &LeafPage{data: leafPage.GetData()}

	newPage, err := tree.bufferPool.NewPage()
	if err != nil {
		leafPage.WUnlock()
		ret := tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), false)
		if retErr := ret; retErr != nil {
			return retErr
		}
		return err
	}
	newPageID := newPage.GetID()
	newPageData := NewLeafPage(uint32(len(newPage.GetData())))

	promotedKey := leaf.Split(newPageData, uint64(newPageID))

	// Insert the new key either in the old or new page
	if bytes.Compare(key, promotedKey) < 0 {
		if err := leaf.Insert(key, value); err != nil {
			leafPage.WUnlock()
			tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), false)
			tree.bufferPool.UnpinPage(newPageID, false)
			return err
		}
	} else {
		if err := newPageData.Insert(key, value); err != nil {
			leafPage.WUnlock()
			tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), false)
			tree.bufferPool.UnpinPage(newPageID, false)
			return err
		}
	}

	// Log to WAL
	walRec := wal.WALRecord{
		TxnID:  txn.GetID(),
		Type:   wal.UPDATE,
		PageID: wal.PageID(leafID),
	}
	tree.walMgr.AppendLog(walRec)

	// Write both pages
	copy(leafPage.GetData(), leaf.data)
	leafPage.SetDirty(true)
	leafPage.WUnlock()
	tree.bufferPool.UnpinPage(buffermanager.PageID(leafID), true)

	copy(newPage.GetData(), newPageData.data)
	newPage.SetDirty(true)
	tree.bufferPool.UnpinPage(newPageID, true)

	return tree.promoteKey(txn, path[:len(path)-1], promotedKey, uint64(newPageID))
}

func (tree *BTree) promoteKey(txn *transaction_manager.Transaction, path []uint64, key []byte, childID uint64) error {
	if len(path) == 0 {
		return tree.createNewRoot(txn, tree.meta.RootPage(), key, childID)
	}

	parentID := path[len(path)-1]
	parentPage, err := tree.bufferPool.FetchPage(buffermanager.PageID(parentID))
	if err != nil {
		return err
	}

	parentPage.WLock()

	parent := &InternalPage{data: parentPage.GetData()}
	err = parent.Insert(key, childID)

	if err == ErrPageFull {
		// Don't unlock here - splitInternal expects the lock to be held
		return tree.splitInternal(txn, path, parentPage, parentID, key, childID)
	}

	if err != nil {
		parentPage.WUnlock()
		tree.bufferPool.UnpinPage(buffermanager.PageID(parentID), false)
		return err
	}

	// Log to WAL
	walRec := wal.WALRecord{
		TxnID:  txn.GetID(),
		Type:   wal.UPDATE,
		PageID: wal.PageID(parentID),
	}
	tree.walMgr.AppendLog(walRec)

	parentPage.SetDirty(true)
	parentPage.WUnlock()

	return tree.bufferPool.UnpinPage(buffermanager.PageID(parentID), true)
}

func (tree *BTree) splitInternal(txn *transaction_manager.Transaction, path []uint64, parentPage *buffermanager.Page, parentID uint64, key []byte, childID uint64) error {
	parent := &InternalPage{data: parentPage.GetData()}

	newPage, err := tree.bufferPool.NewPage()
	if err != nil {
		parentPage.WUnlock()
		ret := tree.bufferPool.UnpinPage(buffermanager.PageID(parentID), false)
		if retErr := ret; retErr != nil {
			return retErr
		}
		return err
	}
	newPageID := newPage.GetID()
	newPageData := NewInternalPage(uint32(len(newPage.GetData())))

	promotedKey := parent.Split(newPageData)

	// Insert the correct side
	if bytes.Compare(key, promotedKey) < 0 {
		if err := parent.Insert(key, childID); err != nil {
			parentPage.WUnlock()
			tree.bufferPool.UnpinPage(buffermanager.PageID(parentID), false)
			tree.bufferPool.UnpinPage(newPageID, false)
			return err
		}
	} else {
		if err := newPageData.Insert(key, childID); err != nil {
			parentPage.WUnlock()
			tree.bufferPool.UnpinPage(buffermanager.PageID(parentID), false)
			tree.bufferPool.UnpinPage(newPageID, false)
			return err
		}
	}

	// Log to WAL
	walRec := wal.WALRecord{
		TxnID:  txn.GetID(),
		Type:   wal.UPDATE,
		PageID: wal.PageID(parentID),
	}
	tree.walMgr.AppendLog(walRec)

	// Write both pages
	copy(parentPage.GetData(), parent.data)
	parentPage.SetDirty(true)
	parentPage.WUnlock()
	tree.bufferPool.UnpinPage(buffermanager.PageID(parentID), true)

	copy(newPage.GetData(), newPageData.data)
	newPage.SetDirty(true)
	tree.bufferPool.UnpinPage(newPageID, true)

	return tree.promoteKey(txn, path[:len(path)-1], promotedKey, uint64(newPageID))
}

func (tree *BTree) createNewRoot(txn *transaction_manager.Transaction, oldRootID uint64, key []byte, rightChildID uint64) error {
	newRootPage, err := tree.bufferPool.NewPage()
	if err != nil {
		return err
	}
	newRootID := newRootPage.GetID()

	newRoot := NewInternalPage(uint32(len(newRootPage.GetData())))
	newRoot.SetRightmostPointer(rightChildID)

	if err := newRoot.Insert(key, oldRootID); err != nil {
		return err
	}

	// Log to WAL
	walRec := wal.WALRecord{
		TxnID:  txn.GetID(),
		Type:   wal.UPDATE,
		PageID: wal.PageID(newRootID),
	}
	tree.walMgr.AppendLog(walRec)

	copy(newRootPage.GetData(), newRoot.data)
	tree.bufferPool.UnpinPage(newRootID, true)

	tree.meta.SetRootPage(uint64(newRootID))

	// Write meta page
	metaPage, err := tree.bufferPool.FetchPage(0)
	if err != nil {
		return err
	}
	metaPage.WLock()
	copy(metaPage.GetData(), tree.meta.data)
	metaPage.SetDirty(true)
	metaPage.WUnlock()

	return tree.bufferPool.UnpinPage(0, true)
}

func (tree *BTree) findLeafPage(pageID uint64, key []byte) ([]uint64, error) {
	var path []uint64

	for {
		path = append(path, pageID) // breadcrumb

		pageObj, err := tree.bufferPool.FetchPage(buffermanager.PageID(pageID))
		if err != nil {
			return nil, err
		}

		pageObj.RLock()
		pageData := pageObj.GetData()
		pageType := PageType(binary.LittleEndian.Uint16(pageData[0:2]))
		pageObj.RUnlock()

		tree.bufferPool.UnpinPage(buffermanager.PageID(pageID), false)

		switch pageType {
		case TypeLeaf:
			return path, nil
		case TypeInternal:
			pageObj2, err := tree.bufferPool.FetchPage(buffermanager.PageID(pageID))
			if err != nil {
				return nil, err
			}
			pageObj2.RLock()
			internal := &InternalPage{data: pageObj2.GetData()}
			pageID = internal.Search(key)
			pageObj2.RUnlock()
			tree.bufferPool.UnpinPage(buffermanager.PageID(pageID), false)
		default:
			return nil, ErrInvalidPageType
		}
	}
}
