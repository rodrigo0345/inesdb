package txn

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/page"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

// WriteOperation represents a write request to the storage engine
// Contains all data needed for coordinated logging, execution, and undo
type WriteOperation struct {
	Key      []byte
	Value    []byte
	PageID   page.ResourcePageID
	Offset   uint16
	IsDelete bool
}

// WriteHandler coordinates write operations across multiple concerns:
// 1. Optional WAL logging (for durability in WAL-based systems)
// 2. Storage engine execution (pure data structure operations)
// 3. Undo log recording (for abort operations)
type WriteHandler interface {
	// HandleWrite executes a coordinated write operation
	// Manages WAL, storage, and undo as a unit
	HandleWrite(txn *Transaction, writeOp WriteOperation) error
}

// DefaultWriteHandler implements WriteHandler with WAL-based coordination
// Coordinates: WAL (optional) → Storage → UndoLog
type DefaultWriteHandler struct {
	storageEngine   StorageEngine
	rollbackManager *RollbackManager
	logManager      log.ILogManager // nil for non-WAL systems
	bufferManager   buffer.IBufferPoolManager
}

// StorageEngine is the interface for pure storage operations (no logging/transactions)
type StorageEngine interface {
	// Put stores a key-value pair
	Put(key []byte, value []byte) error

	// Get retrieves a value by key
	Get(key []byte) ([]byte, error)

	// Delete removes a key
	Delete(key []byte) error
}

// NewDefaultWriteHandler creates a write handler with optional WAL support
// logManager can be nil for systems that don't use WAL (e.g., MVCC)
func NewDefaultWriteHandler(
	storage StorageEngine,
	rollbackMgr *RollbackManager,
	bufferMgr buffer.IBufferPoolManager,
	logMgr log.ILogManager,
) *DefaultWriteHandler {
	return &DefaultWriteHandler{
		storageEngine:   storage,
		rollbackManager: rollbackMgr,
		bufferManager:   bufferMgr,
		logManager:      logMgr,
	}
}

// HandleWrite executes a coordinated write operation
// Flow:
//  1. Capture before-image
//  2. (Optional) Write BEFORE to WAL
//  3. Execute storage operation
//  4. Record operation to UndoLog
//  5. (Optional) Write AFTER to WAL
func (dh *DefaultWriteHandler) HandleWrite(txn *Transaction, writeOp WriteOperation) error {
	// Step 1: Capture before-image (for both WAL and UndoLog)
	var beforeImage []byte
	var err error

	if !writeOp.IsDelete {
		beforeImage, err = dh.storageEngine.Get(writeOp.Key)
		if err != nil {
			// Key might not exist yet (first write) - that's OK
			beforeImage = nil
		}
	} else {
		// For delete, capture current value
		beforeImage, err = dh.storageEngine.Get(writeOp.Key)
		if err != nil {
			return fmt.Errorf("failed to get value before delete: %w", err)
		}
	}

	// Step 2: (Optional) Write to WAL BEFORE execution
	// This ensures durability: crash can recover from the WAL record
	if dh.logManager != nil {
		walRecord := &log.WALRecord{
			TxnID:  txn.GetID(),
			Type:   log.UPDATE,
			PageID: writeOp.PageID,
			Before: beforeImage,
			After:  writeOp.Value,
		}
		if _, err := dh.logManager.AppendLogRecord(walRecord); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	// Step 3: Execute storage operation (PURE - no logging inside)
	if writeOp.IsDelete {
		if err := dh.storageEngine.Delete(writeOp.Key); err != nil {
			return fmt.Errorf("storage delete failed: %w", err)
		}
	} else {
		if err := dh.storageEngine.Put(writeOp.Key, writeOp.Value); err != nil {
			return fmt.Errorf("storage put failed: %w", err)
		}
	}

	// Step 4: Record operation to UndoLog (for abort rollback)
	if _, err := dh.rollbackManager.RecordPageWrite(
		txn,
		writeOp.PageID,
		writeOp.Offset,
		beforeImage,
	); err != nil {
		return fmt.Errorf("failed to record undo operation: %w", err)
	}

	// Step 5: (Optional) Write AFTER record to WAL
	// This is idempotent: redo can safely replay the operation
	if dh.logManager != nil {
		// Some systems use a separate AFTER record for redo
		// Others rely on the single record's After field
		// Implementation choice based on recovery strategy
	}

	return nil
}

// GetStorageEngine returns the underlying storage engine
// Useful for testing and direct storage access when needed
func (dh *DefaultWriteHandler) GetStorageEngine() StorageEngine {
	return dh.storageEngine
}

// MVCCWriteHandler implements WriteHandler for MVCC systems
// MVCC doesn't need immediate undo (snapshot isolation)
// but may still want to log for recovery
type MVCCWriteHandler struct {
	storageEngine StorageEngine
	logManager    log.ILogManager // nil if no WAL needed
	bufferManager buffer.IBufferPoolManager
}

// NewMVCCWriteHandler creates a write handler for MVCC systems
// MVCC systems typically don't use the undo log during normal operation
func NewMVCCWriteHandler(
	storage StorageEngine,
	bufferMgr buffer.IBufferPoolManager,
	logMgr log.ILogManager,
) *MVCCWriteHandler {
	return &MVCCWriteHandler{
		storageEngine: storage,
		logManager:    logMgr,
		bufferManager: bufferMgr,
	}
}

// HandleWrite in MVCC: just execute and optionally log
// MVCC handles rollback via version visibility, not undo
func (mh *MVCCWriteHandler) HandleWrite(txn *Transaction, writeOp WriteOperation) error {
	// MVCC: No undo log needed during operation
	// Versions are managed by the version store
	// Rollback happens by making versions invisible

	// Still want to log for recovery/durability if configured
	if mh.logManager != nil {
		beforeImage, _ := mh.storageEngine.Get(writeOp.Key)
		walRecord := log.WALRecord{
			TxnID:  txn.GetID(),
			Type:   log.UPDATE,
			PageID: writeOp.PageID,
			Before: beforeImage,
			After:  writeOp.Value,
		}
		if _, err := mh.logManager.AppendLogRecord(walRecord); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	// Execute storage operation
	if writeOp.IsDelete {
		if err := mh.storageEngine.Delete(writeOp.Key); err != nil {
			return fmt.Errorf("storage delete failed: %w", err)
		}
	} else {
		if err := mh.storageEngine.Put(writeOp.Key, writeOp.Value); err != nil {
			return fmt.Errorf("storage put failed: %w", err)
		}
	}

	return nil
}
