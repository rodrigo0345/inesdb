package txn

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type MVCCWriteHandler struct {
	storageEngine   storage.IStorageEngine
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	rollbackManager *RollbackManager // For registering cleanup callbacks
	indexManager    *schema.SecondaryIndexManager
	tableSchema     *schema.TableSchema
}

func NewMVCCWriteHandler(
	storage storage.IStorageEngine,
	bufferMgr buffer.IBufferPoolManager,
	logMgr log.ILogManager,
	rollbackMgr *RollbackManager,
) *MVCCWriteHandler {
	return &MVCCWriteHandler{
		storageEngine:   storage,
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		rollbackManager: rollbackMgr,
	}
}

func (mh *MVCCWriteHandler) HandleWrite(txn *Transaction, writeOp WriteOperation) error {
	beforeImage, _ := mh.storageEngine.Get(writeOp.Key)

	if mh.logManager != nil {
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

	// Handle index maintenance for DELETE
	if writeOp.IsDelete && mh.indexManager != nil && mh.tableSchema != nil && beforeImage != nil {
		// Extract indexed column values from before-image
		indexValues, err := ExtractIndexValues(mh.tableSchema, beforeImage)
		if err != nil {
			return fmt.Errorf("failed to extract index values before delete: %w", err)
		}

		// Remove from all indexes
		for indexName, indexValue := range indexValues {
			if err := mh.indexManager.RemoveFromIndex(indexName, indexValue, writeOp.PrimaryKey); err != nil {
				return fmt.Errorf("failed to remove from index %q: %w", indexName, err)
			}

			// Register cleanup to restore index entry if transaction rolls back
			// Capture values for closure
			capturedIndexName := indexName
			capturedIndexValue := indexValue
			capturedPrimaryKey := writeOp.PrimaryKey
			if mh.rollbackManager != nil {
				mh.rollbackManager.RegisterIndexCleanup(txn, func() error {
					return mh.indexManager.AddToIndex(capturedIndexName, capturedIndexValue, capturedPrimaryKey)
				})
			}
		}
	}

	// Perform storage operation
	if writeOp.IsDelete {
		if err := mh.storageEngine.Delete(writeOp.Key); err != nil {
			return fmt.Errorf("storage delete failed: %w", err)
		}
	} else {
		if err := mh.storageEngine.Put(writeOp.Key, writeOp.Value); err != nil {
			return fmt.Errorf("storage put failed: %w", err)
		}

		// Handle index maintenance for INSERT/UPDATE
		if mh.indexManager != nil && mh.tableSchema != nil {
			// Extract indexed column values from new value
			indexValues, err := ExtractIndexValues(mh.tableSchema, writeOp.Value)
			if err != nil {
				return fmt.Errorf("failed to extract index values: %w", err)
			}

			// Add to all indexes
			for indexName, indexValue := range indexValues {
				if err := mh.indexManager.AddToIndex(indexName, indexValue, writeOp.PrimaryKey); err != nil {
					return fmt.Errorf("failed to add to index %q: %w", indexName, err)
				}

				// Register cleanup to remove index entry if transaction rolls back
				// Capture values for closure
				capturedIndexName := indexName
				capturedIndexValue := indexValue
				capturedPrimaryKey := writeOp.PrimaryKey
				if mh.rollbackManager != nil {
					mh.rollbackManager.RegisterIndexCleanup(txn, func() error {
						return mh.indexManager.RemoveFromIndex(capturedIndexName, capturedIndexValue, capturedPrimaryKey)
					})
				}
			}
		}
	}

	return nil
}

// SetIndexContext sets the index manager and schema for automatic index maintenance
func (mh *MVCCWriteHandler) SetIndexContext(tableSchema *schema.TableSchema, indexMgr *schema.SecondaryIndexManager) error {
	mh.tableSchema = tableSchema
	mh.indexManager = indexMgr
	return nil
}
