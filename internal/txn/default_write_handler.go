package txn

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type DefaultWriteHandler struct {
	storageEngine   storage.IStorageEngine
	rollbackManager *RollbackManager
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	indexManager    *schema.SecondaryIndexManager
	tableSchema     *schema.TableSchema
}

func NewDefaultWriteHandler(
	storage storage.IStorageEngine,
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

func (dh *DefaultWriteHandler) HandleWrite(txn *Transaction, writeOp WriteOperation) error {
	var beforeImage []byte
	var err error

	if !writeOp.IsDelete {
		beforeImage, err = dh.storageEngine.Get(writeOp.Key)
		if err != nil {
			beforeImage = nil
		}
	} else {
		beforeImage, err = dh.storageEngine.Get(writeOp.Key)
		if err != nil {
			return fmt.Errorf("failed to get value before delete: %w", err)
		}
	}

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

	if writeOp.IsDelete && dh.indexManager != nil && dh.tableSchema != nil && beforeImage != nil {
		indexValues, err := ExtractIndexValues(dh.tableSchema, beforeImage)
		if err != nil {
			return fmt.Errorf("failed to extract index values before delete: %w", err)
		}

		for indexName, indexValue := range indexValues {
			if err := dh.indexManager.RemoveFromIndex(indexName, indexValue, writeOp.PrimaryKey); err != nil {
				return fmt.Errorf("failed to remove from index %q: %w", indexName, err)
			}

			capturedIndexName := indexName
			capturedIndexValue := indexValue
			capturedPrimaryKey := writeOp.PrimaryKey
			dh.rollbackManager.RegisterIndexCleanup(txn, func() error {
				return dh.indexManager.AddToIndex(capturedIndexName, capturedIndexValue, capturedPrimaryKey)
			})
		}
	}

	if writeOp.IsDelete {
		if err := dh.storageEngine.Delete(writeOp.Key); err != nil {
			return fmt.Errorf("storage delete failed: %w", err)
		}
	} else {
		if err := dh.storageEngine.Put(writeOp.Key, writeOp.Value); err != nil {
			return fmt.Errorf("storage put failed: %w", err)
		}

		if dh.indexManager != nil && dh.tableSchema != nil {
			indexValues, err := ExtractIndexValues(dh.tableSchema, writeOp.Value)
			if err != nil {
				return fmt.Errorf("failed to extract index values: %w", err)
			}

			for indexName, indexValue := range indexValues {
				if err := dh.indexManager.AddToIndex(indexName, indexValue, writeOp.PrimaryKey); err != nil {
					return fmt.Errorf("failed to add to index %q: %w", indexName, err)
				}

				capturedIndexName := indexName
				capturedIndexValue := indexValue
				capturedPrimaryKey := writeOp.PrimaryKey
				dh.rollbackManager.RegisterIndexCleanup(txn, func() error {
					return dh.indexManager.RemoveFromIndex(capturedIndexName, capturedIndexValue, capturedPrimaryKey)
				})
			}
		}
	}

	if _, err := dh.rollbackManager.RecordPageWrite(
		txn,
		writeOp.PageID,
		writeOp.Offset,
		beforeImage,
	); err != nil {
		return fmt.Errorf("failed to record undo operation: %w", err)
	}

	return nil
}

func (dh *DefaultWriteHandler) GetStorageEngine() storage.IStorageEngine {
	return dh.storageEngine
}

func (dh *DefaultWriteHandler) SetIndexContext(tableSchema *schema.TableSchema, indexMgr *schema.SecondaryIndexManager) error {
	dh.tableSchema = tableSchema
	dh.indexManager = indexMgr
	return nil
}
