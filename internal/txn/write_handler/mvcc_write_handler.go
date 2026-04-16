package write_handler

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

type MVCCWriteHandler struct {
	storageEngine   storage.IStorageEngine
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	rollbackManager *rollback.RollbackManager
	indexManager    *schema.SecondaryIndexManager
	tableSchema     *schema.TableSchema
}

func NewMVCCWriteHandler(
	storage storage.IStorageEngine,
	bufferMgr buffer.IBufferPoolManager,
	logMgr log.ILogManager,
	rollbackMgr *rollback.RollbackManager,
) *MVCCWriteHandler {
	return &MVCCWriteHandler{
		storageEngine:   storage,
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		rollbackManager: rollbackMgr,
	}
}

func (mh *MVCCWriteHandler) HandleWrite(txn *txn_unit.Transaction, writeOp WriteOperation) error {
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

	if writeOp.IsDelete && mh.indexManager != nil && mh.tableSchema != nil && beforeImage != nil {
		indexValues, err := schema.ExtractIndexValues(mh.tableSchema, beforeImage)
		if err != nil {
			return fmt.Errorf("failed to extract index values before delete: %w", err)
		}

		for indexName, indexValue := range indexValues {
			if err := mh.indexManager.RemoveFromIndex(indexName, indexValue, writeOp.PrimaryKey); err != nil {
				return fmt.Errorf("failed to remove from index %q: %w", indexName, err)
			}

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

	if writeOp.IsDelete {
		if err := mh.storageEngine.Delete(writeOp.Key); err != nil {
			return fmt.Errorf("storage delete failed: %w", err)
		}
		// Record deletion operation for crash recovery
		txn.RecordRecoveryOperation(log.DELETE, writeOp.Key, nil)
		if mh.logManager != nil {
			mh.logManager.AddTransactionOperation(txn.GetID(), log.DELETE, writeOp.Key, nil)
		}
	} else {
		if err := mh.storageEngine.Put(writeOp.Key, writeOp.Value); err != nil {
			return fmt.Errorf("storage put failed: %w", err)
		}
		// Record put operation for crash recovery
		txn.RecordRecoveryOperation(log.PUT, writeOp.Key, writeOp.Value)
		if mh.logManager != nil {
			mh.logManager.AddTransactionOperation(txn.GetID(), log.PUT, writeOp.Key, writeOp.Value)
		}

		if mh.indexManager != nil && mh.tableSchema != nil {
			indexValues, err := schema.ExtractIndexValues(mh.tableSchema, writeOp.Value)
			if err != nil {
				return fmt.Errorf("failed to extract index values: %w", err)
			}

			for indexName, indexValue := range indexValues {
				if err := mh.indexManager.AddToIndex(indexName, indexValue, writeOp.PrimaryKey); err != nil {
					return fmt.Errorf("failed to add to index %q: %w", indexName, err)
				}

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

func (mh *MVCCWriteHandler) SetIndexContext(tableSchema *schema.TableSchema, indexMgr *schema.SecondaryIndexManager) error {
	mh.tableSchema = tableSchema
	mh.indexManager = indexMgr
	return nil
}
