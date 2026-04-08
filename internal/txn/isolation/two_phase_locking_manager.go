package transaction_manager

import (
	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/logmanager"
	storageengine "github.com/rodrigo0345/omag/storage_engine"
)

type TransactionID uint64

type TwoPhaseLockingManager struct {
	transactions  map[TransactionID]*Transaction
	logManager    logmanager.ILogManager
	bufferManager buffermanager.IBufferPoolManager
	primaryIndex  storageengine.IStorageEngine // TODO: missing secondary indexes
}

func NewTwoPhaseLockingManager(logManager logmanager.ILogManager, primaryIndex storageengine.IStorageEngine) *TwoPhaseLockingManager {
	return &TwoPhaseLockingManager{
		transactions: make(map[TransactionID]*Transaction),
		logManager:   logManager,
		primaryIndex: primaryIndex,
	}
}

func (m *TwoPhaseLockingManager) BeginTransaction(isolationLevel uint8) int64 {
	// uuid7 generate key
	txn := &Transaction{
		txnID:          0, // This should be replaced with a proper ID generation mechanism
		isolationLevel: isolationLevel,
	}
	m.transactions[TransactionID(txn.txnID)] = txn
	return int64(txn.txnID)
}

func (m *TwoPhaseLockingManager) Read(txnID int64, Key []byte) ([]byte, error) {
	return m.primaryIndex.Get(Key)
}

func (m *TwoPhaseLockingManager) Write(txnID int64, Key []byte, Value []byte) error {
	return m.primaryIndex.Put(Key, Value)
}

func (m *TwoPhaseLockingManager) Commit(txnID int64) error {
	txn := m.transactions[TransactionID(txnID)]
	txn.state = COMMITTED

	if m.logManager != nil {
		rec := logmanager.WALRecord{
			TxnID: txn.GetID(),
			Type:  logmanager.COMMIT,
		}
		lsn, err := m.logManager.AppendLogRecord(rec)
		if err != nil {
			return err
		}

		// Force flush on commit to ensure durability
		m.logManager.Flush(lsn)
	}

	// Flush all dirty pages from buffer pool to disk
	if m.bufferManager != nil {
		if bpm, ok := m.bufferManager.(interface{ FlushAll() error }); ok {
			bpm.FlushAll()
		}
	}
	return nil
}

func (m *TwoPhaseLockingManager) Abort(txnID int64) error {
	txn := m.transactions[TransactionID(txnID)]
	txn.state = ABORTED
}

func (m *TwoPhaseLockingManager) Close() error {
	return nil
}
