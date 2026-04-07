package transaction_manager

import "github.com/rodrigo0345/omag/logmanager"

type TransactionID uint64

type TwoPhaseLockingManager struct {
	transactions map[TransactionID]*Transaction
	logManager logmanager.ILogManager
	// TODO: missing indexes parameters
}

func NewTwoPhaseLockingManager(logManager logmanager.ILogManager) *TwoPhaseLockingManager {
	return &TwoPhaseLockingManager{
		transactions: make(map[TransactionID]*Transaction),
		logManager: logManager,
	}
}

func (m *TwoPhaseLockingManager) BeginTransaction(isolationLevel uint8) int64 {
	// uuid7 generate key
	txn := &Transaction{
		txnID:             0, // This should be replaced with a proper ID generation mechanism
		isolationLevel: isolationLevel,
	}
	m.transactions[TransactionID(txn.txnID)] = txn
	return int64(txn.txnID)
}

func (m *TwoPhaseLockingManager) Read(txnID int64, Key []byte) ([]byte, error) {
	return nil, nil
}

func (m *TwoPhaseLockingManager) Write(txnID int64, Key []byte, Value []byte) error {
	return nil
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
	if tm.bufferPool != nil {
		if bpm, ok := tm.bufferPool.(interface{ FlushAll() error }); ok {
			bpm.FlushAll()
		}
	}

	// Sync disk manager
	// (This is done via flush above)

	return nil
}

func (m *TwoPhaseLockingManager) Abort(txnID int64) error {
	return nil
}

func (m *TwoPhaseLockingManager) Close() error {
	return nil
}
