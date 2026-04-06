package transaction_manager

type TwoPhaseLockingManager struct {
}

func (m *TwoPhaseLockingManager) BeginTransaction(isolationLevel uint8) int64 {
	return 0
}

func (m *TwoPhaseLockingManager) Read(txnID int64, Key []byte) ([]byte, error) {
	return nil, nil
}

func (m *TwoPhaseLockingManager) Write(txnID int64, Key []byte, Value []byte) error {
	return nil
}

func (m *TwoPhaseLockingManager) Commit(txnID int64) error {
	return nil
}

func (m *TwoPhaseLockingManager) Abort(txnID int64) error {
	return nil
}

func (m *TwoPhaseLockingManager) Close() error {
	return nil
}
