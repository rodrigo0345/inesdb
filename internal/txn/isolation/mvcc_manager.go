package transaction_manager

type MVCCManager struct {
}

func (m *MVCCManager) BeginTransaction(isolationLevel uint8) int64 {
	return 0
}
func (m *MVCCManager) Read(txnID int64, Key []byte) ([]byte, error) {
	return nil, nil
}
func (m *MVCCManager) Write(txnID int64, Key []byte, Value []byte) error {
	return nil
}
func (m *MVCCManager) Commit(txnID int64) error {
	return nil
}
func (m *MVCCManager) Abort(txnID int64) error {
	return nil
}
func (m *MVCCManager) Close() error {
	return nil
}
