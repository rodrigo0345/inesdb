package transaction_manager

type OptimisticConcurrencyControlManager struct {
}

func (m *OptimisticConcurrencyControlManager) BeginTransaction(isolationLevel uint8) int64 {
	return 0
}
func (m *OptimisticConcurrencyControlManager) Read(txnID int64, Key []byte) ([]byte, error) {
	return nil, nil
}
func (m *OptimisticConcurrencyControlManager) Write(txnID int64, Key []byte, Value []byte) error {
	return nil
}
func (m *OptimisticConcurrencyControlManager) Commit(txnID int64) error {
	return nil
}
func (m *OptimisticConcurrencyControlManager) Abort(txnID int64) error {
	return nil
}
func (m *OptimisticConcurrencyControlManager) Close() error {
	return nil
}
