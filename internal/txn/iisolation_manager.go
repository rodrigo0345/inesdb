package txn

// IIsolationManager defines the interface for different isolation level implementations
type IIsolationManager interface {
	BeginTransaction(isolationLevel uint8) int64
	Read(txnID int64, Key []byte) ([]byte, error)
	Write(txnID int64, Key []byte, Value []byte) error
	Commit(txnID int64) error
	Abort(txnID int64) error
	Close() error
}
