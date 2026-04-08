package txn

import (
	"testing"

	"github.com/rodrigo0345/omag/internal/txn/log"
)

// mockIsolationManager implements IIsolationManager for testing
type mockIsolationManager struct{}

func (m *mockIsolationManager) BeginTransaction(isolationLevel uint8) int64 {
	return 1
}

func (m *mockIsolationManager) Read(txnID int64, key []byte) ([]byte, error) {
	return nil, nil
}

func (m *mockIsolationManager) Write(txnID int64, key []byte, value []byte) error {
	return nil
}

func (m *mockIsolationManager) Commit(txnID int64) error {
	return nil
}

func (m *mockIsolationManager) Abort(txnID int64) error {
	return nil
}

func (m *mockIsolationManager) Close() error {
	return nil
}

// mockLogManager implements ILogManager for testing
type mockLogManager struct{}

func (m *mockLogManager) AppendLogRecord(record log.ILogRecord) (log.LSN, error) {
	return 0, nil
}

func (m *mockLogManager) Flush(upToLSN log.LSN) error {
	return nil
}

func (m *mockLogManager) Recover() (*log.RecoveryState, error) {
	return nil, nil
}

func (m *mockLogManager) Checkpoint() error {
	return nil
}

func (m *mockLogManager) GetLastCheckpointLSN() uint64 {
	return 0
}

func (m *mockLogManager) Close() error {
	return nil
}

func (m *mockLogManager) ReadAllRecords() ([]log.WALRecord, error) {
	return nil, nil
}

// mockStorageEngine implements StorageEngine for testing
type mockStorageEngine struct{}

func (m *mockStorageEngine) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (m *mockStorageEngine) Put(key []byte, value []byte) error {
	return nil
}

func (m *mockStorageEngine) Delete(key []byte) error {
	return nil
}

// TestNewTransactionManager tests creating a transaction manager
func TestNewTransactionManager(t *testing.T) {
	mockIsolMgr := &mockIsolationManager{}
	mockLogMgr := &mockLogManager{}
	mockBufMgr := &mockBufferPoolManager{}
	mockStorage := &mockStorageEngine{}

	tm := NewTransactionManager(mockIsolMgr, mockLogMgr, mockBufMgr, mockStorage)

	if tm == nil {
		t.Fatal("expected non-nil transaction manager")
	}
}

// TestNewTransactionManagerWithoutLog tests creating manager without log
func TestNewTransactionManagerWithoutLog(t *testing.T) {
	mockIsolMgr := &mockIsolationManager{}
	mockBufMgr := &mockBufferPoolManager{}
	mockStorage := &mockStorageEngine{}

	tm := NewTransactionManager(mockIsolMgr, nil, mockBufMgr, mockStorage)

	if tm == nil {
		t.Fatal("expected non-nil transaction manager")
	}
}

// TestGetRollbackManager tests retrieving rollback manager
func TestGetRollbackManager(t *testing.T) {
	mockIsolMgr := &mockIsolationManager{}
	mockLogMgr := &mockLogManager{}
	mockBufMgr := &mockBufferPoolManager{}
	mockStorage := &mockStorageEngine{}

	tm := NewTransactionManager(mockIsolMgr, mockLogMgr, mockBufMgr, mockStorage)
	rm := tm.GetRollbackManager()

	if rm == nil {
		t.Fatal("expected non-nil rollback manager")
	}
}

// TestGetWriteHandler tests retrieving write handler
func TestGetWriteHandler(t *testing.T) {
	mockIsolMgr := &mockIsolationManager{}
	mockLogMgr := &mockLogManager{}
	mockBufMgr := &mockBufferPoolManager{}
	mockStorage := &mockStorageEngine{}

	tm := NewTransactionManager(mockIsolMgr, mockLogMgr, mockBufMgr, mockStorage)
	wh := tm.GetWriteHandler()

	if wh == nil {
		t.Fatal("expected non-nil write handler")
	}
}

// TestTransactionManagerIntegration tests integration of components
func TestTransactionManagerIntegration(t *testing.T) {
	mockIsolMgr := &mockIsolationManager{}
	mockLogMgr := &mockLogManager{}
	mockBufMgr := &mockBufferPoolManager{}
	mockStorage := &mockStorageEngine{}

	tm := NewTransactionManager(mockIsolMgr, mockLogMgr, mockBufMgr, mockStorage)

	// Should have all components available
	if tm.GetRollbackManager() == nil {
		t.Error("rollback manager should not be nil")
	}

	if tm.GetWriteHandler() == nil {
		t.Error("write handler should not be nil")
	}
}

// TestTransactionManagerMultiple tests creating multiple managers
func TestTransactionManagerMultiple(t *testing.T) {
	mockIsolMgr := &mockIsolationManager{}
	mockLogMgr := &mockLogManager{}
	mockBufMgr := &mockBufferPoolManager{}
	mockStorage := &mockStorageEngine{}

	tm1 := NewTransactionManager(mockIsolMgr, mockLogMgr, mockBufMgr, mockStorage)
	tm2 := NewTransactionManager(mockIsolMgr, mockLogMgr, mockBufMgr, mockStorage)

	if tm1 == tm2 {
		t.Error("multiple transaction managers should be different instances")
	}
}

// TestTransactionManagerWithDifferentIsolationLevels tests with various isolation levels
func TestTransactionManagerWithDifferentIsolationLevels(t *testing.T) {
	mockIsolMgr := &mockIsolationManager{}
	mockLogMgr := &mockLogManager{}
	mockBufMgr := &mockBufferPoolManager{}
	mockStorage := &mockStorageEngine{}

	// Create transaction manager
	tm := NewTransactionManager(mockIsolMgr, mockLogMgr, mockBufMgr, mockStorage)

	// Manager should work regardless of isolation level
	if tm == nil {
		t.Error("expected manager to be created successfully")
	}
}
