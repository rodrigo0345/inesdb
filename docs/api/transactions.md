# Transactions API

This documentation is automatically generated from the Transaction Manager source code.

> **Note**: This file is generated during CI/CD. For the most up-to-date API documentation, run:
> ```bash
> go doc ./internal/txn
> ```

## Overview

The Transaction Manager provides ACID guarantees through multiple isolation levels and concurrency control strategies.

## Key Types

- `TransactionManager`: Main transaction orchestrator
- `Transaction`: Individual transaction context
- `TransactionID`: Unique transaction identifier
- `IsolationLevel`: Enum for isolation levels

## Key Methods

### Transaction Lifecycle
- `Begin(isolationLevel IsolationLevel) (Transaction, error)`: Start transaction
- `(txn Transaction) Read(key []byte) ([]byte, error)`: Read within transaction
- `(txn Transaction) Write(key []byte, value []byte) error`: Write within transaction
- `(txn Transaction) Commit() error`: Finalize transaction
- `(txn Transaction) Rollback() error`: Abort transaction

### Advanced Operations
- `(txn Transaction) GetReadSet() [][]byte`: Get keys read
- `(txn Transaction) GetWriteSet() map[string][]byte`: Get modifications
- `(txn Transaction) GetTransactionID() TransactionID`: Get unique ID

## Isolation Levels

```go
const (
    READ_UNCOMMITTED  IsolationLevel = iota
    READ_COMMITTED
    REPEATABLE_READ
    SERIALIZABLE
)
```

## Configuration

```go
type TransactionManagerConfig struct {
    DefaultIsolationLevel IsolationLevel
    IsolationStrategy    string        // "MVCC", "OCC", "2PL"
    LockWaitTimeout      time.Duration
    MaxTransactionSize   uint64
}
```

See source code for detailed API documentation.
