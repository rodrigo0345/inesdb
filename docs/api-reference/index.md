# API Reference

This section contains detailed API documentation for InesDB Storage Engine components, extracted from source code and organized by module.

## Core Storage Backends

### B+ Tree API
Detailed [B+ Tree API Reference](btree.md) covering:
- Tree initialization and configuration
- Insert, search, and delete operations
- Page management and balancing
- Iterator interfaces for range queries

### LSM Tree API
Detailed [LSM Tree API Reference](lsm-tree.md) covering:
- Memtable operations
- Compaction strategies
- SSTable management
- Version control

### Buffer Pool API
Detailed [Buffer Pool API Reference](buffer-pool.md) covering:
- Page frame management
- Replacement policy implementations
- Page pinning/unpinning
- Cache statistics and monitoring

## Transaction Management

### Transactions API
Detailed [Transactions API Reference](transactions.md) covering:
- Transaction lifecycle (begin, execute, commit/rollback)
- Transaction context management
- Isolation level configuration
- Undo/redo logging

### Concurrency Control API
Detailed [Concurrency Control API Reference](concurrency.md) covering:
- MVCC manager interface
- OCC manager interface
- 2PL manager interface
- Lock manager and deadlock detection

## Configuration

Most components support configuration through functional options:

```go
// Example configuration pattern
bpm, err := NewBufferPoolManager(
    WithFrameCount(1000),
    WithReplacementPolicy(PolicyClock),
    WithFlushInterval(time.Second),
)
```

## Error Handling

All API functions return errors following Go conventions:

```go
page, err := bufferPool.GetPage(pageID)
if err != nil {
    // Handle error
    switch err {
    case ErrPageNotFound:
        // Page was not in cache or on disk
    case ErrBufferPoolFull:
        // Cannot evict sufficient pages
    case ErrInvalidPageID:
        // Page ID out of valid range
    default:
        // Other error
    }
}
```

## Interfaces

Key interfaces for extending InesDB:

### Backend Interface
```go
type Backend interface {
    Get(key []byte) ([]byte, error)
    Put(key []byte, value []byte) error
    Delete(key []byte) error
    Scan(start, end []byte) (Iterator, error)
}
```

### IsolationManager Interface
```go
type IsolationManager interface {
    Begin() (TxnID, error)
    Read(txnID TxnID, key []byte) ([]byte, error)
    Write(txnID TxnID, key []byte, value []byte) error
    Commit(txnID TxnID) error
    Rollback(txnID TxnID) error
}
```

## Performance Considerations

When using the API, keep these in mind:

- **Pin/Unpin Symmetry**: Always unpin pages; pinned pages cannot be evicted
- **Transaction Scope**: Keep transactions short to reduce lock contention
- **Buffer Pool Size**: Larger is not always better; monitor hit ratio
- **Isolation Level**: Higher isolation = lower concurrency
- **Batch Operations**: Group related operations for better performance

## Threading Model

- All APIs are thread-safe
- Multiple goroutines can safely access storage engine
- Lock contention depends on workload and isolation level
- Use sync.WaitGroup for coordinating goroutine completion

## Memory Model

- Pages are allocated from buffer pool frames
- Page modifications are visible immediately to owning transaction
- Other transactions see changes only after commit (isolation level dependent)
- Garbage collection handles cleanup of aborted transactions

## Crash Recovery

On startup, the storage engine automatically:
1. Replays Write-Ahead Log (WAL)
2. Commits all completed transactions
3. Aborts incomplete transactions
4. Validates page checksums
5. Reports recovery status

No explicit recovery calls needed from application code.

---

For detailed information on specific components, see the [Architecture](../architecture/overview.md) section.
