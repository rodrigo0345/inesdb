# Concurrency Control API

This documentation is automatically generated from the Concurrency Control source code.

> **Note**: This file is generated during CI/CD. For the most up-to-date API documentation, run:
> ```bash
> go doc ./internal/concurrency
> ```

## Overview

The Concurrency Control layer manages page replacement policies and cache eviction strategies.

## Key Types

- `Replacer`: Interface for replacement policies
- `LRUReplacer`: Least Recently Used replacement
- `ClockReplacer`: Clock algorithm replacement
- `Page`: Cached page frame object

## Key Methods

### LRU Replacer
- `NewLRUReplacer(maxSize int) *LRUReplacer`: Create LRU replacer
- `(r *LRUReplacer) Evict() *Page`: Remove least recently used page
- `(r *LRUReplacer) RecordAccess(pageID PageID)`: Update access time
- `(r *LRUReplacer) Remove(pageID PageID)`: Remove specific page

### Clock Replacer
- `NewClockReplacer(maxFrames int) *ClockReplacer`: Create clock replacer
- `(r *ClockReplacer) Evict() *Page`: Find eviction candidate
- `(r *ClockReplacer) RecordAccess(pageID PageID)`: Set reference bit
- `(r *ClockReplacer) Remove(pageID PageID)`: Remove specific page

### MVCC Support
- `GetVisibleVersion(pageID PageID, txnID TxnID) *Version`: Get appropriate version
- `CreateNewVersion(pageID PageID, data []byte) *Version`: Create new version
- `MarkVersionInvisible(version *Version)`: Hide version from readers

## Configuration

```go
type ReplacementConfig struct {
    Policy    string        // "LRU" or "Clock"
    MaxFrames int
    EvictBatchSize int
}
```

## Performance Considerations

- **LRU**: Better hit ratio, higher overhead
- **Clock**: Lower overhead, good approximation of LRU
- Batch eviction reduces lock contention
- Monitor cache statistics for tuning

See source code for detailed API documentation.
