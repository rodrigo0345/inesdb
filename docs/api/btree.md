# B+ Tree API

This documentation is automatically generated from the B+ Tree backend source code.

> **Note**: This file is generated during CI/CD. For the most up-to-date API documentation, run:
> ```bash
> go doc ./internal/storage/btree
> ```

## Overview

The B+ Tree backend provides a disk-based, balanced tree index structure optimized for both point queries and range scans.

## Key Types

- `BPlusTreeBackend`: Main B+ Tree implementation
- `InternalPage`: Internal node implementation with keys and pointers
- `LeafPage`: Leaf node with key-value pairs
- `MetaPage`: Metadata storage
- `Node`: Generic node wrapper

## Key Methods

### Tree Operations
- `Get(key []byte) ([]byte, error)`: Lookup a value by key
- `Put(key []byte, value []byte) error`: Insert or update a key-value pair
- `Delete(key []byte) error`: Remove a key and its value
- `Scan(start, end []byte) (Iterator, error)`: Range query

### Page Management  
- `Split(page PageID) error`: Split full page into two
- `Merge(page PageID) error`: Merge underutilized pages
- `Rebalance(page PageID) error`: Rebalance pages

## Configuration

```go
type BPlusTreeConfig struct {
    MaxDegree         int
    OverflowThreshold uint32
    MergeThreshold    uint32
    CompressionType   CompressionMethod
}
```

See source code for detailed API documentation.
