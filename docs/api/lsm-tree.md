# LSM Tree API

This documentation is automatically generated from the LSM Tree backend source code.

> **Note**: This file is generated during CI/CD. For the most up-to-date API documentation, run:
> ```bash
> go doc ./internal/storage/lsm
> ```

## Overview

The LSM Tree backend provides write-optimized storage through memtables, multi-level SSTables, and asynchronous compaction.

## Key Types

- `LSMTreeBackend`: Main LSM Tree implementation
- `Memtable`: In-memory sorted structure
- `SSTable`: Sorted string table for on-disk data
- `Level`: Collection of SSTables at a specific level
- `Compaction`: Background merge operation

## Key Methods

### Data Operations
- `Get(key []byte) ([]byte, error)`: Retrieve value from LSM tree
- `Put(key []byte, value []byte) error`: Insert into memtable
- `Delete(key []byte) error`: Mark value as deleted
- `Scan(start, end []byte) (Iterator, error)`: Range query

### Compaction
- `Compact() error`: Trigger manual compaction
- `CompactLevel(level int) error`: Compact specific level
- `Flush() error`: Flush memtable to level 0

### Configuration
- `SetCompactionThreshold(level int, size uint64)`: Configure trigger
- `SetBloomFilterBits(bits int)`: Bits per key for bloom filters
- `SetLevelMultiplier(multiplier int)`: Size ratio between levels

## Configuration

```go
type LSMTreeConfig struct {
    MemtableSize      uint64
    LevelMultiplier   int
    BloomFilterBits   int
    TargetFileSize    uint64
    MaxLevels         int
}
```

See source code for detailed API documentation.
