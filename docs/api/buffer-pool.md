# Buffer Pool API

This documentation is automatically generated from the Buffer Pool Manager source code.

> **Note**: This file is generated during CI/CD. For the most up-to-date API documentation, run:
> ```bash
> go doc ./internal/storage/buffer
> ```

## Overview

The Buffer Pool Manager coordinates in-memory page caching, replacement policies, and persistent storage I/O.

## Key Types

- `BufferPoolManager`: Main buffer pool orchestrator
- `DiskManager`: Handles disk I/O operations
- `PageFrame`: Individual cache frame
- `ReplacementPolicy`: Interface for eviction strategies

## Key Methods

### Buffer Operations
- `GetPage(pageID PageID) (*Page, error)`: Retrieve page from cache/disk
- `NewPage() (PageID, *Page, error)`: Create new page and get frame
- `DeletePage(pageID PageID) error`: Remove page from cache and disk
- `UnpinPage(pageID PageID, isDirty bool) error`: Release page for eviction

### Statistics & Monitoring
- `GetFrameCount() int`: Number of available frames
- `GetCacheHitRatio() float64`: Hit rate of cache
- `FlushPage(pageID PageID) error`: Write dirty page to disk
- `FlushAllPages() error`: Write all dirty pages

## Configuration

```go
type BufferPoolConfig struct{
    FrameCount       int
    ReplacementPolicy string  // "LRU" or "Clock"
    DirtyPageRatio   float64
    FlushInterval    time.Duration
}
```

See source code for detailed API documentation.
