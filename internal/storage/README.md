# Storage Engine Layer (`internal/storage/`)

This layer contains all storage engine implementations and the underlying buffer/page management for physical data access.

## Package Overview

### `buffer/` - Buffer Pool Management
**From:** `buffermanager/`

Manages in-memory page caching and physical disk I/O operations.

- **BufferPoolManager**: Main component managing a fixed-size buffer pool
- **DiskManager**: Abstracts physical I/O operations
- **IBufferPoolManager**: Interface for buffer operations

**Key Responsibilities:**
- Pin/unpin pages in the buffer pool
- Manage page eviction via replacer algorithm
- Coordinate with disk manager for I/O

**Usage:**
```go
bufferMgr := buffer.NewBufferPoolManager(numFrames, diskMgr)
page, err := bufferMgr.FetchPage(pageID)
defer bufferMgr.UnpinPage(pageID, isDirty)
```

### `page/` - Page Abstraction
**From:** `resource_page/`

Defines the page structure and interface for all memory-resident pages.

- **IResourcePage**: Interface for page operations
- **ResourcePage**: Concrete page implementation with locking

**Key Responsibilities:**
- Provide data accessor methods (GetData, SetData)
- Manage page metadata (ID, pin count, dirty flag)
- Provide page-level locking (RLock, WLock)

**Usage:**
```go
page, err := bufferMgr.FetchPage(pageID)
page.RLock()
defer page.RUnlock()
data := page.GetData()
```

### `btree/` - B+ Tree Storage Engine
**From:** `bplus_tree_backend/`

B+ tree implementation as a storage engine. Pure data structure operations (no direct WAL calls).

- **BPlusTreeBackend**: Main B+ tree engine
- **LeafPage**: Leaf node implementation
- **InternalPage**: Internal node implementation

**Key Responsibilities:**
- Implement ordered key-value store via B+ tree
- Handle node splits and merges
- Provide Get, Put, Delete, Scan operations

**Important:** 
- ❌ NO direct logmanager imports
- ❌ NO transaction awareness
- ✅ Uses only buffer manager for page I/O
- ✅ Thread-safe via page latching

**Usage:**
```go
backend := btree.NewBPlusTreeBackend(bufferMgr)
err := backend.Put(key, value)
value, err := backend.Get(key)
```

### `lsm/` - LSM Tree Storage Engine
**From:** `lsm_tree_backend/`

LSM (Log-Structured Merge) tree implementation as an alternative storage engine.

- **LSMTreeBackend**: Main LSM tree engine
- **SSTableManager**: Manages sorted string tables
- **Compactor**: Handles compaction operations

**Key Responsibilities:**
- Implement fast writes via write-ahead structure
- Eventually merge SSTables via compaction
- Provide Get, Put, Delete, Scan operations

**Note:** Currently a placeholder; implementation mirrors `btree/`.

## Design Principles

### 1. **Pure Storage Engines**
Storage engines implement the `StorageEngine` interface:
```go
type StorageEngine interface {
    Put(key []byte, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    Scan(lower, upper []byte) (Iterator, error)
}
```

No direct knowledge of:
- Transactions (TxnID, isolation strategies)
- Logging (WAL, undo logs)
- Locking (transaction-level locks)

### 2. **Buffer as Common Dependency**
All storage engines **only** depend on:
- `BufferManager`: for page I/O
- `ILatchStrategy` (latching): for physical concurrency control on pages

### 3. **Coordination via WriteHandler**
Write operations are coordinated at the transaction layer via `WriteHandler`:
- Captures before-images
- Optionally logs to WAL
- Calls storage engine
- Records to UndoLog

Keeps storage engines pure and testable.

## Directory Structure

```
internal/storage/
├── buffer/
│   ├── manager.go          # BufferPoolManager
│   ├── disk.go             # DiskManager  
│   ├── interface.go        # IBufferPoolManager interface
│   └── *_test.go           # Tests
│
├── page/
│   ├── interface.go        # IResourcePage interface
│   ├── page.go             # ResourcePage implementation
│   └── *_test.go           # Tests
│
├── btree/
│   ├── backend.go          # BPlusTreeBackend
│   ├── leaf.go             # Leaf page implementation
│   ├── internal.go         # Internal page implementation
│   ├── utils.go            # Helper functions
│   └── *_test.go           # Tests
│
├── lsm/
│   ├── backend.go          # LSMTreeBackend
│   ├── sstable.go          # SSTable implementation
│   ├── compactor.go        # Compaction logic
│   └── *_test.go           # Tests
│
└── engine.go               # Common StorageEngine interface
```

## Migration Notes

- Package name changes: `buffermanager` → `buffer`
- All files moved, imports will be updated systematically
- B+ tree `appendLogUsingWAL()` calls must be removed (handled by WriteHandler)
- Tests remain unchanged logically, just directory organization

## Testing

```bash
# Test all storage components
go test ./internal/storage/...

# Test specific component
go test ./internal/storage/btree/...

# With coverage
go test ./internal/storage/... -cover
```

## Future Improvements

- [ ] Implement true LSM tree backend
- [ ] Add pluggable latch strategies
- [ ] Add metadata page versioning
- [ ] Implement recovery from metadata
- [ ] Add storage engine benchmarks
