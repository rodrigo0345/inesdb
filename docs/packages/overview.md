# Package Overview

InesDB Storage Engine is organized into logical packages, each with clear responsibilities.

## Directory Structure

```
internal/
├── storage/          - Core storage backend implementations
│   ├── btree/       - B+ Tree backend
│   ├── buffer/      - Buffer pool and disk management
│   ├── lsm/         - Log-Structured Merge Tree backend
│   └── page/        - Page abstraction and management
│
├── txn/             - Transaction management and ACID support
│   ├── isolation/   - Isolation level implementations
│   │   ├── mvcc_manager.go
│   │   ├── optimistic_concurrency_control_manager.go
│   │   └── two_phase_locking_manager.go
│   │
│   ├── lock/        - Lock manager and deadlock detection
│   │   ├── manager.go
│   │   └── wait_for_graph.go
│   │
│   ├── log/         - Write-Ahead Logging (WAL)
│   │   ├── ilog_manager.go
│   │   ├── wal_manager.go
│   │   └── wal_record.go
│   │
│   └── undo/        - Undo logs for rollback support
│       ├── log.go
│       └── operations.go
│
└── concurrency/     - Concurrency control and caching
    ├── ireplacer.go
    ├── replacer_lru.go
    └── replacer_clock.go
```

## Package Descriptions

### storage/btree
**B+ Tree Implementation**

Responsibilities:
- Balanced tree structure for disk-based indexing
- Point lookup and range query support
- Automatic page splitting/merging
- Overflow page management

Key files:
- `bplus_tree_backend.go`: Main B+ Tree implementation
- `internal_page.go`: Internal node implementation
- `page_leaf.go`: Leaf node implementation
- `meta_page.go`: Metadata management

Use when: You need efficient indexing with balanced tree properties

### storage/buffer
**Buffer Pool Manager**

Responsibilities:
- In-memory page caching
- Disk I/O management
- Page replacement policies (LRU, Clock)
- Dirty page tracking and write-back

Key files:
- `buffer_pool_manager.go`: Main buffer pool orchestration
- `disk_manager.go`: Persistent storage operations
- `ibuffer_pool_manager.go`: Interface definition

Use when: You need to optimize disk access patterns

### storage/lsm
**Log-Structured Merge Tree**

Responsibilities:
- Write-optimized storage backend
- Multi-level SSTable management
- Asynchronous compaction
- Memtable-to-disk flushing

Key files:
- `lsm_tree_backend.go`: Main LSM Tree implementation

Use when: You have write-heavy workloads

### storage/page
**Page Abstraction**

Responsibilities:
- Page interface definition
- Page state management
- Slotted page format implementation
- Page serialization/deserialization

Key files:
- `iresource_page.go`: Page interface
- `page.go`: Core page implementation

Use when: Implementing new storage backends

### txn/manager
**Transaction Orchestrator**

Responsibilities:
- Transaction lifecycle management
- Coordinating isolation managers
- Transaction context tracking
- Visibility determination

Key files:
- `manager.go`: Main transaction manager
- `transaction.go`: Transaction object definition

Use when: Managing multi-statement operations

### txn/isolation
**Isolation Managers**

Responsibilities (per manager):
- Implementing specific isolation levels
- Version management (MVCC)
- Validation (OCC)
- Lock coordination (2PL)

Key implementations:
- `mvcc_manager.go`: Multi-Version Concurrency Control
- `optimistic_concurrency_control_manager.go`: OCC implementation
- `two_phase_locking_manager.go`: 2PL implementation

Use when: Controlling transaction isolation and concurrency

### txn/lock
**Lock Management**

Responsibilities:
- Lock acquisition/release
- Deadlock detection
- Priority-based lock granting
- Wait-for graph cycle detection

Key files:
- `manager.go`: Lock manager
- `wait_for_graph.go`: Deadlock detection

Use when: Managing concurrent access conflicts

### txn/log
**Write-Ahead Logging**

Responsibilities:
- Durable transaction logging
- Log record serialization
- WAL recovery
- Checkpoint management

Key files:
- `wal_manager.go`: WAL orchestration
- `wal_record.go`: Log entry format
- `ilog_manager.go`: Interface definition

Use when: Ensuring crash recovery and durability

### txn/undo
**Undo/Rollback Logging**

Responsibilities:
- Recording pre-operation values
- Supporting transaction rollback
- Undo log chaining
- Memory-efficient undo tracking

Key files:
- `log.go`: Undo log management
- `operations.go`: Operation definitions

Use when: Rolling back failed transactions

### concurrency
**Concurrency Control & Caching**

Responsibilities:
- Page replacement policies
- Cache eviction strategies
- Lock-free data structures
- Performance optimization

Key implementations:
- `replacer_lru.go`: LRU replacement policy
- `replacer_clock.go`: Clock algorithm replacement policy
- `ireplacer.go`: Interface definition

Use when: Optimizing cache performance

## Component Dependencies

```
Application
    │
    ├──► Transaction Manager (txn/)
    │    ├──► Isolation Manager (txn/isolation/)
    │    ├──► Lock Manager (txn/lock/)
    │    ├──► WAL Manager (txn/log/)
    │    └──► Undo Manager (txn/undo/)
    │
    └──► Backend (storage/)
         ├──► B+ Tree Backend (storage/btree/)
         │    └──► Page Interface (storage/page/)
         │
         ├──► LSM Backend (storage/lsm/)
         │    └──► Page Interface (storage/page/)
         │
         └──► Buffer Pool (storage/buffer/)
              ├──► Page Interface (storage/page/)
              ├──► Replacer (concurrency/)
              └──► Disk Manager (storage/buffer/)
```

## Module Import Paths

```go
// B+ Tree Backend
import "github.com/rodrigo0345/omag/internal/storage/btree"

// LSM Tree Backend
import "github.com/rodrigo0345/omag/internal/storage/lsm"

// Buffer Pool Manager
import "github.com/rodrigo0345/omag/internal/storage/buffer"

// Transaction Management
import "github.com/rodrigo0345/omag/internal/txn"

// Concurrency Control
import "github.com/rodrigo0345/omag/internal/concurrency"
```

## Design Patterns

### Interfaces First
Each major component defines an interface before implementation, enabling:
- Multiple implementations (e.g., LRU vs Clock)
- Easy testing with mocks
- Loose coupling between components

### Builder Pattern
Configuration often uses builder/options pattern:
```go
pool := NewBufferPoolManager(
    WithSize(1000),
    WithPolicy(LRU),
)
```

### Error Wrapping
Errors propagate with context:
```go
if err != nil {
    return fmt.Errorf("buffer pool get page: %w", err)
}
```

### Resource Management
Pin/Unpin semantics for resource safety:
- Pin: Increment reference count, prevent eviction
- Unpin: Decrement reference count, allow eviction
- Ensures pages aren't evicted while in use

## Testing

Each package has comprehensive tests:
- Unit tests: `*_test.go` files in same directory
- Benchmarks: `*_benchmark_test.go` files
- Integration tests: Cross-package functionality

Run tests with:
```bash
go test ./internal/storage/btree/...
go test -bench=. ./internal/concurrency/
```

## Performance Optimization

Key optimization points:
1. **Buffer Pool Hit Ratio**: Tune frame count and policy
2. **Lock Contention**: Choose appropriate isolation level
3. **I/O Patterns**: Prefetch related pages
4. **Memory Usage**: Configure replacement policy thresholds

Monitor via package-provided statistics and profiling tools.
