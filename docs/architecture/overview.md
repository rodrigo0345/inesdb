# Architecture Overview

InesDB storage engine is built on a layered architecture that separates concerns and provides flexibility for different storage backends and transaction isolation strategies.

## Architectural Layers

### 1. Access Methods Layer
The access methods layer provides different ways to store and retrieve data efficiently:

- **B+ Tree Backend**: A balanced tree structure optimized for both point queries and range scans
  - Logarithmic time complexity for search, insert, and delete operations
  - Efficient range queries through leaf node linking
  - Automatic balancing through node splitting and merging
  - Support for overflow pages for large records

- **LSM Tree Backend**: A log-structured merge tree optimized for write-heavy workloads
  - Write-optimized design with fast inserts
  - Asynchronous compaction of levels
  - Efficient range query support through sorted runs

### 2. Buffer Pool Layer
The buffer pool manager sits between the access methods and disk manager:

- **Page Caching**: Keeps frequently accessed pages in memory
- **Replacement Policies**: Multiple strategies for evicting pages
  - **LRU (Least Recently Used)**: Evicts the least recently accessed page
  - **Clock Algorithm**: Efficient approximation of LRU with reduced overhead
- **Dirty Page Management**: Tracks modified pages for write-back
- **Page Locks**: Ensures consistency during concurrent reads/writes

### 3. Transaction Management Layer
Provides ACID guarantees across operations:

- **Atomicity**: All-or-nothing semantics via Write-Ahead Logging (WAL)
- **Consistency**: Enforced through transaction isolation
- **Isolation**: Multiple isolation levels (Repeatable Read, Serializable)
  - MVCC for concurrency without blocking
  - Optimistic Concurrency Control (OCC) for low-conflict workloads
  - Two-Phase Locking (2PL) for high-conflict scenarios
- **Durability**: Persistent logging via WAL

### 4. Concurrency Control Layer
Manages concurrent transaction execution:

- **Lock Manager**: Distributed deadlock detection
- **Wait-for Graphs**: Detects and resolves circular dependencies
- **Isolation Managers**: Implements different isolation strategies
  - MVCC Manager: Multi-version concurrency control
  - OCC Manager: Optimistic conflict detection
  - 2PL Manager: Pessimistic two-phase locking

### 5. Disk Manager Layer
Handles persistent storage operations:

- **Page-Based I/O**: Fixed-size page reads/writes
- **File Management**: Manages data files and metadata
- **Page Allocation**: Tracks available disk space
- **Recovery**: Supports crash recovery through WAL replay

## Page Layout

InesDB uses a sophisticated slotted page format:

```
┌─────────────────────────────────────┐
│         Page Header                 │
│  - Page ID                          │
│  - Page Type (Leaf/Internal/Meta)  │
│  - Free Space Pointer               │
│  - Slot Count                       │
│  - Right Neighbor Pointer           │
│  - Magic Number (versioning)        │
└─────────────────────────────────────┘
│                                     │
│  Slot Directory                     │
│  ┌─────────────┐                   │
│  │ Offset,Len  │  ← Entries        │
│  ├─────────────┤                   │
│  │ Offset,Len  │                   │
│  └─────────────┘                   │
│                                     │
│  (Free Space)                       │
│                                     │
├─────────────────────────────────────┤
│          Record Data                │
│  (Variable-size records stored      │
│   here, managed by slot directory)  │
└─────────────────────────────────────┘
```

## Transaction Lifecycle

```
1. BEGIN TRANSACTION
   │
   ├─ Create transaction ID
   ├─ Set isolation level
   └─ Initialize read/write sets

2. EXECUTE STATEMENTS
   │
   ├─ Acquire locks (depending on isolation level)
   ├─ Read from pages or versions
   ├─ Write to write-ahead log
   └─ Update in-memory state

3. COMMIT/ROLLBACK
   │
   ├─ If COMMIT:
   │  ├─ Flush WAL to disk
   │  ├─ Release locks
   │  ├─ Update versions
   │  └─ Mark committed in log
   │
   └─ If ROLLBACK:
      ├─ Execute undo operations
      ├─ Release locks
      └─ Discard changes
```

## Data Flow

```
Application
    │
    ▼
┌─────────────────────────┐
│ Transaction Manager     │
└────────────┬────────────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
┌─────────┐      ┌──────────────┐
│ Read    │      │ Write (WAL)  │
└────┬────┘      └──────┬───────┘
     │                  │
     └──────────┬───────┘
                ▼
         ┌─────────────────┐
         │ Isolation Mgr   │ ◄─┐
         │ & Lock Manager  │   │
         └────────┬────────┘   │
                  │            │
                  ▼            │
         ┌─────────────────┐   │
         │ Buffer Pool     │───┘
         │ - Cache pages   │
         │ - Replace pages │
         └────────┬────────┘
                  │
                  ▼
         ┌─────────────────┐
         │ Disk Manager    │
         │ - Read/Write I/O│
         └─────────────────┘
```

## Design Principles

1. **Separation of Concerns**: Each layer handles a specific responsibility
2. **Layered Isolation**: Low-level changes don't affect high-level interfaces
3. **Pluggable Components**: Support multiple implementations (btree, lsm, isolation strategies)
4. **Lock-Free Where Possible**: Minimize contention for better concurrency
5. **Durability First**: All modifications logged before acknowledged to caller
