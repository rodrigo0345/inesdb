# Features

InesDB Storage Engine provides a comprehensive set of features for building high-performance database systems.

## Access Methods

### B+ Tree Implementation
The B+ Tree backend provides traditional disk-based tree indexing with:

- **Balanced Structure**: All leaves at uniform depth ensuring O(log n) access
- **Range Query Support**: Leaf nodes linked for efficient sequential access
- **Automatic Balancing**: Node splitting and merging maintain balance
- **Overflow Pages**: Support for records larger than page size
- **Variable-Size Records**: Slotted page format for efficient space utilization
- **Compression Support**: Pluggable compression (row-wise or page-wise)

Ideal for: Mixed read/write workloads, range queries, traditional database operations

### LSM Tree Implementation
The Log-Structured Merge Tree backend optimizes for write-heavy workloads:

- **Write-Optimized**: Fast sequential writes to memtables
- **Asynchronous Compaction**: Background merging of sorted runs
- **Multi-Level Structure**: Different levels with increasing page counts
- **Bloom Filters**: Quick negative lookups without I/O
- **Level-Based Compaction**: Efficient hierarchical merging
- **Streaming Reads**: Efficient range scans across levels

Ideal for: Write-heavy workloads, time-series data, log storage, high-throughput scenarios

## Page Layout & Organization

### Slotted Page Format
- **Header**: Fixed-size header with metadata (35 bytes minimum)
- **Slot Directory**: Maps logical records to physical offsets
- **Free Space**: Dynamic region between slots and data
- **Record Data**: Variable-size records stored near page end

### Page Types
- **Leaf Pages**: Store key-value pairs (B+Tree) or records (Sequential)
- **Internal Pages**: Store keys and child pointers for navigation
- **Meta Pages**: Store tree/table metadata and versioning information
- **Overflow Pages**: Extended storage for records exceeding page capacity

### Byte Organization
- **Little Endian (LSB)**: All multi-byte values stored in little-endian format
- **Alignment**: Records aligned for efficient CPU access
- **Magic Numbers**: Version checking through magic numbers in headers
- **Checksums**: Optional checksum verification for data integrity

## Transaction Support

### ACID Compliance

**Atomicity**
- Write-Ahead Logging (WAL) ensures all-or-nothing semantics
- Comprehensive undo logs support complete rollback
- Crash recovery restores database to consistent state

**Consistency**
- Transaction isolation prevents inconsistent reads
- Lock-based and MVCC-based consistency options
- Automatic cascade operations via transaction hooks

**Isolation**
- Multiple isolation levels: READ UNCOMMITTED through SERIALIZABLE
- MVCC for snapshot isolation without locking
- Optimistic and pessimistic concurrency control strategies

**Durability**
- Write-Ahead Logging to persistent storage
- Batch commits reduce I/O overhead
- Checkpoint-based recovery for faster startup

### Isolation Levels

1. **READ UNCOMMITTED**: Fastest, dirty reads possible
2. **READ COMMITTED**: Typical default, non-repeatable reads possible
3. **REPEATABLE READ**: Snapshot isolation, phantom reads possible
4. **SERIALIZABLE**: Strictest, equivalent to serial execution

## Concurrency Control

### Multi-Version Concurrency Control (MVCC)
- Multiple versions of each record maintained
- Each transaction sees consistent snapshot
- Readers never block, minimal writer blocking
- Automatic version garbage collection

### Optimistic Concurrency Control (OCC)
- No locking during transaction execution
- Validation at commit time
- Aborts on detected conflicts
- Best for low-conflict workloads

### Two-Phase Locking (2PL)
- Explicit shared/exclusive locks on resources
- Deadlock detection via wait-for graphs
- Growing phase (acquiring locks) → shrinking phase (releasing)
- Best for high-conflict workloads

### Lock Manager Features
- Multiple lock levels: Record, Page, Table
- Lock compatibility checking
- Deadlock detection and resolution
- Priority-based lock granting

## Buffer Pool Management

### Caching Strategy
- **Configurable Size**: Adjustable memory allocation for page cache
- **Smart Replacement**: LRU or Clock algorithm
- **Dirty Page Tracking**: Efficient write-back batching
- **Prefetching**: Proactive page loading for sequential access

### Replacement Policies

**LRU (Least Recently Used)**
- Evicts least recently accessed pages first
- Optimal for many access patterns
- Higher memory overhead than alternatives

**Clock Algorithm**
- Approximates LRU with reduced overhead
- Circular buffer with reference bits
- Efficient second-chance page handling
- Better scalability for large cache sizes

### Page States
- FREE: Available for allocation
- VALID: In memory and ready for use
- LOADING: I/O operation in progress
- PINNED: In use by transaction (cannot evict)
- INVALIDATED: Marked for removal

## Write-Ahead Logging (WAL)

### Durability Guarantee
- All modifications logged before being applied
- Enables crash recovery via log replay
- Supports both undo and redo logging

### Log Entry Format
- **Transaction ID**: Which transaction made the change
- **Operation Type**: INSERT, UPDATE, DELETE, etc.
- **Before Image**: Original value (for undo)
- **After Image**: New value (for redo)
- **LSN**: Log sequence number for ordering
- **Checksum**: Detect corrupted log entries

### Checkpointing
- Periodic snapshots of database state
- Reduces recovery time after crashes
- Freezes dirty pages to disk
- Truncates old log entries

## Storage Backend Options

### B+Tree for Traditional Workloads
```
Good for:
✓ Mixed read/write operations
✓ Range queries across keys
✓ Point lookups
✓ PRIMARY KEY constraints
✓ Index-heavy applications

Trade-offs:
- Random I/O on inserts/updates
- More complex page management
+ Predictable latency
+ Excellent for OLTP
```

### LSM Tree for Write-Heavy Workloads
```
Good for:
✓ High write throughput
✓ Append-heavy workloads
✓ Time-series data
✓ Log storage
✓ Write-optimized scenarios

Trade-offs:
- More complex read path
- Space amplification from compaction
+ Sequential I/O for writes
+ Excellent for OLAP/streaming
+ Lower memory footprint
```

## Advanced Features

### Pluggable Components
- Multiple access methods (B+Tree, LSM Tree)
- Multiple isolation managers (MVCC, OCC, 2PL)
- Multiple replacement policies (LRU, Clock)
- Custom disk managers for different storage backends

### Monitoring & Observability
- Cache hit/miss statistics
- Lock contention metrics
- WAL statistics
- Transaction throughput measurements
- Performance profiling support

### Reliability
- Page checksums for corruption detection
- Transaction rollback on errors
- Deadlock detection and resolution
- Crash recovery from WAL replay

### Performance Optimization
- Index support for faster lookups
- Bloom filters in LSM Trees
- Page prefetching strategies
- Lock batching to reduce overhead
- Tuple packing to maximize page efficiency

## Configuration Capabilities

Each component offers tuning options:

- **Buffer Pool**: Frame count, replacement policy, flush interval
- **B+Tree**: Max degree, merge threshold, overflow strategy
- **LSM Tree**: Level size, compaction strategy, bloom filter bits
- **Transactions**: Isolation level, lock wait timeout, log buffer size
- **Concurrency**: Deadlock detection interval, victim selection strategy
