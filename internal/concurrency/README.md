# Concurrency & Latching (`internal/concurrency/`)

This layer provides physical concurrency control via page-level latching and eviction strategies.

## Package Overview

### Page Latching (`latch.go`)

Provides fine-grained concurrency control for in-memory pages.

**Latch Modes:**
- **Read (Shared)**: Multiple readers, no writers
- **Write (Exclusive)**: Single writer, no readers

**Hierarchy:** Page-level latching (not transaction-level locking)

**Thread Safety:** Latches protect page data structure integrity during physical operations (reads, writes, splits, etc.)

### Page Replacer

The replacer algorithm manages victim selection when the buffer pool is full.

#### Clock Replacer (`clock.go`)
**From:** `replacer/replacer_clock.go`

Implements clock (second chance) page replacement.

- Simple circular buffer pointer
- Gives pages a "second chance" before eviction
- Reference bit reset on each clock pass

**Algorithm:**
1. Traverse frames in circular order
2. If referenced=1, set to 0, continue
3. If referenced=0, evict this frame

**Performance:** O(1) amortized, good cache locality

#### LRU Replacer (`lru.go`)
**From:** `replacer/replacer_lru.go`

Implements Least Recently Used replacement.

- Doubly-linked list of page access order
- Most recent access moves to front
- Evict least recently used (back of list)

**Performance:** O(1) all operations, more memory overhead

### ILatchStrategy Interface

For pluggable latch implementations.

```go
type ILatchStrategy interface {
    AcquirePageLatch(pageID PageID, mode LatchMode) error
    ReleasePageLatch(pageID PageID) error
}
```

## Design Principles

### 1. **Separate from Transaction Locking**
- **Latching**: Physical structure concurrency (page-level, short-duration)
- **Locking**: Logical transaction concurrency (record-level, long-duration)

### 2. **Low-Level Protection**
Latches protect:
- Page data during reads/writes
- Tree structure during node splits/merges
- Index updates

Latches do NOT enforce isolation (that's locking/MVCC's job).

### 3. **Pluggable Replacer**
Different workloads benefit from different replacement policies:
- **Clock**: General purpose, low overhead
- **LRU**: Good for working set fitting in memory
- **FIFO**: Sequential access patterns
- **Custom**: Application-specific patterns

## Implementation Details

### Clock Replacer Method Flow

```go
// Internal state
frames []Frame          // Circular buffer
clockHand uint32      // Current pointer

// Algorithm
func (cr *ClockReplacer) Evict() (PageID, error) {
    for {
        if frames[clockHand].pinCount == 0 {
            if frames[clockHand].referenced == 0 {
                victimID := frames[clockHand].pageID
                frames[clockHand] = nil
                return victimID, nil
            }
            frames[clockHand].referenced = 0
        }
        clockHand = (clockHand + 1) % capacity
    }
}
```

### LRU Replacer Method Flow

```go
// Internal state
list *DoublyLinkedList  // Access order
pageMap map[PageID]*Node  // Page→Node mapping

// Algorithm
func (lru *LRUReplacer) Evict() (PageID, error) {
    if list.Empty() {
        return nil, ErrAllPagesPinned
    }
    back := list.Back()  // LRU (tail)
    pageID := back.Value
    list.Remove(back)
    delete(pageMap, pageID)
    return pageID, nil
}

func (lru *LRUReplacer) Access(pageID PageID) {
    if node, exists := pageMap[pageID]; exists {
        list.Remove(node)  // Remove from current position
    }
    node := list.PushFront(pageID)  // Add to front (MRU)
    pageMap[pageID] = node
}
```

## Directory Structure

```
internal/concurrency/
├── latch.go              # Latch modes and interface
├── page_latch.go         # Page-level latch implementation
│
├── replacer.go           # ILatchStrategy interface (actually replacer interface)
├── clock.go              # Clock replacement algorithm
├── lru.go                # LRU replacement algorithm
│
└── *_test.go             # Tests
```

## Buffer Pool Integration

The BufferManager uses the replacer:

```go
type BufferPoolManager struct {
    frames []Frame
    replacer IReplacer      // Pluggable: Clock, LRU, etc.
    // ...
}

func (bpm *BufferPoolManager) EvictPage() (PageID, error) {
    victimID, err := bpm.replacer.Evict()  // Replacer decides
    if err != nil {
        return 0, err
    }
    // Flush victim if dirty
    // Clear from hash table
    // Return victim's frame to pool
    return victimID, nil
}
```

## Performance Considerations

### Clock Replacer
- **Pros**: Simple, O(1), minimal memory overhead
- **Cons**: Less accurate LRU, can thrash on unfriendly patterns
- **Best for**: General workloads, memory-constrained systems

### LRU Replacer
- **Pros**: Optimal for standard workloads, provably good
- **Cons**: O(1) operations but with higher constant factor, extra memory
- **Best for**: Well-defined working sets, sufficient memory

### When to Use Clock
```
- Buffer pool capacity < 1000 frames
- Memory-constrained environment
- General-purpose workload
- Don't want to tune replacement policy
```

### When to Use LRU
```
- Buffer pool capacity > 1000 frames
- Working set known and fits in memory
- Critical performance application
- Willing to tune/monitor eviction
```

## Advanced Topics

### Inclusive vs Exclusive Latching

**Inclusive (Current)**: Multiple readers can hold latches simultaneously
**Exclusive**: Only one latch holder at a time

Trade-offs:
- Inclusive: Better concurrency, harder to implement
- Exclusive: Simpler implementation, reduced concurrency

### Latch-Free Data Structures

Future enhancement: Implementation without explicit latches using atomic CAS operations.

### Adaptive Replacement Cache (ARC)

Alternative: Balanced between LRU and LFU (Least Frequently Used).

## Usage Example

```go
// Create buffer pool with clock replacer
replacer := concurrency.NewClockReplacer(450)
bufferMgr := buffer.NewBufferPoolManager(450, diskMgr, replacer)

// Or with LRU
replacer := concurrency.NewLRUReplacer(450)
bufferMgr := buffer.NewBufferPoolManager(450, diskMgr, replacer)

// Buffer pool automatically uses replacer for eviction
page, err := bufferMgr.FetchPage(1)  // Evicts via replacer if full
```

## Testing

```bash
# Test all concurrency components
go test ./internal/concurrency/...

# Test replacer implementations
go test ./internal/concurrency/... -run Replacer -v

# Benchmark replacers
go test ./internal/concurrency/ -bench=. -benchmem
```

## Future Improvements

- [ ] Implement FIFO replacer
- [ ] Implement workload-adaptive replacer
- [ ] Add replacer statistics/monitoring
- [ ] Implement latch-free alternative
- [ ] Add benchmarks against real workloads
