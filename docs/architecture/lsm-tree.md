# LSM Tree Backend

## Overview

The LSM (Log-Structured Merge) Tree is a write-optimized data structure designed for high-throughput insertion and update scenarios. It trades read performance for exceptional write performance through deferred merging.

## Key Characteristics

- **Write-Optimized**: Fast sequential writes to in-memory structures
- **Level-Based Structure**: Multiple sorted runs at different levels
- **Asynchronous Compaction**: Background merging of levels
- **Bloom Filters**: Quick negative lookups to avoid unnecessary I/O

## Architecture

```
┌─────────────────────────────────┐
│ Memtable (In-Memory)            │
│ - Red-Black Tree or SkipList    │
│ - Sorted by key                 │
│ - Fast writes and reads         │
└────────────┬────────────────────┘
             │ (when full)
             ▼
┌─────────────────────────────────┐
│ Level 0 (On-Disk SSTables)      │
│ - Multiple overlapping runs     │
│ - Unsorted between runs         │
│ - Uncompacted structure         │
└────────────┬────────────────────┘
             │ (Compaction)
             ▼
┌─────────────────────────────────┐
│ Level 1 (On-Disk SSTables)      │
│ - Sorted runs (no overlap)      │
│ - Size ×10 of Level 0           │
└────────────┬────────────────────┘
             │ (Compaction)
             ▼
┌─────────────────────────────────┐
│ Level 2, 3, ... (Final Levels)  │
│ - Progressively larger          │
│ - Minimal overlap               │
└─────────────────────────────────┘
```

## Data Flow

### Write Path
1. Client writes key-value pair
2. Write logged to WAL (for durability)
3. Insert into memtable
4. Return success to client
5. Background: Eventually flush to Level 0

Benefits: Single sequential write to WAL + one random write to memtable

### Read Path
1. Check memtable first (fastest)
2. Check L0 SSTables in reverse insertion order
3. Check L1-Ln SSTables using binary search
4. Use bloom filters to skip unnecessary SSTables

## SSTables (Sorted String Tables)

SSTables are immutable, sorted files containing key-value pairs:

```
┌──────────────────────────────┐
│ Header                       │
│ - Version                    │
│ - Key Range                  │
│ - Bloom Filter               │
├──────────────────────────────┤
│ Data Blocks                  │
│ - Index Block                │
│ - Data entries               │
├──────────────────────────────┤
│ Footer                       │
│ - Checksum                   │
│ - Index offset               │
│ - Metadata offset            │
└──────────────────────────────┘
```

## Bloom Filters

Probabilistic data structures for efficient negative lookups:

- **False Positive**: May indicate key exists when it doesn't
- **False Negative**: Never occurs - if bloom says "no", key definitely absent
- **Performance**: O(1) lookups avoiding unnecessary I/O

## Compaction Strategy

Compaction merges overlapping SSTables into larger files:

### Level-Based Compaction

- **L0 → L1**: When L0 accumulates enough SSTables
- **L1 → L2**: When L1 exceeds size limit
- **Ln → L(n+1)**: Cascading up the levels

Each level is typically 10x the size of the previous level.

### Compaction Process

```
L0 Compactions (overlapping runs):
SSTables at L0 ─┬─ Merge sort ─┐
                └─ Combine    ──┴──► L1 SSTable

Li → Li+1 Compaction (non-overlapping):
L1 SSTables ────┬─ Merge sort ─┐
                └─ Combine    ──┴──► L2 SSTable
```

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|-----------------|-------|
| Write     | O(log n) avg    | Amortized, due to compaction |
| Read      | O(log n) + I/O  | Logarithmic seeks + block I/O |
| Range Scan| O(k log n)      | k results, requires level traversal |
| Space     | O(n×1.1)        | Small overhead due to compaction |

## Trade-offs vs B+ Tree

| Aspect | LSM Tree | B+ Tree |
|--------|----------|---------|
| Writes | Much faster | Slower (random I/O) |
| Reads | Slower | Faster |
| Space Amp. | Low | Higher (balanced tree) |
| Write Amp. | High (compaction) | Low |
| Range Scans | Good | Excellent |
| Latency | Variable | Predictable |

## Configuration Parameters

- **Memtable Size**: When to flush to L0 (affects write latency)
- **Level Size Multiplier**: Ratio between consecutive levels (typically 10)
- **Target File Size**: Size of SSTables at each level
- **Bloom Filter Bits**: Bits per key for bloom filters (affects false positive rate)
- **Compaction Strategy**: When and how to trigger compaction
