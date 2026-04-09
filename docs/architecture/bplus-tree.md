# B+ Tree Backend

## Overview

The B+ Tree backend is a classical implementation of a B+ tree data structure, optimized for disk-based storage. It supports efficient point lookups, range queries, and automatic balancing.

## Key Characteristics

- **Balanced Tree**: All leaves are at the same depth
- **Range Query Friendly**: Linked leaf nodes enable efficient range scans
- **Variable-Size Records**: Support for records larger than a page via overflow pages
- **Logarithmic Performance**: O(log n) for search, insert, and delete operations

## Tree Structure

### Internal Nodes
- Contain keys and child pointers
- Direct all searches to appropriate child
- Automatically split when full
- Merge with siblings when underfull

### Leaf Nodes
- Contain keys and actual data values (or references)
- Linked to adjacent leaf nodes for range traversal
- Can contain overflow page references for large records
- Sorted for sequential access

## Page Management

### Internal Page Format
```
┌─────────────────────────────────┐
│ Header (35 bytes)               │
├─────────────────────────────────┤
│ Keys: K1 | K2 | K3 | ... | Kn   │
├─────────────────────────────────┤
│ Pointers: P0|P1|P2|...|Pn       │
└─────────────────────────────────┘
```

Internal pages store alternating keys and child pointers. The first pointer P0 points to children with keys < K1, P1 points to children with K1 <= keys < K2, and so on.

### Leaf Page Format
```
┌─────────────────────────────────┐
│ Header (35 bytes)               │
├─────────────────────────────────┤
│ Keys: K1 | K2 | K3 | ... | Kn   │
├─────────────────────────────────┤
│ Values/References: V1|V2|...|Vn │
└─────────────────────────────────┘
```

## Meta Page

Meta pages store tree-level metadata:

```
┌──────────────────────────┐
│ Magic Number             │
│ (version checking)       │
├──────────────────────────┤
│ Root Page ID             │
├──────────────────────────┤
│ Page Count               │
├──────────────────────────┤
│ Max Page ID              │
└──────────────────────────┘
```

## Operations

### Search Operation
1. Start at root
2. Binary search keys in current node
3. Follow appropriate child pointer
4. Repeat until leaf node reached
5. Binary search in leaf to find value
6. If value spans overflow page, fetch additional pages

### Insert Operation
1. Search to find leaf node
2. Insert key-value pair in sorted order
3. If leaf overflows:
   - Split leaf into two nodes
   - Promote middle key to parent
   - If parent overflows, recursively split upward
   - If root overflows, create new root

### Delete Operation
1. Search to find key
2. Remove from leaf node
3. If leaf underflows:
   - Try to borrow from sibling
   - If borrowing fails, merge with sibling
   - Recursively handle parent underflow
   - If root becomes empty, lower tree height

## Page Merging Strategy

Empty pages are automatically merged to recover disk space:

- **Leaf Merging**: Empty leaf nodes are merged with siblings
- **Internal Node Merging**: Empty internal nodes are consolidated
- **Breadcrumb Stack**: Maintains path information for efficient navigation during merge operations

## Overflow Pages

For records exceeding page size:

1. Store main record in leaf/internal page
2. Reference overflow page chain
3. Overflow pages linked like leaf pages for sequential access
4. Automatic cleanup when record is deleted

## Compression Support

The B+ Tree supports pluggable compression methods:

- **Row-wise Compression**: Compress individual rows
- **Page-wise Compression**: Compress entire page data
- Compression applied at storage level, transparent to tree logic

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|-----------------|-------|
| Search    | O(log n)        | Requires height number of disk I/Os |
| Insert    | O(log n)        | May trigger splits up the tree |
| Delete    | O(log n)        | May trigger merges |
| Range Scan| O(k + log n)    | k is number of results |

## Configuration Parameters

- **Max Degree**: Affects tree shape and disk I/O patterns
- **Page Size**: Standard 4KB (configurable)
- **Overflow Threshold**: Size limit before overflow pages needed
- **Merge Threshold**: Occupancy percentage for triggering merges
