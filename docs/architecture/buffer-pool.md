# Buffer Pool Manager

## Overview

The Buffer Pool Manager is a critical component that caches pages in memory, reducing expensive disk I/O operations. It implements sophisticated page management and replacement policies.

## Core Responsibilities

1. **Page Caching**: Keep hot pages in memory
2. **Replacement Policy**: Decide which pages to evict
3. **Dirty Page Tracking**: Monitor modified pages
4. **Concurrency Control**: Manage concurrent access to pages
5. **Eviction Strategy**: Flush dirty pages to disk when needed

## Architecture

```
┌─────────────────────────────────────┐
│ Buffer Pool Manager                 │
├─────────────────────────────────────┤
│  Page Request Handler               │
│  ├─ Cache Lookup                    │
│  ├─ Disk I/O (if needed)           │
│  └─ Replacement Policy Driver       │
├─────────────────────────────────────┤
│  Page Table (Hash Map)              │
│  ├─ Page ID → Page Frame mapping    │
│  └─ Frame State Information         │
├─────────────────────────────────────┤
│  Replacement Policy                 │
│  ├─ LRU Tracker                     │
│  ├─ Clock Hand                      │
│  └─ Eviction Queue                  │
├─────────────────────────────────────┤
│  Disk Manager Interface             │
│  ├─ Read Requests                   │
│  └─ Write Requests                  │
└─────────────────────────────────────┘
```

## Page Frame States

Each page frame can be in one of several states:

```
                  ┌─────────┐
                  │  FREE   │
                  └────┬────┘
                       │ (allocate)
                  ┌────▼───────┐
                  │  LOADING   │
                  └────┬───────┘
                       │ (I/O complete)
                  ┌────▼──────────┐
      ┌──────────►│  VALID         │◄────────┐
      │           │ (potentially   │         │
      │           │  both dirty    │         │
      │           │  and pinned)   ├─────────┤
      │           └────┬──────────┘ │ (unpin)
      │                │            │
      │ (unpin)        │(evict)     │
      │                ▼            │
      │           ┌─────────────┐   │
      └───────────│ INVALIDATED │   │
                  └─────────────┘   │
                        ▲           │
                        │(error)    │
                        └───────────┘
```

## Replacement Policies

### LRU (Least Recently Used)

```
Most Recent ◄──────────────────────► Least Recent
    │                                     │
 ┌──▼──┐  ┌──────┐  ┌──────┐  ┌────────▼┐
 │ P5  │◄─┤ P3   │◄─┤ P1   │◄─┤ P2 (evict)
 └─────┘  └──────┘  └──────┘  └─────────┘
```

Algorithm:
- On pg access: Move page to front of list
- On eviction: Remove page from end of list
- Time Complexity: O(1) with doubly-linked list

Trade-offs:
- **Pros**: Optimal in theory for many workloads
- **Cons**: High overhead in practice (pointer manipulations)

### Clock Algorithm

```
         ┌─────────────┐
         │    Page 1   │
         │   ref = 0   │
   ┌─────┤─────────────├─────┐
   │     │↑ clock hand │     │
   │  ┌──┴───────────┐ │     │
   │  │  Page 2      │ │     │
   │  │  ref = 1     │ │     │
   │  └──────────────┘ │     │
   │     ┌─────────────┤     │
   │     │  Page 3     │     │
   │     │  ref = 1    │     │
   │  ┌──┴─────────────┘     │
   │  │   ┌───────────────┐  │
   │  └──►│  Page 4       │  │
   │      │  ref = 0      │  │
   │      └───────────────┘  │
   │           ▲             │
   └───────────┼─────────────┘
               (link to next)
```

Algorithm:
1. Maintain circular buffer of pages
2. Each page has reference bit
3. Clock hand scans the buffer
4. On page access: Set reference bit to 1
5. On eviction: Clear ref bits scanning until finding ref=0

Benefits:
- **Pros**: Low overhead, approximates LRU well
- **Cons**: Variable eviction time

## Page Request Flow

```
Page Request (page_id)
          │
          ▼
   ┌─────────────────┐
   │ In Buffer Pool? │
   └────┬──────┬────┘
        │yes   │no
        │      │
        │      ▼
        │   ┌────────────┐
        │   │ Read from  │
        │   │ Disk       │
        │   └──────┬─────┘
        │          │
        │          ▼
        │   ┌────────────────┐
        │   │ Space in       │
        │   │ Buffer Pool?   │
        │   └───┬────────┬───┘
        │       │yes     │no
        │       │        │
        │       │        ▼
        │       │   ┌──────────────────┐
        │       │   │ Evict Page Using │
        │       │   │ Replacement      │
        │       │   │ Policy           │
        │       │   └────────┬─────────┘
        │       │            │
        │       └────┬───────┘
        │            ▼
        │   ┌──────────────────┐
        │   │ Write Dirty Page │
        │   │ to Disk (if any) │
        │   └────────┬─────────┘
        │            │
        △            △
        └─────┬──────┘
              │
              ▼
        ┌─────────────┐
        │ Pin Page    │
        │ in Buffer   │
        └─────┬───────┘
              │
              ▼
        ┌─────────────┐
        │ Return Page │
        │ Pointer     │
        └─────────────┘
```

## Configuration Parameters

- **Pool Size**: Total number of page frames (e.g., 1000 frames for 4MB pool)
- **Page Size**: Standard 4KB or configurable
- **Replacement Policy**: LRU, Clock, or FIFO
- **Dirty Page Ratio**: Threshold for background flush
- **Flush Interval**: How often to write dirty pages

## Concurrency Considerations

- **Pin Count**: Track number of threads accessing a page
- **Page Locks**: Read/Write locks for concurrent access
- **Preemptive Unpinning**: Prevent deadlocks
- **Thread-Safe Operations**: Atomic page table updates

## Performance Optimization Tips

1. **Cache Locality**: Ensure working set fits in buffer pool
2. **Appropriate Policy**: LRU for general workloads, Clock for specialized
3. **Prefetching**: Load related pages proactively
4. **Batching**: Group I/O operations
5. **Monitor Hits**: Track cache hit ratio for tuning
