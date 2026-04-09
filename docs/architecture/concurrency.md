# Concurrency Control

## Overview

InesDB's concurrency control layer ensures safe execution of multiple transactions simultaneously while maintaining data consistency and maximizing throughput.

## Concurrency Challenges

### The Problem

Multiple transactions accessing the same data create several issues:

```
Txn1: READ   X=10
Txn2: READ   X=10
Txn2: WRITE  X=20
Txn1: WRITE  X=15
Txn2: COMMIT
Txn1: COMMIT
Result: X=15  [Lost Update - Txn2's write lost!]
```

## Isolation Manager

Selects and enforces appropriate isolation strategy:

```
Isolation Manager
    │
    ├─► MVCC Manager
    │   ├─ Version tracking
    │   ├─ Snapshot creation
    │   └─ Version cleanup
    │
    ├─► OCC Manager
    │   ├─ Read set tracking
    │   ├─ Write set tracking
    │   └─ Validation at commit
    │
    └─► 2PL Manager
        ├─ Lock acquisition
        ├─ Lock release
        └─ Conflict detection
```

## Lock Manager

Central component for concurrency control:

### Lock Request Queue

```
Lock Request: (Resource, TxnId, Mode, Priority)
              │
              ▼
     ┌────────────────────┐
     │  Compatible with   │
     │  current locks?    │
     └────┬───────────┬───┘
          │ yes       │ no
          │           │
          ▼           ▼
    ┌──────────┐  ┌────────────┐
    │   Grant  │  │   Queue    │
    └──────────┘  │   Request  │
                  └──────┬─────┘
                         │
                    (lock release)
                         │
                         ▼
                   ┌─────────────┐
                   │ Grant Next  │
                   │ Queued Req. │
                   └─────────────┘
```

## Wait-for Graph

Detects circular wait conditions (deadlocks):

```
Scenario:
- Txn1 holds lock on R1, waits for R2
- Txn2 holds lock on R2, waits for R3
- Txn3 holds lock on R3, waits for R1

Wait-for Graph:
                    ┌─────┐
                    │Txn1 │
                    └──┬──┘
                       │ waits for
                       ▼
           ┌─────┐   ┌─────┐
           │Txn3 │   │Txn2 │
           └──┬──┘   └──┬──┘
      waits   │ waits    │ waits for
        for   │          ▼
              └─────────►R3
                  R1
                        R2

Cycle: Txn1 → Txn2 → Txn3 → Txn1
   Action: Abort one transaction
```

## Optimistic Concurrency Control Details

### Validation Phase

Three-step validation before commit:

```
1. Check Read Set Validness
   ├─ For each (Object, Version) in read set
   ├─ Verify version still valid
   ├─ Verify no conflicting writes
   └─ Abort if validation fails

2. Check Write Set Conflicts
   ├─ For each (Object, New Value) in write set
   ├─ Verify no other committed txn wrote it
   ├─ Verify no other committed txn depends on old value
   └─ Abort if conflict found

3. Check Serialization Order
   ├─ Verify transaction can be serialized
   ├─ Ensure all dependencies respected
   └─ Proceed if valid
```

### Performance Trade-off

```
OCC Performance vs Conflict Rate:

Throughput▲
         │     ┌─────────────────┐
         │    ╱                   ╲
         │   ╱ OCC performs well  ╲
         │  ╱ at low conflicts     ╲
         │ ╱                        ╲
         │╱______2PL (stable)_____╲ Performance
         │  2PL steady            ╲ Drop Zone
         └─────────────────────────────────►
             Conflict Rate
```

At high conflict rates: Frequent aborts hurt OCC, 2PL blocking is preferable

## MVCC - Version Storage Overhead

Each version requires metadata:

```
Key: "user:1000"
Version History:

[Version 1] ──────────────┐
 Value: {name: "Alice"}   │
 TxnStart: 100            │
 TxnEnd: 150              ├─ Storage Overhead
 Visible: [100-150]       │  ~30-50% per extra
                          │  version in worst case
[Version 2] ──────────────┤
 Value: {name: "Bob"}     │
 TxnStart: 151            │
 TxnEnd: ∞                │
 Visible: [151-∞]         │
├───────────────────────────┘

Total: 2 versions stored instead of 1
Version overhead: key_size + value_size + metadata(~50 bytes)
```

## Deadlock Resolution Strategies

### Strategy 1: Victim Selection

Choose transaction to abort with minimal cost:

```
Candidates:
- Txn1: 100 ms runtime, 50 rows modified
- Txn2: 5 ms runtime, 2 rows modified ◄─ Lowest cost
- Txn3: 200 ms runtime, 1000 rows modified

Select Txn2 for abort (minimum cost)
```

### Strategy 2: Wait-Die Scheme

Younger transactions wait, older ones die:

```
Old Transaction ──► Younger Transaction
   Wait            (younger waits)

Younger Transaction ──► Old Transaction
   Die              (younger aborts)

Properties:
✓ No cyclic waits → No deadlock
✓ Younger txns aborted (less work lost)
✗ Livelock possible (repeated aborts)
```

### Strategy 3: Wound-Wait Scheme

Older transactions aggressive, younger ones abort:

```
Old Transaction ──► Younger Transaction
   Wounds          (younger aborts)

Younger Transaction ──► Old Transaction
   Waits             (younger waits)

Properties:
✓ No cyclic waits → No deadlock
✓ Livelock prevented (older always proceed)
✗ Older txns may be aborted unexpectedly
```

## Performance Optimization

### Lock Granularity Trade-offs

```
                           Page Level
                           Locks Per Txn: ↑
                           Lock Overhead: ↑
                           Conflicts: ↓
                                ▲
                                │
                           Record Level
                           Locks Per Txn: ↑
                           Lock Overhead: Medium
                           Conflicts: Low
                                ▲
                                │
                           Table Level
                           Locks Per Txn: ↓
                           Lock Overhead: ↓
                           Conflicts: ↑
```

### Adaptive Isolation

Switch isolation levels based on workload:

```
READ UNCOMMITTED
│
├─ Conflict Rate High? ──► Upgrade to READ COMMITTED
│
└─ Conflict Rate Low? ────► May downgrade if needed

Process:
- Monitor conflict rate
- Adapt isolation level dynamically
- Balance consistency vs performance
```

## Concurrency Metrics to Monitor

- **Lock Contention Rate**: % of lock requests that must wait
- **Deadlock Frequency**: Deadlocks per second
- **Transaction Abort Rate**: % aborted (OCC) or waiting (2PL)
- **Average Lock Hold Time**: Duration of lock grants
- **Cache Hit Ratio**: % of pages in buffer pool
- **Average Transaction Latency**: End-to-end transaction time

## Configuration Parameters

- **Lock Mode**: Shared, Exclusive, Intent
- **Deadlock Detection Interval**: How often to check for cycles
- **Lock Wait Timeout**: Maximum wait before abort
- **Victim Selection Strategy**: Which txn to abort
- **MVCC Retention**: How many versions to keep
- **Version Cleanup**: Frequency of garbage collection
