# Transactions

## Overview

InesDB provides comprehensive ACID transaction support with multiple isolation levels and concurrency control strategies. Transactions ensure data consistency and enable safe concurrent execution of queries.

## ACID Guarantees

### Atomicity

All-or-nothing semantics using Write-Ahead Logging (WAL):

- All transaction changes are logged before being applied
- On failure, transactions can be rolled back completely via undo logs
- Partial transactions never visible to other transactions
- Crash recovery replays WAL to restore consistent state

### Consistency

Transactions maintain database invariants:

- Transaction isolation prevents dirty reads
- Locks ensure data locked before modification
- Undo logs support automatic rollback
- Constraints enforced at transaction boundaries

### Isolation

Multiple concurrent transactions don't interfere:

- Isolation levels: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- Isolation managers implement different strategies
- Lock managers prevent conflicts
- Version managers support multi-version schemes

### Durability

Committed changes survive system failures:

- Write-Ahead Log (WAL) written to persistent storage
- Batched writes reduce I/O overhead
- Checkpointing speeds recovery
- Page versions tracked on disk

## Transaction Lifecycle

```
1. BEGIN TRANSACTION
   ├─ Allocate Transaction ID (XID)
   ├─ Determine Isolation Level
   ├─ Initialize Read Set (empty)
   ├─ Initialize Write Set (empty)
   ├─ Record Start Time
   └─ Thread-local Context Created

2. EXECUTE STATEMENTS
   ├─ READ: Check version visibility
   │   ├─ Check own writes first
   │   ├─ Check committed versions
   │   ├─ Check isolation level
   │   └─ Return appropriate version
   │
   └─ WRITE: Log and modify
       ├─ Acquire locks (if 2PL)
       ├─ Log to WAL
       ├─ Create new version or modify
       ├─ Add to write set
       └─ Update in-memory state

3. COMMIT
   ├─ Validate (if OCC)
   │   ├─ Check for conflicts
   │   ├─ Abort if conflict detected
   │   └─ Proceed if valid
   │
   ├─ Flush WAL to disk
   ├─ Mark transaction committed in log
   ├─ Release all locks
   ├─ Publish modified versions
   ├─ Update visibility information
   └─ Transaction Context Destroyed

4. ABORT/ROLLBACK
   ├─ Execute undo operations
   │   ├─ Reverse writes in reverse order
   │   ├─ Delete new versions
   │   └─ Restore old values
   │
   ├─ Release all locks
   ├─ Log rollback in WAL
   ├─ Mark aborted in log
   └─ Transaction Context Destroyed
```

## Isolation Levels

### READ UNCOMMITTED
- Most permissive
- Transactions see uncommitted changes (dirty reads possible)
- Minimal locking overhead
- Use when: Approximate results acceptable

### READ COMMITTED
- Default for most databases
- Transaction sees queries as of start of each statement (not transaction)
- Committed changes visible during transaction
- Non-repeatable reads possible
- Use when: Standard correctness with good performance

### REPEATABLE READ
- Snapshot isolation
- Transaction sees consistent snapshot from its start
- Prevents dirty reads and non-repeatable reads
- Phantom reads possible
- Use when: Consistency within transaction important

### SERIALIZABLE
- Strictest isolation
- Equivalent to serial execution of transactions
- All anomalies prevented (dirty reads, non-repeatable reads, phantoms)
- Lowest concurrency

## Isolation Strategies

### MVCC (Multi-Version Concurrency Control)

Each transaction sees a consistent snapshot of data:

```
Version History of Key K:
Value₁──► Value₂──► Value₃──► Current
 │        │        │
 Txn1     Txn2     Txn3,Txn4
 [1-3]    [4-8]    [9-∞]

Transaction Visibility:
- Txn10 (started after Txn3): Sees Value₃
- Txn7: Sees Value₂ (REPEATABLE READ)
- Txn2: Sees Value₁ (as of Txn2 start)
```

Benefits:
- Readers don't block writers
- Writers don't block readers
- Long transactions don't hold locks

### OCC (Optimistic Concurrency Control)

Transactions proceed without locks, validating at commit:

```
Phase 1: EXECUTION
├─ Read from consistent snapshot
├─ Perform computations
└─ Buffer writes

Phase 2: VALIDATION
├─ Check read set not modified by committed txns
├─ Check write set has no conflicts
└─ Abort if conflicts detected

Phase 3: WRITE
├─ Install all writes
└─ Release resources
```

Best for: Low-conflict workloads

### 2PL (Two-Phase Locking)

Explicit locking before access:

```
Phase 1: GROWING PHASE
├─ Acquire all locks needed
├─ No locks released yet
└─ Conflict detection: Wait or Deadlock

Phase 2: SHRINKING PHASE
├─ After last lock acquired
├─ Begin releasing locks
└─ No new locks acquired

Timeline:
Txn1: S-Lock K1 ──► S-Lock K2 ──► Release K1 ──► Release K2
Txn2:                        X-Lock K2 (wait)        (proceeds)
```

Best for: High-conflict workloads

## Lock Manager

Handles distributed deadlock detection:

### Lock Types
- **Shared (S)**: Multiple readers, no writers
- **Exclusive (X)**: Single writer, no readers
- **Intent Shared (IS)**: Intent to acquire shared locks on children
- **Intent Exclusive (IX)**: Intent to acquire exclusive locks on children

### Lock Compatibility Matrix
```
        │  S  │  X  │ IS  │ IX  │
────────┼─────┼─────┼─────┼─────┤
  S     │  Y  │  N  │  Y  │  N  │
  X     │  N  │  N  │  N  │  N  │
  IS    │  Y  │  N  │  Y  │  Y  │
  IX    │  N  │  N  │  Y  │  Y  │
```

### Deadlock Detection

Wait-for graphs track lock dependencies:

```
Txn1 holds lock on R1, waits for R2
Txn2 holds lock on R2, waits for R1

Wait-for Graph:
Txn1 ──► Txn2
 ▲────────┘

Cycle detected → Deadlock!
Solution: Abort one transaction
```

## Write-Ahead Logging (WAL)

All modifications logged before being applied:

```
Application Request
        │
        ▼
Write to WAL ◄─── Flushed to disk
        │
        ▼
Modify In-Memory State
        │
        ▼
Return Success

On Crash:
├─ Replay all logged operations
├─ Ignore uncommitted transactions
└─ Restore database to consistent state
```

## Undo Logging

Supports transaction rollback:

```
Operation: UPDATE K FROM V1 TO V2

Log Entry:
├─ Undo Log: (K, V1)  [store old value]
├─ Redo Log: (K, V2)  [store new value]
└─ Txn ID and LSN

On ROLLBACK:
├─ For each log entry (in reverse)
├─ Restore value from undo log
└─ Delete redo log entry
```

## Configuration Parameters

- **Isolation Level**: Transaction default
- **Lock Wait Timeout**: How long to wait for deadlock resolution
- **Max Transaction Log Size**: Memory limit for txn state
- **WAL Buffer Size**: Batch size before flushing
- **Checkpoint Interval**: How often to create recovery points
