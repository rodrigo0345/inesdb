# Transaction Layer (`internal/txn/`)

This layer manages all transaction-related functionality: isolation strategies, undo/rollback, coordinate writes, and durability logging.

## Package Overview

### Core Transaction Management

#### `transaction.go` - Transaction Entity
- **Transaction**: Represents a single transaction with state and undo log
- Manages transaction ID, state (ACTIVE, COMMITTED, ABORTED)
- Holds reference to UndoLog for abort operations
- Thread-safe access to transaction state

**Key Methods:**
```go
func (t *Transaction) RecordOperation(op Operation) error    // Add to undo log
func (t *Transaction) Rollback(bufferMgr) error             // Execute abort
func (t *Transaction) SavePoint() int                        // Create savepoint
func (t *Transaction) RollbackToPoint(point int, bufferMgr) error  // Partial rollback
```

#### `manager.go` - TransactionManager (Main Coordinator)
**From:** `transaction_manager/transaction_manager.go`

Central coordinator that ties together all transaction components.

- **TransactionManager**: Orchestrates isolation, undo, WAL, and writes
- Creates and manages RollbackManager and WriteHandler
- Provides access to isolation manager, buffer manager, WAL

**Responsibilities:**
- Initialize transaction components
- Provide accessors to RollbackManager and WriteHandler
- Coordinate commit/abort across all subsystems
- Determine WAL flushing strategy per isolation level

**Key Methods:**
```go
func (tm *TransactionManager) GetRollbackManager() *RollbackManager
func (tm *TransactionManager) GetWriteHandler() WriteHandler
func (tm *TransactionManager) ForceWALPushOnCommit() bool
```

### Undo & Rollback (`undo/` Directory)

#### `operation.go` - Operation Interface
Defines reversible operations for abort handling.

**Operation Interface:**
```go
type Operation interface {
    Undo(bufferMgr IBufferPoolManager) error
    GetID() uint64
}
```

**Implementations:**
- **PageWriteOp**: Single page write with before-image
- **CompositeOp**: Groups multiple operations for atomic undo

#### `log.go` - UndoLog Manager
**From:** `transaction_manager/undo_log.go`

Per-transaction undo log storing all reversible operations.

- **UndoLog**: Maintains list of operations, in execution order
- Thread-safe (RWMutex protected)
- Supports savepoints for partial rollback

**Key Methods:**
```go
func (ul *UndoLog) RecordOp(op Operation) error
func (ul *UndoLog) Rollback(bufferMgr) error              // Full rollback
func (ul *UndoLog) RollbackToPoint(point int, bufferMgr) error  // Partial
func (ul *UndoLog) SavePoint() int                        // Capture position
```

#### `rollback.go` - RollbackManager
**From:** `transaction_manager/rollback_manager.go`

Coordinates rollback operations with optional pre/post callbacks.

**Key Methods:**
```go
func (rm *RollbackManager) RecordPageWrite(...) (opID uint64, error)
func (rm *RollbackManager) RollbackTransaction(txn, before, after func()) error
func (rm *RollbackManager) RollbackToSavePoint(txn, point int) error
func (rm *RollbackManager) HasOperations(txn) bool
```

#### `write_handler.go` - Write Coordination
**From:** `transaction_manager/write_handler.go`

NEW LAYER that coordinates writes across:
1. Optional WAL logging (durability)
2. Storage engine execution (pure)
3. UndoLog recording (abort recovery)

**WriteHandler Interface:**
```go
type WriteHandler interface {
    HandleWrite(txn *Transaction, op WriteOperation) error
}
```

**Implementations:**
- **DefaultWriteHandler**: WAL-based systems (2PL, OCC)
  - Logs BEFORE to WAL
  - Executes storage operation
  - Records to UndoLog
- **MVCCWriteHandler**: Snapshot-based systems
  - No undo needed (version isolation)
  - Still logs for durability if configured

### Isolation Strategies (`isolation/` Directory)

#### `manager.go` - IsolationManager Interface
- **IsolationManager**: Common interface for all isolation strategies
- Defines Begin, Read, Write, Commit, Abort contract

#### Two-Phase Locking Implementation
**From:** `transaction_manager/two_phase_locking_manager.go`

- **TwoPhaseLockingManager**: Implements 2PL protocol
- Uses LockManager for lock acquisition/release
- Coordinates with WriteHandler for writes
- Uses RollbackManager for abort

#### MVCC Implementation  
**From:** `transaction_manager/mvcc_manager.go`

- **MVCCManager**: Implements MVCC (Multi-Versioned Concurrency Control)
- Manages version store and timestamp oracle
- No immediate undo needed (snapshot isolation)
- Uses MVCCWriteHandler

#### OCC Implementation
**From:** `transaction_manager/optimistic_concurrency_control_manager.go`

- **OCCManager**: Implements Optimistic Concurrency Control
- Track read/write sets during execution
- Validate at commit time
- Uses RollbackManager on validation failure

### Lock Management (`lock/` Directory)

#### `manager.go` - LockManager
**From:** `transaction_manager_bk/lock_manager.go`

Manages locks for 2PL and other pessimistic strategies.

- **LockManager**: Global lock table
- Lock modes: Shared (read), Exclusive (write)
- Grant/wait/abort based on compatibility
- Coordinates with deadlock detector

#### `wait_for_graph.go` - Deadlock Detection
**From:** `transaction_manager_bk/wait_for_graph.go`

Detects cycles in transaction wait-for relationships.

- **WaitForGraph**: Builds and detects deadlocks
- Cycle detection algorithm (DFS)
- Victim selection and abort

### Logging & WAL (`log/` Directory)

#### `manager.go` - ILogManager Interface
**From:** `logmanager/wal_manager.go`

- **ILogManager**: Common interface for logging
- Abstracts WAL implementation details

#### `wal.go` - WAL Implementation
**From:** `logmanager/wal_manager.go`

ARIES-compliant Write-Ahead Logging.

- **WALManager**: Implements Write-Ahead Log
- LSN (Log Sequence Number) generation
- Checkpoint and recovery operations
- Dirty page table (DPT) tracking

#### `record.go` - WAL Record Format
**From:** `logmanager/wal_record.go`

WAL record structure and serialization.

- **WALRecord**: Contains operation metadata
- Before/after images for redo/undo
- Transaction ID and page ID tracking

## Critical Design Points

### 1. **Undo Layer Independence from WAL**
- UndoLog and Operations work independently
- WAL is OPTIONAL via WriteHandler
- Rollback doesn't depend on WAL records
- Allows easy migration to MVCC (no undo needed)

### 2. **WriteHandler as Coordination Point**
```
Isolation Manager
       ↓
   WriteHandler ← Coordinates:
    /    |    \
   /     |     \
WAL  Storage  UndoLog
```

This decoupling enables:
- Swapping durability strategies (WAL, MVCC, pure undo)
- Pure storage engines (no logging inside)
- Testable components in isolation

### 3. **Isolation Strategy Flexibility**
Each isolation manager:
- ✅ Implements same IsolationManager interface
- ✅ Uses WriteHandler for writes (strategy-agnostic)
- ✅ Uses RollbackManager for abort (if needed)
- ✅ Can be swapped at runtime

## Directory Structure

```
internal/txn/
├── transaction.go           # Transaction entity
├── manager.go               # TransactionManager coordinator
│
├── undo/
│   ├── operation.go         # Operation interface
│   ├── log.go               # UndoLog manager
│   ├── rollback.go          # RollbackManager
│   ├── write_handler.go     # WriteHandler coordination
│   └── *_test.go
│
├── isolation/
│   ├── manager.go           # IsolationManager interface
│   ├── two_phase_locking.go # 2PL implementation
│   ├── mvcc.go              # MVCC implementation
│   ├── optimistic.go        # OCC implementation
│   └── *_test.go
│
├── lock/
│   ├── manager.go           # LockManager
│   ├── wait_for_graph.go    # Deadlock detection
│   └── *_test.go
│
├── log/
│   ├── manager.go           # ILogManager interface
│   ├── wal.go               # WAL implementation
│   ├── record.go            # Record types/serialization
│   └── *_test.go
│
└── README.md                # This file
```

## Usage Example

```go
// Initialize storage
storage := btree.NewBPlusTreeBackend(bufferMgr)

// Initialize transaction layer
txnMgr := txn.NewTransactionManager(
    isolation.New2PLManager(),
    log.NewWALManager(walPath),
    bufferMgr,
    storage,
)

// Begin transaction
txn, err := isolation_mgr.Begin(SERIALIZABLE)

// Execute write
writeOp := txn.NewWriteOperation{
    Key:   []byte("user:123"),
    Value: []byte("John Doe"),
}
err := txnMgr.GetWriteHandler().HandleWrite(txn, writeOp)

// Commit or abort
if err != nil {
    txn.Abort()
    _ := txnMgr.GetRollbackManager().RollbackTransaction(txn, nil, nil)
} else {
    _ := isolation_mgr.Commit(txn.GetID())
}
```

## Testing

```bash
# Test all transaction components
go test ./internal/txn/...

# Test specific component
go test ./internal/txn/isolation/...
go test ./internal/txn/undo/...

# With coverage
go test ./internal/txn/... -cover
```

## Future Improvements

- [ ] Implement callback hooks for custom isolation strategies
- [ ] Add metrics/monitoring for transaction performance
- [ ] Implement savepoint management
- [ ] Add distributed transaction support
- [ ] Implement cascading rollback
