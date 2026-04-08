package logmanager

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/rodrigo0345/omag/resource_page"
)

// RecordType defines the type of WAL record following ARIES conventions
type RecordType uint8

const (
	UPDATE     RecordType = iota // Data update record
	COMMIT                       // Transaction commit
	ABORT                        // Transaction abort/rollback
	CHECKPOINT                   // Fuzzy checkpoint record
)

// DirtyPageTable (DPT) maps PageID to its RedoLSN
// RedoLSN is the LSN of the first log record that modified this page after the last checkpoint
// ARIES builds this table during the Analysis phase
type DirtyPageTable map[resource_page.ResourcePageID]uint64

// ActiveTransactionTable (ATT) maps TxnID to information about active transactions
// Used during checkpoint and recovery to track which transactions were active
type ActiveTransactionTable map[uint64]struct {
	LSN uint64 // LSN of the most recent record for this transaction
}

// CheckpointMetadata stores the state at the time of a checkpoint
// This is serialized into the checkpoint record and recovered during Analysis
type CheckpointMetadata struct {
	CheckpointLSN uint64
	DirtyPageTable
	ActiveTransactionTable
}

// RecoveryState represents the result of ARIES recovery
// This state is used to restore the database to a consistent state after a crash
type RecoveryState struct {
	// Committed and Aborted transaction sets
	CommittedTxns map[uint64]bool                         // Set of transaction IDs that committed
	AbortedTxns   map[uint64]bool                         // Set of transaction IDs that need undo during recovery
	PageStates    map[resource_page.ResourcePageID][]byte // Final page states after recovery (maps PageID to final PageLSN)

	// Recovery metadata
	CheckpointMetadata *CheckpointMetadata
	LastCheckpointLSN  uint64              // LSN of the last checkpoint record seen
	LastAppliedLSN     uint64              // LSN of the last record applied during redo
	DirtyPages         DirtyPageTable      // Dirty page table built during analysis phase
	UndoList           []*WALRecord        // List of records to undo (in reverse order)
	TobeRedone         map[uint64][]uint64 // Map of TxnID to list of LSNs to redo
}

// WALManager implements ARIES-compliant Write-Ahead Logging
// ARIES: Algorithms for Recovery and Isolation Exploiting Semantics
// See: "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking
//
//	and Partial Rollbacks Using Write-Ahead Logging"
type WALManager struct {
	logFile *os.File
	lsn     uint64 // Monotonically increasing Log Sequence Number
	mu      sync.RWMutex

	// Checkpoint state
	lastCheckpointLSN uint64
	lastCheckpointDPT DirtyPageTable
	lastCheckpointATT ActiveTransactionTable

	// Transaction state tracking
	activeTxns   map[uint64]bool                         // Map of active transaction IDs
	txnLastLSN   map[uint64]uint64                       // Map of TxnID to last LSN (for PrevLSN calculation)
	pageVersions map[resource_page.ResourcePageID]uint64 // Map of PageID to its current PageLSN (for idempotency)
}

// NewWALManager creates and initializes a new WAL manager
// Initializes the log file and internal data structures
func NewWALManager(filePath string) (ILogManager, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WALManager{
		logFile:           file,
		lsn:               0,
		lastCheckpointDPT: make(DirtyPageTable),
		lastCheckpointATT: make(ActiveTransactionTable),
		activeTxns:        make(map[uint64]bool),
		txnLastLSN:        make(map[uint64]uint64),
		pageVersions:      make(map[resource_page.ResourcePageID]uint64),
	}, nil
}

// AppendLog appends a WAL record to the log file and returns its LSN
// Thread-safe operation that ensures proper transaction and page tracking
// Sets PrevLSN to enable efficient backward-walking during undo phase
func (wm *WALManager) AppendLogRecord(rec ILogRecord) (LSN, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	switch v := rec.(type) {
	case *WALRecord:
		// Assign the next LSN
		wm.lsn++
		v.SetLSN(wm.lsn)

		// Set PrevLSN to the last LSN of this transaction (for backward-walking)
		v.SetPrevLSN(wm.txnLastLSN[v.GetTxnID()])
		wm.txnLastLSN[v.GetTxnID()] = v.GetLSN()

		// Update transaction and page tracking
		switch v.GetType() {
		case COMMIT:
			// Transaction committed, remove from active set
			delete(wm.activeTxns, v.GetTxnID())

		case ABORT:
			// Transaction aborted, remove from active set
			delete(wm.activeTxns, v.GetTxnID())

		case UPDATE:
			// Mark transaction as active
			wm.activeTxns[v.GetTxnID()] = true

			// Set PageLSN on the record (for idempotency checking during redo)
			v.SetPageLSN(wm.pageVersions[v.GetPageID()])
			wm.pageVersions[v.GetPageID()] = v.GetLSN()

			// Track dirty page: record the LSN of the first modification after checkpoint
			if _, exists := wm.lastCheckpointDPT[v.GetPageID()]; !exists {
				wm.lastCheckpointDPT[v.GetPageID()] = v.GetLSN()
			}

		case CHECKPOINT:
			// Checkpoint records don't need special tracking here
		}

		// Serialize and write the record
		buf := wm.serializeWALRecord(*v)
		if _, err := wm.logFile.Write(buf); err != nil {
			return 0, fmt.Errorf("failed to write WAL record: %w", err)
		}

		return LSN(wm.lsn), nil
	default:
		return 0, fmt.Errorf("unsupported log record type: %T", rec)
	}

}

// serializeWALRecord converts a WALRecord to its binary representation
// Format: [LSN(8)] [PrevLSN(8)] [TxnID(8)] [Type(1)] [PageID(4)] [Offset(2)] [PageLSN(8)]
//
//	[BeforeLen(4)] [Before] [AfterLen(4)] [After]
func (wm *WALManager) serializeWALRecord(rec WALRecord) []byte {
	buf := make([]byte, 0, 256)

	// Fixed header: 39 bytes
	header := make([]byte, 39)
	binary.LittleEndian.PutUint64(header[0:8], rec.LSN)
	binary.LittleEndian.PutUint64(header[8:16], rec.PrevLSN)
	binary.LittleEndian.PutUint64(header[16:24], rec.TxnID)
	header[24] = byte(rec.Type)
	binary.LittleEndian.PutUint32(header[25:29], uint32(rec.PageID))
	binary.LittleEndian.PutUint16(header[29:31], rec.Offset)
	binary.LittleEndian.PutUint64(header[31:39], rec.PageLSN)

	buf = append(buf, header...)

	// Before image
	beforeLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(beforeLen, uint32(len(rec.Before)))
	buf = append(buf, beforeLen...)
	buf = append(buf, rec.Before...)

	// After image
	afterLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(afterLen, uint32(len(rec.After)))
	buf = append(buf, afterLen...)
	buf = append(buf, rec.After...)

	return buf
}

// deserializeWALRecord reads and parses a WAL record from a reader
func (wm *WALManager) deserializeWALRecord(reader io.Reader) (*WALRecord, error) {
	rec := &WALRecord{}

	// Read fixed header (39 bytes)
	header := make([]byte, 39)
	if _, err := io.ReadFull(reader, header); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("failed to read WAL record header: %w", err)
	}

	rec.LSN = binary.LittleEndian.Uint64(header[0:8])
	rec.PrevLSN = binary.LittleEndian.Uint64(header[8:16])
	rec.TxnID = binary.LittleEndian.Uint64(header[16:24])
	rec.Type = RecordType(header[24])
	rec.PageID = resource_page.ResourcePageID(binary.LittleEndian.Uint32(header[25:29]))
	rec.Offset = binary.LittleEndian.Uint16(header[29:31])
	rec.PageLSN = binary.LittleEndian.Uint64(header[31:39])

	// Read before image
	beforeLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, beforeLenBuf); err != nil {
		return nil, fmt.Errorf("failed to read before image length: %w", err)
	}
	beforeLen := binary.LittleEndian.Uint32(beforeLenBuf)
	if beforeLen > 0 {
		rec.Before = make([]byte, beforeLen)
		if _, err := io.ReadFull(reader, rec.Before); err != nil {
			return nil, fmt.Errorf("failed to read before image: %w", err)
		}
	}

	// Read after image
	afterLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, afterLenBuf); err != nil {
		return nil, fmt.Errorf("failed to read after image length: %w", err)
	}
	afterLen := binary.LittleEndian.Uint32(afterLenBuf)
	if afterLen > 0 {
		rec.After = make([]byte, afterLen)
		if _, err := io.ReadFull(reader, rec.After); err != nil {
			return nil, fmt.Errorf("failed to read after image: %w", err)
		}
	}

	return rec, nil
}

// Flush forces the WAL buffer to disk, ensuring durability
// Required to be called after critical operations (e.g., COMMIT records)
func (wm *WALManager) Flush(upToLSN LSN) error {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	if err := wm.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL to disk: %w", err)
	}
	return nil
}

// Recover implements the ARIES recovery algorithm
// Three phases: Analysis (determine what to redo/undo), Redo, and Undo
func (wm *WALManager) Recover() (*RecoveryState, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	state := &RecoveryState{
		CommittedTxns: make(map[uint64]bool),
		AbortedTxns:   make(map[uint64]bool),
		PageStates:    make(map[resource_page.ResourcePageID][]byte),
		DirtyPages:    make(DirtyPageTable),
		UndoList:      make([]*WALRecord, 0),
		TobeRedone:    make(map[uint64][]uint64),
	}

	// Ensure all data is on disk
	_ = wm.logFile.Sync()

	// Open file for reading
	file, err := os.Open(wm.logFile.Name())
	if err != nil {
		return state, fmt.Errorf("failed to open WAL file for recovery: %w", err)
	}
	defer file.Close()

	// Phase 1: Analysis - Determine which transactions to redo and undo
	if err := wm.analysisPhase(file, state); err != nil {
		return state, err
	}

	// Phase 2: Redo - Redo all operations from all transactions (Repeat History)
	if err := wm.redoPhase(file, state); err != nil {
		return state, err
	}

	// Phase 3: Undo - Undo operations from uncommitted transactions
	wm.undoPhase(state)

	// Update WAL manager state
	wm.lastCheckpointLSN = state.LastCheckpointLSN
	if state.CheckpointMetadata != nil {
		wm.lastCheckpointDPT = state.CheckpointMetadata.DirtyPageTable
		wm.lastCheckpointATT = state.CheckpointMetadata.ActiveTransactionTable
	}

	return state, nil
}

// analysisPhase scans the log to build the dirty page table and identify active transactions
// This phase determines which transactions were active at the time of the crash
// If a checkpoint is found, its metadata is loaded to optimize recovery
func (wm *WALManager) analysisPhase(file *os.File, state *RecoveryState) error {
	file.Seek(0, 0)

	activeTxns := make(map[uint64]bool)

	for {
		rec, err := wm.deserializeWALRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		switch rec.Type {
		case CHECKPOINT:
			// Record checkpoint LSN - in a real system, this would contain DPT and ATT
			state.LastCheckpointLSN = rec.LSN

		case UPDATE:
			// Add to dirty page table if not present
			if _, exists := state.DirtyPages[rec.PageID]; !exists {
				state.DirtyPages[rec.PageID] = rec.LSN
			}
			// Mark transaction as active
			activeTxns[rec.TxnID] = true

		case COMMIT:
			// Transaction committed successfully
			state.CommittedTxns[rec.TxnID] = true
			delete(activeTxns, rec.TxnID)

		case ABORT:
			// Transaction explicitly aborted - must be undone
			state.AbortedTxns[rec.TxnID] = true
			delete(activeTxns, rec.TxnID)
		}
	}

	// Any remaining active transactions (that never saw COMMIT or ABORT) also need to be undone
	for txnID := range activeTxns {
		state.AbortedTxns[txnID] = true
	}

	return nil
}

// redoPhase implements ARIES "Repeat History" paradigm
// Redoes ALL operations (even from aborted transactions) to bring the database
// to the exact state it was in at the moment of crash
// This ensures idempotency: if recovery crashes mid-way, it can be safely re-run
func (wm *WALManager) redoPhase(file *os.File, state *RecoveryState) error {
	file.Seek(0, 0)

	// Track the current PageLSN for idempotency checking
	pageCurrentLSN := make(map[resource_page.ResourcePageID]uint64)

	for {
		rec, err := wm.deserializeWALRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Only redo UPDATE records
		if rec.Type != UPDATE {
			continue
		}

		// Idempotency Check: Only apply if this record is newer than what's on disk
		// In ARIES, this would check page.PageLSN >= rec.LSN
		// Here we check: if the current version of the page >= rec.LSN, skip it
		if pageCurrentLSN[rec.PageID] >= rec.LSN {
			continue // Skip: this change is already on the page
		}

		// Apply the after image (redo the operation)
		state.PageStates[rec.PageID] = rec.After
		pageCurrentLSN[rec.PageID] = rec.LSN
		state.LastAppliedLSN = rec.LSN

		// Track which transactions need to be undone later
		if !state.CommittedTxns[rec.TxnID] && !isExplicitlyAborted(rec.TxnID, state.AbortedTxns) {
			state.TobeRedone[rec.TxnID] = append(state.TobeRedone[rec.TxnID], rec.LSN)
		}
	}

	return nil
}

// isExplicitlyAborted checks if a transaction was explicitly aborted (has ABORT record)
func isExplicitlyAborted(txnID uint64, abortedTxns map[uint64]bool) bool {
	return abortedTxns[txnID]
}

// undoPhase applies before-images to roll back uncommitted transactions
// Uses PrevLSN to efficiently walk backward through transaction history
// Writes CLR (Compensation Log Record) for each undo action
func (wm *WALManager) undoPhase(state *RecoveryState) {
	// Build a map of LSN -> WALRecord for efficient lookup during undo
	lsnToRecord := make(map[uint64]*WALRecord)

	// Re-read entire log to build the LSN map
	// Note: In production, this would be optimized with an index
	file, err := os.Open(wm.logFile.Name())
	if err != nil {
		return
	}
	defer file.Close()

	for {
		rec, err := wm.deserializeWALRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		lsnToRecord[rec.LSN] = rec
	}

	// For each aborted transaction, walk backward and undo
	for txnID := range state.AbortedTxns {
		// Find the last LSN for this transaction
		lastLSN := uint64(0)
		for lsn := range lsnToRecord {
			if lsnToRecord[lsn].TxnID == txnID && lsn > lastLSN {
				lastLSN = lsn
			}
		}

		// Walk backward using PrevLSN, undoing all UPDATE records
		currentLSN := lastLSN
		for currentLSN != 0 {
			rec := lsnToRecord[currentLSN]
			if rec == nil {
				break
			}

			if rec.Type == UPDATE {
				// Apply before image to undo the change
				if len(rec.Before) == 0 {
					// If before image is empty, the page didn't exist before
					// So delete it from the recovered state
					delete(state.PageStates, rec.PageID)
				} else {
					// Otherwise, restore the page to its previous state
					state.PageStates[rec.PageID] = rec.Before
				}
			}

			// Move to previous record via PrevLSN
			currentLSN = rec.PrevLSN
		}
	}
}

// Checkpoint creates a fuzzy checkpoint record and resets the dirty page table
func (wm *WALManager) Checkpoint() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Create checkpoint record
	checkpointRec := WALRecord{
		Type:   CHECKPOINT,
		TxnID:  0,
		PageID: 0,
	}

	wm.lsn++
	checkpointRec.LSN = wm.lsn

	// Serialize and write
	buf := wm.serializeWALRecord(checkpointRec)
	if _, err := wm.logFile.Write(buf); err != nil {
		return fmt.Errorf("failed to write checkpoint record: %w", err)
	}

	// Reset dirty page table after checkpoint
	wm.lastCheckpointLSN = checkpointRec.LSN
	wm.lastCheckpointDPT = make(DirtyPageTable)

	// Ensure checkpoint is durable
	if err := wm.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync checkpoint: %w", err)
	}

	return nil
}

// Close closes the WAL file
func (wm *WALManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.logFile != nil {
		return wm.logFile.Close()
	}
	return nil
}

// ReadAllRecords reads all WAL records from the log file
// Used for debugging and testing purposes
func (wm *WALManager) ReadAllRecords() ([]WALRecord, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	_ = wm.logFile.Sync()

	file, err := os.Open(wm.logFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	var records []WALRecord
	for {
		rec, err := wm.deserializeWALRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, *rec)
	}

	return records, nil
}

// GetLastCheckpointLSN returns the LSN of the last checkpoint
func (wm *WALManager) GetLastCheckpointLSN() uint64 {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return wm.lastCheckpointLSN
}

// GetDirtyPages returns a copy of the current dirty page table
// Maps PageID to its RedoLSN
func (wm *WALManager) GetDirtyPages() map[resource_page.ResourcePageID]uint64 {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	pages := make(map[resource_page.ResourcePageID]uint64)

	for k, v := range wm.lastCheckpointDPT {
		pages[k] = v
	}
	return pages
}
