package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage/page"
)

type RecordType uint8

const (
	UPDATE RecordType = iota
	COMMIT
	ABORT
	CHECKPOINT
	PUT                RecordType = 10
	DELETE             RecordType = 11
	OPERATION          RecordType = 12 // Operation record for recovery
	REPLICATION_INTENT RecordType = 13 // Replication intent metadata for coordinators
	CREATE_TABLE       RecordType = 14
	DROP_TABLE         RecordType = 15
	CREATE_INDEX       RecordType = 16
	DROP_INDEX         RecordType = 17
)

type DirtyPageTable map[page.ResourcePageID]uint64

type ActiveTransactionTable map[uint64]struct {
	LSN uint64
}

type CheckpointMetadata struct {
	CheckpointLSN uint64
	DirtyPageTable
	ActiveTransactionTable
}

// RecoveryOperation represents a high-level database operation for recovery
type RecoveryOperation struct {
	TxnID     uint64
	TableName string
	Type      RecordType
	Key       []byte
	Value     []byte
}

type RecoveryState struct {
	CommittedTxns map[uint64]bool
	AbortedTxns   map[uint64]bool
	PageStates    map[page.ResourcePageID][]byte
	MaxTxnID      uint64
	CommitRecords uint64

	CheckpointMetadata *CheckpointMetadata
	LastCheckpointLSN  uint64
	LastAppliedLSN     uint64
	DirtyPages         DirtyPageTable
	UndoList           []*WALRecord
	TobeRedone         map[uint64][]uint64
	Operations         []RecoveryOperation // High-level operations for transaction replay
}

type WALManager struct {
	logFile *os.File
	writer  *bufio.Writer
	lsn     uint64
	mu      sync.RWMutex

	lastCheckpointLSN uint64
	lastCheckpointDPT DirtyPageTable
	lastCheckpointATT ActiveTransactionTable

	activeTxns    map[uint64]bool
	txnLastLSN    map[uint64]uint64
	pageVersions  map[page.ResourcePageID]uint64
	txnOperations map[uint64][]RecoveryOperation // Track operations per transaction
	lastFlushedLSN uint64
}

func NewWALManager(filePath string) (ILogManager, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WALManager{
		logFile:           file,
		writer:            bufio.NewWriterSize(file, 64*1024),
		lsn:               0,
		lastCheckpointDPT: make(DirtyPageTable),
		lastCheckpointATT: make(ActiveTransactionTable),
		activeTxns:        make(map[uint64]bool),
		txnLastLSN:        make(map[uint64]uint64),
		pageVersions:      make(map[page.ResourcePageID]uint64),
		txnOperations:     make(map[uint64][]RecoveryOperation),
	}, nil
}

func (wm *WALManager) AppendLogRecord(rec ILogRecord) (LSN, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	switch v := rec.(type) {
	case *WALRecord:
		return wm.appendWALRecord(v)
	case WALRecord:
		return wm.appendWALRecord(&v)
	default:
		return 0, fmt.Errorf("unsupported log record type: %T", rec)
	}
}

func (wm *WALManager) appendWALRecord(v *WALRecord) (LSN, error) {
	wm.lsn++
	v.SetLSN(wm.lsn)

	v.SetPrevLSN(wm.txnLastLSN[v.GetTxnID()])
	wm.txnLastLSN[v.GetTxnID()] = v.GetLSN()

	switch v.GetType() {
	case COMMIT:
		delete(wm.activeTxns, v.GetTxnID())

	case ABORT:
		delete(wm.activeTxns, v.GetTxnID())

	case UPDATE:
		wm.activeTxns[v.GetTxnID()] = true

		v.SetPageLSN(wm.pageVersions[v.GetPageID()])
		wm.pageVersions[v.GetPageID()] = v.GetLSN()

		if _, exists := wm.lastCheckpointDPT[v.GetPageID()]; !exists {
			wm.lastCheckpointDPT[v.GetPageID()] = v.GetLSN()
		}

	case CHECKPOINT:
		// no-op

	case OPERATION:
		if len(v.Before) >= 3 {
			opType := RecordType(v.Before[0])
			tableNameLen := int(binary.LittleEndian.Uint16(v.Before[1:3]))
			if len(v.Before) >= 3+tableNameLen {
				tableName := string(v.Before[3 : 3+tableNameLen])
				key := v.Before[3+tableNameLen:]
				value := v.After

				if _, exists := wm.txnOperations[v.TxnID]; !exists {
					wm.txnOperations[v.TxnID] = make([]RecoveryOperation, 0)
				}

				op := RecoveryOperation{
					TxnID:     v.TxnID,
					TableName: tableName,
					Type:      opType,
					Key:       key,
					Value:     value,
				}
				wm.txnOperations[v.TxnID] = append(wm.txnOperations[v.TxnID], op)
			}
		}
	}

	buf := wm.serializeWALRecord(*v)
	if _, err := wm.writer.Write(buf); err != nil {
		return 0, fmt.Errorf("failed to write WAL record: %w", err)
	}

	return LSN(wm.lsn), nil
}

func (wm *WALManager) serializeWALRecord(rec WALRecord) []byte {
	buf := make([]byte, 0, 256)

	header := make([]byte, 39)
	binary.LittleEndian.PutUint64(header[0:8], rec.LSN)
	binary.LittleEndian.PutUint64(header[8:16], rec.PrevLSN)
	binary.LittleEndian.PutUint64(header[16:24], rec.TxnID)
	header[24] = byte(rec.Type)
	binary.LittleEndian.PutUint32(header[25:29], uint32(rec.PageID))
	binary.LittleEndian.PutUint16(header[29:31], rec.Offset)
	binary.LittleEndian.PutUint64(header[31:39], rec.PageLSN)

	buf = append(buf, header...)

	beforeLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(beforeLen, uint32(len(rec.Before)))
	buf = append(buf, beforeLen...)
	buf = append(buf, rec.Before...)

	afterLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(afterLen, uint32(len(rec.After)))
	buf = append(buf, afterLen...)
	buf = append(buf, rec.After...)

	return buf
}

func (wm *WALManager) deserializeWALRecord(reader io.Reader) (*WALRecord, error) {
	rec := &WALRecord{}

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
	rec.PageID = page.ResourcePageID(binary.LittleEndian.Uint32(header[25:29]))
	rec.Offset = binary.LittleEndian.Uint16(header[29:31])
	rec.PageLSN = binary.LittleEndian.Uint64(header[31:39])

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

func (wm *WALManager) Flush(upToLSN LSN) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if uint64(upToLSN) <= wm.lastFlushedLSN {
		return nil
	}

	if wm.writer != nil {
		if err := wm.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush WAL buffer: %w", err)
		}
	}

	if err := wm.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL to disk: %w", err)
	}
	wm.lastFlushedLSN = uint64(upToLSN)
	return nil
}

func (wm *WALManager) Recover() (*RecoveryState, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Rebuild transaction operation cache from WAL for each recovery run.
	wm.txnOperations = make(map[uint64][]RecoveryOperation)

	state := &RecoveryState{
		CommittedTxns: make(map[uint64]bool),
		AbortedTxns:   make(map[uint64]bool),
		PageStates:    make(map[page.ResourcePageID][]byte),
		DirtyPages:    make(DirtyPageTable),
		UndoList:      make([]*WALRecord, 0),
		TobeRedone:    make(map[uint64][]uint64),
		Operations:    make([]RecoveryOperation, 0),
	}

	_ = wm.logFile.Sync()
	if wm.writer != nil {
		_ = wm.writer.Flush()
	}

	file, err := os.Open(wm.logFile.Name())
	if err != nil {
		return state, fmt.Errorf("failed to open WAL file for recovery: %w", err)
	}
	defer file.Close()

	if err := wm.analysisPhase(file, state); err != nil {
		return state, err
	}

	if err := wm.redoPhase(file, state); err != nil {
		return state, err
	}

	wm.undoPhase(state)

	wm.lastCheckpointLSN = state.LastCheckpointLSN
	if state.CheckpointMetadata != nil {
		wm.lastCheckpointDPT = state.CheckpointMetadata.DirtyPageTable
		wm.lastCheckpointATT = state.CheckpointMetadata.ActiveTransactionTable
	}

	return state, nil
}

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

		if rec.TxnID > state.MaxTxnID {
			state.MaxTxnID = rec.TxnID
		}

		switch rec.Type {
		case CHECKPOINT:
			state.LastCheckpointLSN = rec.LSN

		case UPDATE:
			if _, exists := state.DirtyPages[rec.PageID]; !exists {
				state.DirtyPages[rec.PageID] = rec.LSN
			}
			activeTxns[rec.TxnID] = true

		case COMMIT:
			state.CommittedTxns[rec.TxnID] = true
			state.CommitRecords++
			delete(activeTxns, rec.TxnID)

		case ABORT:
			state.AbortedTxns[rec.TxnID] = true
			delete(activeTxns, rec.TxnID)

		case OPERATION:
			// Reconstruct operation from WAL record
			// Before = operation type byte + table name length + table name + key
			if len(rec.Before) > 3 {
				opType := RecordType(rec.Before[0])
				tableNameLen := int(binary.LittleEndian.Uint16(rec.Before[1:3]))
				if len(rec.Before) >= 3+tableNameLen {
					tableName := string(rec.Before[3 : 3+tableNameLen])
					key := rec.Before[3+tableNameLen:]
					value := rec.After

					if _, exists := wm.txnOperations[rec.TxnID]; !exists {
						wm.txnOperations[rec.TxnID] = make([]RecoveryOperation, 0)
					}

					op := RecoveryOperation{
						TxnID:     rec.TxnID,
						TableName: tableName,
						Type:      opType,
						Key:       key,
						Value:     value,
					}
					wm.txnOperations[rec.TxnID] = append(wm.txnOperations[rec.TxnID], op)
				}
			}
		}
	}

	for txnID := range activeTxns {
		state.AbortedTxns[txnID] = true
	}

	// Transfer operations from committed transactions to recovery state
	for txnID, isCommitted := range state.CommittedTxns {
		if isCommitted {
			if ops, exists := wm.txnOperations[txnID]; exists {
				state.Operations = append(state.Operations, ops...)
			}
		}
	}

	return nil
}

func (wm *WALManager) redoPhase(file *os.File, state *RecoveryState) error {
	file.Seek(0, 0)

	pageCurrentLSN := make(map[page.ResourcePageID]uint64)

	for {
		rec, err := wm.deserializeWALRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if rec.Type != UPDATE {
			continue
		}

		if pageCurrentLSN[rec.PageID] >= rec.LSN {
			continue
		}

		state.PageStates[rec.PageID] = rec.After
		pageCurrentLSN[rec.PageID] = rec.LSN
		state.LastAppliedLSN = rec.LSN

		// Add to TobeRedone if:
		// 1. Transaction is committed (must redo committed transactions), OR
		// 2. Transaction is uncertain (not committed and not explicitly aborted)
		// Skip only if explicitly aborted
		if !isExplicitlyAborted(rec.TxnID, state.AbortedTxns) {
			state.TobeRedone[rec.TxnID] = append(state.TobeRedone[rec.TxnID], rec.LSN)
		}
	}

	return nil
}

func isExplicitlyAborted(txnID uint64, abortedTxns map[uint64]bool) bool {
	return abortedTxns[txnID]
}

func (wm *WALManager) undoPhase(state *RecoveryState) {
	lsnToRecord := make(map[uint64]*WALRecord)

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

	for txnID := range state.AbortedTxns {
		lastLSN := uint64(0)
		for lsn := range lsnToRecord {
			if lsnToRecord[lsn].TxnID == txnID && lsn > lastLSN {
				lastLSN = lsn
			}
		}

		currentLSN := lastLSN
		for currentLSN != 0 {
			rec := lsnToRecord[currentLSN]
			if rec == nil {
				break
			}

			if rec.Type == UPDATE {
				if len(rec.Before) == 0 {
					delete(state.PageStates, rec.PageID)
				} else {
					state.PageStates[rec.PageID] = rec.Before
				}
			}

			currentLSN = rec.PrevLSN
		}
	}
}

func (wm *WALManager) Checkpoint() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	checkpointRec := WALRecord{
		Type:   CHECKPOINT,
		TxnID:  0,
		PageID: 0,
	}

	wm.lsn++
	checkpointRec.LSN = wm.lsn

	buf := wm.serializeWALRecord(checkpointRec)
	if _, err := wm.logFile.Write(buf); err != nil {
		return fmt.Errorf("failed to write checkpoint record: %w", err)
	}

	wm.lastCheckpointLSN = checkpointRec.LSN
	wm.lastCheckpointDPT = make(DirtyPageTable)

	if err := wm.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync checkpoint: %w", err)
	}

	return nil
}

func (wm *WALManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.logFile != nil {
		if wm.writer != nil {
			if err := wm.writer.Flush(); err != nil {
				_ = wm.logFile.Close()
				return err
			}
		}
		return wm.logFile.Close()
	}
	return nil
}

func (wm *WALManager) ReadAllRecords() ([]WALRecord, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.writer != nil {
		_ = wm.writer.Flush()
	}
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

func (wm *WALManager) GetLastCheckpointLSN() uint64 {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return wm.lastCheckpointLSN
}

func (wm *WALManager) GetDirtyPages() map[page.ResourcePageID]uint64 {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	pages := make(map[page.ResourcePageID]uint64)

	for k, v := range wm.lastCheckpointDPT {
		pages[k] = v
	}
	return pages
}

// AddTransactionOperation records an operation for a transaction (used during normal processing)
func (wm *WALManager) AddTransactionOperation(txnID uint64, tableName string, opType RecordType, key []byte, value []byte) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Also write operation to WAL for persistence across manager restarts
	// We store operations as structured records with Before=operation metadata, After=value
	rec := WALRecord{
		TxnID:     txnID,
		TableName: tableName,
		Type:      OPERATION,
		Before:    appendOperationBefore(opType, tableName, append([]byte(nil), key...)),
		After:     value,
	}
	_, _ = wm.appendWALRecord(&rec)
}

// GetTransactionOperations returns a snapshot of the structured operations recorded for a transaction.
func (wm *WALManager) GetTransactionOperations(txnID uint64) []RecoveryOperation {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	ops := wm.txnOperations[txnID]
	if len(ops) == 0 {
		return nil
	}
	out := make([]RecoveryOperation, len(ops))
	copy(out, ops)
	return out
}

func (wm *WALManager) LogReplicationIntent(txnID uint64, tableName string, opType RecordType, key []byte, value []byte) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	rec := WALRecord{
		TxnID:     txnID,
		TableName: tableName,
		Type:      REPLICATION_INTENT,
		Before:    appendOperationBefore(opType, tableName, key),
		After:     value,
	}
	_, _ = wm.appendWALRecord(&rec)
}

func appendOperationBefore(opType RecordType, tableName string, key []byte) []byte {
	tableNameBytes := []byte(tableName)
	buf := make([]byte, 0, 3+len(tableNameBytes)+len(key))
	buf = append(buf, byte(opType))
	lenBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(lenBytes, uint16(len(tableNameBytes)))
	buf = append(buf, lenBytes...)
	buf = append(buf, tableNameBytes...)
	buf = append(buf, key...)
	return buf
}

// CleanupTransactionOperations removes operations for a transaction (e.g., on ABORT)
func (wm *WALManager) CleanupTransactionOperations(txnID uint64) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	delete(wm.txnOperations, txnID)
}
