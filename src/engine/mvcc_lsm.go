package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/buffer"
	"github.com/rodrigo0345/omag/src/storage/btree"
	"github.com/rodrigo0345/omag/src/storage/lsm"
	"github.com/rodrigo0345/omag/src/storage/schema"
	"github.com/rodrigo0345/omag/src/storage/vector"
	txn_unit "github.com/rodrigo0345/omag/src/engine/txn"
	isolation "github.com/rodrigo0345/omag/src/engine/mvcc"
	log "github.com/rodrigo0345/omag/src/engine/wal"
	"github.com/rodrigo0345/omag/src/engine/rollback"
	"github.com/rodrigo0345/omag/pkg/pkglog"
)

const (
	defaultBufferPoolSize = 2048
	catalogFile           = "catalog.json"
)

// --- Schema catalog (persisted to lsmDataDir/catalog.json) ---

type catalogColumn struct {
	Name          string          `json:"name"`
	Type          schema.DataType `json:"type"`
	AutoIncrement bool            `json:"auto_increment,omitempty"`
	IsUUID        bool            `json:"is_uuid,omitempty"`
	Nullable      bool            `json:"nullable"`
}

type catalogIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique,omitempty"`
}

type catalogForeignKey struct {
	Name              string   `json:"name"`
	LocalColumns      []string `json:"local_columns"`
	ReferencedTable   string   `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns"`
	OnDelete          string   `json:"on_delete,omitempty"`
	OnUpdate          string   `json:"on_update,omitempty"`
}

type catalogTable struct {
	Name          string              `json:"name"`
	Columns       []catalogColumn     `json:"columns"`
	Indexes       []catalogIndex      `json:"indexes"`
	ForeignKeys   []catalogForeignKey `json:"foreign_keys,omitempty"`
	StorageEngine string              `json:"storage_engine,omitempty"`
}

type catalog struct {
	Tables []catalogTable `json:"tables"`
}

// Engine is the production MVCC + LSM-tree database.
type Engine struct {
	mvcc      *isolation.MVCCManager
	tables    *schema.TableManager
	walMgr    log.ILogManager
	bufferPool buffer.IBufferPoolManager
	lsmDataDir string
	telemetry  *pkglog.TelemetryCollector

	mu           sync.RWMutex
	tableEngines map[string]storage.IStorageEngine // primary engines only (for WAL flush)

	// Per-engine lifecycle resources.
	btreeDiskManagers map[string]*buffer.DiskManager    // disk managers for BTree tables
	vectorEngines     map[string]*vector.VectorEngine   // vector engines for close

	// Sequence counters for AUTO_INCREMENT columns: "tableName\x00colName" → next value.
	sequences map[string]int64
	seqMu     sync.Mutex
}

var _ Database = (*Engine)(nil)

// OpenMVCCLSM opens the database. Called from main.go.
func OpenMVCCLSM(opts Options) (*Engine, error) {
	if opts.BufferPoolSize <= 0 {
		opts.BufferPoolSize = defaultBufferPoolSize
	}
	if opts.DBPath == "" {
		opts.DBPath = "omag.db"
	}
	if opts.LSMDataDir == "" {
		opts.LSMDataDir = "lsm_data"
	}
	if opts.WALPath == "" {
		opts.WALPath = "omag.wal"
	}

	diskMgr, err := buffer.NewDiskManager(opts.DBPath)
	if err != nil {
		return nil, fmt.Errorf("disk manager: %w", err)
	}

	bufferPool := buffer.NewBufferPoolManager(opts.BufferPoolSize, diskMgr)

	walMgr, err := log.NewWALManager(opts.WALPath)
	if err != nil {
		_ = diskMgr.Close()
		return nil, fmt.Errorf("WAL manager: %w", err)
	}

	tables := schema.NewTableManager()
	rollbackMgr := rollback.NewRollbackManager(bufferPool)

	mvcc := isolation.NewMVCCManager(walMgr, bufferPool, rollbackMgr, tables, nil)

	e := &Engine{
		mvcc:              mvcc,
		tables:            tables,
		walMgr:            walMgr,
		bufferPool:        bufferPool,
		lsmDataDir:        opts.LSMDataDir,
		telemetry:         pkglog.NewTelemetryCollector(),
		tableEngines:      make(map[string]storage.IStorageEngine),
		btreeDiskManagers: make(map[string]*buffer.DiskManager),
		vectorEngines:     make(map[string]*vector.VectorEngine),
		sequences:         make(map[string]int64),
	}

	// Restore schema from catalog; each CreateTable call also loads existing SSTables.
	if err := e.loadCatalog(); err != nil {
		pkglog.Warn("[Recovery] failed to load catalog: %v", err)
	}

	// Replay committed operations from WAL (handles crash recovery).
	if err := e.replayWAL(); err != nil {
		pkglog.Warn("[Recovery] WAL replay failed: %v", err)
	}

	return e, nil
}

// replayWAL runs ARIES recovery, applies committed ops to storage, advances
// the txn counter, flushes to SSTables, then truncates the WAL.
func (e *Engine) replayWAL() error {
	state, err := e.walMgr.Recover()
	if err != nil {
		return fmt.Errorf("recover: %w", err)
	}

	for _, op := range state.Operations {
		eng, lookupErr := e.tables.GetPrimaryStorage(op.TableName)
		if lookupErr != nil {
			continue
		}
		if op.Type == log.PUT {
			_ = eng.Put(op.Key, op.Value)
		}
	}

	if len(state.AbortedTxns) > 0 {
		e.mvcc.RegisterAbortedTxns(state.AbortedTxns)
	}

	if state.MaxTxnID > 0 {
		e.mvcc.SetNextTxnIDFloor(int64(state.MaxTxnID) + 1)
	}

	// Flush replayed data to SSTables before truncating WAL.
	for _, tableName := range e.tables.GetAllTables() {
		ts, _ := e.tables.GetTableSchema(tableName)
		if ts == nil {
			continue
		}
		for _, idx := range ts.GetAllIndexes() {
			if f, ok := idx.Engine.(interface{ FlushMemtable() }); ok {
				f.FlushMemtable()
			}
		}
	}

	if err := e.walMgr.TruncateAndReset(); err != nil {
		pkglog.Warn("[Recovery] WAL truncation failed: %v", err)
	}

	return nil
}

func (e *Engine) Close() error {
	// Close all index engines (primary + secondary) for every table.
	for _, tableName := range e.tables.GetAllTables() {
		ts, _ := e.tables.GetTableSchema(tableName)
		if ts == nil {
			continue
		}
		for _, idx := range ts.GetAllIndexes() {
			if idx.Engine == nil {
				continue
			}
			if c, ok := idx.Engine.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}
	}

	// Close BTree disk managers.
	for _, dm := range e.btreeDiskManagers {
		_ = dm.Close()
	}

	// Flush vector engines to disk.
	for _, ve := range e.vectorEngines {
		_ = ve.Close()
	}

	var errs []error
	if e.walMgr != nil {
		if err := e.walMgr.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if e.bufferPool != nil {
		if err := e.bufferPool.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// --- Transaction lifecycle ---

func (e *Engine) BeginTransaction(isolationLevel uint8) int64 {
	return e.mvcc.BeginTransaction(isolationLevel)
}

func (e *Engine) Commit(txnID int64) error {
	err := e.mvcc.Commit(txn_unit.TransactionID(txnID))
	if err != nil {
		e.telemetry.RecordConflict(txnID)
	}
	return err
}

func (e *Engine) Abort(txnID int64) error {
	e.telemetry.RecordAbort(txnID)
	return e.mvcc.Abort(txn_unit.TransactionID(txnID))
}

// --- DML ---

func (e *Engine) Read(txnID int64, tableName string, key []byte) ([]byte, error) {
	return e.mvcc.Read(txn_unit.TransactionID(txnID), tableName, "PRIMARY", key)
}

func (e *Engine) Scan(txnID int64, tableName string, opts storage.ScanOptions) (storage.ICursor, error) {
	return e.mvcc.Scan(txn_unit.TransactionID(txnID), tableName, "PRIMARY", opts)
}

func (e *Engine) Write(txnID int64, tableName string, key []byte, value []byte) error {
	return e.mvcc.Write(txn_unit.TransactionID(txnID), tableName, "PRIMARY", key, value)
}

func (e *Engine) Delete(txnID int64, tableName string, key []byte) error {
	return e.mvcc.Delete(txn_unit.TransactionID(txnID), tableName, "PRIMARY", key)
}

// --- DDL ---

// createStorageEngine creates the primary storage engine for a table based on engineType.
func (e *Engine) createStorageEngine(tableName, engineType, tableDir string, ts *schema.TableSchema) (storage.IStorageEngine, error) {
	switch strings.ToUpper(engineType) {
	case "", "LSM":
		return lsm.NewLSMTreeBackendWithDataDir(e.walMgr, e.bufferPool, tableDir), nil

	case "BTREE":
		if err := os.MkdirAll(tableDir, 0755); err != nil {
			return nil, fmt.Errorf("create table dir: %w", err)
		}
		btreeFile := filepath.Join(tableDir, "btree.db")
		diskMgr, err := buffer.NewDiskManager(btreeFile)
		if err != nil {
			return nil, fmt.Errorf("btree disk manager: %w", err)
		}
		bufMgr := buffer.NewBufferPoolManager(128, diskMgr)
		adapter, err := btree.NewBTreeAdapter(bufMgr, diskMgr)
		if err != nil {
			_ = diskMgr.Close()
			return nil, fmt.Errorf("btree adapter: %w", err)
		}
		e.mu.Lock()
		e.btreeDiskManagers[tableName] = diskMgr
		e.mu.Unlock()
		return adapter, nil

	case "VECTOR":
		ext := buildVecExtractor(ts)
		var ve *vector.VectorEngine
		if ext != nil {
			ve = vector.NewVectorEngineWithExtractor(tableDir, ext, vector.DefaultHNSWConfig(), vector.MetricL2)
		} else {
			ve = vector.NewVectorEngine(tableDir)
		}
		e.mu.Lock()
		e.vectorEngines[tableName] = ve
		e.mu.Unlock()
		return ve, nil

	default:
		return nil, fmt.Errorf("unknown storage engine %q; valid options: LSM, BTREE, VECTOR", engineType)
	}
}

// buildVecExtractor returns a closure that extracts the first VECTOR column
// from a raw serialised row. Returns nil when the schema has no VECTOR column.
func buildVecExtractor(ts *schema.TableSchema) func([]byte) ([]float32, bool) {
	if ts == nil {
		return nil
	}
	var vecColName string
	for _, col := range ts.GetColumns() {
		if col.Type == schema.TypeVector {
			vecColName = col.Name
			break
		}
	}
	if vecColName == "" {
		return nil
	}
	return func(row []byte) ([]float32, bool) {
		// MVCC Write prepends a 1-byte op code (0x01 OpInsert) before the
		// schema-encoded row. Strip it so DecodeRow finds _txn_op at byte 0.
		if len(row) < 2 {
			return nil, false
		}
		row = row[1:]
		v := ts.DecodeRow(vecColName, row)
		if v.Error() != nil || v.IsNull() {
			return nil, false
		}
		inner := v.Inner()
		vecs, ok := inner.([]float32)
		if !ok || len(vecs) == 0 {
			return nil, false
		}
		return vecs, true
	}
}

// VectorSearch performs approximate k-NN search on a VECTOR-engine table
// using the HNSW index built by VectorEngine.
func (e *Engine) VectorSearch(txnID int64, tableName string, query []float32, k int) ([]storage.ScanEntry, error) {
	e.mu.RLock()
	ve, ok := e.vectorEngines[tableName]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	return ve.VectorSearch(query, k)
}

func (e *Engine) CreateTable(ts *schema.TableSchema) error {
	tableDir := filepath.Join(e.lsmDataDir, "tables", ts.Name)

	primaryEngine, err := e.createStorageEngine(ts.Name, ts.GetStorageEngine(), tableDir, ts)
	if err != nil {
		return err
	}

	if ts.GetIndex("PRIMARY") == nil {
		userCols := ts.GetColumns()
		if len(userCols) == 0 {
			if c, ok := primaryEngine.(interface{ Close() error }); ok {
				_ = c.Close()
			}
			return fmt.Errorf("table %s has no columns", ts.Name)
		}
		ts.AddIndex("PRIMARY", []string{userCols[0].Name}, primaryEngine)
	} else {
		idx := ts.GetIndex("PRIMARY")
		idx.Engine = primaryEngine
	}

	// Create engines for secondary indexes (always LSM for index data).
	for _, idx := range ts.GetAllIndexes() {
		if idx.Name == "PRIMARY" {
			continue
		}
		if idx.Engine != nil {
			continue
		}
		idxDir := filepath.Join(e.lsmDataDir, "tables", ts.Name, "idx_"+idx.Name)
		idx.Engine = lsm.NewLSMTreeBackendWithDataDir(e.walMgr, e.bufferPool, idxDir)
	}

	if err := e.tables.CreateTable(ts, false); err != nil {
		if c, ok := primaryEngine.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		return err
	}

	e.mu.Lock()
	e.tableEngines[ts.Name] = primaryEngine
	e.mu.Unlock()

	if err := e.saveCatalog(); err != nil {
		pkglog.Warn("[Catalog] failed to save catalog after CreateTable: %v", err)
	}
	return nil
}

func (e *Engine) DropTable(tableName string) error {
	// Capture index engines before removing schema.
	var engsToClose []storage.IStorageEngine
	if ts, err := e.tables.GetTableSchema(tableName); err == nil {
		for _, idx := range ts.GetAllIndexes() {
			if idx.Engine != nil {
				engsToClose = append(engsToClose, idx.Engine)
			}
		}
	}

	if err := e.tables.DropTable(tableName); err != nil {
		return err
	}

	e.mu.Lock()
	delete(e.tableEngines, tableName)
	e.mu.Unlock()

	for _, eng := range engsToClose {
		if c, ok := eng.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}

	// Remove sequences for this table.
	e.seqMu.Lock()
	for k := range e.sequences {
		if len(k) > len(tableName) && k[:len(tableName)] == tableName {
			delete(e.sequences, k)
		}
	}
	e.seqMu.Unlock()

	if err := e.saveCatalog(); err != nil {
		pkglog.Warn("[Catalog] failed to save catalog after DropTable: %v", err)
	}
	return nil
}

func (e *Engine) GetTableSchema(tableName string) (schema.ITableSchema, error) {
	return e.tables.GetTableSchema(tableName)
}

func (e *Engine) ListTables() []string {
	return e.tables.GetAllTables()
}

// --- Sequences ---

// NextSequenceValue returns the next integer for an AUTO_INCREMENT column.
// The counter is initialised lazily by scanning the table for the current max.
func (e *Engine) NextSequenceValue(tableName, colName string) int64 {
	key := tableName + "\x00" + colName

	e.seqMu.Lock()
	defer e.seqMu.Unlock()

	if _, ok := e.sequences[key]; !ok {
		e.sequences[key] = e.scanMaxSequence(tableName, colName)
	}
	e.sequences[key]++
	return e.sequences[key]
}

// scanMaxSequence scans the primary storage for the current max value of colName.
func (e *Engine) scanMaxSequence(tableName, colName string) int64 {
	ts, err := e.tables.GetTableSchema(tableName)
	if err != nil {
		return 0
	}

	txnID := e.mvcc.BeginTransaction(txn_unit.READ_COMMITTED)
	cursor, err := e.mvcc.Scan(txn_unit.TransactionID(txnID), tableName, "PRIMARY", storage.ScanOptions{Inclusive: true})
	if err != nil {
		_ = e.mvcc.Abort(txn_unit.TransactionID(txnID))
		return 0
	}

	var maxVal int64
	for cursor.Next() {
		v := ts.DecodeRow(colName, cursor.Entry().Value)
		if v.Error() == nil && v.Int() > maxVal {
			maxVal = v.Int()
		}
	}
	cursor.Close()
	_ = e.mvcc.Abort(txn_unit.TransactionID(txnID))
	return maxVal
}

// --- Catalog ---

func (e *Engine) saveCatalog() error {
	if err := os.MkdirAll(e.lsmDataDir, 0755); err != nil {
		return fmt.Errorf("mkdir lsmDataDir: %w", err)
	}

	tableNames := e.tables.GetAllTables()
	cat := catalog{Tables: make([]catalogTable, 0, len(tableNames))}

	for _, name := range tableNames {
		ts, err := e.tables.GetTableSchema(name)
		if err != nil {
			continue
		}

		ct := catalogTable{
			Name:          name,
			Columns:       make([]catalogColumn, 0),
			Indexes:       make([]catalogIndex, 0),
			StorageEngine: ts.GetStorageEngine(),
		}
		for _, col := range ts.GetColumns() {
			ct.Columns = append(ct.Columns, catalogColumn{
				Name:          col.Name,
				Type:          col.Type,
				AutoIncrement: col.AutoIncrement,
				IsUUID:        col.IsUUID,
				Nullable:      col.Nullable,
			})
		}
		for _, idx := range ts.GetAllIndexes() {
			ct.Indexes = append(ct.Indexes, catalogIndex{
				Name:    idx.Name,
				Columns: idx.Columns,
				Unique:  idx.Unique,
			})
		}
		for _, fk := range ts.GetForeignKeys() {
			ct.ForeignKeys = append(ct.ForeignKeys, catalogForeignKey{
				Name:              fk.Name,
				LocalColumns:      fk.LocalColumns,
				ReferencedTable:   fk.ReferencedTable,
				ReferencedColumns: fk.ReferencedColumns,
				OnDelete:          fk.OnDelete,
				OnUpdate:          fk.OnUpdate,
			})
		}
		cat.Tables = append(cat.Tables, ct)
	}

	data, err := json.MarshalIndent(cat, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal catalog: %w", err)
	}

	path := filepath.Join(e.lsmDataDir, catalogFile)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write catalog: %w", err)
	}
	return nil
}

func (e *Engine) loadCatalog() error {
	path := filepath.Join(e.lsmDataDir, catalogFile)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read catalog: %w", err)
	}

	var cat catalog
	if err := json.Unmarshal(data, &cat); err != nil {
		return fmt.Errorf("unmarshal catalog: %w", err)
	}

	for _, ct := range cat.Tables {
		cols := make([]schema.Column, len(ct.Columns))
		for i, c := range ct.Columns {
			cols[i] = schema.Column{
				Name:          c.Name,
				Type:          c.Type,
				AutoIncrement: c.AutoIncrement,
				IsUUID:        c.IsUUID,
				Nullable:      c.Nullable,
			}
		}
		ts := schema.NewTableSchema(ct.Name, cols)
		for _, idx := range ct.Indexes {
			ts.AddIndex(idx.Name, idx.Columns, nil) // engine set by CreateTable
			if idx.Unique {
				ts.SetIndexUnique(idx.Name)
			}
		}
		for _, fk := range ct.ForeignKeys {
			ts.AddForeignKey(schema.ForeignKey{
				Name:              fk.Name,
				LocalColumns:      fk.LocalColumns,
				ReferencedTable:   fk.ReferencedTable,
				ReferencedColumns: fk.ReferencedColumns,
				OnDelete:          fk.OnDelete,
				OnUpdate:          fk.OnUpdate,
			})
		}
		ts.SetStorageEngine(ct.StorageEngine)
		if err := e.CreateTable(ts); err != nil {
			pkglog.Warn("[Catalog] failed to restore table %s: %v", ct.Name, err)
		}
	}
	return nil
}

// --- Observability ---

func (e *Engine) GetTelemetry() *pkglog.TelemetryCollector {
	return e.telemetry
}
