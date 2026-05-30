package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/buffer"
	"github.com/rodrigo0345/omag/src/storage/lsm"
	"github.com/rodrigo0345/omag/src/storage/schema"
	txn_unit "github.com/rodrigo0345/omag/src/engine/txn"
	isolation "github.com/rodrigo0345/omag/src/engine/mvcc"
	log "github.com/rodrigo0345/omag/src/engine/wal"
	"github.com/rodrigo0345/omag/src/engine/rollback"
	"github.com/rodrigo0345/omag/pkg/pkglog"
)

const (
	defaultBufferPoolSize = 2048
)

// Engine is the production MVCC + LSM-tree database.
type Engine struct {
	mvcc      *isolation.MVCCManager
	tables    *schema.TableManager
	walMgr    log.ILogManager
	bufferPool buffer.IBufferPoolManager
	lsmDataDir string
	telemetry  *pkglog.TelemetryCollector

	mu           sync.RWMutex
	tableEngines map[string]storage.IStorageEngine
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
		mvcc:         mvcc,
		tables:       tables,
		walMgr:       walMgr,
		bufferPool:   bufferPool,
		lsmDataDir:   opts.LSMDataDir,
		telemetry:    pkglog.NewTelemetryCollector(),
		tableEngines: make(map[string]storage.IStorageEngine),
	}
	return e, nil
}

func (e *Engine) Close() error {
	e.mu.RLock()
	engines := make([]storage.IStorageEngine, 0, len(e.tableEngines))
	for _, eng := range e.tableEngines {
		engines = append(engines, eng)
	}
	e.mu.RUnlock()

	var errs []error
	for _, eng := range engines {
		if c, ok := eng.(interface{ Close() error }); ok {
			if err := c.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
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
		// MVCC write-write conflict causes abort — flag it in telemetry.
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

func (e *Engine) CreateTable(ts *schema.TableSchema) error {
	tableDir := filepath.Join(e.lsmDataDir, "tables", ts.Name)
	tableEngine := lsm.NewLSMTreeBackendWithDataDir(e.walMgr, e.bufferPool, tableDir)

	if ts.GetIndex("PRIMARY") == nil {
		userCols := ts.GetColumns()
		if len(userCols) == 0 {
			_ = tableEngine.Close()
			return fmt.Errorf("table %s has no columns", ts.Name)
		}
		ts.AddIndex("PRIMARY", []string{userCols[0].Name}, tableEngine)
	} else {
		idx := ts.GetIndex("PRIMARY")
		idx.Engine = tableEngine
	}

	if err := e.tables.CreateTable(ts, false); err != nil {
		_ = tableEngine.Close()
		return err
	}

	e.mu.Lock()
	e.tableEngines[ts.Name] = tableEngine
	e.mu.Unlock()
	return nil
}

func (e *Engine) DropTable(tableName string) error {
	if err := e.tables.DropTable(tableName); err != nil {
		return err
	}

	e.mu.Lock()
	eng, ok := e.tableEngines[tableName]
	delete(e.tableEngines, tableName)
	e.mu.Unlock()

	if ok {
		if c, ok := eng.(interface{ Close() error }); ok {
			return c.Close()
		}
	}
	return nil
}

func (e *Engine) GetTableSchema(tableName string) (schema.ITableSchema, error) {
	return e.tables.GetTableSchema(tableName)
}

func (e *Engine) ListTables() []string {
	return e.tables.GetAllTables()
}

// --- Observability ---

func (e *Engine) GetTelemetry() *pkglog.TelemetryCollector {
	return e.telemetry
}
