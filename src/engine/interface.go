package engine

import (
	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/schema"
	"github.com/rodrigo0345/omag/pkg/pkglog"
)

// Options configures the database engine.
type Options struct {
	DBPath         string
	LSMDataDir     string
	WALPath        string
	BufferPoolSize int
}

// Database is the primary interface for all database operations.
type Database interface {
	Close() error

	// Transaction lifecycle
	BeginTransaction(isolationLevel uint8) int64
	Commit(txnID int64) error
	Abort(txnID int64) error

	// DML — always targets the PRIMARY index.
	Read(txnID int64, tableName string, key []byte) ([]byte, error)
	Scan(txnID int64, tableName string, opts storage.ScanOptions) (storage.ICursor, error)
	Write(txnID int64, tableName string, key []byte, value []byte) error
	Delete(txnID int64, tableName string, key []byte) error

	// DDL
	CreateTable(ts *schema.TableSchema) error
	DropTable(tableName string) error
	GetTableSchema(tableName string) (schema.ITableSchema, error)
	ListTables() []string

	// Observability — always returns a non-nil collector.
	GetTelemetry() *pkglog.TelemetryCollector
}
