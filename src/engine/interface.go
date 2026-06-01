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

	// Sequences — returns the next auto-increment value for the given column.
	NextSequenceValue(tableName, colName string) int64

	// Observability — always returns a non-nil collector.
	GetTelemetry() *pkglog.TelemetryCollector

	// VectorSearch performs approximate k-NN search on a VECTOR-engine table
	// using the HNSW index. Results are returned in ascending distance order.
	// Returns (nil, nil) when the table is not backed by a VectorEngine or the
	// index is not configured, signalling the caller to fall back to a full scan.
	VectorSearch(txnID int64, tableName string, query []float32, k int) ([]storage.ScanEntry, error)
}
