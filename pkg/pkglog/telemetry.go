package pkglog

import (
	"sync"
	"sync/atomic"
	"time"
)

// QuerySpan records one statement execution. TxnID is the anchor that lets
// future replication and partitioning layers correlate a span to WAL entries
// and key-range accesses.
type QuerySpan struct {
	SpanID        uint64
	TxnID         int64
	SQL           string
	StatementType string // SELECT | INSERT | UPDATE | DELETE | DDL | ANALYZE
	TableName     string
	StartTime     time.Time
	Duration      time.Duration
	RowsAffected  int64
	Error         string // empty on success
	WasConflict   bool   // true when MVCC write-write conflict caused abort
}

// ColumnStat holds per-column statistics collected by ANALYZE.
type ColumnStat struct {
	NullCount     int64
	DistinctCount int64 // exact count via map deduplication during scan
	MinValue      string
	MaxValue      string
}

// TableStats is populated by ANALYZE and stored for future planner use.
type TableStats struct {
	TableName  string
	RowCount   int64
	AnalyzedAt time.Time
	Columns    map[string]ColumnStat
}

// TelemetryCollector accumulates query spans and table stats. All methods are
// safe for concurrent use.
type TelemetryCollector struct {
	mu sync.RWMutex

	nextID atomic.Uint64

	// Ring buffer of the 256 most recent spans.
	spans [256]*QuerySpan
	head  int // next write position (mod 256)

	// Aggregate counters — exported so future monitoring code can read them
	// directly without locking.
	TotalQueries   atomic.Int64
	TotalErrors    atomic.Int64
	TotalConflicts atomic.Int64
	TotalAborts    atomic.Int64

	// Per-table statistics set by ANALYZE.
	tableStats map[string]*TableStats
}

// NewTelemetryCollector creates a ready-to-use collector.
func NewTelemetryCollector() *TelemetryCollector {
	return &TelemetryCollector{
		tableStats: make(map[string]*TableStats),
	}
}

// StartSpan begins recording a new query span. The caller must call EndSpan
// when the statement finishes.
func (c *TelemetryCollector) StartSpan(txnID int64, sql, stmtType, table string) *QuerySpan {
	return &QuerySpan{
		SpanID:        c.nextID.Add(1),
		TxnID:         txnID,
		SQL:           sql,
		StatementType: stmtType,
		TableName:     table,
		StartTime:     time.Now(),
	}
}

// EndSpan finalises a span and stores it in the ring buffer.
func (c *TelemetryCollector) EndSpan(span *QuerySpan, rowsAffected int64, err error) {
	if span == nil {
		return
	}
	span.Duration = time.Since(span.StartTime)
	span.RowsAffected = rowsAffected
	if err != nil {
		span.Error = err.Error()
		c.TotalErrors.Add(1)
	}

	c.TotalQueries.Add(1)

	c.mu.Lock()
	c.spans[c.head%256] = span
	c.head++
	c.mu.Unlock()
}

// RecordConflict marks a span's WasConflict flag and increments the counter.
// txnID is used to find the most recent span for that transaction.
func (c *TelemetryCollector) RecordConflict(txnID int64) {
	c.TotalConflicts.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	// Walk backwards through the ring to find the latest span for this txn.
	for i := 1; i <= 256; i++ {
		idx := (c.head - i + 256) % 256
		if s := c.spans[idx]; s != nil && s.TxnID == txnID {
			s.WasConflict = true
			s.Error = "write conflict: transaction aborted"
			break
		}
	}
}

// RecordAbort increments the abort counter.
func (c *TelemetryCollector) RecordAbort(txnID int64) {
	c.TotalAborts.Add(1)
}

// SetTableStats stores statistics produced by ANALYZE.
func (c *TelemetryCollector) SetTableStats(stats *TableStats) {
	if stats == nil {
		return
	}
	c.mu.Lock()
	c.tableStats[stats.TableName] = stats
	c.mu.Unlock()
}

// GetTableStats returns statistics for the named table, if ANALYZE has run.
func (c *TelemetryCollector) GetTableStats(tableName string) (*TableStats, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.tableStats[tableName]
	return s, ok
}

// RecentSpans returns the most recent n spans (up to 256), newest first.
func (c *TelemetryCollector) RecentSpans(n int) []*QuerySpan {
	if n <= 0 || n > 256 {
		n = 256
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]*QuerySpan, 0, n)
	for i := 1; i <= 256 && len(out) < n; i++ {
		idx := (c.head - i + 512) % 256
		if s := c.spans[idx]; s != nil {
			out = append(out, s)
		}
	}
	return out
}
