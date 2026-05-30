package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/pkg/pkglog"
)

// ExecAnalyze performs a full table scan and collects per-column statistics.
// It stores the result in the engine's TelemetryCollector and returns a result
// set suitable for display in psql.
func ExecAnalyze(db engine.Database, txnID int64, tableName string) (*Result, error) {
	tableName = strings.Trim(strings.TrimSpace(tableName), `"` +"`")
	if tableName == "" {
		return nil, fmt.Errorf("ANALYZE requires a table name: ANALYZE <table>")
	}

	ts, err := db.GetTableSchema(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %q not found: %w", tableName, err)
	}

	cursor, err := db.Scan(txnID, tableName, storage.ScanOptions{Inclusive: true})
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}
	defer cursor.Close()

	userCols := ts.GetColumns()

	// Per-column accumulators.
	type colAcc struct {
		nullCount int64
		distinct  map[string]struct{}
		minVal    string
		maxVal    string
		hasVal    bool
	}
	accs := make(map[string]*colAcc, len(userCols))
	for _, col := range userCols {
		accs[col.Name] = &colAcc{distinct: make(map[string]struct{})}
	}

	var rowCount int64
	for cursor.Next() {
		entry := cursor.Entry()
		row, decErr := DecodeRow(ts, entry.Value)
		if decErr != nil {
			continue
		}
		rowCount++

		for colName, acc := range accs {
			v := row[colName]
			if v == nil {
				acc.nullCount++
				continue
			}
			s := fmt.Sprintf("%v", v)
			acc.distinct[s] = struct{}{}
			if !acc.hasVal {
				acc.minVal = s
				acc.maxVal = s
				acc.hasVal = true
			} else {
				if s < acc.minVal {
					acc.minVal = s
				}
				if s > acc.maxVal {
					acc.maxVal = s
				}
			}
		}
	}
	if err := cursor.Error(); err != nil {
		return nil, err
	}

	// Build TableStats and store in telemetry.
	stats := &pkglog.TableStats{
		TableName:  tableName,
		RowCount:   rowCount,
		AnalyzedAt: time.Now(),
		Columns:    make(map[string]pkglog.ColumnStat, len(userCols)),
	}
	for colName, acc := range accs {
		stats.Columns[colName] = pkglog.ColumnStat{
			NullCount:     acc.nullCount,
			DistinctCount: int64(len(acc.distinct)),
			MinValue:      acc.minVal,
			MaxValue:      acc.maxVal,
		}
	}
	db.GetTelemetry().SetTableStats(stats)

	// Build result set: one row per column.
	cols := []string{"table", "column", "row_count", "null_count", "distinct_count", "min_value", "max_value"}
	var rows [][]any
	for _, col := range userCols {
		acc := accs[col.Name]
		rows = append(rows, []any{
			tableName,
			col.Name,
			rowCount,
			acc.nullCount,
			int64(len(acc.distinct)),
			acc.minVal,
			acc.maxVal,
		})
	}

	return &Result{
		Columns:      cols,
		Rows:         rows,
		RowsAffected: int64(len(rows)),
		Tag:          "ANALYZE",
	}, nil
}
