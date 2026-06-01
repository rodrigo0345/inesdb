package executor

import (
	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/schema"
)

// rowEntry holds the raw storage bytes and decoded column map for one scanned row.
type rowEntry struct {
	key   []byte
	value []byte
	row   map[string]any
}

// evalPredicate evaluates expr against row and returns whether the row passes.
// Returns true when expr is nil (no filter). Returns false for errors, non-bool
// results, and SQL NULL — consistent with SQL three-valued logic.
func evalPredicate(expr ast.Expression, row map[string]any) bool {
	if expr == nil {
		return true
	}
	result, err := evalExpr(expr, row)
	if err != nil || result == nil {
		return false
	}
	b, ok := result.(bool)
	return ok && b
}

// collectMatchingRows performs a full table scan and returns all rows that
// satisfy the WHERE expression. Pass nil for where to collect every row.
func collectMatchingRows(
	db engine.Database,
	txnID int64,
	tableName string,
	ts schema.ITableSchema,
	where ast.Expression,
) ([]rowEntry, error) {
	cursor, err := db.Scan(txnID, tableName, storage.ScanOptions{Inclusive: true})
	if err != nil {
		return nil, err
	}

	var results []rowEntry
	for cursor.Next() {
		e := cursor.Entry()

		row, decErr := DecodeRow(ts, e.Value)
		if decErr != nil {
			continue
		}

		if !evalPredicate(where, row) {
			continue
		}

		k := make([]byte, len(e.Key))
		v := make([]byte, len(e.Value))
		copy(k, e.Key)
		copy(v, e.Value)
		results = append(results, rowEntry{key: k, value: v, row: row})
	}

	if scanErr := cursor.Error(); scanErr != nil {
		cursor.Close()
		return nil, scanErr
	}
	cursor.Close()
	return results, nil
}
