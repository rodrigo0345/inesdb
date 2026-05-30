package executor

import (
	"fmt"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/schema"
)

func execSelect(db database.Database, txnID int64, stmt *ast.SelectStatement) (*Result, error) {
	// Handle SELECT without a table (e.g. SELECT 1).
	tableName := stmt.TableName
	if tableName == "" && len(stmt.From) > 0 {
		tableName = stmt.From[0].Name
	}
	if tableName == "" {
		return execSelectConst(stmt), nil
	}

	ts, err := db.GetTableSchema(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %q not found: %w", tableName, err)
	}

	// Build scan options. WHERE filtering is done in the executor loop (not as
	// a ComplexFilter) because the raw cursor entries still carry the MVCC op
	// byte prefix; we need the MVCC-decoded value first.
	opts := storage.ScanOptions{Inclusive: true}

	cursor, err := db.Scan(txnID, tableName, opts)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	defer cursor.Close()

	// Wrap with SortCursor for ORDER BY.
	if len(stmt.OrderBy) > 0 {
		cursor = buildSortCursor(cursor, ts, stmt.OrderBy)
	}

	// Resolve column projection.
	targetCols := resolveColumns(ts, stmt.Columns)

	// Apply OFFSET/LIMIT after WHERE filtering.
	offset := 0
	limit := 0
	if stmt.Offset != nil {
		offset = *stmt.Offset
	}
	if stmt.Limit != nil {
		limit = *stmt.Limit
	}

	var rows [][]any
	skipped := 0
	for cursor.Next() {
		entry := cursor.Entry()
		row, decErr := DecodeRow(ts, entry.Value)
		if decErr != nil {
			continue
		}

		// Apply WHERE predicate.
		if stmt.Where != nil {
			result, predErr := evalExpr(stmt.Where, row)
			if predErr != nil || result == nil {
				continue
			}
			if b, ok := result.(bool); !ok || !b {
				continue
			}
		}

		// OFFSET/LIMIT.
		if skipped < offset {
			skipped++
			continue
		}
		if limit > 0 && len(rows) >= limit {
			break
		}

		projected := make([]any, len(targetCols))
		for i, col := range targetCols {
			projected[i] = row[col]
		}
		rows = append(rows, projected)
	}
	if err := cursor.Error(); err != nil {
		return nil, err
	}

	return &Result{
		Columns:      targetCols,
		Rows:         rows,
		RowsAffected: int64(len(rows)),
		Tag:          fmt.Sprintf("SELECT %d", len(rows)),
	}, nil
}

// execSelectConst handles SELECT without a FROM clause (e.g. SELECT 1).
func execSelectConst(stmt *ast.SelectStatement) *Result {
	var row []any
	var cols []string
	for i, col := range stmt.Columns {
		cols = append(cols, fmt.Sprintf("?column%d?", i+1))
		if lit, ok := col.(*ast.LiteralValue); ok {
			row = append(row, ToNative(fmt.Sprintf("%v", lit.Value), lit.Type))
		} else {
			row = append(row, col.TokenLiteral())
		}
	}
	return &Result{Columns: cols, Rows: [][]any{row}, RowsAffected: 1, Tag: "SELECT 1"}
}

// resolveColumns returns the list of column names to include in the result.
func resolveColumns(ts schema.ITableSchema, exprs []ast.Expression) []string {
	userCols := ts.GetColumns()
	if len(exprs) == 0 {
		names := make([]string, len(userCols))
		for i, c := range userCols {
			names[i] = c.Name
		}
		return names
	}
	var names []string
	for _, e := range exprs {
		lit := e.TokenLiteral()
		if lit == "*" {
			for _, c := range userCols {
				names = append(names, c.Name)
			}
			continue
		}
		if id, ok := e.(*ast.Identifier); ok {
			names = append(names, id.Name)
		} else {
			names = append(names, lit)
		}
	}
	return names
}

// buildSortCursor wraps the cursor with a SortCursor based on ORDER BY clauses.
func buildSortCursor(base storage.ICursor, ts schema.ITableSchema, orderBy []ast.OrderByExpression) storage.ICursor {
	if len(orderBy) == 0 {
		return base
	}
	// Use only the first ORDER BY column for simplicity.
	first := orderBy[0]
	colName := ""
	if id, ok := first.Expression.(*ast.Identifier); ok {
		colName = id.Name
	}
	if colName == "" {
		return base
	}

	colNameCopy := colName
	ascending := first.Ascending

	lessFn := func(a, b storage.ScanEntry) bool {
		rowA, errA := DecodeRow(ts, a.Value)
		rowB, errB := DecodeRow(ts, b.Value)
		if errA != nil || errB != nil {
			return false
		}
		cmp, err := compareOrd(rowA[colNameCopy], rowB[colNameCopy])
		if err != nil {
			return false
		}
		if ascending {
			return cmp < 0
		}
		return cmp > 0
	}

	return schema.NewSortCursor(base, lessFn)
}
