package executor

import (
	"fmt"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/schema"
)

func execSelect(db engine.Database, txnID int64, stmt *ast.SelectStatement) (*Result, error) {
	// Route to JOIN executor when JOIN clauses are present.
	if len(stmt.Joins) > 0 {
		return execJoin(db, txnID, stmt)
	}

	// If ORDER BY is a function call expression, use in-memory sort with error propagation.
	if len(stmt.OrderBy) > 0 {
		if _, isFunc := stmt.OrderBy[0].Expression.(*ast.FunctionCall); isFunc {
			return execSelectWithFuncOrderBy(db, txnID, stmt)
		}
	}

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
		if !evalPredicate(stmt.Where, row) {
			continue
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

// execVecDistKNN is the fast path for ORDER BY vec_dist(...) LIMIT k on
// VECTOR-engine tables. It calls the HNSW index directly, bypassing the full
// table scan. Returns (nil, nil) to signal the caller to fall back to the
// generic sort path (e.g. when the table uses a non-VECTOR backend).
func execVecDistKNN(
	db engine.Database, txnID int64,
	tableName string, ts schema.ITableSchema,
	stmt *ast.SelectStatement, fn *ast.FunctionCall,
) (*Result, error) {
	if len(fn.Arguments) != 2 {
		return nil, nil
	}

	// Extract query vector from the second argument (always a literal).
	queryRaw, err := evalExpr(fn.Arguments[1], map[string]any{})
	if err != nil || queryRaw == nil {
		return nil, nil
	}
	queryVec, err := toFloat32Slice(queryRaw)
	if err != nil {
		return nil, nil
	}

	offset, limit := 0, 0
	if stmt.Offset != nil {
		offset = *stmt.Offset
	}
	if stmt.Limit != nil {
		limit = *stmt.Limit
	}
	k := limit + offset
	if k <= 0 {
		k = 100 // default cap when no LIMIT is specified
	}

	entries, err := db.VectorSearch(txnID, tableName, queryVec, k)
	if err != nil {
		return nil, err
	}
	if entries == nil {
		return nil, nil // not a VECTOR-engine table; fall through to full scan
	}

	targetCols := resolveColumns(ts, stmt.Columns)
	var rows [][]any
	for i, entry := range entries {
		if i < offset {
			continue
		}
		if limit > 0 && len(rows) >= limit {
			break
		}
		row, decErr := DecodeRow(ts, entry.Value)
		if decErr != nil {
			continue
		}
		projected := make([]any, len(targetCols))
		for j, col := range targetCols {
			projected[j] = row[col]
		}
		rows = append(rows, projected)
	}
	return &Result{
		Columns:      targetCols,
		Rows:         rows,
		RowsAffected: int64(len(rows)),
		Tag:          fmt.Sprintf("SELECT %d", len(rows)),
	}, nil
}

// execSelectWithFuncOrderBy handles single-table SELECT where ORDER BY is a function
// call (e.g. vec_dist). It collects all rows in memory so errors from function
// evaluation propagate correctly.
func execSelectWithFuncOrderBy(db engine.Database, txnID int64, stmt *ast.SelectStatement) (*Result, error) {
	tableName := stmt.TableName
	if tableName == "" && len(stmt.From) > 0 {
		tableName = stmt.From[0].Name
	}

	ts, err := db.GetTableSchema(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %q not found: %w", tableName, err)
	}

	// Fast path: use HNSW index for vec_dist queries on VECTOR-engine tables.
	if len(stmt.OrderBy) > 0 && stmt.Where == nil {
		if fn, ok := stmt.OrderBy[0].Expression.(*ast.FunctionCall); ok &&
			strings.EqualFold(fn.Name, "vec_dist") {
			if result, err := execVecDistKNN(db, txnID, tableName, ts, stmt, fn); result != nil || err != nil {
				return result, err
			}
			// nil, nil means fall through to full-scan sort below
		}
	}

	matches, err := collectMatchingRows(db, txnID, tableName, ts, stmt.Where)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	// Validate function expression on first row (surfaces errors like dimension mismatch).
	if len(matches) > 0 && len(stmt.OrderBy) > 0 {
		if _, testErr := evalExpr(stmt.OrderBy[0].Expression, matches[0].row); testErr != nil {
			return nil, testErr
		}
	}

	// Sort in-memory using the function expression.
	if len(stmt.OrderBy) > 0 {
		merged := make([]map[string]any, len(matches))
		for i, m := range matches {
			merged[i] = m.row
		}
		sortRowsByExpr(merged, stmt.OrderBy[0].Expression, stmt.OrderBy[0].Ascending)
		for i, m := range merged {
			matches[i].row = m
		}
	}

	targetCols := resolveColumns(ts, stmt.Columns)
	offset := 0
	limit := 0
	if stmt.Offset != nil {
		offset = *stmt.Offset
	}
	if stmt.Limit != nil {
		limit = *stmt.Limit
	}

	var rows [][]any
	for i, m := range matches {
		if i < offset {
			continue
		}
		if limit > 0 && len(rows) >= limit {
			break
		}
		projected := make([]any, len(targetCols))
		for j, col := range targetCols {
			projected[j] = m.row[col]
		}
		rows = append(rows, projected)
	}

	return &Result{
		Columns:      targetCols,
		Rows:         rows,
		RowsAffected: int64(len(rows)),
		Tag:          fmt.Sprintf("SELECT %d", len(rows)),
	}, nil
}

// buildSortCursor wraps the cursor with a SortCursor based on ORDER BY clauses.
// Supports both column identifiers and function call expressions (e.g. vec_dist).
func buildSortCursor(base storage.ICursor, ts schema.ITableSchema, orderBy []ast.OrderByExpression) storage.ICursor {
	if len(orderBy) == 0 {
		return base
	}
	// Use only the first ORDER BY expression for simplicity.
	first := orderBy[0]
	expr := first.Expression
	ascending := first.Ascending

	lessFn := func(a, b storage.ScanEntry) bool {
		rowA, errA := DecodeRow(ts, a.Value)
		rowB, errB := DecodeRow(ts, b.Value)
		if errA != nil || errB != nil {
			return false
		}
		va, _ := evalExpr(expr, rowA)
		vb, _ := evalExpr(expr, rowB)
		cmp, err := compareOrd(va, vb)
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
