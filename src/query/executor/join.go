package executor

import (
	"fmt"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/storage/schema"
)

// colRef describes one output column in a JOIN result.
type colRef struct {
	displayName string // column name shown in Result.Columns
	rowKey      string // key used to look up the value in the merged row map
}

// execJoin handles SELECT statements that contain at least one JOIN clause.
// It supports INNER JOIN and LEFT [OUTER] JOIN via nested-loop execution.
func execJoin(db engine.Database, txnID int64, stmt *ast.SelectStatement) (*Result, error) {
	if len(stmt.From) == 0 {
		return nil, fmt.Errorf("JOIN query must have a FROM clause")
	}

	baseRef := stmt.From[0]
	baseTS, err := db.GetTableSchema(baseRef.Name)
	if err != nil {
		return nil, fmt.Errorf("table %q not found: %w", baseRef.Name, err)
	}

	// Seed the accumulated result with all rows from the base table.
	// We don't filter with WHERE yet — LEFT JOIN requires seeing all left rows.
	baseRows, err := collectMatchingRows(db, txnID, baseRef.Name, baseTS, nil)
	if err != nil {
		return nil, fmt.Errorf("scan %q: %w", baseRef.Name, err)
	}

	// Track which table references contribute columns (for SELECT *).
	allRefs := []ast.TableReference{baseRef}
	allSchemas := map[string]schema.ITableSchema{baseRef.Name: baseTS}

	// accumulated is a slice of merged row maps built up join-by-join.
	accumulated := make([]map[string]any, len(baseRows))
	for i, br := range baseRows {
		accumulated[i] = qualifyRow(br.row, baseTS, baseRef)
	}

	// Apply each JOIN clause left-to-right (left-linear).
	for _, jc := range stmt.Joins {
		joinType := strings.ToUpper(strings.TrimSpace(jc.Type))
		if joinType != "INNER" && joinType != "LEFT" && joinType != "LEFT OUTER" {
			return nil, fmt.Errorf("unsupported JOIN type %q; only INNER and LEFT JOIN are supported", jc.Type)
		}

		rightRef := jc.Right
		rightTS, err := db.GetTableSchema(rightRef.Name)
		if err != nil {
			return nil, fmt.Errorf("table %q not found: %w", rightRef.Name, err)
		}

		rightRows, err := collectMatchingRows(db, txnID, rightRef.Name, rightTS, nil)
		if err != nil {
			return nil, fmt.Errorf("scan %q: %w", rightRef.Name, err)
		}

		allRefs = append(allRefs, rightRef)
		allSchemas[rightRef.Name] = rightTS

		isLeft := joinType == "LEFT" || joinType == "LEFT OUTER"
		emptyRight := nullRow(rightTS, rightRef)

		var next []map[string]any
		for _, leftMerged := range accumulated {
			matched := false
			for _, rr := range rightRows {
				rightMerged := qualifyRow(rr.row, rightTS, rightRef)
				candidate := mergeMaps(leftMerged, rightMerged)
				if evalPredicate(jc.Condition, candidate) {
					next = append(next, candidate)
					matched = true
				}
			}
			if isLeft && !matched {
				next = append(next, mergeMaps(leftMerged, emptyRight))
			}
		}
		accumulated = next
	}

	// Apply the post-join WHERE predicate.
	var filtered []map[string]any
	for _, row := range accumulated {
		if evalPredicate(stmt.Where, row) {
			filtered = append(filtered, row)
		}
	}

	// Resolve output columns.
	cols := resolveJoinColumns(stmt, allRefs, allSchemas)

	// Apply ORDER BY (single expression, in-memory).
	if len(stmt.OrderBy) > 0 {
		first := stmt.OrderBy[0]
		sortRowsByExpr(filtered, first.Expression, first.Ascending)
	}

	// Apply OFFSET / LIMIT.
	offset := 0
	limit := 0
	if stmt.Offset != nil {
		offset = *stmt.Offset
	}
	if stmt.Limit != nil {
		limit = *stmt.Limit
	}

	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.displayName
	}

	var rows [][]any
	for i, row := range filtered {
		if i < offset {
			continue
		}
		if limit > 0 && len(rows) >= limit {
			break
		}
		projected := make([]any, len(cols))
		for j, c := range cols {
			projected[j] = row[c.rowKey]
		}
		rows = append(rows, projected)
	}

	return &Result{
		Columns:      colNames,
		Rows:         rows,
		RowsAffected: int64(len(rows)),
		Tag:          fmt.Sprintf("SELECT %d", len(rows)),
	}, nil
}

// qualifyRow adds qualified keys ("alias.col", "tableName.col") to a decoded row map.
// Unqualified keys are also preserved (they come from DecodeRow directly).
func qualifyRow(row map[string]any, ts schema.ITableSchema, ref ast.TableReference) map[string]any {
	out := make(map[string]any, len(row)*3)
	for _, col := range ts.GetColumns() {
		v := row[col.Name]
		out[col.Name] = v
		out[ref.Name+"."+col.Name] = v
		if ref.Alias != "" {
			out[ref.Alias+"."+col.Name] = v
		}
	}
	return out
}

// nullRow builds a merged row with nil values for all columns of ts.
func nullRow(ts schema.ITableSchema, ref ast.TableReference) map[string]any {
	out := make(map[string]any)
	for _, col := range ts.GetColumns() {
		out[col.Name] = nil
		out[ref.Name+"."+col.Name] = nil
		if ref.Alias != "" {
			out[ref.Alias+"."+col.Name] = nil
		}
	}
	return out
}

// mergeMaps creates a new map containing all key-value pairs from both inputs.
// Right-side keys overwrite left-side keys on conflict (used for unqualified names).
func mergeMaps(left, right map[string]any) map[string]any {
	out := make(map[string]any, len(left)+len(right))
	for k, v := range left {
		out[k] = v
	}
	for k, v := range right {
		out[k] = v
	}
	return out
}

// resolveJoinColumns returns the display names and row-map keys for a JOIN result.
// SELECT * expands to all columns from all tables using "tableName.colName" keys.
// SELECT u.id produces displayName="id", rowKey="u.id".
func resolveJoinColumns(
	stmt *ast.SelectStatement,
	tableRefs []ast.TableReference,
	schemas map[string]schema.ITableSchema,
) []colRef {
	// SELECT * — expand all tables in order.
	isStarOnly := len(stmt.Columns) == 0 ||
		(len(stmt.Columns) == 1 && stmt.Columns[0].TokenLiteral() == "*")

	if isStarOnly {
		var cols []colRef
		for _, ref := range tableRefs {
			ts := schemas[ref.Name]
			if ts == nil {
				continue
			}
			for _, col := range ts.GetColumns() {
				key := ref.Name + "." + col.Name
				display := col.Name
				cols = append(cols, colRef{displayName: display, rowKey: key})
			}
		}
		return cols
	}

	var cols []colRef
	for _, expr := range stmt.Columns {
		lit := expr.TokenLiteral()
		if lit == "*" {
			// Embedded * within a mixed list — expand all.
			for _, ref := range tableRefs {
				ts := schemas[ref.Name]
				if ts == nil {
					continue
				}
				for _, col := range ts.GetColumns() {
					cols = append(cols, colRef{
						displayName: col.Name,
						rowKey:      ref.Name + "." + col.Name,
					})
				}
			}
			continue
		}

		id, ok := expr.(*ast.Identifier)
		if !ok {
			cols = append(cols, colRef{displayName: lit, rowKey: lit})
			continue
		}

		if id.Table != "" {
			// Qualified: u.id → rowKey="u.id", displayName="id".
			cols = append(cols, colRef{
				displayName: id.Name,
				rowKey:      id.Table + "." + id.Name,
			})
		} else {
			// Unqualified: id → rowKey="id".
			cols = append(cols, colRef{displayName: id.Name, rowKey: id.Name})
		}
	}
	return cols
}

// sortRowsByExpr sorts the merged row slice by evaluating expr against each row.
// Handles both column identifiers and function calls (e.g. vec_dist).
func sortRowsByExpr(rows []map[string]any, expr ast.Expression, ascending bool) {
	// Insertion sort — suitable for small result sets.
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			va, _ := evalExpr(expr, rows[j-1])
			vb, _ := evalExpr(expr, rows[j])
			cmp, err := compareOrd(va, vb)
			if err != nil {
				break
			}
			if (ascending && cmp > 0) || (!ascending && cmp < 0) {
				rows[j-1], rows[j] = rows[j], rows[j-1]
			} else {
				break
			}
		}
	}
}
