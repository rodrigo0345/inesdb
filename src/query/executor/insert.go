package executor

import (
	"fmt"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/src/engine"
)

func execInsert(db engine.Database, txnID int64, stmt *ast.InsertStatement) (*Result, error) {
	ts, err := db.GetTableSchema(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %q not found: %w", stmt.TableName, err)
	}

	// Resolve column names from the INSERT column list.
	colNames := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		if id, ok := col.(*ast.Identifier); ok {
			colNames[i] = id.Name
		} else {
			colNames[i] = col.TokenLiteral()
		}
	}

	var inserted int64
	for _, rowExprs := range stmt.Values {
		values := make(map[string]any, len(colNames))
		for i, expr := range rowExprs {
			if i >= len(colNames) {
				break
			}
			v, evalErr := evalExpr(expr, nil)
			if evalErr != nil {
				return nil, fmt.Errorf("evaluate value for column %s: %w", colNames[i], evalErr)
			}
			values[colNames[i]] = v
		}

		rowBytes, primaryKey, encErr := EncodeRow(ts, values)
		if encErr != nil {
			return nil, fmt.Errorf("encode row: %w", encErr)
		}

		if writeErr := db.Write(txnID, stmt.TableName, primaryKey, rowBytes); writeErr != nil {
			return nil, fmt.Errorf("write row: %w", writeErr)
		}
		inserted++
	}

	return &Result{
		RowsAffected: inserted,
		Tag:          fmt.Sprintf("INSERT 0 %d", inserted),
	}, nil
}
