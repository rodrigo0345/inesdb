package executor

import (
	"fmt"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/storage"
)

func execUpdate(db engine.Database, txnID int64, stmt *ast.UpdateStatement) (*Result, error) {
	ts, err := db.GetTableSchema(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %q not found: %w", stmt.TableName, err)
	}

	cursor, err := db.Scan(txnID, stmt.TableName, storage.ScanOptions{Inclusive: true})
	if err != nil {
		return nil, fmt.Errorf("scan for update: %w", err)
	}

	// Collect matching rows before modifying (avoids cursor/write interference).
	type rowEntry struct {
		key   []byte
		value []byte
	}
	var matches []rowEntry
	for cursor.Next() {
		e := cursor.Entry()

		if stmt.Where != nil {
			row, decErr := DecodeRow(ts, e.Value)
			if decErr != nil {
				continue
			}
			res, predErr := evalExpr(stmt.Where, row)
			if predErr != nil {
				continue
			}
			if b, ok := res.(bool); !ok || !b {
				continue
			}
		}

		k := make([]byte, len(e.Key))
		v := make([]byte, len(e.Value))
		copy(k, e.Key)
		copy(v, e.Value)
		matches = append(matches, rowEntry{key: k, value: v})
	}
	if scanErr := cursor.Error(); scanErr != nil {
		cursor.Close()
		return nil, scanErr
	}
	cursor.Close()

	var updated int64
	for _, m := range matches {
		row, decErr := DecodeRow(ts, m.value)
		if decErr != nil {
			continue
		}

		// Apply SET assignments.
		for _, assign := range stmt.Assignments {
			colName := ""
			if id, ok := assign.Column.(*ast.Identifier); ok {
				colName = id.Name
			} else {
				colName = assign.Column.TokenLiteral()
			}
			newVal, evalErr := evalExpr(assign.Value, row)
			if evalErr != nil {
				return nil, fmt.Errorf("evaluate SET value for %s: %w", colName, evalErr)
			}
			row[colName] = newVal
		}

		// Re-encode and write back with the same key.
		rowBytes, _, encErr := EncodeRow(ts, row)
		if encErr != nil {
			return nil, fmt.Errorf("re-encode row: %w", encErr)
		}
		if writeErr := db.Write(txnID, stmt.TableName, m.key, rowBytes); writeErr != nil {
			return nil, fmt.Errorf("write updated row: %w", writeErr)
		}
		updated++
	}

	return &Result{
		RowsAffected: updated,
		Tag:          fmt.Sprintf("UPDATE %d", updated),
	}, nil
}

func execDelete(db engine.Database, txnID int64, stmt *ast.DeleteStatement) (*Result, error) {
	ts, err := db.GetTableSchema(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %q not found: %w", stmt.TableName, err)
	}

	cursor, err := db.Scan(txnID, stmt.TableName, storage.ScanOptions{Inclusive: true})
	if err != nil {
		return nil, fmt.Errorf("scan for delete: %w", err)
	}

	// Collect keys first.
	var keys [][]byte
	for cursor.Next() {
		e := cursor.Entry()

		if stmt.Where != nil {
			row, decErr := DecodeRow(ts, e.Value)
			if decErr != nil {
				continue
			}
			res, predErr := evalExpr(stmt.Where, row)
			if predErr != nil {
				continue
			}
			if b, ok := res.(bool); !ok || !b {
				continue
			}
		}

		k := make([]byte, len(e.Key))
		copy(k, e.Key)
		keys = append(keys, k)
	}
	if scanErr := cursor.Error(); scanErr != nil {
		cursor.Close()
		return nil, scanErr
	}
	cursor.Close()

	var deleted int64
	for _, k := range keys {
		if delErr := db.Delete(txnID, stmt.TableName, k); delErr != nil {
			return nil, fmt.Errorf("delete row: %w", delErr)
		}
		deleted++
	}

	return &Result{
		RowsAffected: deleted,
		Tag:          fmt.Sprintf("DELETE %d", deleted),
	}, nil
}
