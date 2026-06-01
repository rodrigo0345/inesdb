package executor

import (
	"fmt"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/storage"
	"github.com/rodrigo0345/omag/src/storage/schema"
)

func execUpdate(db engine.Database, txnID int64, stmt *ast.UpdateStatement) (*Result, error) {
	ts, err := db.GetTableSchema(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %q not found: %w", stmt.TableName, err)
	}

	// Collect matching rows before modifying (avoids cursor/write interference).
	matches, err := collectMatchingRows(db, txnID, stmt.TableName, ts, stmt.Where)
	if err != nil {
		return nil, fmt.Errorf("scan for update: %w", err)
	}

	var updated int64
	for _, m := range matches {
		row := m.row

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

		// Enforce UNIQUE constraints, excluding the current row from the check.
		if err := checkUniqueConstraints(db, txnID, stmt.TableName, row, m.key); err != nil {
			return nil, err
		}

		// Enforce NOT NULL constraints on updated values.
		for _, col := range ts.GetColumns() {
			if !col.Nullable {
				if row[col.Name] == nil {
					return nil, fmt.Errorf(
						"ERROR:  null value in column %q of relation %q violates not-null constraint\nDETAIL:  Failing row contains a null value in column %q.",
						col.Name, stmt.TableName, col.Name,
					)
				}
			}
		}

		// Enforce outbound FK constraints: new values must still satisfy all FKs.
		if err := checkForeignKeyConstraints(db, txnID, stmt.TableName, row); err != nil {
			return nil, err
		}

		// Enforce inbound FK constraints: if columns referenced by child tables change,
		// apply the ON UPDATE action for each referencing FK.
		oldRow, decErr2 := DecodeRow(ts, m.value)
		if decErr2 == nil {
			if err := handleInboundFKOnUpdate(db, txnID, stmt.TableName, oldRow, row); err != nil {
				return nil, err
			}
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

	// Collect matching rows first (avoids cursor/write interference).
	matches, err := collectMatchingRows(db, txnID, stmt.TableName, ts, stmt.Where)
	if err != nil {
		return nil, fmt.Errorf("scan for delete: %w", err)
	}

	var deleted int64
	for _, m := range matches {
		// Apply inbound FK ON DELETE before removing the row.
		if fkErr := handleInboundFKOnDelete(db, txnID, stmt.TableName, m.row); fkErr != nil {
			return nil, fkErr
		}

		if delErr := db.Delete(txnID, stmt.TableName, m.key); delErr != nil {
			return nil, fmt.Errorf("delete row: %w", delErr)
		}
		deleted++
	}

	return &Result{
		RowsAffected: deleted,
		Tag:          fmt.Sprintf("DELETE %d", deleted),
	}, nil
}

// handleInboundFKOnDelete applies ON DELETE actions for all tables that have
// a FK referencing `parentTable`. `parentRow` is the decoded row being deleted.
func handleInboundFKOnDelete(
	db engine.Database,
	txnID int64,
	parentTable string,
	parentRow map[string]any,
) error {
	for _, childTableName := range db.ListTables() {
		childTS, err := db.GetTableSchema(childTableName)
		if err != nil {
			continue
		}
		for _, fk := range childTS.GetForeignKeys() {
			if !strings.EqualFold(fk.ReferencedTable, parentTable) {
				continue
			}
			// Find child rows that match this FK.
			cursor, err := db.Scan(txnID, childTableName, storage.ScanOptions{Inclusive: true})
			if err != nil {
				continue
			}

			type childMatch struct {
				key   []byte
				value []byte
				row   map[string]any
			}
			var matches []childMatch

			for cursor.Next() {
				e := cursor.Entry()
				childRow, decErr := DecodeRow(childTS, e.Value)
				if decErr != nil {
					continue
				}
				if fkRowsMatch(childRow, fk.LocalColumns, parentRow, fk.ReferencedColumns) {
					k := make([]byte, len(e.Key))
					v := make([]byte, len(e.Value))
					copy(k, e.Key)
					copy(v, e.Value)
					matches = append(matches, childMatch{key: k, value: v, row: childRow})
				}
			}
			cursor.Close()

			if len(matches) == 0 {
				continue
			}

			action := strings.ToUpper(fk.OnDelete)
			switch action {
			case "CASCADE":
				for _, m := range matches {
					// Cascade recursively: apply inbound FK checks on the child before deleting.
					if nestedErr := handleInboundFKOnDelete(db, txnID, childTableName, m.row); nestedErr != nil {
						return nestedErr
					}
					if delErr := db.Delete(txnID, childTableName, m.key); delErr != nil {
						return fmt.Errorf("cascade delete in %q: %w", childTableName, delErr)
					}
				}
			case "SET NULL":
				for _, m := range matches {
					// Validate that child FK columns allow NULL.
					for _, lc := range fk.LocalColumns {
						col := findColumnInSchema(childTS, lc)
						if col != nil && !col.Nullable {
							return fmt.Errorf(
								"ERROR:  null value in column %q of relation %q violates not-null constraint\nDETAIL:  ON DELETE SET NULL cannot set non-nullable FK column %q to NULL.",
								lc, childTableName, lc,
							)
						}
					}
					for _, lc := range fk.LocalColumns {
						m.row[lc] = nil
					}
					updatedBytes, _, encErr := EncodeRow(childTS, m.row)
					if encErr != nil {
						return fmt.Errorf("set null encode in %q: %w", childTableName, encErr)
					}
					if writeErr := db.Write(txnID, childTableName, m.key, updatedBytes); writeErr != nil {
						return fmt.Errorf("set null write in %q: %w", childTableName, writeErr)
					}
				}
			case "", "RESTRICT", "NO ACTION":
				// Report the first referencing row.
				refCols := strings.Join(fk.ReferencedColumns, ", ")
				var refVals []string
				for _, rc := range fk.ReferencedColumns {
					refVals = append(refVals, fmt.Sprintf("%v", parentRow[rc]))
				}
				return fmt.Errorf(
					"ERROR:  update or delete on table %q violates foreign key constraint %q on table %q\nDETAIL:  Key (%s)=(%s) is still referenced from table %q.",
					parentTable, fk.Name, childTableName,
					refCols, strings.Join(refVals, ", "),
					childTableName,
				)
			default:
				return fmt.Errorf("unsupported ON DELETE action %q on constraint %q", fk.OnDelete, fk.Name)
			}
		}
	}
	return nil
}

// handleInboundFKOnUpdate applies ON UPDATE actions for all tables that have
// a FK referencing `parentTable`. `oldRow` and `newRow` are decoded before/after images.
func handleInboundFKOnUpdate(
	db engine.Database,
	txnID int64,
	parentTable string,
	oldRow map[string]any,
	newRow map[string]any,
) error {
	for _, childTableName := range db.ListTables() {
		childTS, err := db.GetTableSchema(childTableName)
		if err != nil {
			continue
		}
		for _, fk := range childTS.GetForeignKeys() {
			if !strings.EqualFold(fk.ReferencedTable, parentTable) {
				continue
			}
			// Check if any referenced column actually changed.
			changed := false
			for _, rc := range fk.ReferencedColumns {
				if fmt.Sprintf("%v", oldRow[rc]) != fmt.Sprintf("%v", newRow[rc]) {
					changed = true
					break
				}
			}
			if !changed {
				continue
			}

			// Find child rows that matched the OLD parent values.
			cursor, err := db.Scan(txnID, childTableName, storage.ScanOptions{Inclusive: true})
			if err != nil {
				continue
			}

			type childMatch struct {
				key   []byte
				value []byte
				row   map[string]any
			}
			var matches []childMatch

			for cursor.Next() {
				e := cursor.Entry()
				childRow, decErr := DecodeRow(childTS, e.Value)
				if decErr != nil {
					continue
				}
				if fkRowsMatch(childRow, fk.LocalColumns, oldRow, fk.ReferencedColumns) {
					k := make([]byte, len(e.Key))
					v := make([]byte, len(e.Value))
					copy(k, e.Key)
					copy(v, e.Value)
					matches = append(matches, childMatch{key: k, value: v, row: childRow})
				}
			}
			cursor.Close()

			if len(matches) == 0 {
				continue
			}

			action := strings.ToUpper(fk.OnUpdate)
			switch action {
			case "CASCADE":
				for _, m := range matches {
					// Update child FK columns to the new parent values.
					for i, lc := range fk.LocalColumns {
						rc := fk.ReferencedColumns[i]
						m.row[lc] = newRow[rc]
					}
					updatedBytes, _, encErr := EncodeRow(childTS, m.row)
					if encErr != nil {
						return fmt.Errorf("cascade update encode in %q: %w", childTableName, encErr)
					}
					if writeErr := db.Write(txnID, childTableName, m.key, updatedBytes); writeErr != nil {
						return fmt.Errorf("cascade update write in %q: %w", childTableName, writeErr)
					}
				}
			case "", "RESTRICT", "NO ACTION":
				refCols := strings.Join(fk.ReferencedColumns, ", ")
				var refVals []string
				for _, rc := range fk.ReferencedColumns {
					refVals = append(refVals, fmt.Sprintf("%v", oldRow[rc]))
				}
				return fmt.Errorf(
					"ERROR:  update or delete on table %q violates foreign key constraint %q on table %q\nDETAIL:  Key (%s)=(%s) is still referenced from table %q.",
					parentTable, fk.Name, childTableName,
					refCols, strings.Join(refVals, ", "),
					childTableName,
				)
			default:
				return fmt.Errorf("unsupported ON UPDATE action %q on constraint %q", fk.OnUpdate, fk.Name)
			}
		}
	}
	return nil
}

// fkRowsMatch returns true when all `localCols` in `childRow` match the
// corresponding `refCols` in `parentRow`.
func fkRowsMatch(childRow map[string]any, localCols []string, parentRow map[string]any, refCols []string) bool {
	for i, lc := range localCols {
		rc := refCols[i]
		if fmt.Sprintf("%v", childRow[lc]) != fmt.Sprintf("%v", parentRow[rc]) {
			return false
		}
	}
	return true
}

// findColumnInSchema returns the Column definition for the given name, or nil.
func findColumnInSchema(ts schema.ITableSchema, columnName string) *schema.Column {
	for _, col := range ts.GetColumns() {
		if col.Name == columnName {
			c := col
			return &c
		}
	}
	return nil
}
