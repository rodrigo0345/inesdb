package executor

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/google/uuid"
	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/storage"
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

	// When no column list is given (positional INSERT), default to schema order.
	if len(colNames) == 0 {
		for _, col := range ts.GetColumns() {
			colNames = append(colNames, col.Name)
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

		// Auto-fill AUTO_INCREMENT and UUID columns, then validate NOT NULL.
		for _, col := range ts.GetColumns() {
			if _, provided := values[col.Name]; provided {
				continue
			}
			if col.AutoIncrement {
				if sp, ok := db.(interface {
					NextSequenceValue(tableName, colName string) int64
				}); ok {
					values[col.Name] = sp.NextSequenceValue(stmt.TableName, col.Name)
				}
			} else if col.IsUUID {
				values[col.Name] = uuid.New().String()
			} else if col.Nullable {
				values[col.Name] = nil // omitted nullable column → SQL NULL
			} else {
				return nil, fmt.Errorf(
					"ERROR:  null value in column %q of relation %q violates not-null constraint\nDETAIL:  Failing row contains a null value in column %q.",
					col.Name, stmt.TableName, col.Name,
				)
			}
		}

		// Validate that explicitly provided NULL values don't violate NOT NULL.
		for _, col := range ts.GetColumns() {
			val, provided := values[col.Name]
			if provided && val == nil && !col.Nullable {
				return nil, fmt.Errorf(
					"ERROR:  null value in column %q of relation %q violates not-null constraint\nDETAIL:  Failing row contains a null value in column %q.",
					col.Name, stmt.TableName, col.Name,
				)
			}
		}

		// Enforce UNIQUE constraints before writing.
		if err := checkUniqueConstraints(db, txnID, stmt.TableName, values, nil); err != nil {
			return nil, err
		}

		// Enforce FOREIGN KEY constraints before writing.
		if err := checkForeignKeyConstraints(db, txnID, stmt.TableName, values); err != nil {
			return nil, err
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

// checkUniqueConstraints scans the table for any existing row that already
// holds the same value for a UNIQUE column.
// excludeKey is the user-space primary key of the row being updated, so it is
// not flagged as a conflict with itself; pass nil on INSERT.
func checkUniqueConstraints(
	db engine.Database,
	txnID int64,
	tableName string,
	values map[string]any,
	excludeKey []byte,
) error {
	ts, err := db.GetTableSchema(tableName)
	if err != nil {
		return nil
	}

	// Collect columns that participate in a UNIQUE index.
	uniqueColSet := make(map[string]struct{})
	for _, idx := range ts.GetAllIndexes() {
		if idx.Unique {
			for _, col := range idx.Columns {
				uniqueColSet[col] = struct{}{}
			}
		}
	}
	if len(uniqueColSet) == 0 {
		return nil
	}

	// Only check columns that are both UNIQUE and being written.
	var toCheck []string
	for col := range uniqueColSet {
		if _, ok := values[col]; ok {
			toCheck = append(toCheck, col)
		}
	}
	if len(toCheck) == 0 {
		return nil
	}

	cursor, err := db.Scan(txnID, tableName, storage.ScanOptions{Inclusive: true})
	if err != nil {
		return nil // skip rather than block
	}
	defer cursor.Close()

	for cursor.Next() {
		entry := cursor.Entry()
		if excludeKey != nil && bytes.Equal(entry.Key, excludeKey) {
			continue
		}
		existing, decErr := DecodeRow(ts, entry.Value)
		if decErr != nil {
			continue
		}
		for _, colName := range toCheck {
			newVal := values[colName]
			if fmt.Sprintf("%v", existing[colName]) == fmt.Sprintf("%v", newVal) {
				return fmt.Errorf(
					"ERROR:  duplicate key value violates unique constraint on column %q\nDETAIL:  Key (%s)=(%v) already exists.",
					colName, colName, newVal,
				)
			}
		}
	}
	return nil
}

// checkForeignKeyConstraints verifies that every outbound FK in tableName is
// satisfied by `values`: each referenced row must already exist in its parent table.
func checkForeignKeyConstraints(
	db engine.Database,
	txnID int64,
	tableName string,
	values map[string]any,
) error {
	ts, err := db.GetTableSchema(tableName)
	if err != nil {
		return nil
	}

	for _, fk := range ts.GetForeignKeys() {
		// Only check when at least one local FK column is present in values.
		anyPresent := false
		for _, lc := range fk.LocalColumns {
			if _, ok := values[lc]; ok {
				anyPresent = true
				break
			}
		}
		if !anyPresent {
			continue
		}

		// Scan the referenced table to find a row matching all FK columns.
		refTS, err := db.GetTableSchema(fk.ReferencedTable)
		if err != nil {
			return fmt.Errorf(
				"ERROR:  insert or update on table %q violates foreign key constraint %q\nDETAIL:  Referenced table %q does not exist.",
				tableName, fk.Name, fk.ReferencedTable,
			)
		}
		_ = refTS

		cursor, err := db.Scan(txnID, fk.ReferencedTable, storage.ScanOptions{Inclusive: true})
		if err != nil {
			return fmt.Errorf(
				"ERROR:  insert or update on table %q violates foreign key constraint %q\nDETAIL:  Could not scan referenced table %q.",
				tableName, fk.Name, fk.ReferencedTable,
			)
		}

		found := false
		for cursor.Next() {
			entry := cursor.Entry()
			refRow, decErr := DecodeRow(refTS, entry.Value)
			if decErr != nil {
				continue
			}
			match := true
			for i, lc := range fk.LocalColumns {
				rc := fk.ReferencedColumns[i]
				localVal := values[lc]
				refVal := refRow[rc]
				if fmt.Sprintf("%v", localVal) != fmt.Sprintf("%v", refVal) {
					match = false
					break
				}
			}
			if match {
				found = true
				break
			}
		}
		cursor.Close()

		if !found {
			localCols := strings.Join(fk.LocalColumns, ", ")
			var valParts []string
			for _, lc := range fk.LocalColumns {
				valParts = append(valParts, fmt.Sprintf("%v", values[lc]))
			}
			return fmt.Errorf(
				"ERROR:  insert or update on table %q violates foreign key constraint %q\nDETAIL:  Key (%s)=(%s) is not present in table %q.",
				tableName, fk.Name,
				localCols, strings.Join(valParts, ", "),
				fk.ReferencedTable,
			)
		}
	}
	return nil
}
