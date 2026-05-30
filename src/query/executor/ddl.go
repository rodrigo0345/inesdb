package executor

import (
	"fmt"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/storage/schema"
)

func execCreateTable(db engine.Database, stmt *ast.CreateTableStatement) (*Result, error) {
	cols := []schema.Column{}
	for _, colDef := range stmt.Columns {
		dt, err := MapSQLType(colDef.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", colDef.Name, err)
		}
		cols = append(cols, schema.Column{Name: colDef.Name, Type: dt})
	}

	ts := schema.NewTableSchema(stmt.Name, cols)

	// Determine primary key columns.
	pkCols := detectPrimaryKey(stmt)
	if len(pkCols) > 0 {
		// AddIndex is called with nil engine; Engine.CreateTable will set the real engine.
		ts.AddIndex("PRIMARY", pkCols, nil)
	}
	// If no PK detected, Engine.CreateTable falls back to first column.

	if err := db.CreateTable(ts); err != nil {
		if stmt.IfNotExists {
			// Swallow "already exists" errors.
			if strings.Contains(err.Error(), "already exists") {
				return &Result{Tag: "CREATE TABLE"}, nil
			}
		}
		return nil, err
	}
	return &Result{Tag: "CREATE TABLE"}, nil
}

func execDropTable(db engine.Database, stmt *ast.DropStatement) (*Result, error) {
	if strings.ToUpper(stmt.ObjectType) != "TABLE" {
		return nil, fmt.Errorf("DROP %s is not supported", stmt.ObjectType)
	}
	for _, name := range stmt.Names {
		if err := db.DropTable(name); err != nil {
			if stmt.IfExists && strings.Contains(err.Error(), "not exist") {
				continue
			}
			return nil, err
		}
	}
	return &Result{Tag: "DROP TABLE"}, nil
}

// detectPrimaryKey finds the primary key columns from a CREATE TABLE statement.
// It checks table-level constraints first, then column-level constraints.
func detectPrimaryKey(stmt *ast.CreateTableStatement) []string {
	// Table-level constraint: PRIMARY KEY (col1, col2, ...)
	for _, c := range stmt.Constraints {
		if strings.EqualFold(c.Type, "PRIMARY KEY") && len(c.Columns) > 0 {
			return c.Columns
		}
	}
	// Column-level constraint: col INT PRIMARY KEY
	for _, col := range stmt.Columns {
		for _, cc := range col.Constraints {
			if strings.EqualFold(cc.Type, "PRIMARY KEY") {
				return []string{col.Name}
			}
		}
	}
	return nil
}
