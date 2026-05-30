package executor

import (
	"fmt"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/internal/database"
)

// Result holds the output of a successfully executed SQL statement.
type Result struct {
	Columns      []string // column names for SELECT results
	Rows         [][]any  // decoded rows; nil for non-SELECT statements
	RowsAffected int64
	Tag          string // command tag: "SELECT", "INSERT 0 N", "UPDATE N", "DELETE N", etc.
}

// Execute dispatches a single GoSQLX AST statement and returns its result.
// txnID must be a valid active transaction obtained from db.BeginTransaction().
func Execute(db database.Database, txnID int64, stmt ast.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *ast.SelectStatement:
		return execSelect(db, txnID, s)
	case *ast.InsertStatement:
		return execInsert(db, txnID, s)
	case *ast.UpdateStatement:
		return execUpdate(db, txnID, s)
	case *ast.DeleteStatement:
		return execDelete(db, txnID, s)
	case *ast.CreateTableStatement:
		return execCreateTable(db, s)
	case *ast.DropStatement:
		return execDropTable(db, s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}
