package executor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/storage/schema"
)

// collectForeignKeyConstraints parses FK definitions from column-level REFERENCES
// clauses and table-level FOREIGN KEY constraints. It validates that the referenced
// table and columns exist at CREATE TABLE time.
func collectForeignKeyConstraints(db engine.Database, stmt *ast.CreateTableStatement, owningTable string) ([]schema.ForeignKey, error) {
	var fks []schema.ForeignKey

	// Column-level: col INT REFERENCES parent(col)
	for _, col := range stmt.Columns {
		for _, cc := range col.Constraints {
			if cc.References == nil {
				continue
			}
			ref := cc.References
			if err := validateFKReference(db, ref.Table, ref.Columns); err != nil {
				return nil, fmt.Errorf("column %s: %w", col.Name, err)
			}
			fk := schema.ForeignKey{
				LocalColumns:      []string{col.Name},
				ReferencedTable:   ref.Table,
				ReferencedColumns: ref.Columns,
				OnDelete:          strings.ToUpper(ref.OnDelete),
				OnUpdate:          strings.ToUpper(ref.OnUpdate),
			}
			fk.Name = autoFKName(owningTable, fk.LocalColumns)
			if err := validateFKAction(fk.OnDelete); err != nil {
				return nil, fmt.Errorf("column %s ON DELETE: %w", col.Name, err)
			}
			if err := validateFKAction(fk.OnUpdate); err != nil {
				return nil, fmt.Errorf("column %s ON UPDATE: %w", col.Name, err)
			}
			fks = append(fks, fk)
		}
	}

	// Table-level: FOREIGN KEY (cols) REFERENCES parent(cols)
	for _, c := range stmt.Constraints {
		if !strings.EqualFold(c.Type, "FOREIGN KEY") || c.References == nil {
			continue
		}
		ref := c.References
		if err := validateFKReference(db, ref.Table, ref.Columns); err != nil {
			return nil, fmt.Errorf("foreign key constraint: %w", err)
		}
		fk := schema.ForeignKey{
			Name:              c.Name,
			LocalColumns:      c.Columns,
			ReferencedTable:   ref.Table,
			ReferencedColumns: ref.Columns,
			OnDelete:          strings.ToUpper(ref.OnDelete),
			OnUpdate:          strings.ToUpper(ref.OnUpdate),
		}
		if fk.Name == "" {
			fk.Name = autoFKName(owningTable, fk.LocalColumns)
		}
		if err := validateFKAction(fk.OnDelete); err != nil {
			return nil, fmt.Errorf("foreign key %s ON DELETE: %w", fk.Name, err)
		}
		if err := validateFKAction(fk.OnUpdate); err != nil {
			return nil, fmt.Errorf("foreign key %s ON UPDATE: %w", fk.Name, err)
		}
		fks = append(fks, fk)
	}

	return fks, nil
}

func autoFKName(owningTable string, localCols []string) string {
	return "fk_" + owningTable + "_" + strings.Join(localCols, "_")
}

func validateFKReference(db engine.Database, referencedTable string, referencedCols []string) error {
	ts, err := db.GetTableSchema(referencedTable)
	if err != nil {
		return fmt.Errorf("referenced table %q does not exist", referencedTable)
	}
	colSet := make(map[string]struct{})
	for _, col := range ts.GetColumns() {
		colSet[col.Name] = struct{}{}
	}
	for _, col := range referencedCols {
		if _, ok := colSet[col]; !ok {
			return fmt.Errorf("referenced column %q not found in table %q", col, referencedTable)
		}
	}
	return nil
}

func validateFKAction(action string) error {
	switch action {
	case "", "RESTRICT", "NO ACTION", "CASCADE", "SET NULL":
		return nil
	case "SET DEFAULT":
		return fmt.Errorf("action %q is not supported", action)
	default:
		return fmt.Errorf("unknown referential action %q", action)
	}
}

// parseVectorDimension extracts the dimension n from a "VECTOR(n)" type string.
// Returns 0 if the string is not in that form.
func parseVectorDimension(sqlType string) int {
	s := strings.ToUpper(strings.TrimSpace(sqlType))
	if !strings.HasPrefix(s, "VECTOR(") || !strings.HasSuffix(s, ")") {
		return 0
	}
	inner := s[len("VECTOR(") : len(s)-1]
	n, err := strconv.Atoi(strings.TrimSpace(inner))
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

func execCreateTable(db engine.Database, stmt *ast.CreateTableStatement) (*Result, error) {
	cols := []schema.Column{}
	for _, colDef := range stmt.Columns {
		sqlType := strings.ToUpper(strings.TrimSpace(colDef.Type))

		dt, err := MapSQLType(colDef.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", colDef.Name, err)
		}

		// Columns are nullable by default (SQL standard).
		col := schema.Column{Name: colDef.Name, Type: dt, Nullable: true}

		// SERIAL / BIGSERIAL imply AUTO_INCREMENT
		if sqlType == "SERIAL" || sqlType == "BIGSERIAL" {
			col.AutoIncrement = true
		}

		// UUID type — stored as TypeString but auto-generated on insert
		if sqlType == "UUID" {
			col.IsUUID = true
		}

		// VECTOR(n) — extract dimension into MaxLen
		if dt == schema.TypeVector {
			col.MaxLen = uint32(parseVectorDimension(colDef.Type))
		}

		// Column-level constraints
		for _, cc := range colDef.Constraints {
			if cc.AutoIncrement || strings.EqualFold(cc.Type, "AUTO_INCREMENT") {
				col.AutoIncrement = true
			}
			if strings.EqualFold(cc.Type, "NOT NULL") {
				col.Nullable = false
			}
		}

		cols = append(cols, col)
	}

	ts := schema.NewTableSchema(stmt.Name, cols)

	// Parse ENGINE option (e.g. ENGINE = VECTOR).
	for _, opt := range stmt.Options {
		if strings.EqualFold(opt.Name, "ENGINE") {
			ts.SetStorageEngine(strings.ToUpper(opt.Value))
		}
	}

	// Determine primary key columns — PK columns are implicitly NOT NULL.
	pkCols := detectPrimaryKey(stmt)
	if len(pkCols) > 0 {
		ts.AddIndex("PRIMARY", pkCols, nil)
		for _, pkCol := range pkCols {
			ts.SetColumnNotNull(pkCol)
		}
	}

	// Collect UNIQUE constraints (column-level and table-level).
	uniqueColSets := collectUniqueConstraints(stmt)
	for _, colNames := range uniqueColSets {
		idxName := "UNIQUE_" + strings.Join(colNames, "_")
		ts.AddIndex(idxName, colNames, nil)
		ts.SetIndexUnique(idxName)
	}

	// Collect FOREIGN KEY constraints (column-level and table-level).
	fks, fkErr := collectForeignKeyConstraints(db, stmt, ts.GetName())
	if fkErr != nil {
		return nil, fkErr
	}
	for _, fk := range fks {
		ts.AddForeignKey(fk)
	}

	if err := db.CreateTable(ts); err != nil {
		if stmt.IfNotExists && strings.Contains(err.Error(), "already exists") {
			return &Result{Tag: "CREATE TABLE"}, nil
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
func detectPrimaryKey(stmt *ast.CreateTableStatement) []string {
	for _, c := range stmt.Constraints {
		if strings.EqualFold(c.Type, "PRIMARY KEY") && len(c.Columns) > 0 {
			return c.Columns
		}
	}
	for _, col := range stmt.Columns {
		for _, cc := range col.Constraints {
			if strings.EqualFold(cc.Type, "PRIMARY KEY") {
				return []string{col.Name}
			}
		}
	}
	return nil
}

// collectUniqueConstraints gathers UNIQUE constraints from column- and table-level definitions.
// It returns each unique group as a slice of column names.
func collectUniqueConstraints(stmt *ast.CreateTableStatement) [][]string {
	var groups [][]string
	seen := make(map[string]bool)

	// Column-level: col TEXT UNIQUE
	for _, col := range stmt.Columns {
		for _, cc := range col.Constraints {
			if strings.EqualFold(cc.Type, "UNIQUE") {
				key := col.Name
				if !seen[key] {
					seen[key] = true
					groups = append(groups, []string{col.Name})
				}
			}
		}
	}

	// Table-level: UNIQUE (col1, col2)
	for _, c := range stmt.Constraints {
		if strings.EqualFold(c.Type, "UNIQUE") && len(c.Columns) > 0 {
			key := strings.Join(c.Columns, ",")
			if !seen[key] {
				seen[key] = true
				groups = append(groups, c.Columns)
			}
		}
	}

	return groups
}
