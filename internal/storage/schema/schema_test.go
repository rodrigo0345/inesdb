package schema

import (
	"testing"
)

func TestTableSchema_NewTableSchema(t *testing.T) {
	schema := NewTableSchema("users", "id")

	if schema.Name != "users" {
		t.Fatalf("expected name 'users', got %q", schema.Name)
	}

	if schema.PrimaryKey != "id" {
		t.Fatalf("expected primary key 'id', got %q", schema.PrimaryKey)
	}

	if len(schema.Columns) != 0 {
		t.Fatalf("expected no columns, got %d", len(schema.Columns))
	}
}

func TestTableSchema_AddColumn(t *testing.T) {
	schema := NewTableSchema("users", "id")

	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("name", DataTypeString, false)
	schema.AddColumn("email", DataTypeString, true)

	if len(schema.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(schema.Columns))
	}

	idCol, _ := schema.GetColumn("id")
	if idCol.DataType != DataTypeInt64 {
		t.Fatalf("expected id type int64, got %s", idCol.DataType)
	}

	emailCol, _ := schema.GetColumn("email")
	if !emailCol.Nullable {
		t.Fatalf("expected email to be nullable")
	}

	err := schema.AddColumn("id", DataTypeInt64, false)
	if err == nil {
		t.Fatalf("expected error adding duplicate column")
	}
}

func TestTableSchema_AddIndex(t *testing.T) {
	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("email", DataTypeString, false)
	schema.AddColumn("username", DataTypeString, false)

	err := schema.AddIndex("id_pk", IndexTypePrimary, []string{"id"}, false)
	if err != nil {
		t.Fatalf("failed to add primary key index: %v", err)
	}

	err = schema.AddIndex("email_idx", IndexTypeSecondary, []string{"email"}, true)
	if err != nil {
		t.Fatalf("failed to add secondary index: %v", err)
	}

	err = schema.AddIndex("user_email_idx", IndexTypeSecondary, []string{"username", "email"}, false)
	if err != nil {
		t.Fatalf("failed to add composite index: %v", err)
	}

	if len(schema.Indexes) != 3 {
		t.Fatalf("expected 3 indexes, got %d", len(schema.Indexes))
	}

	emailIdx, _ := schema.GetIndex("email_idx")
	if !emailIdx.IsUnique {
		t.Fatalf("expected email_idx to be unique")
	}

	err = schema.AddIndex("bad_idx", IndexTypeSecondary, []string{"nonexistent"}, false)
	if err == nil {
		t.Fatalf("expected error adding index on non-existent column")
	}
}

func TestTableSchema_GetIndexesForColumn(t *testing.T) {
	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("email", DataTypeString, false)
	schema.AddColumn("username", DataTypeString, false)

	schema.AddIndex("id_pk", IndexTypePrimary, []string{"id"}, false)
	schema.AddIndex("email_idx", IndexTypeSecondary, []string{"email"}, false)
	schema.AddIndex("user_email_idx", IndexTypeSecondary, []string{"username", "email"}, false)

	emailIndexes := schema.GetIndexesForColumn("email")
	if len(emailIndexes) != 2 {
		t.Fatalf("expected 2 indexes for email, got %d", len(emailIndexes))
	}

	usernameIndexes := schema.GetIndexesForColumn("username")
	if len(usernameIndexes) != 1 {
		t.Fatalf("expected 1 index for username, got %d", len(usernameIndexes))
	}

	idIndexes := schema.GetIndexesForColumn("id")
	if len(idIndexes) != 1 {
		t.Fatalf("expected 1 index for id (primary key), got %d", len(idIndexes))
	}
}

func TestTableSchema_Validate(t *testing.T) {
	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)

	if err := schema.Validate(); err != nil {
		t.Fatalf("expected valid schema, got error: %v", err)
	}

	emptySchema := NewTableSchema("", "id")
	if err := emptySchema.Validate(); err == nil {
		t.Fatalf("expected error for empty name")
	}

	badSchema := NewTableSchema("table", "missing_pk")
	badSchema.AddColumn("id", DataTypeInt64, false)
	if err := badSchema.Validate(); err == nil {
		t.Fatalf("expected error for missing primary key column")
	}

	noColSchema := NewTableSchema("table", "id")
	if err := noColSchema.Validate(); err == nil {
		t.Fatalf("expected error for empty columns")
	}
}

func TestTableSchema_Serialization(t *testing.T) {
	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("name", DataTypeString, false)
	schema.AddColumn("email", DataTypeString, true)
	schema.AddIndex("id_pk", IndexTypePrimary, []string{"id"}, false)
	schema.AddIndex("email_idx", IndexTypeSecondary, []string{"email"}, true)

	jsonData, err := schema.ToJSON()
	if err != nil {
		t.Fatalf("failed to serialize schema: %v", err)
	}

	restored, err := FromJSON(jsonData)
	if err != nil {
		t.Fatalf("failed to deserialize schema: %v", err)
	}

	if restored.Name != schema.Name {
		t.Fatalf("name mismatch after deserialization")
	}

	if restored.PrimaryKey != schema.PrimaryKey {
		t.Fatalf("primary key mismatch after deserialization")
	}

	if len(restored.Columns) != len(schema.Columns) {
		t.Fatalf("column count mismatch: expected %d, got %d", len(schema.Columns), len(restored.Columns))
	}

	if len(restored.Indexes) != len(schema.Indexes) {
		t.Fatalf("index count mismatch: expected %d, got %d", len(schema.Indexes), len(restored.Indexes))
	}

	for i, colName := range schema.ColumnList {
		if restored.ColumnList[i] != colName {
			t.Fatalf("column order mismatch at position %d: expected %q, got %q", i, colName, restored.ColumnList[i])
		}
	}
}

func TestTableSchema_String(t *testing.T) {
	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("email", DataTypeString, true)
	schema.AddIndex("id_pk", IndexTypePrimary, []string{"id"}, false)

	str := schema.String()
	if len(str) == 0 {
		t.Fatalf("expected non-empty string representation")
	}

	if !contains(str, "Table: users") {
		t.Fatalf("string doesn't contain table name")
	}

	if !contains(str, "Primary Key: id") {
		t.Fatalf("string doesn't contain primary key")
	}
}

func TestTableSchema_ColumnDataType(t *testing.T) {
	schema := NewTableSchema("users", "id")
	schema.AddColumn("id", DataTypeInt64, false)
	schema.AddColumn("age", DataTypeInt64, false)

	dtype, err := schema.ColumnDataType("id")
	if err != nil {
		t.Fatalf("failed to get column type: %v", err)
	}

	if dtype != DataTypeInt64 {
		t.Fatalf("expected int64, got %s", dtype)
	}

	_, err = schema.ColumnDataType("nonexistent")
	if err == nil {
		t.Fatalf("expected error for non-existent column")
	}
}

func contains(s, substr string) bool {
	for i := 0; i < len(s)-len(substr)+1; i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
