package schema

import (
	"encoding/json"
	"testing"
)

func TestIndexValueExtraction(t *testing.T) {
	tableSchema := NewTableSchema("users", "id")
	tableSchema.AddColumn("id", DataTypeInt64, false)
	tableSchema.AddColumn("email", DataTypeString, false)
	tableSchema.AddColumn("age", DataTypeInt64, true)
	tableSchema.AddIndex("email_idx", IndexTypeSecondary, []string{"email"}, false)
	tableSchema.AddIndex("age_idx", IndexTypeSecondary, []string{"age"}, false)

	rowData := map[string]interface{}{
		"id":    int64(1),
		"email": "alice@example.com",
		"age":   int64(30),
	}
	rowJSON, _ := json.Marshal(rowData)

	indexValues, err := ExtractIndexValues(tableSchema, rowJSON)
	if err != nil {
		t.Fatalf("failed to extract index values: %v", err)
	}

	if len(indexValues) != 2 {
		t.Errorf("expected 2 index values, got %d", len(indexValues))
	}

	if _, exists := indexValues["email_idx"]; !exists {
		t.Error("expected email_idx in extracted values")
	}

	if _, exists := indexValues["age_idx"]; !exists {
		t.Error("expected age_idx in extracted values")
	}
}

func TestMultipleIndexesSchema(t *testing.T) {
	tableSchema := NewTableSchema("users", "id")
	tableSchema.AddColumn("id", DataTypeInt64, false)
	tableSchema.AddColumn("email", DataTypeString, false)
	tableSchema.AddColumn("username", DataTypeString, false)
	tableSchema.AddIndex("email_idx", IndexTypeSecondary, []string{"email"}, false)
	tableSchema.AddIndex("username_idx", IndexTypeSecondary, []string{"username"}, false)

	if len(tableSchema.Indexes) != 2 {
		t.Errorf("expected 2 secondary indexes, got %d", len(tableSchema.Indexes))
	}
}

func TestCompoundIndexExtraction(t *testing.T) {
	tableSchema := NewTableSchema("orders", "id")
	tableSchema.AddColumn("id", DataTypeInt64, false)
	tableSchema.AddColumn("customer_id", DataTypeInt64, false)
	tableSchema.AddColumn("status", DataTypeString, false)
	tableSchema.AddIndex("customer_status_idx", IndexTypeSecondary, []string{"customer_id", "status"}, false)

	rowData := map[string]interface{}{
		"id":          int64(100),
		"customer_id": int64(5),
		"status":      "active",
	}
	rowJSON, _ := json.Marshal(rowData)

	indexValues, err := ExtractIndexValues(tableSchema, rowJSON)
	if err != nil {
		t.Fatalf("failed to extract compound index values: %v", err)
	}

	if len(indexValues) != 1 {
		t.Errorf("expected 1 compound index, got %d", len(indexValues))
	}
}
