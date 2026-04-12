package txn

import (
	"encoding/json"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/schema"
)


func TestACIDAtomicity_InsertRollback(t *testing.T) {
	tableSchema := schema.NewTableSchema("users", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("email", schema.DataTypeString, false)
	tableSchema.AddIndex("email_idx", schema.IndexTypeSecondary, []string{"email"}, false)

	txn := NewTransaction(1, READ_COMMITTED)
	txn.SetTableContext("users", tableSchema)

	rowData := map[string]interface{}{
		"id":    int64(1),
		"email": "alice@example.com",
	}
	rowJSON, _ := json.Marshal(rowData)

	indexValues, err := ExtractIndexValues(tableSchema, rowJSON)
	if err != nil {
		t.Fatalf("failed to extract index values: %v", err)
	}

	if len(indexValues) != 1 {
		t.Errorf("expected 1 index entry, got %d", len(indexValues))
	}

	txn.Abort()
	if txn.GetState() != ABORTED {
		t.Errorf("expected transaction to be aborted")
	}
}

func TestACIDConsistency_IndexSync(t *testing.T) {
	tableSchema := schema.NewTableSchema("users", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("email", schema.DataTypeString, false)
	tableSchema.AddColumn("username", schema.DataTypeString, false)
	tableSchema.AddIndex("email_idx", schema.IndexTypeSecondary, []string{"email"}, false)
	tableSchema.AddIndex("username_idx", schema.IndexTypeSecondary, []string{"username"}, false)

	rowData := map[string]interface{}{
		"id":       int64(1),
		"email":    "alice@example.com",
		"username": "alice",
	}
	rowJSON, _ := json.Marshal(rowData)

	indexValues, err := ExtractIndexValues(tableSchema, rowJSON)
	if err != nil {
		t.Fatalf("failed to extract index values: %v", err)
	}

	if len(indexValues) != 2 {
		t.Errorf("expected 2 index entries, got %d", len(indexValues))
	}

	_, ok1 := indexValues["email_idx"]
	_, ok2 := indexValues["username_idx"]

	if !ok1 || !ok2 {
		t.Error("expected both index entries to be present")
	}
}

func TestACIDIsolation_ConcurrentTransactions(t *testing.T) {
	tableSchema := schema.NewTableSchema("users", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("email", schema.DataTypeString, false)
	tableSchema.AddIndex("email_idx", schema.IndexTypeSecondary, []string{"email"}, false)

	txn1 := NewTransaction(1, SERIALIZABLE)
	txn1.SetTableContext("users", tableSchema)

	txn2 := NewTransaction(2, SERIALIZABLE)
	txn2.SetTableContext("users", tableSchema)

	if txn1.GetID() != 1 || txn2.GetID() != 2 {
		t.Error("expected different transaction IDs")
	}

	if txn1.GetIsolationLevel() != SERIALIZABLE {
		t.Errorf("expected SERIALIZABLE isolation, got %d", txn1.GetIsolationLevel())
	}
}

func TestACIDDurability_CleanupCallbacks(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	var cleanupOrder []int
	txn.RegisterCleanupCallback(func() error {
		cleanupOrder = append(cleanupOrder, 1)
		return nil
	})
	txn.RegisterCleanupCallback(func() error {
		cleanupOrder = append(cleanupOrder, 2)
		return nil
	})
	txn.RegisterCleanupCallback(func() error {
		cleanupOrder = append(cleanupOrder, 3)
		return nil
	})

	if len(txn.cleanupCallbacks) != 3 {
		t.Errorf("expected 3 cleanup callbacks, got %d", len(txn.cleanupCallbacks))
	}

	err := txn.ExecuteCleanupCallbacks()
	if err != nil {
		t.Errorf("expected no error during cleanup, got %v", err)
	}

	if len(cleanupOrder) != 3 {
		t.Errorf("expected all cleanup callbacks to run, got %d", len(cleanupOrder))
	}

	for i, order := range cleanupOrder {
		if order != i+1 {
			t.Errorf("expected cleanup order %d, got %d", i+1, order)
		}
	}
}

func TestMultiIndexACID_CompoundIndexes(t *testing.T) {
	tableSchema := schema.NewTableSchema("orders", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("customer_id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("status", schema.DataTypeString, false)
	tableSchema.AddIndex("composite_idx", schema.IndexTypeSecondary, []string{"customer_id", "status"}, false)

	rowData := map[string]interface{}{
		"id":          int64(1),
		"customer_id": int64(5),
		"status":      "active",
	}
	rowJSON, _ := json.Marshal(rowData)

	indexValues, err := ExtractIndexValues(tableSchema, rowJSON)
	if err != nil {
		t.Fatalf("failed to extract index values: %v", err)
	}

	if len(indexValues) != 1 {
		t.Errorf("expected 1 compound index, got %d", len(indexValues))
	}
}

func TestTransactionRollbackWithMultipleStates(t *testing.T) {
	tableSchema := schema.NewTableSchema("users", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("email", schema.DataTypeString, false)
	tableSchema.AddIndex("email_idx", schema.IndexTypeSecondary, []string{"email"}, false)

	txn := NewTransaction(1, READ_COMMITTED)
	txn.SetTableContext("users", tableSchema)

	if txn.GetState() != ACTIVE {
		t.Errorf("expected ACTIVE state, got %v", txn.GetState())
	}

	txn.Commit()
	if txn.GetState() != COMMITTED {
		t.Errorf("expected COMMITTED state, got %v", txn.GetState())
	}
}

func TestIndexMaintenance_WithContextSwitch(t *testing.T) {
	schema1 := schema.NewTableSchema("users", "id")
	schema1.AddColumn("id", schema.DataTypeInt64, false)
	schema1.AddColumn("email", schema.DataTypeString, false)
	schema1.AddIndex("email_idx", schema.IndexTypeSecondary, []string{"email"}, false)

	schema2 := schema.NewTableSchema("orders", "id")
	schema2.AddColumn("id", schema.DataTypeInt64, false)
	schema2.AddColumn("customer_id", schema.DataTypeInt64, false)
	schema2.AddIndex("customer_idx", schema.IndexTypeSecondary, []string{"customer_id"}, false)

	txn := NewTransaction(1, SERIALIZABLE)

	txn.SetTableContext("users", schema1)
	name1, s1 := txn.GetTableContext()
	if name1 != "users" || s1 == nil {
		t.Error("expected users context")
	}

	txn.SetTableContext("orders", schema2)
	name2, s2 := txn.GetTableContext()
	if name2 != "orders" || s2 == nil {
		t.Error("expected orders context")
	}

	if len(s1.Indexes) != 1 || len(s2.Indexes) != 1 {
		t.Error("expected 1 index in each schema")
	}
}

func TestTransactionIsolationLevelPropagation(t *testing.T) {
	tests := []struct {
		isolation uint8
		name      string
	}{
		{READ_UNCOMMITTED, "READ_UNCOMMITTED"},
		{READ_COMMITTED, "READ_COMMITTED"},
		{REPEATABLE_READ, "REPEATABLE_READ"},
		{SERIALIZABLE, "SERIALIZABLE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tbl := schema.NewTableSchema("test", "id")
			tbl.AddColumn("id", schema.DataTypeInt64, false)

			txn := NewTransaction(1, test.isolation)
			txn.SetTableContext("test", tbl)

			if txn.GetIsolationLevel() != test.isolation {
				t.Errorf("expected isolation %d, got %d", test.isolation, txn.GetIsolationLevel())
			}
		})
	}
}
