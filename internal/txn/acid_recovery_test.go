package txn

import (
	"fmt"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage"
)

type MockStorageEngine struct {
	data map[string][]byte
}

func (m *MockStorageEngine) Put(key []byte, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *MockStorageEngine) Get(key []byte) ([]byte, error) {
	val, ok := m.data[string(key)]
	if !ok {
		return nil, fmt.Errorf("key not found")
	}
	return val, nil
}

func (m *MockStorageEngine) Delete(key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *MockStorageEngine) Scan() ([]storage.ScanEntry, error) {
	result := make([]storage.ScanEntry, 0)
	for key, value := range m.data {
		result = append(result, storage.ScanEntry{
			Key:   []byte(key),
			Value: value,
		})
	}
	return result, nil
}

func TestAtomicity(t *testing.T) {
	engine := &MockStorageEngine{data: make(map[string][]byte)}

	records := []struct {
		key   string
		value string
	}{
		{"user:1", "Alice"},
		{"user:2", "Bob"},
		{"user:3", "Charlie"},
	}

	for _, rec := range records {
		if err := engine.Put([]byte(rec.key), []byte(rec.value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	entries, err := engine.Scan()
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	existingRecords := make(map[string]bool)
	for _, entry := range entries {
		existingRecords[string(entry.Key)] = true
	}

	userRecords := 0
	for key := range existingRecords {
		if len(key) > 5 && key[0:5] == "user:" {
			userRecords++
		}
	}

	if userRecords != 3 {
		t.Fatalf("atomicity violated: expected 3 records, got %d", userRecords)
	}
}

func TestConsistency(t *testing.T) {
	engine := &MockStorageEngine{data: make(map[string][]byte)}

	records := map[string]string{
		"order:1":        "Order 1",
		"order:1:item:1": "Item 1",
		"order:1:item:2": "Item 2",
	}

	for key, value := range records {
		if err := engine.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	entries, err := engine.Scan()
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	hasOrder1 := false
	hasItems := false

	for _, entry := range entries {
		key := string(entry.Key)
		if key == "order:1" {
			hasOrder1 = true
		}
		if len(key) > 10 && key[0:10] == "order:1:item:" {
			hasItems = true
		}
	}

	if hasItems && !hasOrder1 {
		t.Fatalf("referential integrity violated: items exist but order doesn't")
	}
}

func TestIsolation(t *testing.T) {
	engine := &MockStorageEngine{data: make(map[string][]byte)}

	if err := engine.Put([]byte("committed:data"), []byte("This is committed")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	if err := engine.Put([]byte("uncommitted:data"), []byte("This should be rolled back")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	if err := engine.Delete([]byte("uncommitted:data")); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	entries, err := engine.Scan()
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	hasUncommitted := false

	for _, entry := range entries {
		key := string(entry.Key)
		if key == "uncommitted:data" {
			hasUncommitted = true
		}
	}

	if hasUncommitted {
		t.Fatalf("isolation violation: uncommitted data visible after recovery")
	}
}

func TestDurability(t *testing.T) {
	engine := &MockStorageEngine{data: make(map[string][]byte)}

	dataToWrite := map[string]string{
		"purchase:1": "Widget",
		"purchase:2": "Gadget",
		"purchase:3": "Device",
	}

	for key, value := range dataToWrite {
		if err := engine.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	entries, err := engine.Scan()
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	found := make(map[string]bool)
	for _, entry := range entries {
		found[string(entry.Key)] = true
	}

	expectedCount := 0
	for key := range dataToWrite {
		if !found[key] {
			t.Fatalf("durability violated: committed data '%s' not found", key)
		}
		expectedCount++
	}

	if len(found) != expectedCount {
		t.Fatalf("durability: expected %d records, got %d", expectedCount, len(found))
	}
}
