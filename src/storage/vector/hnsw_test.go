package vector

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/rodrigo0345/omag/src/storage"
)

// --- Distance function tests ---

func TestDistances_L2KnownValues(t *testing.T) {
	a := []float32{3.0, 4.0}
	b := []float32{0.0, 0.0}
	// squared L2 from [3,4] to [0,0] = 9+16 = 25
	if got := L2Distance(a, b); got != 25.0 {
		t.Errorf("L2Distance([3,4],[0,0]): want 25, got %v", got)
	}
	// same vector → 0
	if got := L2Distance(a, a); got != 0.0 {
		t.Errorf("L2Distance(a,a): want 0, got %v", got)
	}
}

func TestDistances_CosineOrthogonal(t *testing.T) {
	a := []float32{1.0, 0.0}
	b := []float32{0.0, 1.0}
	// orthogonal → cosine = 0 → distance = 1
	got := CosineDistance(a, b)
	if math.Abs(float64(got)-1.0) > 1e-6 {
		t.Errorf("CosineDistance(orthogonal): want 1.0, got %v", got)
	}
}

func TestDistances_CosineIdentical(t *testing.T) {
	a := []float32{1.0, 2.0, 3.0}
	got := CosineDistance(a, a)
	if math.Abs(float64(got)) > 1e-6 {
		t.Errorf("CosineDistance(a,a): want 0, got %v", got)
	}
}

func TestDistances_DotSimilar(t *testing.T) {
	a := []float32{1.0, 0.0}
	b := []float32{1.0, 0.0}
	// dot(a,b)=1 → DotDistance=-1 (smaller = more similar)
	if got := DotDistance(a, b); got != -1.0 {
		t.Errorf("DotDistance([1,0],[1,0]): want -1, got %v", got)
	}
}

// --- HNSW insertion and search ---

func TestHNSW_InsertAndSearchSingle(t *testing.T) {
	h := NewHNSW(DefaultHNSWConfig(), MetricL2)
	key := []byte("v1")
	vec := []float32{1.0, 0.0, 0.0}
	if err := h.Insert(key, vec); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	results, err := h.SearchKNN([]float32{1.0, 0.0, 0.0}, 1)
	if err != nil {
		t.Fatalf("SearchKNN: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("want 1 result, got %d", len(results))
	}
	if string(results[0].StorageKey) != "v1" {
		t.Errorf("want key v1, got %s", results[0].StorageKey)
	}
	if results[0].Distance > 1e-6 {
		t.Errorf("want distance 0 for exact match, got %v", results[0].Distance)
	}
}

func TestHNSW_TopKCorrectOrder(t *testing.T) {
	h := NewHNSW(DefaultHNSWConfig(), MetricL2)
	// Insert vectors at distances 10, 1, 2, 5 from [0,0]
	vecs := map[string][]float32{
		"far":    {10.0, 0.0},
		"close":  {1.0, 0.0},
		"mid":    {2.0, 0.0},
		"medium": {5.0, 0.0},
	}
	for k, v := range vecs {
		if err := h.Insert([]byte(k), v); err != nil {
			t.Fatalf("Insert %s: %v", k, err)
		}
	}

	results, err := h.SearchKNN([]float32{0.0, 0.0}, 2)
	if err != nil {
		t.Fatalf("SearchKNN: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("want 2 results, got %d", len(results))
	}
	if string(results[0].StorageKey) != "close" {
		t.Errorf("want 'close' as nearest, got %s", results[0].StorageKey)
	}
	if string(results[1].StorageKey) != "mid" {
		t.Errorf("want 'mid' as second, got %s", results[1].StorageKey)
	}
}

func TestHNSW_SoftDelete(t *testing.T) {
	h := NewHNSW(DefaultHNSWConfig(), MetricL2)
	if err := h.Insert([]byte("keep"), []float32{1.0, 0.0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Insert([]byte("del"), []float32{0.5, 0.0}); err != nil {
		t.Fatal(err)
	}

	h.Delete([]byte("del"))

	results, err := h.SearchKNN([]float32{0.0, 0.0}, 5)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range results {
		if string(r.StorageKey) == "del" {
			t.Errorf("deleted node 'del' should not appear in results")
		}
	}
	if len(results) != 1 || string(results[0].StorageKey) != "keep" {
		t.Errorf("expected only 'keep' in results, got %v", results)
	}
}

func TestHNSW_DimensionMismatch(t *testing.T) {
	h := NewHNSW(DefaultHNSWConfig(), MetricL2)
	if err := h.Insert([]byte("v1"), []float32{1.0, 2.0, 3.0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Insert([]byte("v2"), []float32{1.0, 2.0}); err == nil {
		t.Error("expected dimension mismatch error")
	}
	_, err := h.SearchKNN([]float32{1.0, 2.0}, 1)
	if err == nil {
		t.Error("expected dimension mismatch error from SearchKNN")
	}
}

func TestHNSW_UpdateExistingKey(t *testing.T) {
	h := NewHNSW(DefaultHNSWConfig(), MetricL2)
	key := []byte("v1")
	if err := h.Insert(key, []float32{100.0, 0.0}); err != nil {
		t.Fatal(err)
	}
	// Update same key to a closer position
	if err := h.Insert(key, []float32{1.0, 0.0}); err != nil {
		t.Fatal(err)
	}
	results, err := h.SearchKNN([]float32{0.0, 0.0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("want 1 result, got %d", len(results))
	}
	// Distance should be ~1.0 (updated position), not ~100.0
	if results[0].Distance > 2.0 {
		t.Errorf("expected distance ~1.0 after update, got %v", results[0].Distance)
	}
}

func TestHNSW_EmptySearch(t *testing.T) {
	h := NewHNSW(DefaultHNSWConfig(), MetricL2)
	results, err := h.SearchKNN([]float32{1.0, 2.0}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("empty index should return 0 results, got %d", len(results))
	}
}

func TestHNSW_LargeInsertAndSearch(t *testing.T) {
	h := NewHNSW(HNSWConfig{M: 8, M0: 16, EfConstruction: 50, EfSearch: 20}, MetricL2)

	const n = 200
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("v%d", i))
		vec := []float32{float32(i), 0.0}
		if err := h.Insert(key, vec); err != nil {
			t.Fatalf("Insert v%d: %v", i, err)
		}
	}

	// Query nearest to [0, 0]: should be v0 (distance 0)
	results, err := h.SearchKNN([]float32{0.0, 0.0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) < 1 {
		t.Fatal("expected at least 1 result")
	}
	if string(results[0].StorageKey) != "v0" {
		t.Errorf("expected v0 as nearest neighbor, got %s (dist=%v)", results[0].StorageKey, results[0].Distance)
	}
}

// --- Persistence tests ---

func TestHNSW_PersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hnsw.bin")

	h := NewHNSW(DefaultHNSWConfig(), MetricL2)
	data := map[string][]float32{
		"a": {1.0, 0.0},
		"b": {0.0, 1.0},
		"c": {0.5, 0.5},
	}
	for k, v := range data {
		if err := h.Insert([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}

	if err := writeHNSWBin(path, h); err != nil {
		t.Fatalf("writeHNSWBin: %v", err)
	}

	h2, err := loadHNSWBin(path)
	if err != nil {
		t.Fatalf("loadHNSWBin: %v", err)
	}
	if h2 == nil {
		t.Fatal("loaded HNSW is nil")
	}

	// Search should give same results
	r1, _ := h.SearchKNN([]float32{1.0, 0.0}, 1)
	r2, _ := h2.SearchKNN([]float32{1.0, 0.0}, 1)
	if len(r1) != len(r2) {
		t.Fatalf("result count mismatch: orig=%d loaded=%d", len(r1), len(r2))
	}
	if len(r1) > 0 && string(r1[0].StorageKey) != string(r2[0].StorageKey) {
		t.Errorf("nearest key: orig=%s loaded=%s", r1[0].StorageKey, r2[0].StorageKey)
	}
}

func TestHNSW_PersistenceAbsentFile(t *testing.T) {
	h, err := loadHNSWBin("/nonexistent/path/hnsw.bin")
	if err != nil {
		t.Fatalf("expected nil error for absent file, got %v", err)
	}
	if h != nil {
		t.Error("expected nil HNSW for absent file")
	}
}

func TestVectors_BinRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vectors.bin")

	keys := [][]byte{[]byte("k1"), []byte("k2")}
	data := map[string][]byte{
		"k1": {0x01, 0x02, 0x03},
		"k2": {0x04, 0x05},
	}

	if err := writeVectorsBin(path, keys, data); err != nil {
		t.Fatalf("writeVectorsBin: %v", err)
	}

	k2, d2, err := loadVectorsBin(path)
	if err != nil {
		t.Fatalf("loadVectorsBin: %v", err)
	}
	if len(k2) != 2 {
		t.Fatalf("want 2 keys, got %d", len(k2))
	}
	for k, v := range data {
		if loaded, ok := d2[k]; !ok {
			t.Errorf("key %s missing from loaded data", k)
		} else if string(loaded) != string(v) {
			t.Errorf("key %s: want %v got %v", k, v, loaded)
		}
	}
	_ = k2
}

func TestVectors_BinAbsentFile(t *testing.T) {
	keys, data, err := loadVectorsBin("/nonexistent/vectors.bin")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if keys != nil {
		t.Error("expected nil keys for absent file")
	}
	if data == nil {
		t.Error("expected non-nil empty map for absent file")
	}
}

func TestHNSW_RebuildOnHighTombstoneRatio(t *testing.T) {
	cfg := HNSWConfig{M: 4, M0: 8, EfConstruction: 20, EfSearch: 10}
	h := NewHNSW(cfg, MetricL2)

	const n = 20
	for i := 0; i < n; i++ {
		_ = h.Insert([]byte(fmt.Sprintf("v%d", i)), []float32{float32(i), 0.0})
	}

	// Delete >20% to trigger rebuild
	for i := 0; i < 6; i++ {
		h.Delete([]byte(fmt.Sprintf("v%d", i)))
	}

	// After rebuild, search should still work and not return deleted nodes
	results, err := h.SearchKNN([]float32{float32(n - 1), 0.0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range results {
		for i := 0; i < 6; i++ {
			if string(r.StorageKey) == fmt.Sprintf("v%d", i) {
				t.Errorf("deleted node v%d appeared in search results", i)
			}
		}
	}
}

// --- VectorEngine integration tests (without MVCC) ---

func TestVectorEngine_BasicOps(t *testing.T) {
	dir := t.TempDir()
	e := NewVectorEngine(dir)

	if err := e.Put([]byte("k1"), []byte("val1")); err != nil {
		t.Fatal(err)
	}
	v, err := e.Get([]byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "val1" {
		t.Errorf("want val1, got %s", v)
	}

	if err := e.Delete([]byte("k1")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Get([]byte("k1")); err == nil {
		t.Error("expected error after delete")
	}
}

func TestVectorEngine_ScanAll(t *testing.T) {
	dir := t.TempDir()
	e := NewVectorEngine(dir)

	for i := 0; i < 5; i++ {
		_ = e.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}

	cursor, err := e.Scan(storage.ScanOptions{Inclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for cursor.Next() {
		count++
	}
	if count != 5 {
		t.Errorf("want 5 entries, got %d", count)
	}
}

func TestVectorEngine_PersistAndReload(t *testing.T) {
	dir := t.TempDir()
	e := NewVectorEngine(dir)
	_ = e.Put([]byte("k1"), []byte("val1"))
	_ = e.Put([]byte("k2"), []byte("val2"))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := NewVectorEngine(dir)
	v, err := e2.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("k1 missing after reload: %v", err)
	}
	if string(v) != "val1" {
		t.Errorf("want val1, got %s", v)
	}
}

func TestVectorEngine_NoIndexWithoutExtractor(t *testing.T) {
	dir := t.TempDir()
	e := NewVectorEngine(dir)
	_ = e.Put([]byte("k1"), []byte("data"))

	// Without extractor, VectorSearch returns nil (fall-through signal)
	result, err := e.VectorSearch([]float32{1.0, 2.0}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Error("expected nil result when no extractor is configured")
	}
}

