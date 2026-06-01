package vector

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/rodrigo0345/omag/src/storage"
)

// VectorEngine is the primary storage backend for VECTOR-engine tables. It keeps
// all rows in memory for fast iteration and maintains an HNSW index for
// approximate nearest-neighbour search. Data is persisted to disk on Close().
type VectorEngine struct {
	mu      sync.RWMutex
	data    map[string][]byte // string(key) → raw row bytes
	keys    [][]byte          // insertion-order key list (stable Scan ordering)
	dataDir string

	index     *HNSW                        // nil when no extractor is configured
	extractor func([]byte) ([]float32, bool) // extracts the vector field from a raw row
}

var _ storage.IStorageEngine = (*VectorEngine)(nil)

// NewVectorEngine creates a VectorEngine without an HNSW index. Useful for
// tables that do not yet have a schema injected or for testing the raw store.
func NewVectorEngine(dataDir string) *VectorEngine {
	e := &VectorEngine{
		data:    make(map[string][]byte),
		dataDir: dataDir,
	}
	e.mustLoad()
	return e
}

// NewVectorEngineWithExtractor creates a VectorEngine backed by an HNSW index.
// ext extracts the vector from a raw serialised row; it is called on every Put.
func NewVectorEngineWithExtractor(
	dataDir string,
	ext func([]byte) ([]float32, bool),
	cfg HNSWConfig,
	metric Metric,
) *VectorEngine {
	e := &VectorEngine{
		data:      make(map[string][]byte),
		dataDir:   dataDir,
		extractor: ext,
		index:     NewHNSW(cfg, metric),
	}
	e.mustLoad()
	return e
}

// mustLoad restores state from disk, ignoring "not found" errors.
func (e *VectorEngine) mustLoad() {
	vPath := filepath.Join(e.dataDir, "vectors.bin")
	keys, data, err := loadVectorsBin(vPath)
	if err == nil && data != nil {
		e.keys = keys
		e.data = data
	}

	if e.index == nil {
		return
	}

	hPath := filepath.Join(e.dataDir, "hnsw.bin")
	loaded, err := loadHNSWBin(hPath)
	if err == nil && loaded != nil {
		e.index = loaded
		return
	}

	// hnsw.bin absent or corrupt: rebuild from vector data.
	for _, k := range e.keys {
		v, ok := e.data[string(k)]
		if !ok {
			continue
		}
		if vec, ok := e.extractor(v); ok {
			_ = e.index.Insert(k, vec)
		}
	}
}

// Put stores key→value and indexes the vector field if an extractor is set.
func (e *VectorEngine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	k := string(key)
	if _, exists := e.data[k]; !exists {
		kCopy := make([]byte, len(key))
		copy(kCopy, key)
		e.keys = append(e.keys, kCopy)
	}
	vCopy := make([]byte, len(value))
	copy(vCopy, value)
	e.data[k] = vCopy

	if e.index != nil && e.extractor != nil {
		if vec, ok := e.extractor(value); ok {
			_ = e.index.Insert(key, vec)
		}
	}
	return nil
}

// Get returns a copy of the stored value for key.
func (e *VectorEngine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	v, ok := e.data[string(key)]
	if !ok {
		return nil, errNotFound
	}
	result := make([]byte, len(v))
	copy(result, v)
	return result, nil
}

// Delete removes key from both the data store and the HNSW index.
func (e *VectorEngine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	k := string(key)
	if _, ok := e.data[k]; !ok {
		return nil
	}
	delete(e.data, k)
	for i, kk := range e.keys {
		if string(kk) == k {
			e.keys = append(e.keys[:i], e.keys[i+1:]...)
			break
		}
	}

	if e.index != nil {
		e.index.Delete(key)
	}
	return nil
}

// Scan returns a cursor over all entries in insertion order.
// ComplexFilter in ScanOptions is applied; other options (LowerBound, Limit,
// etc.) are handled at the executor layer.
func (e *VectorEngine) Scan(opts storage.ScanOptions) (storage.ICursor, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	entries := make([]storage.ScanEntry, 0, len(e.keys))
	for _, k := range e.keys {
		v, ok := e.data[string(k)]
		if !ok {
			continue
		}
		entry := storage.ScanEntry{Key: k, Value: v}
		if opts.ComplexFilter != nil && !opts.ComplexFilter(entry) {
			continue
		}
		kCopy := make([]byte, len(k))
		vCopy := make([]byte, len(v))
		copy(kCopy, k)
		copy(vCopy, v)
		entries = append(entries, storage.ScanEntry{Key: kCopy, Value: vCopy})
	}
	return newSliceCursor(entries), nil
}

// VectorSearch performs approximate k-NN search using the HNSW index.
// Returns (nil, nil) when no HNSW index is configured for this engine,
// signalling the executor to fall back to a full-scan sort.
// Returns a non-nil (possibly empty) slice when the index is available.
func (e *VectorEngine) VectorSearch(query []float32, k int) ([]storage.ScanEntry, error) {
	if e.index == nil {
		return nil, nil
	}

	results, err := e.index.SearchKNN(query, k)
	if err != nil {
		return nil, err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	entries := make([]storage.ScanEntry, 0, len(results))
	for _, r := range results {
		raw, ok := e.data[string(r.StorageKey)]
		if !ok || len(raw) == 0 {
			continue // deleted between search and data lookup; skip
		}
		// Strip the MVCC op byte (prepended by the MVCC Write layer) so that
		// callers receive values in the same format as an MVCC scan cursor.
		v := raw[1:]
		kCopy := make([]byte, len(r.StorageKey))
		copy(kCopy, r.StorageKey)
		vCopy := make([]byte, len(v))
		copy(vCopy, v)
		entries = append(entries, storage.ScanEntry{Key: kCopy, Value: vCopy})
	}
	return entries, nil
}

// Close persists all data and the HNSW graph to disk.
func (e *VectorEngine) Close() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if err := writeVectorsBin(filepath.Join(e.dataDir, "vectors.bin"), e.keys, e.data); err != nil {
		return err
	}
	if e.index != nil {
		if err := writeHNSWBin(filepath.Join(e.dataDir, "hnsw.bin"), e.index); err != nil {
			return err
		}
	}
	return nil
}

// FlushMemtable is a no-op. VectorEngine is always in-memory; Close flushes.
func (e *VectorEngine) FlushMemtable() {}

var errNotFound = fmt.Errorf("key not found")
