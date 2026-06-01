package vector

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
)

var endian = binary.LittleEndian

// writeVectorsBin serialises the raw data store to dataDir/vectors.bin.
// Format: [numEntries uint64] ([keyLen uint32][key][valLen uint32][val])*
func writeVectorsBin(path string, keys [][]byte, data map[string][]byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var hdr [8]byte
	endian.PutUint64(hdr[:], uint64(len(keys)))
	if _, err := f.Write(hdr[:]); err != nil {
		return err
	}
	for _, k := range keys {
		v, ok := data[string(k)]
		if !ok {
			continue
		}
		if err := writeFrame(f, k); err != nil {
			return err
		}
		if err := writeFrame(f, v); err != nil {
			return err
		}
	}
	return nil
}

// loadVectorsBin reads the raw data store from dataDir/vectors.bin.
// Returns nil error if the file does not exist (fresh database).
func loadVectorsBin(path string) (keys [][]byte, data map[string][]byte, err error) {
	data = make(map[string][]byte)
	f, ferr := os.Open(path)
	if os.IsNotExist(ferr) {
		return nil, data, nil
	}
	if ferr != nil {
		return nil, nil, ferr
	}
	defer f.Close()

	var hdr [8]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return nil, nil, err
	}
	n := endian.Uint64(hdr[:])

	keys = make([][]byte, 0, n)
	for i := uint64(0); i < n; i++ {
		k, err := readFrame(f)
		if err != nil {
			return nil, nil, err
		}
		v, err := readFrame(f)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, k)
		data[string(k)] = v
	}
	return keys, data, nil
}

// writeHNSWBin serialises the HNSW graph to path.
//
// Binary layout (all little-endian):
//
//	[4]  magic "HNSW"
//	[1]  version = 1
//	[1]  metric (0=L2, 1=Cosine, 2=Dot)
//	[2]  M uint16
//	[2]  M0 uint16
//	[4]  EfConstruction uint32
//	[4]  EfSearch uint32
//	[4]  dim int32
//	[4]  entryPoint int32
//	[4]  maxLevel int32
//	[4]  numNodes uint32
//	-- per node --
//	[4]  id int32
//	[1]  deleted uint8
//	[4]  keyLen uint32 + [keyLen] bytes
//	[4]  vecLen uint32 + [vecLen*4] bytes (float32)
//	[1]  numLayers uint8
//	-- per layer --
//	[2]  numNeighbours uint16
//	[4*n] neighbour ids int32
func writeHNSWBin(path string, h *HNSW) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	h.mu.RLock()
	defer h.mu.RUnlock()

	// Header
	if _, err := f.Write([]byte("HNSW")); err != nil {
		return err
	}
	if err := writeByte(f, 1); err != nil { // version
		return err
	}
	if err := writeByte(f, byte(h.metric)); err != nil {
		return err
	}
	if err := writeUint16(f, uint16(h.cfg.M)); err != nil {
		return err
	}
	if err := writeUint16(f, uint16(h.cfg.M0)); err != nil {
		return err
	}
	if err := writeUint32(f, uint32(h.cfg.EfConstruction)); err != nil {
		return err
	}
	if err := writeUint32(f, uint32(h.cfg.EfSearch)); err != nil {
		return err
	}
	if err := writeInt32(f, int32(h.dim)); err != nil {
		return err
	}
	if err := writeInt32(f, h.entryPoint); err != nil {
		return err
	}
	if err := writeInt32(f, int32(h.maxLevel)); err != nil {
		return err
	}

	// Count live nodes only (skip tombstones to keep file compact)
	liveNodes := make([]*HNSWNode, 0, len(h.nodes))
	for _, node := range h.nodes {
		if !node.deleted {
			liveNodes = append(liveNodes, node)
		}
	}
	if err := writeUint32(f, uint32(len(liveNodes))); err != nil {
		return err
	}

	for _, node := range liveNodes {
		if err := writeInt32(f, node.id); err != nil {
			return err
		}
		if err := writeByte(f, 0); err != nil { // not deleted
			return err
		}
		if err := writeFrame(f, node.storageKey); err != nil {
			return err
		}
		// Write vector as [count uint32][float32...]
		if err := writeUint32(f, uint32(len(node.vector))); err != nil {
			return err
		}
		for _, fv := range node.vector {
			if err := writeUint32(f, math.Float32bits(fv)); err != nil {
				return err
			}
		}
		// Write neighbour lists
		numLayers := uint8(len(node.neighbours))
		if err := writeByte(f, numLayers); err != nil {
			return err
		}
		for _, layer := range node.neighbours {
			if err := writeUint16(f, uint16(len(layer))); err != nil {
				return err
			}
			for _, nbID := range layer {
				if err := writeInt32(f, nbID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// loadHNSWBin deserialises an HNSW graph from path.
// Returns (nil, nil) if the file does not exist.
func loadHNSWBin(path string) (*HNSW, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Magic
	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil {
		return nil, err
	}
	if string(magic) != "HNSW" {
		return nil, fmt.Errorf("hnsw.bin: invalid magic %q", magic)
	}

	version, err := readByte(f)
	if err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, fmt.Errorf("hnsw.bin: unsupported version %d", version)
	}

	metricByte, err := readByte(f)
	if err != nil {
		return nil, err
	}
	metric := Metric(metricByte)

	mVal, err := readUint16(f)
	if err != nil {
		return nil, err
	}
	m0Val, err := readUint16(f)
	if err != nil {
		return nil, err
	}
	efC, err := readUint32(f)
	if err != nil {
		return nil, err
	}
	efS, err := readUint32(f)
	if err != nil {
		return nil, err
	}

	cfg := HNSWConfig{
		M:              int(mVal),
		M0:             int(m0Val),
		EfConstruction: int(efC),
		EfSearch:       int(efS),
	}
	h := NewHNSW(cfg, metric)

	dimVal, err := readInt32(f)
	if err != nil {
		return nil, err
	}
	h.dim = int(dimVal)

	ep, err := readInt32(f)
	if err != nil {
		return nil, err
	}
	h.entryPoint = ep

	ml, err := readInt32(f)
	if err != nil {
		return nil, err
	}
	h.maxLevel = int(ml)

	numNodes, err := readUint32(f)
	if err != nil {
		return nil, err
	}

	for i := uint32(0); i < numNodes; i++ {
		id, err := readInt32(f)
		if err != nil {
			return nil, err
		}
		deleted, err := readByte(f)
		if err != nil {
			return nil, err
		}
		key, err := readFrame(f)
		if err != nil {
			return nil, err
		}
		vecLen, err := readUint32(f)
		if err != nil {
			return nil, err
		}
		vec := make([]float32, vecLen)
		for j := range vec {
			bits, err := readUint32(f)
			if err != nil {
				return nil, err
			}
			vec[j] = math.Float32frombits(bits)
		}
		numLayers, err := readByte(f)
		if err != nil {
			return nil, err
		}
		neighbours := make([][]int32, numLayers)
		for l := range neighbours {
			count, err := readUint16(f)
			if err != nil {
				return nil, err
			}
			layer := make([]int32, count)
			for k := range layer {
				nbID, err := readInt32(f)
				if err != nil {
					return nil, err
				}
				layer[k] = nbID
			}
			neighbours[l] = layer
		}
		maxLevel := int(numLayers) - 1
		if maxLevel < 0 {
			maxLevel = 0
		}
		node := &HNSWNode{
			id:         id,
			vector:     vec,
			storageKey: key,
			maxLevel:   maxLevel,
			deleted:    deleted != 0,
			neighbours: neighbours,
		}
		h.nodes[id] = node
		h.keyToID[string(key)] = id
		if !node.deleted {
			atomic.AddInt32(&h.totalCount, 1)
		} else {
			atomic.AddInt32(&h.deletedCount, 1)
			atomic.AddInt32(&h.totalCount, 1)
		}
		if id >= h.nextID {
			h.nextID = id + 1
		}
	}
	return h, nil
}

// --- Low-level I/O helpers ---

func writeFrame(w io.Writer, data []byte) error {
	var lenBuf [4]byte
	endian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := endian.Uint32(lenBuf[:])
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func writeByte(w io.Writer, b byte) error {
	_, err := w.Write([]byte{b})
	return err
}

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

func writeUint16(w io.Writer, v uint16) error {
	var b [2]byte
	endian.PutUint16(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func readUint16(r io.Reader) (uint16, error) {
	var b [2]byte
	_, err := io.ReadFull(r, b[:])
	return endian.Uint16(b[:]), err
}

func writeUint32(w io.Writer, v uint32) error {
	var b [4]byte
	endian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func readUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	_, err := io.ReadFull(r, b[:])
	return endian.Uint32(b[:]), err
}

func writeInt32(w io.Writer, v int32) error {
	return writeUint32(w, uint32(v))
}

func readInt32(r io.Reader) (int32, error) {
	v, err := readUint32(r)
	return int32(v), err
}
