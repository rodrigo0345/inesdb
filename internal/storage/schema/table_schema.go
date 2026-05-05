package schema

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/rodrigo0345/omag/internal/storage"
)

type DataType uint8

const TRANSACTIONAL_OP = "_txn_op"

const (
	TypeMetadata DataType = iota
	TypeInt32
	TypeInt64
	TypeFloat64
	TypeBool
	TypeString
)
// Decoder signature for low-level byte parsing.
type Decoder func(row []byte, offset int) (value any, size int, err error)

// TypeDecoders is the registry of how to turn bytes into Go types.
var TypeDecoders = map[DataType]Decoder{
	TypeMetadata: func(row []byte, offset int) (any, int, error) {
		if offset >= len(row) { return nil, 0, fmt.Errorf("EOF") }
		return row[offset], 1, nil
	},
	TypeBool: func(row []byte, offset int) (any, int, error) {
		if offset >= len(row) { return nil, 0, fmt.Errorf("EOF") }
		return row[offset] != 0, 1, nil
	},
	TypeInt32: func(row []byte, offset int) (any, int, error) {
		if offset+4 > len(row) { return nil, 0, fmt.Errorf("EOF") }
		return int32(binary.BigEndian.Uint32(row[offset:])), 4, nil
	},
	TypeInt64: func(row []byte, offset int) (any, int, error) {
		if offset+8 > len(row) { return nil, 0, fmt.Errorf("EOF") }
		return int64(binary.BigEndian.Uint64(row[offset:])), 8, nil
	},
	TypeFloat64: func(row []byte, offset int) (any, int, error) {
		if offset+8 > len(row) { return nil, 0, fmt.Errorf("EOF") }
		bits := binary.BigEndian.Uint64(row[offset:])
		return math.Float64frombits(bits), 8, nil
	},
	TypeString: func(row []byte, offset int) (any, int, error) {
		if offset+4 > len(row) { return nil, 0, fmt.Errorf("truncated string header") }
		strLen := int(binary.BigEndian.Uint32(row[offset : offset+4]))
		if offset+4+strLen > len(row) { return nil, 0, fmt.Errorf("truncated string data") }
		return string(row[offset+4 : offset+4+strLen]), 4 + strLen, nil
	},
}

// --- Value Wrapper (The "Easy to Use" part) ---

// Value wraps the result of a decode operation, allowing for fluent access.
type Value struct {
	inner any
	err   error
}

func (v Value) Int() int64 {
	if v.err != nil { return 0 }
	switch val := v.inner.(type) {
	case int32: return int64(val)
	case int64: return val
	case byte:  return int64(val)
	default:    return 0
	}
}

func (v Value) String() string {
	if v.err != nil { return "" }
	s, _ := v.inner.(string)
	return s
}

func (v Value) Float() float64 {
	if v.err != nil { return 0.0 }
	f, _ := v.inner.(float64)
	return f
}

func (v Value) Bool() bool {
	if v.err != nil { return false }
	b, _ := v.inner.(bool)
	return b
}

// Datetime assumes the underlying data is a Unix Timestamp (Int64).
func (v Value) Datetime() time.Time {
	if v.err != nil { return time.Time{} }
	return time.Unix(v.Int(), 0)
}

func (v Value) Error() error { return v.err }

// --- Table Schema Implementation ---

type Column struct {
	Name   string
	Type   DataType
	MaxLen uint32
}

type Index struct {
	Name    string
	Columns []string
	Engine  storage.IStorageEngine
}

type TableSchema struct {
	Name    string
	Columns []Column
	Indexes map[string]*Index
}

func NewTableSchema(name string, columns []Column) *TableSchema {
	actualColumns := append([]Column{
		{Name: TRANSACTIONAL_OP, Type: TypeMetadata},
	}, columns...)

	return &TableSchema{
		Name:    name,
		Columns: actualColumns,
		Indexes: make(map[string]*Index),
	}
}

// DecodeRow is the main user entry point. It handles offset walking and wrapping.
func (ts *TableSchema) DecodeRow(columnName string, row []byte) Value {
	if len(row) == 0 {
		return Value{err: fmt.Errorf("cannot decode empty row")}
	}

	offset := 0
	for i := range ts.Columns {
		col := &ts.Columns[i]

		if col.Name == columnName {
			decoder, ok := TypeDecoders[col.Type]
			if !ok {
				return Value{err: fmt.Errorf("unsupported type %d", col.Type)}
			}
			val, _, err := decoder(row, offset)
			if err != nil {
				return Value{err: fmt.Errorf("decode failed for %s: %w", col.Name, err)}
			}
			return Value{inner: val}
		}

		size, err := ts.calculateSize(col.Type, row, offset)
		if err != nil {
			return Value{err: fmt.Errorf("skip failed at %s: %w", col.Name, err)}
		}
		offset += size
	}

	return Value{err: fmt.Errorf("column %q not found in schema", columnName)}
}

func (ts *TableSchema) calculateSize(t DataType, row []byte, offset int) (int, error) {
	switch t {
	case TypeMetadata, TypeBool:
		return 1, nil
	case TypeInt32:
		return 4, nil
	case TypeInt64, TypeFloat64:
		return 8, nil
	case TypeString:
		if offset+4 > len(row) {
			return 0, fmt.Errorf("truncated string header")
		}
		strLen := int(binary.BigEndian.Uint32(row[offset : offset+4]))
		return 4 + strLen, nil
	default:
		return 0, fmt.Errorf("unknown type size")
	}
}

func (ts *TableSchema) GetColumnTypeDecoder(columnName string) (Decoder, error) {
	for i := range ts.Columns {
		if ts.Columns[i].Name == columnName {
			return TypeDecoders[ts.Columns[i].Type], nil
		}
	}
	return nil, fmt.Errorf("column %s not found", columnName)
}

// ExtractIndexValues parses the row once and builds index keys.
func (ts *TableSchema) ExtractIndexValues(row []byte) (map[string][]byte, error) {
	colSlices := make(map[string][]byte)
	offset := 0

	for i := range ts.Columns {
		col := &ts.Columns[i]
		start := offset

		_, size, err := TypeDecoders[col.Type](row, offset)
		if err != nil {
			return nil, fmt.Errorf("extract err at %s: %w", col.Name, err)
		}

		colSlices[col.Name] = row[start : offset+size]
		offset += size
	}

	indexValues := make(map[string][]byte)
	for name, idx := range ts.Indexes {
		var keyBuf []byte
		for _, colName := range idx.Columns {
			b, ok := colSlices[colName]
			if !ok {
				return nil, fmt.Errorf("index %s refers to unknown column %s", name, colName)
			}
			keyBuf = append(keyBuf, b...)
		}
		indexValues[name] = keyBuf
	}
	return indexValues, nil
}

// --- Metadata Methods ---

func (ts *TableSchema) GetName() string { return ts.Name }

func (ts *TableSchema) GetColumns() []Column {
	if len(ts.Columns) <= 1 { return nil }
	return ts.Columns[1:]
}

func (ts *TableSchema) AddIndex(name string, columns []string, engine storage.IStorageEngine) {
	ts.Indexes[name] = &Index{Name: name, Columns: columns, Engine: engine}
}

func (ts *TableSchema) GetIndex(name string) *Index { return ts.Indexes[name] }

func (ts *TableSchema) GetAllIndexes() []*Index {
	indexes := make([]*Index, 0, len(ts.Indexes))
	for _, idx := range ts.Indexes {
		indexes = append(indexes, idx)
	}
	return indexes
}

func (ts *TableSchema) GetIndexesByColumn(columnName string) []*Index {
	var matched []*Index
	for _, idx := range ts.Indexes {
		if len(idx.Columns) > 0 && idx.Columns[0] == columnName {
			matched = append(matched, idx)
		}
	}
	return matched
}

func (ts *TableSchema) ToJSON() ([]byte, error) {
	idxMap := make(map[string][]string)
	for name, idx := range ts.Indexes {
		idxMap[name] = idx.Columns
	}
	return json.Marshal(map[string]any{
		"name":    ts.Name,
		"columns": ts.Columns,
		"indexes": idxMap,
	})
}
