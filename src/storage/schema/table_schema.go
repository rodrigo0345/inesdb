package schema

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/rodrigo0345/omag/src/storage"
)

type DataType uint8

var DbEndian binary.ByteOrder = binary.LittleEndian

const TRANSACTIONAL_OP = "_txn_op"

const (
	TypeMetadata DataType = iota
	TypeInt32
	TypeInt64
	TypeFloat64
	TypeBool
	TypeString
	TypeVector // []float32; MaxLen holds the expected dimension count
)

// Decoder signature for low-level byte parsing.
type Decoder func(row []byte, offset int) (value any, size int, err error)

// TypeDecoders is the registry of how to turn bytes into Go types.
var TypeDecoders = map[DataType]Decoder{
	TypeMetadata: func(row []byte, offset int) (any, int, error) {
		if offset >= len(row) {
			return nil, 0, fmt.Errorf("EOF")
		}
		return row[offset], 1, nil
	},
	TypeBool: func(row []byte, offset int) (any, int, error) {
		if offset >= len(row) {
			return nil, 0, fmt.Errorf("EOF")
		}
		return row[offset] != 0, 1, nil
	},
	TypeInt32: func(row []byte, offset int) (any, int, error) {
		if offset+4 > len(row) {
			return nil, 0, fmt.Errorf("EOF")
		}
		return int32(DbEndian.Uint32(row[offset:])), 4, nil
	},
	TypeInt64: func(row []byte, offset int) (any, int, error) {
		if offset+8 > len(row) {
			return nil, 0, fmt.Errorf("EOF")
		}
		return int64(DbEndian.Uint64(row[offset:])), 8, nil
	},
	TypeFloat64: func(row []byte, offset int) (any, int, error) {
		if offset+8 > len(row) {
			return nil, 0, fmt.Errorf("EOF")
		}
		bits := DbEndian.Uint64(row[offset:])
		return math.Float64frombits(bits), 8, nil
	},
	TypeString: func(row []byte, offset int) (any, int, error) {
		if offset+4 > len(row) {
			return nil, 0, fmt.Errorf("truncated string header")
		}
		strLen := int(DbEndian.Uint32(row[offset : offset+4]))
		if offset+4+strLen > len(row) {
			return nil, 0, fmt.Errorf("truncated string data, original data: %x", row[offset:offset+4])
		}
		return string(row[offset+4 : offset+4+strLen]), 4 + strLen, nil
	},
	TypeVector: func(row []byte, offset int) (any, int, error) {
		if offset+4 > len(row) {
			return nil, 0, fmt.Errorf("EOF reading vector dimension")
		}
		n := int(DbEndian.Uint32(row[offset : offset+4]))
		size := 4 + n*4
		if offset+size > len(row) {
			return nil, 0, fmt.Errorf("truncated vector data: need %d bytes, have %d", size, len(row)-offset)
		}
		vecs := make([]float32, n)
		for i := range vecs {
			bits := DbEndian.Uint32(row[offset+4+i*4:])
			vecs[i] = math.Float32frombits(bits)
		}
		return vecs, size, nil
	},
}

// --- Value Wrapper (The "Easy to Use" part) ---

// Value wraps the result of a decode operation, allowing for fluent access.
type Value struct {
	inner  any
	err    error
	isNull bool
}

// IsNull reports whether this value is a SQL NULL (distinct from a zero value).
func (v Value) IsNull() bool { return v.isNull && v.err == nil }

func (v Value) Int() int64 {
	if v.err != nil || v.isNull {
		return 0
	}
	switch val := v.inner.(type) {
	case int32:
		return int64(val)
	case int64:
		return val
	case byte:
		return int64(val)
	default:
		return 0
	}
}

func (v Value) String() string {
	if v.err != nil || v.isNull {
		return ""
	}
	s, _ := v.inner.(string)
	return s
}

func (v Value) Float() float64 {
	if v.err != nil || v.isNull {
		return 0.0
	}
	f, _ := v.inner.(float64)
	return f
}

func (v Value) Bool() bool {
	if v.err != nil || v.isNull {
		return false
	}
	b, _ := v.inner.(bool)
	return b
}

// Datetime assumes the underlying data is a Unix Timestamp (Int64).
func (v Value) Datetime() time.Time {
	if v.err != nil {
		return time.Time{}
	}
	return time.Unix(v.Int(), 0)
}

func (v Value) Error() error { return v.err }

// Inner returns the raw decoded value. Useful for types not covered by the
// typed accessors (e.g. []float32 for TypeVector).
func (v Value) Inner() any { return v.inner }

// --- Table Schema Implementation ---

type Column struct {
	Name          string
	Type          DataType
	MaxLen        uint32
	AutoIncrement bool // auto-assign next integer on INSERT if no value provided
	IsUUID        bool // auto-generate UUID v4 on INSERT if no value provided
	Nullable      bool // true = column allows NULL (default); false = NOT NULL constraint
}

type Index struct {
	Name    string
	Columns []string
	Engine  storage.IStorageEngine
	Unique  bool // enforce UNIQUE constraint on writes
}

// ForeignKey describes a referential integrity constraint on this table.
// OnDelete/OnUpdate: "CASCADE", "RESTRICT", "NO ACTION", "" (treated as RESTRICT).
type ForeignKey struct {
	Name              string
	LocalColumns      []string
	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          string
	OnUpdate          string
}

type TableSchema struct {
	Name          string
	Columns       []Column
	Indexes       map[string]*Index
	ForeignKeys   []ForeignKey
	StorageEngine string // "LSM" (default), "BTREE", "VECTOR"
}

func (ts *TableSchema) GetStorageEngine() string { return ts.StorageEngine }
func (ts *TableSchema) SetStorageEngine(e string) { ts.StorageEngine = e }

func NewTableSchema(name string, columns []Column) *TableSchema {
	// User columns default to nullable (SQL standard).
	// Callers that want NOT NULL must call SetColumnNotNull after construction,
	// or pass Nullable=false in the column if they control construction directly.
	userCols := make([]Column, len(columns))
	for i, c := range columns {
		if !c.Nullable {
			// Preserve explicit Nullable=false from the caller; otherwise set true.
			// Since false is Go's zero value we can't distinguish "not set" from
			// "set to NOT NULL", so callers that want nullable must set Nullable=true
			// explicitly — the DDL executor always does this.
			userCols[i] = c
		} else {
			userCols[i] = c // already true
		}
	}

	actualColumns := append([]Column{
		{Name: TRANSACTIONAL_OP, Type: TypeMetadata},
	}, userCols...)

	return &TableSchema{
		Name:        name,
		Columns:     actualColumns,
		Indexes:     make(map[string]*Index),
		ForeignKeys: nil,
	}
}

// SetColumnNotNull marks a column as NOT NULL.
func (ts *TableSchema) SetColumnNotNull(columnName string) {
	for i := range ts.Columns {
		if ts.Columns[i].Name == columnName {
			ts.Columns[i].Nullable = false
			return
		}
	}
}

// DecodeRow is the main user entry point. It handles offset walking and wrapping.
//
// Row layout: [0x01 _txn_op][null_flag_0..null_flag_{N-1}][col0_data..colN_data]
// where N = number of user columns (Columns[1:]).
func (ts *TableSchema) DecodeRow(columnName string, row []byte) Value {
	if len(row) == 0 {
		return Value{err: fmt.Errorf("cannot decode empty row")}
	}

	// Special case: _txn_op is always at byte 0.
	if columnName == TRANSACTIONAL_OP {
		decoder := TypeDecoders[TypeMetadata]
		val, _, err := decoder(row, 0)
		if err != nil {
			return Value{err: err}
		}
		return Value{inner: val}
	}

	numUserCols := len(ts.Columns) - 1
	nullFlagsStart := 1
	dataStart := 1 + numUserCols

	if len(row) < dataStart {
		return Value{err: fmt.Errorf("row too short: need %d bytes, got %d", dataStart, len(row))}
	}

	dataOffset := dataStart
	for i, col := range ts.Columns[1:] {
		isNull := row[nullFlagsStart+i] == 1

		if col.Name == columnName {
			if isNull {
				return Value{isNull: true}
			}
			decoder, ok := TypeDecoders[col.Type]
			if !ok {
				return Value{err: fmt.Errorf("unsupported type %d", col.Type)}
			}
			val, _, err := decoder(row, dataOffset)
			if err != nil {
				return Value{err: fmt.Errorf("decode failed for %s: %w", col.Name, err)}
			}
			return Value{inner: val}
		}

		size, err := ts.calculateSize(col.Type, row, dataOffset)
		if err != nil {
			return Value{err: fmt.Errorf("skip failed at %s: %w", col.Name, err)}
		}
		dataOffset += size
	}

	return Value{err: fmt.Errorf("column %q not found in schema", columnName)}
}

func (ts *TableSchema) GetRowExpectedSize() int {
	// 1 byte for _txn_op + 1 null-flag byte per user column.
	numUserCols := len(ts.Columns) - 1
	size := 1 + numUserCols

	// Add data bytes for each user column.
	for i := 1; i < len(ts.Columns); i++ {
		col := &ts.Columns[i]
		switch col.Type {
		case TypeBool:
			size += 1
		case TypeInt32:
			size += 4
		case TypeInt64, TypeFloat64:
			size += 8
		case TypeString:
			size += 4 + int(col.MaxLen)
		case TypeVector:
			size += 4 + 4*int(col.MaxLen) // 4-byte count + 4 bytes per float32
		}
	}
	return size
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
		strLen := int(DbEndian.Uint32(row[offset : offset+4]))
		return 4 + strLen, nil
	case TypeVector:
		if offset+4 > len(row) {
			return 0, fmt.Errorf("truncated vector header")
		}
		n := int(DbEndian.Uint32(row[offset : offset+4]))
		return 4 + n*4, nil
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
// NULL columns contribute their zero-valued data bytes to index keys.
func (ts *TableSchema) ExtractIndexValues(row []byte) (map[string][]byte, error) {
	numUserCols := len(ts.Columns) - 1
	dataStart := 1 + numUserCols // skip _txn_op byte + null flags

	if len(row) < dataStart {
		return nil, fmt.Errorf("row too short for ExtractIndexValues: need %d bytes, got %d", dataStart, len(row))
	}

	colSlices := make(map[string][]byte)

	// _txn_op lives at byte 0.
	colSlices[TRANSACTIONAL_OP] = row[0:1]

	// Walk user columns starting after _txn_op + null flags.
	offset := dataStart
	for _, col := range ts.Columns[1:] {
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
	if len(ts.Columns) <= 1 {
		return nil
	}
	return ts.Columns[1:]
}

func (ts *TableSchema) AddIndex(name string, columns []string, engine storage.IStorageEngine) {
	ts.Indexes[name] = &Index{Name: name, Columns: columns, Engine: engine}
}

func (ts *TableSchema) SetIndexUnique(name string) {
	if idx := ts.Indexes[name]; idx != nil {
		idx.Unique = true
	}
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

func (ts *TableSchema) AddForeignKey(fk ForeignKey) {
	ts.ForeignKeys = append(ts.ForeignKeys, fk)
}

func (ts *TableSchema) GetForeignKeys() []ForeignKey {
	return ts.ForeignKeys
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
