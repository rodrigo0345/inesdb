package executor

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/rodrigo0345/omag/src/storage/schema"
)

// parseVectorLiteral parses a string like "[0.1, -0.5, 3.14]" into []float32.
func parseVectorLiteral(s string) ([]float32, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if s == "" {
		return []float32{}, nil
	}
	parts := strings.Split(s, ",")
	result := make([]float32, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, fmt.Errorf("invalid vector component %q: %w", p, err)
		}
		result[i] = float32(f)
	}
	return result, nil
}

// EncodeRow serialises a column→value map into the byte format the storage
// engine expects:
//   [0x01 _txn_op][null_flag_0..null_flag_{N-1}][col0_data..colN_data]
//
// null_flag_i == 1 means column i is NULL; the data bytes for that column are
// still written as zeros to preserve fixed offsets.
// It also returns the primary key bytes (concatenated bytes of PRIMARY index columns).
func EncodeRow(ts schema.ITableSchema, values map[string]any) (rowBytes []byte, primaryKey []byte, err error) {
	// _txn_op metadata byte always 0x01 (insert marker at schema level).
	rowBytes = append(rowBytes, 0x01)

	userCols := ts.GetColumns()
	primaryCols := primaryKeyColumns(ts)
	pkSet := make(map[string]struct{}, len(primaryCols))
	for _, c := range primaryCols {
		pkSet[c] = struct{}{}
	}

	// Phase 1: write null flags (one byte per user column).
	for _, col := range userCols {
		v := values[col.Name]
		if v == nil {
			rowBytes = append(rowBytes, 0x01) // NULL
		} else {
			rowBytes = append(rowBytes, 0x00) // not NULL
		}
	}

	// Phase 2: write column data (zero bytes for NULL columns).
	for _, col := range userCols {
		v, ok := values[col.Name]
		if !ok || v == nil {
			v = zeroValue(col.Type)
		}
		encoded, encErr := encodeValue(col.Type, v)
		if encErr != nil {
			return nil, nil, fmt.Errorf("encode column %s: %w", col.Name, encErr)
		}
		rowBytes = append(rowBytes, encoded...)
		if _, isPK := pkSet[col.Name]; isPK {
			primaryKey = append(primaryKey, encoded...)
		}
	}
	return rowBytes, primaryKey, nil
}

// DecodeRow deserialises engine row bytes into a column→value map.
// rowBytes is the value returned by the MVCC cursor (has _txn_op prefix).
// NULL columns are represented as nil in the returned map.
func DecodeRow(ts schema.ITableSchema, rowBytes []byte) (map[string]any, error) {
	result := make(map[string]any)
	for _, col := range ts.GetColumns() {
		v := ts.DecodeRow(col.Name, rowBytes)
		if v.Error() != nil {
			return nil, fmt.Errorf("decode column %s: %w", col.Name, v.Error())
		}
		if v.IsNull() {
			result[col.Name] = nil
		} else {
			result[col.Name] = schemaValueToNative(col.Type, v)
		}
	}
	return result, nil
}

// MapSQLType converts a GoSQLX ColumnDef.Type string to schema.DataType.
func MapSQLType(sqlType string) (schema.DataType, error) {
	switch strings.ToUpper(strings.TrimSpace(sqlType)) {
	case "INT", "INTEGER", "INT4", "SERIAL":
		return schema.TypeInt32, nil
	case "BIGINT", "INT8", "INT64", "BIGSERIAL":
		return schema.TypeInt64, nil
	case "FLOAT", "FLOAT4", "REAL":
		return schema.TypeInt32, nil // use int32 as approximation
	case "FLOAT8", "DOUBLE PRECISION", "NUMERIC", "DECIMAL":
		return schema.TypeFloat64, nil
	case "BOOL", "BOOLEAN":
		return schema.TypeBool, nil
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER VARYING", "STRING":
		return schema.TypeString, nil
	case "UUID":
		return schema.TypeString, nil
	default:
		// VECTOR(n) type
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlType)), "VECTOR") {
			return schema.TypeVector, nil
		}
		// unknown types default to string for flexibility
		return schema.TypeString, nil
	}
}

// primaryKeyColumns returns the column names that form the PRIMARY index.
func primaryKeyColumns(ts schema.ITableSchema) []string {
	idx := ts.GetIndex("PRIMARY")
	if idx == nil {
		cols := ts.GetColumns()
		if len(cols) > 0 {
			return []string{cols[0].Name}
		}
		return nil
	}
	return idx.Columns
}

// encodeValue encodes a Go value into the binary format for a given schema type.
func encodeValue(dt schema.DataType, v any) ([]byte, error) {
	switch dt {
	case schema.TypeMetadata:
		b, err := toUint8(v)
		return []byte{b}, err
	case schema.TypeBool:
		boolVal, err := toBool(v)
		if err != nil {
			return nil, err
		}
		if boolVal {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case schema.TypeInt32:
		n, err := toInt64(v)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 4)
		schema.DbEndian.PutUint32(buf, uint32(int32(n)))
		return buf, nil
	case schema.TypeInt64:
		n, err := toInt64(v)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 8)
		schema.DbEndian.PutUint64(buf, uint64(n))
		return buf, nil
	case schema.TypeFloat64:
		f, err := toFloat64(v)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 8)
		schema.DbEndian.PutUint64(buf, math.Float64bits(f))
		return buf, nil
	case schema.TypeString:
		s := fmt.Sprintf("%v", v)
		buf := make([]byte, 4+len(s))
		schema.DbEndian.PutUint32(buf[:4], uint32(len(s)))
		copy(buf[4:], s)
		return buf, nil
	case schema.TypeVector:
		var vecs []float32
		switch x := v.(type) {
		case []float32:
			vecs = x
		case string:
			parsed, err := parseVectorLiteral(x)
			if err != nil {
				return nil, fmt.Errorf("invalid vector literal: %w", err)
			}
			vecs = parsed
		default:
			return nil, fmt.Errorf("cannot convert %T to []float32", v)
		}
		buf := make([]byte, 4+len(vecs)*4)
		schema.DbEndian.PutUint32(buf[:4], uint32(len(vecs)))
		for i, f := range vecs {
			schema.DbEndian.PutUint32(buf[4+i*4:], math.Float32bits(f))
		}
		return buf, nil
	default:
		return nil, fmt.Errorf("unsupported type %d", dt)
	}
}

func zeroValue(dt schema.DataType) any {
	switch dt {
	case schema.TypeBool:
		return false
	case schema.TypeInt32, schema.TypeInt64:
		return int64(0)
	case schema.TypeFloat64:
		return float64(0)
	case schema.TypeString:
		return ""
	case schema.TypeVector:
		return []float32{}
	default:
		return 0
	}
}

func schemaValueToNative(dt schema.DataType, v schema.Value) any {
	switch dt {
	case schema.TypeBool:
		return v.Bool()
	case schema.TypeInt32, schema.TypeInt64:
		return v.Int()
	case schema.TypeFloat64:
		return v.Float()
	case schema.TypeString:
		return v.String()
	case schema.TypeVector:
		inner := v.Inner()
		if vecs, ok := inner.([]float32); ok {
			return vecs
		}
		return []float32{}
	default:
		return v.Int()
	}
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case float32:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			// try float
			f, ferr := strconv.ParseFloat(strings.TrimSpace(x), 64)
			if ferr != nil {
				return 0, fmt.Errorf("cannot convert %q to int: %w", x, err)
			}
			return int64(f), nil
		}
		return n, nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to float: %w", x, err)
		}
		return f, nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toBool(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int, int32, int64:
		n, _ := toInt64(v)
		return n != 0, nil
	case string:
		s := strings.ToLower(strings.TrimSpace(x))
		switch s {
		case "true", "t", "yes", "1":
			return true, nil
		case "false", "f", "no", "0":
			return false, nil
		}
		return false, fmt.Errorf("cannot convert %q to bool", x)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

func toUint8(v any) (uint8, error) {
	n, err := toInt64(v)
	return uint8(n), err
}

// ToNative converts a LiteralValue string+type into a Go value.
func ToNative(literalValue string, literalType string) any {
	switch strings.ToLower(literalType) {
	case "int", "integer", "bigint", "smallint", "numeric", "decimal":
		n, err := strconv.ParseInt(strings.TrimSpace(literalValue), 10, 64)
		if err != nil {
			f, ferr := strconv.ParseFloat(strings.TrimSpace(literalValue), 64)
			if ferr == nil {
				return int64(f)
			}
			return int64(0)
		}
		return n
	case "float", "double", "real":
		f, err := strconv.ParseFloat(strings.TrimSpace(literalValue), 64)
		if err != nil {
			return float64(0)
		}
		return f
	case "bool", "boolean":
		s := strings.ToLower(strings.TrimSpace(literalValue))
		return s == "true" || s == "t" || s == "1" || s == "yes"
	default:
		return literalValue
	}
}
