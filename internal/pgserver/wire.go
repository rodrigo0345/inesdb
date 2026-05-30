package pgserver

import (
	"fmt"
	"strconv"

	"github.com/jackc/pgproto3/v2"
	"github.com/rodrigo0345/omag/internal/executor"
)

// PostgreSQL type OIDs for common types (text protocol).
const (
	oidText    uint32 = 25
	oidInt4    uint32 = 23
	oidInt8    uint32 = 20
	oidFloat8  uint32 = 701
	oidBool    uint32 = 16
)

// sendResult writes RowDescription + DataRows + CommandComplete to the backend.
func sendResult(be *pgproto3.Backend, result *executor.Result) error {
	if result == nil {
		return nil
	}

	// SELECT-like results have column metadata.
	if len(result.Columns) > 0 {
		fields := make([]pgproto3.FieldDescription, len(result.Columns))
		for i, col := range result.Columns {
			fields[i] = pgproto3.FieldDescription{
				Name:        []byte(col),
				DataTypeOID: oidText,
				Format:      pgproto3.TextFormat,
			}
		}
		if err := be.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
			return err
		}
		for _, row := range result.Rows {
			vals := make([][]byte, len(row))
			for i, v := range row {
				if v == nil {
					vals[i] = nil
				} else {
					vals[i] = encodeTextValue(v)
				}
			}
			if err := be.Send(&pgproto3.DataRow{Values: vals}); err != nil {
				return err
			}
		}
	}

	tag := result.Tag
	if tag == "" {
		tag = "OK"
	}
	return be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// sendError sends an ErrorResponse to the client.
func sendError(be *pgproto3.Backend, err error) {
	_ = be.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  err.Error(),
	})
}

// encodeTextValue converts a Go value to its PostgreSQL text wire representation.
func encodeTextValue(v any) []byte {
	switch x := v.(type) {
	case nil:
		return nil
	case bool:
		if x {
			return []byte("t")
		}
		return []byte("f")
	case int:
		return []byte(strconv.Itoa(x))
	case int32:
		return []byte(strconv.FormatInt(int64(x), 10))
	case int64:
		return []byte(strconv.FormatInt(x, 10))
	case float32:
		return []byte(strconv.FormatFloat(float64(x), 'g', -1, 32))
	case float64:
		return []byte(strconv.FormatFloat(x, 'g', -1, 64))
	case string:
		return []byte(x)
	case []byte:
		return x
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}
