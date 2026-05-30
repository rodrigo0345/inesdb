package executor

import (
	"testing"

	"github.com/rodrigo0345/omag/src/storage/schema"
)

func makeTestSchema(primaryCol string) schema.ITableSchema {
	cols := []schema.Column{
		{Name: primaryCol, Type: schema.TypeInt32},
		{Name: "val", Type: schema.TypeInt32},
	}
	ts := schema.NewTableSchema("test", cols)
	ts.AddIndex("PRIMARY", []string{primaryCol}, nil)
	return ts
}

func makeFullSchema() schema.ITableSchema {
	cols := []schema.Column{
		{Name: "id32", Type: schema.TypeInt32},
		{Name: "id64", Type: schema.TypeInt64},
		{Name: "score", Type: schema.TypeFloat64},
		{Name: "active", Type: schema.TypeBool},
		{Name: "name", Type: schema.TypeString},
	}
	ts := schema.NewTableSchema("full", cols)
	ts.AddIndex("PRIMARY", []string{"id32"}, nil)
	return ts
}

// --- Encode/Decode roundtrip ---

func TestEncodeDecodeRoundtrip_Int32(t *testing.T) {
	ts := makeTestSchema("id")
	values := map[string]any{"id": int64(42), "val": int64(99)}
	rowBytes, _, err := EncodeRow(ts, values)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	got, err := DecodeRow(ts, rowBytes)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	if got["id"].(int64) != 42 {
		t.Errorf("id: expected 42, got %v", got["id"])
	}
	if got["val"].(int64) != 99 {
		t.Errorf("val: expected 99, got %v", got["val"])
	}
}

func TestEncodeDecodeRoundtrip_AllTypes(t *testing.T) {
	ts := makeFullSchema()
	values := map[string]any{
		"id32":   int64(1),
		"id64":   int64(9999999999),
		"score":  float64(3.14),
		"active": true,
		"name":   "alice",
	}
	rowBytes, pk, err := EncodeRow(ts, values)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	if len(pk) == 0 {
		t.Error("expected non-empty primary key")
	}

	got, err := DecodeRow(ts, rowBytes)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	if got["id64"].(int64) != 9999999999 {
		t.Errorf("id64: expected 9999999999, got %v", got["id64"])
	}
	if got["active"].(bool) != true {
		t.Errorf("active: expected true, got %v", got["active"])
	}
	if got["name"].(string) != "alice" {
		t.Errorf("name: expected alice, got %v", got["name"])
	}
}

func TestEncodeRow_DefaultZeroValues(t *testing.T) {
	ts := makeTestSchema("id")
	rowBytes, pk, err := EncodeRow(ts, map[string]any{}) // no values provided
	if err != nil {
		t.Fatalf("EncodeRow with empty map: %v", err)
	}
	if len(rowBytes) == 0 {
		t.Error("expected non-empty row bytes")
	}
	_ = pk
}

func TestEncodeRow_PrimaryKeyExtracted(t *testing.T) {
	ts := makeTestSchema("id")
	values := map[string]any{"id": int64(7), "val": int64(0)}
	_, pk, err := EncodeRow(ts, values)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	if len(pk) == 0 {
		t.Error("expected non-empty primary key bytes for id=7")
	}
}

// --- MapSQLType ---

func TestMapSQLType_Int(t *testing.T) {
	for _, s := range []string{"INT", "INTEGER", "int4", "SERIAL"} {
		dt, err := MapSQLType(s)
		if err != nil || dt != schema.TypeInt32 {
			t.Errorf("MapSQLType(%q) = %v, %v; want TypeInt32", s, dt, err)
		}
	}
}

func TestMapSQLType_Bigint(t *testing.T) {
	for _, s := range []string{"BIGINT", "INT8", "BIGSERIAL"} {
		dt, err := MapSQLType(s)
		if err != nil || dt != schema.TypeInt64 {
			t.Errorf("MapSQLType(%q) = %v, %v; want TypeInt64", s, dt, err)
		}
	}
}

func TestMapSQLType_Float(t *testing.T) {
	for _, s := range []string{"FLOAT8", "DOUBLE PRECISION", "NUMERIC"} {
		dt, err := MapSQLType(s)
		if err != nil || dt != schema.TypeFloat64 {
			t.Errorf("MapSQLType(%q) = %v, %v; want TypeFloat64", s, dt, err)
		}
	}
}

func TestMapSQLType_Bool(t *testing.T) {
	for _, s := range []string{"BOOL", "BOOLEAN"} {
		dt, err := MapSQLType(s)
		if err != nil || dt != schema.TypeBool {
			t.Errorf("MapSQLType(%q) = %v, %v; want TypeBool", s, dt, err)
		}
	}
}

func TestMapSQLType_String(t *testing.T) {
	for _, s := range []string{"TEXT", "VARCHAR", "STRING"} {
		dt, err := MapSQLType(s)
		if err != nil || dt != schema.TypeString {
			t.Errorf("MapSQLType(%q) = %v, %v; want TypeString", s, dt, err)
		}
	}
}

func TestMapSQLType_Unknown(t *testing.T) {
	dt, err := MapSQLType("JSONB")
	if err != nil || dt != schema.TypeString {
		t.Errorf("unknown type should default to TypeString, got %v %v", dt, err)
	}
}

// --- ToNative ---

func TestToNative_IntStrings(t *testing.T) {
	v := ToNative("42", "int")
	if v.(int64) != 42 {
		t.Errorf("ToNative int: expected 42, got %v", v)
	}
}

func TestToNative_FloatStrings(t *testing.T) {
	v := ToNative("3.14", "float")
	if v.(float64) < 3.13 || v.(float64) > 3.15 {
		t.Errorf("ToNative float: expected ~3.14, got %v", v)
	}
}

func TestToNative_BoolStrings(t *testing.T) {
	if ToNative("true", "bool").(bool) != true {
		t.Error("ToNative true failed")
	}
	if ToNative("false", "bool").(bool) != false {
		t.Error("ToNative false failed")
	}
}

func TestToNative_StringPassthrough(t *testing.T) {
	v := ToNative("hello", "string")
	if v.(string) != "hello" {
		t.Errorf("ToNative string: expected hello, got %v", v)
	}
}
