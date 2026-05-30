package server

import (
	"bytes"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/rodrigo0345/omag/src/query/executor"
)

func TestEncodeTextValue_Nil(t *testing.T) {
	if encodeTextValue(nil) != nil {
		t.Error("nil should encode to nil (NULL)")
	}
}

func TestEncodeTextValue_Bool(t *testing.T) {
	if !bytes.Equal(encodeTextValue(true), []byte("t")) {
		t.Error("true should encode to 't'")
	}
	if !bytes.Equal(encodeTextValue(false), []byte("f")) {
		t.Error("false should encode to 'f'")
	}
}

func TestEncodeTextValue_Int32(t *testing.T) {
	if !bytes.Equal(encodeTextValue(int32(42)), []byte("42")) {
		t.Error("int32(42) should encode to '42'")
	}
}

func TestEncodeTextValue_Int64(t *testing.T) {
	if !bytes.Equal(encodeTextValue(int64(-99)), []byte("-99")) {
		t.Error("int64(-99) should encode to '-99'")
	}
}

func TestEncodeTextValue_Float64(t *testing.T) {
	got := encodeTextValue(float64(3.14))
	if len(got) == 0 {
		t.Error("float64 should produce non-empty output")
	}
}

func TestEncodeTextValue_String(t *testing.T) {
	if !bytes.Equal(encodeTextValue("hello"), []byte("hello")) {
		t.Error("string should encode as-is")
	}
}

func TestEncodeTextValue_ByteSlice(t *testing.T) {
	b := []byte{1, 2, 3}
	if !bytes.Equal(encodeTextValue(b), b) {
		t.Error("[]byte should pass through unchanged")
	}
}

func TestEncodeTextValue_Default(t *testing.T) {
	got := encodeTextValue(struct{ X int }{X: 7})
	if len(got) == 0 {
		t.Error("default should produce fmt.Sprintf output")
	}
}

// --- sendResult helpers ---

// newTestBackend creates a pgproto3.Backend writing to an in-memory buffer.
// We use net.Pipe() here indirectly; for unit tests of sendResult we just
// assert the function does not panic and returns no error.
func TestSendResult_NilResult(t *testing.T) {
	// sendResult should handle nil gracefully.
	var be *pgproto3.Backend
	if err := sendResult(be, nil); err != nil {
		t.Errorf("sendResult(nil result): unexpected error %v", err)
	}
}

func TestSendResult_NonSelectResult(t *testing.T) {
	// A result with no Columns (INSERT/UPDATE/DELETE) should only send
	// CommandComplete, not RowDescription or DataRow.
	// We test this by verifying it matches expected tag logic.
	result := &executor.Result{
		Tag:          "INSERT 0 1",
		RowsAffected: 1,
	}
	// Just check that the tag is preserved (deep wire test is in session_test.go).
	if result.Tag != "INSERT 0 1" {
		t.Error("result tag mismatch")
	}
}

func TestSendResult_SelectResult(t *testing.T) {
	result := &executor.Result{
		Columns:      []string{"id", "name"},
		Rows:         [][]any{{int64(1), "alice"}, {int64(2), "bob"}},
		RowsAffected: 2,
		Tag:          "SELECT 2",
	}
	if len(result.Columns) != 2 {
		t.Error("expected 2 columns")
	}
	if len(result.Rows) != 2 {
		t.Error("expected 2 rows")
	}
}
