package server_test

import (
	"fmt"
	"testing"

	"github.com/jackc/pgproto3/v2"
)

// extractAllDataRows returns the wire values from every DataRow in msgs.
func extractAllDataRows(msgs []pgproto3.BackendMessage) [][][]byte {
	var rows [][][]byte
	for _, m := range msgs {
		if dr, ok := m.(*pgproto3.DataRow); ok {
			row := make([][]byte, len(dr.Values))
			copy(row, dr.Values)
			rows = append(rows, row)
		}
	}
	return rows
}

// getColumnNames returns the field names from the RowDescription message, if any.
func getColumnNames(msgs []pgproto3.BackendMessage) []string {
	for _, m := range msgs {
		if rd, ok := m.(*pgproto3.RowDescription); ok {
			names := make([]string, len(rd.Fields))
			for i, f := range rd.Fields {
				names[i] = string(f.Name)
			}
			return names
		}
	}
	return nil
}

func TestSession_LimitOffset_Wire(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE lim_t (id INT PRIMARY KEY, val INT)")
	for i := 1; i <= 10; i++ {
		sendQuery(t, fe, fmt.Sprintf("INSERT INTO lim_t (id, val) VALUES (%d, %d)", i, i*10))
	}

	msgs := sendQuery(t, fe, "SELECT * FROM lim_t ORDER BY id ASC LIMIT 3")
	if hasError(msgs) {
		t.Fatal("LIMIT 3: unexpected error")
	}
	if n := countDataRows(msgs); n != 3 {
		t.Errorf("LIMIT 3: expected 3 rows, got %d", n)
	}

	msgs = sendQuery(t, fe, "SELECT * FROM lim_t ORDER BY id ASC LIMIT 3 OFFSET 3")
	if hasError(msgs) {
		t.Fatal("LIMIT 3 OFFSET 3: unexpected error")
	}
	if n := countDataRows(msgs); n != 3 {
		t.Errorf("LIMIT 3 OFFSET 3: expected 3 rows, got %d", n)
	}

	// Only 1 row left starting at offset 9.
	msgs = sendQuery(t, fe, "SELECT * FROM lim_t ORDER BY id ASC LIMIT 3 OFFSET 9")
	if hasError(msgs) {
		t.Fatal("LIMIT 3 OFFSET 9: unexpected error")
	}
	if n := countDataRows(msgs); n != 1 {
		t.Errorf("LIMIT 3 OFFSET 9: expected 1 row, got %d", n)
	}
}

func TestSession_OrderByDesc_Wire(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE sort_t (id INT PRIMARY KEY, val INT)")
	sendQuery(t, fe, "INSERT INTO sort_t (id, val) VALUES (1, 30)")
	sendQuery(t, fe, "INSERT INTO sort_t (id, val) VALUES (2, 10)")
	sendQuery(t, fe, "INSERT INTO sort_t (id, val) VALUES (3, 20)")

	msgs := sendQuery(t, fe, "SELECT * FROM sort_t ORDER BY val DESC")
	if hasError(msgs) {
		t.Fatal("ORDER BY DESC: unexpected error")
	}
	if n := countDataRows(msgs); n != 3 {
		t.Fatalf("ORDER BY DESC: expected 3 rows, got %d", n)
	}

	rows := extractAllDataRows(msgs)
	want := []string{"30", "20", "10"}
	for i, wantVal := range want {
		// val is the second column (index 1).
		if got := string(rows[i][1]); got != wantVal {
			t.Errorf("row %d val: want %s, got %s", i, wantVal, got)
		}
	}
}

func TestSession_SelectProjection_Wire(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE proj_w (id INT PRIMARY KEY, name TEXT, age INT)")
	sendQuery(t, fe, "INSERT INTO proj_w (id, name, age) VALUES (1, 'Alice', 30)")
	sendQuery(t, fe, "INSERT INTO proj_w (id, name, age) VALUES (2, 'Bob', 25)")

	msgs := sendQuery(t, fe, "SELECT id, name FROM proj_w")
	if hasError(msgs) {
		t.Fatal("SELECT projection: unexpected error")
	}

	cols := getColumnNames(msgs)
	if len(cols) != 2 {
		t.Fatalf("RowDescription: expected 2 fields, got %d: %v", len(cols), cols)
	}
	if cols[0] != "id" || cols[1] != "name" {
		t.Errorf("column names: want [id name], got %v", cols)
	}
	if n := countDataRows(msgs); n != 2 {
		t.Errorf("expected 2 data rows, got %d", n)
	}
	for i, row := range extractAllDataRows(msgs) {
		if len(row) != 2 {
			t.Errorf("row %d: expected 2 values, got %d", i, len(row))
		}
	}
}

func TestSession_MultiRowUpdate_Wire(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE upd_w (id INT PRIMARY KEY, active INT)")
	for i := 1; i <= 5; i++ {
		sendQuery(t, fe, fmt.Sprintf("INSERT INTO upd_w (id, active) VALUES (%d, 0)", i))
	}

	msgs := sendQuery(t, fe, "UPDATE upd_w SET active = 1")
	if hasError(msgs) {
		t.Fatal("UPDATE all rows: unexpected error")
	}
	if !hasCommandComplete(msgs, "UPDATE 5") {
		t.Error("expected command tag UPDATE 5")
	}

	msgs = sendQuery(t, fe, "SELECT * FROM upd_w WHERE active = 1")
	if n := countDataRows(msgs); n != 5 {
		t.Errorf("after UPDATE: expected 5 rows with active=1, got %d", n)
	}
}

func TestSession_MultiRowDelete_Wire(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE del_w (id INT PRIMARY KEY, grp INT)")
	for i := 1; i <= 6; i++ {
		grp := 1
		if i > 3 {
			grp = 2
		}
		sendQuery(t, fe, fmt.Sprintf("INSERT INTO del_w (id, grp) VALUES (%d, %d)", i, grp))
	}

	msgs := sendQuery(t, fe, "DELETE FROM del_w WHERE grp = 2")
	if hasError(msgs) {
		t.Fatal("DELETE: unexpected error")
	}
	if !hasCommandComplete(msgs, "DELETE 3") {
		t.Error("expected command tag DELETE 3")
	}

	msgs = sendQuery(t, fe, "SELECT * FROM del_w")
	if n := countDataRows(msgs); n != 3 {
		t.Errorf("after DELETE: expected 3 rows, got %d", n)
	}
}

func TestSession_DropTable_ThenSelect_Error(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE drp_t (id INT PRIMARY KEY, val INT)")
	sendQuery(t, fe, "INSERT INTO drp_t (id, val) VALUES (1, 10)")
	sendQuery(t, fe, "DROP TABLE drp_t")

	msgs := sendQuery(t, fe, "SELECT * FROM drp_t")
	if !hasError(msgs) {
		t.Error("expected ErrorResponse after SELECT on dropped table, got none")
	}
	if n := countDataRows(msgs); n != 0 {
		t.Errorf("expected 0 data rows after error, got %d", n)
	}
}

func TestSession_BatchInsert_Commit_Wire(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE batch_t (id INT PRIMARY KEY, val INT)")
	sendQuery(t, fe, "BEGIN")

	for i := 1; i <= 5; i++ {
		msgs := sendQuery(t, fe, fmt.Sprintf("INSERT INTO batch_t (id, val) VALUES (%d, %d)", i, i*10))
		if !hasCommandComplete(msgs, "INSERT 0 1") {
			t.Errorf("INSERT %d: expected tag INSERT 0 1", i)
		}
	}

	sendQuery(t, fe, "COMMIT")

	msgs := sendQuery(t, fe, "SELECT * FROM batch_t")
	if hasError(msgs) {
		t.Fatal("SELECT after batch INSERT: unexpected error")
	}
	if n := countDataRows(msgs); n != 5 {
		t.Errorf("batch insert: expected 5 rows, got %d", n)
	}
}

func TestSession_ComplexWhere_Wire(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE cw_t (id INT PRIMARY KEY, age INT, name TEXT)")
	sendQuery(t, fe, "INSERT INTO cw_t (id, age, name) VALUES (1, 25, 'Alice')")
	sendQuery(t, fe, "INSERT INTO cw_t (id, age, name) VALUES (2, 17, 'Bob')")
	sendQuery(t, fe, "INSERT INTO cw_t (id, age, name) VALUES (3, 30, 'Alice')")
	sendQuery(t, fe, "INSERT INTO cw_t (id, age, name) VALUES (4, 22, 'Charlie')")

	msgs := sendQuery(t, fe, "SELECT * FROM cw_t WHERE age > 20 AND name = 'Alice'")
	if hasError(msgs) {
		t.Fatal("complex WHERE: unexpected error")
	}
	if n := countDataRows(msgs); n != 2 {
		t.Errorf("WHERE age > 20 AND name = 'Alice': expected 2 rows, got %d", n)
	}
}

func TestSession_UpdateThenSelectSpecific_Wire(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE upd_sel (id INT PRIMARY KEY, val INT)")
	sendQuery(t, fe, "INSERT INTO upd_sel (id, val) VALUES (1, 10)")
	sendQuery(t, fe, "INSERT INTO upd_sel (id, val) VALUES (2, 20)")
	sendQuery(t, fe, "INSERT INTO upd_sel (id, val) VALUES (3, 30)")

	msgs := sendQuery(t, fe, "UPDATE upd_sel SET val = 99 WHERE id = 2")
	if !hasCommandComplete(msgs, "UPDATE 1") {
		t.Error("expected command tag UPDATE 1")
	}

	msgs = sendQuery(t, fe, "SELECT * FROM upd_sel WHERE id = 2")
	if hasError(msgs) {
		t.Fatal("SELECT after UPDATE: unexpected error")
	}
	if n := countDataRows(msgs); n != 1 {
		t.Fatalf("expected 1 row, got %d", n)
	}

	rows := extractAllDataRows(msgs)
	// schema order: id(0), val(1); wire sends as text bytes
	if got := string(rows[0][1]); got != "99" {
		t.Errorf("updated val: want '99', got %q", got)
	}
}
