package server_test

import (
	"net"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jackc/pgproto3/v2"
	dbengine "github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/src/query/server"
)

// openTestDB creates a real in-process database for session tests.
func openTestDB(t *testing.T) dbengine.Database {
	t.Helper()
	tmpDir := t.TempDir()
	db, err := dbengine.OpenMVCCLSM(dbengine.Options{
		DBPath:     filepath.Join(tmpDir, "test.db"),
		LSMDataDir: filepath.Join(tmpDir, "lsm"),
		WALPath:    filepath.Join(tmpDir, "test.wal"),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// startSession creates a net.Pipe, runs the server session in a goroutine,
// and performs the PostgreSQL startup handshake. Returns a Frontend ready
// to send queries and a stop function.
func startSession(t *testing.T, db dbengine.Database) (*pgproto3.Frontend, func()) {
	t.Helper()
	serverConn, clientConn := net.Pipe()

	srv := server.New(db)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = srv.RunConn(serverConn)
	}()

	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)

	// Perform startup handshake.
	if err := fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber}); err != nil {
		t.Fatalf("send startup: %v", err)
	}
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatalf("receive startup: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	stop := func() {
		_ = fe.Send(&pgproto3.Terminate{})
		_ = clientConn.Close()
		wg.Wait()
	}
	return fe, stop
}

// sendQuery sends a simple-query message and collects all response messages
// until ReadyForQuery.
func sendQuery(t *testing.T, fe *pgproto3.Frontend, sql string) []pgproto3.BackendMessage {
	t.Helper()
	if err := fe.Send(&pgproto3.Query{String: sql}); err != nil {
		t.Fatalf("send %q: %v", sql, err)
	}
	var msgs []pgproto3.BackendMessage
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatalf("receive for %q: %v", sql, err)
		}
		msgs = append(msgs, msg)
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	return msgs
}

func hasCommandComplete(msgs []pgproto3.BackendMessage, tag string) bool {
	for _, m := range msgs {
		if cc, ok := m.(*pgproto3.CommandComplete); ok {
			return string(cc.CommandTag) == tag
		}
	}
	return false
}

func hasError(msgs []pgproto3.BackendMessage) bool {
	for _, m := range msgs {
		if _, ok := m.(*pgproto3.ErrorResponse); ok {
			return true
		}
	}
	return false
}

func countDataRows(msgs []pgproto3.BackendMessage) int {
	n := 0
	for _, m := range msgs {
		if _, ok := m.(*pgproto3.DataRow); ok {
			n++
		}
	}
	return n
}

// ---- Tests ----

func TestSession_Handshake(t *testing.T) {
	db := openTestDB(t)
	_, stop := startSession(t, db)
	defer stop()
	// If startSession completes without error, handshake succeeded.
}

func TestSession_NoopSet(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	msgs := sendQuery(t, fe, "SET client_encoding = 'UTF8'")
	if hasError(msgs) {
		t.Error("SET should not produce an error")
	}
	if !hasCommandComplete(msgs, "SET") {
		t.Error("expected SET command tag")
	}
}

func TestSession_SelectConstant(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	msgs := sendQuery(t, fe, "SELECT 1")
	if hasError(msgs) {
		t.Fatal("SELECT 1 failed")
	}
	if countDataRows(msgs) != 1 {
		t.Errorf("expected 1 data row, got %d", countDataRows(msgs))
	}
}

func TestSession_CreateInsertSelect(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	if msgs := sendQuery(t, fe, "CREATE TABLE wire_t (id INT PRIMARY KEY, val INT)"); hasError(msgs) {
		t.Fatal("CREATE TABLE failed")
	}
	sendQuery(t, fe, "INSERT INTO wire_t (id, val) VALUES (1, 10)")
	sendQuery(t, fe, "INSERT INTO wire_t (id, val) VALUES (2, 20)")

	msgs := sendQuery(t, fe, "SELECT * FROM wire_t")
	if hasError(msgs) {
		t.Fatal("SELECT failed")
	}
	if countDataRows(msgs) != 2 {
		t.Errorf("expected 2 rows, got %d", countDataRows(msgs))
	}
}

func TestSession_WhereFilter(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE filter_t (id INT PRIMARY KEY, val INT)")
	sendQuery(t, fe, "INSERT INTO filter_t (id, val) VALUES (1, 10)")
	sendQuery(t, fe, "INSERT INTO filter_t (id, val) VALUES (2, 20)")
	sendQuery(t, fe, "INSERT INTO filter_t (id, val) VALUES (3, 30)")

	msgs := sendQuery(t, fe, "SELECT * FROM filter_t WHERE val > 10")
	if countDataRows(msgs) != 2 {
		t.Errorf("WHERE val > 10: expected 2 rows, got %d", countDataRows(msgs))
	}
}

func TestSession_BeginCommit(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	if msgs := sendQuery(t, fe, "BEGIN"); !hasCommandComplete(msgs, "BEGIN") {
		t.Error("expected BEGIN command tag")
	}
	if msgs := sendQuery(t, fe, "COMMIT"); !hasCommandComplete(msgs, "COMMIT") {
		t.Error("expected COMMIT command tag")
	}
}

func TestSession_Rollback(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE rollback_t (id INT PRIMARY KEY, val INT)")
	sendQuery(t, fe, "BEGIN")
	sendQuery(t, fe, "INSERT INTO rollback_t (id, val) VALUES (1, 42)")
	sendQuery(t, fe, "ROLLBACK")

	msgs := sendQuery(t, fe, "SELECT * FROM rollback_t")
	if countDataRows(msgs) != 0 {
		t.Errorf("after rollback: expected 0 rows, got %d", countDataRows(msgs))
	}
}

func TestSession_Analyze(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	sendQuery(t, fe, "CREATE TABLE analyze_t (id INT PRIMARY KEY, val INT)")
	sendQuery(t, fe, "INSERT INTO analyze_t (id, val) VALUES (1, 10)")
	sendQuery(t, fe, "INSERT INTO analyze_t (id, val) VALUES (2, 20)")

	msgs := sendQuery(t, fe, "ANALYZE analyze_t")
	if hasError(msgs) {
		t.Fatal("ANALYZE returned an error")
	}
	// ANALYZE should return one row per column (id + val = 2 rows).
	if countDataRows(msgs) != 2 {
		t.Errorf("ANALYZE: expected 2 stat rows, got %d", countDataRows(msgs))
	}
}

func TestSession_UnknownStatement(t *testing.T) {
	db := openTestDB(t)
	fe, stop := startSession(t, db)
	defer stop()

	msgs := sendQuery(t, fe, "THIS IS NOT SQL AT ALL")
	if !hasError(msgs) {
		t.Error("expected ErrorResponse for invalid SQL")
	}
}

func TestSession_Terminate(t *testing.T) {
	db := openTestDB(t)
	_, stop := startSession(t, db)
	stop() // should complete without panic
}
