package pgserver

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/synchronization"
)

type capturingReplicationCoordinator struct {
	mu      sync.Mutex
	commits [][]log.RecoveryOperation
}

func (c *capturingReplicationCoordinator) Strategy() synchronization.SyncStrategy { return synchronization.SyncStrategyRaft }
func (c *capturingReplicationCoordinator) Configure(config synchronization.ReplicationConfig) error {
	_ = config
	return nil
}
func (c *capturingReplicationCoordinator) ConnectNode(ctx context.Context, endpoint synchronization.NodeEndpoint) error {
	_, _ = ctx, endpoint
	return nil
}
func (c *capturingReplicationCoordinator) DisconnectNode(ctx context.Context, nodeID string) error {
	_, _ = ctx, nodeID
	return nil
}
func (c *capturingReplicationCoordinator) ConnectedNodes() []synchronization.NodeEndpoint { return nil }
func (c *capturingReplicationCoordinator) SynchronizeRead(ctx context.Context, txnID int64, tableName string, key []byte) error {
	_, _, _, _ = ctx, txnID, tableName, key
	return nil
}
func (c *capturingReplicationCoordinator) ReplicateWrite(ctx context.Context, txnID int64, tableName string, key []byte, value []byte) error {
	_, _, _, _, _ = ctx, txnID, tableName, key, value
	return nil
}
func (c *capturingReplicationCoordinator) ReplicateDelete(ctx context.Context, txnID int64, tableName string, key []byte) error {
	_, _, _, _ = ctx, txnID, tableName, key
	return nil
}
func (c *capturingReplicationCoordinator) Commit(ctx context.Context, txnID int64, operations []log.RecoveryOperation) error {
	_, _ = ctx, txnID
	cloned := make([]log.RecoveryOperation, len(operations))
	copy(cloned, operations)
	c.mu.Lock()
	c.commits = append(c.commits, cloned)
	c.mu.Unlock()
	return nil
}
func (c *capturingReplicationCoordinator) Abort(ctx context.Context, txnID int64) error {
	_, _ = ctx, txnID
	return nil
}
func (c *capturingReplicationCoordinator) Close() error { return nil }

func TestServerExecuteSimpleRoundTrip(t *testing.T) {
	tmp := t.TempDir()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	srv := New(engine)
	sess := &connSession{status: 'I'}

	if _, err := srv.executeStatement(sess, "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "INSERT INTO users (id, name) VALUES ('user-1', 'Ada')"); err != nil {
		t.Fatalf("INSERT error = %v", err)
	}

	res, err := srv.executeStatement(sess, "SELECT id, name FROM users WHERE id = 'user-1'")
	if err != nil {
		t.Fatalf("SELECT error = %v", err)
	}
	if len(res.messages) != 3 {
		t.Fatalf("expected 3 messages (row description, row, completion), got %d", len(res.messages))
	}

	if _, ok := res.messages[0].(*pgproto3.RowDescription); !ok {
		t.Fatalf("expected first message to be RowDescription, got %T", res.messages[0])
	}
	dr, ok := res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected second message to be DataRow, got %T", res.messages[1])
	}
	if len(dr.Values) != 2 || string(dr.Values[0]) != "user-1" || string(dr.Values[1]) != "Ada" {
		t.Fatalf("unexpected row values: %#v", dr.Values)
	}

	if _, err := srv.executeStatement(sess, "UPDATE users SET name = 'Bea' WHERE id = 'user-1'"); err != nil {
		t.Fatalf("UPDATE error = %v", err)
	}
	res, err = srv.executeStatement(sess, "SELECT name FROM users WHERE id = 'user-1'")
	if err != nil {
		t.Fatalf("SELECT after update error = %v", err)
	}
	dr, ok = res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected DataRow after update, got %T", res.messages[1])
	}
	if string(dr.Values[0]) != "Bea" {
		t.Fatalf("expected updated name Bea, got %q", string(dr.Values[0]))
	}

	if _, err := srv.executeStatement(sess, "DELETE FROM users WHERE id = 'user-1'"); err != nil {
		t.Fatalf("DELETE error = %v", err)
	}
	res, err = srv.executeStatement(sess, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT after delete error = %v", err)
	}
	if cc, ok := res.messages[len(res.messages)-1].(*pgproto3.CommandComplete); !ok || string(cc.CommandTag) != "SELECT 0" {
		t.Fatalf("expected SELECT 0 command complete, got %T %#v", res.messages[len(res.messages)-1], res.messages[len(res.messages)-1])
	}
}

func TestServerReplicatesDDLAndDMLOnlyOnCommit(t *testing.T) {
	tmp := t.TempDir()
	repl := &capturingReplicationCoordinator{}
	cfg := synchronization.DefaultReplicationConfig()
	cfg.Backend = synchronization.ReplicationBackendGRPC
	cfg.Strategy = synchronization.SyncStrategyRaft

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:                 filepath.Join(tmp, "db.db"),
		LSMDataDir:             filepath.Join(tmp, "lsm"),
		WALPath:                filepath.Join(tmp, "wal.log"),
		BufferPoolSize:         8,
		ReplacerCapacity:       4,
		ReplicationConfig:      cfg,
		ReplicationCoordinator: repl,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	srv := New(engine)
	sess := &connSession{status: 'I'}

	if _, err := srv.executeStatement(sess, "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if len(repl.commits) != 1 {
		t.Fatalf("expected 1 commit after CREATE TABLE, got %d", len(repl.commits))
	}
	if len(repl.commits[0]) == 0 {
		t.Fatalf("expected CREATE_TABLE commit batch, got %#v", repl.commits[0])
	}
	for _, op := range repl.commits[0] {
		if op.Type != log.CREATE_TABLE {
			t.Fatalf("expected CREATE_TABLE commit batch, got %#v", repl.commits[0])
		}
	}

	if _, err := srv.executeStatement(sess, "INSERT INTO users (id, name) VALUES ('user-1', 'Ada')"); err != nil {
		t.Fatalf("INSERT error = %v", err)
	}
	if len(repl.commits) != 2 {
		t.Fatalf("expected 2 commits after INSERT, got %d", len(repl.commits))
	}
	if len(repl.commits[1]) == 0 {
		t.Fatalf("expected PUT commit batch, got %#v", repl.commits[1])
	}
	for _, op := range repl.commits[1] {
		if op.Type != log.PUT {
			t.Fatalf("expected PUT commit batch, got %#v", repl.commits[1])
		}
	}
}

func TestServerHandlesTypedOrderingAndConstraints(t *testing.T) {
	tmp := t.TempDir()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	srv := New(engine)
	sess := &connSession{status: 'I'}

	if _, err := srv.executeStatement(sess, "CREATE TABLE metrics (id INT PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "INSERT INTO metrics (id, value) VALUES (10, 'ten')"); err != nil {
		t.Fatalf("INSERT 10 error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "INSERT INTO metrics (id, value) VALUES (2, 'two')"); err != nil {
		t.Fatalf("INSERT 2 error = %v", err)
	}

	if _, err := srv.executeStatement(sess, "INSERT INTO metrics (id, value) VALUES (2, 'duplicate')"); err == nil {
		t.Fatalf("expected duplicate primary key insert to fail")
	}

	res, err := srv.executeStatement(sess, "SELECT id FROM metrics")
	if err != nil {
		t.Fatalf("SELECT error = %v", err)
	}
	if len(res.messages) != 4 {
		t.Fatalf("expected 4 messages for 2-row select, got %d", len(res.messages))
	}
	dr1, ok := res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected first data row, got %T", res.messages[1])
	}
	dr2, ok := res.messages[2].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected second data row, got %T", res.messages[2])
	}
	if string(dr1.Values[0]) != "2" || string(dr2.Values[0]) != "10" {
		t.Fatalf("expected numeric ordering 2 then 10, got %q then %q", string(dr1.Values[0]), string(dr2.Values[0]))
	}

	res, err = srv.executeStatement(sess, "SHOW DateStyle")
	if err != nil {
		t.Fatalf("SHOW DateStyle error = %v", err)
	}
	dr, ok := res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected SHOW to return a data row, got %T", res.messages[1])
	}
	if string(dr.Values[0]) != "ISO, MDY" {
		t.Fatalf("expected DateStyle value ISO, MDY, got %q", string(dr.Values[0]))
	}

	if _, err := srv.executeStatement(sess, "UPDATE metrics SET id = 3 WHERE id = 2"); err == nil {
		t.Fatalf("expected primary key update to fail")
	}
}

func TestServerPersistsCommittedDataAcrossRestart(t *testing.T) {
	tmp := t.TempDir()

	opts := database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	}

	engine, err := database.OpenMVCCLSM(opts)
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}

	srv := New(engine)
	sess := &connSession{status: 'I'}

	if _, err := srv.executeStatement(sess, "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "BEGIN"); err != nil {
		t.Fatalf("BEGIN error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "INSERT INTO users (id, name) VALUES ('user-1', 'Grace')"); err != nil {
		t.Fatalf("INSERT inside transaction error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "COMMIT"); err != nil {
		t.Fatalf("COMMIT error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() first engine error = %v", err)
	}

	engine2, err := database.OpenMVCCLSM(opts)
	if err != nil {
		t.Fatalf("OpenMVCCLSM() restart error = %v", err)
	}
	defer func() { _ = engine2.Close() }()

	srv2 := New(engine2)
	sess2 := &connSession{status: 'I'}
	res, err := srv2.executeStatement(sess2, "SELECT id, name FROM users WHERE id = 'user-1'")
	if err != nil {
		t.Fatalf("SELECT after restart error = %v", err)
	}
	if len(res.messages) < 3 {
		t.Fatalf("expected select response with data row, got %d messages", len(res.messages))
	}
	dr, ok := res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected DataRow after restart, got %T", res.messages[1])
	}
	if string(dr.Values[0]) != "user-1" || string(dr.Values[1]) != "Grace" {
		t.Fatalf("unexpected recovered row: %#v", dr.Values)
	}
}

func TestServerDropTableIfExists(t *testing.T) {
	tmp := t.TempDir()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	srv := New(engine)
	sess := &connSession{status: 'I'}

	// Create a table for testing
	if _, err := srv.executeStatement(sess, "CREATE TABLE test_table (id TEXT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	// Drop existing table with IF EXISTS (should succeed)
	if _, err := srv.executeStatement(sess, "DROP TABLE IF EXISTS test_table"); err != nil {
		t.Fatalf("DROP TABLE IF EXISTS on existing table error = %v", err)
	}

	// Drop non-existent table with IF EXISTS (should succeed, not error)
	if _, err := srv.executeStatement(sess, "DROP TABLE IF EXISTS nonexistent_table"); err != nil {
		t.Fatalf("DROP TABLE IF EXISTS on non-existent table should not error, got = %v", err)
	}

	// Drop non-existent table WITHOUT IF EXISTS (should error)
	if _, err := srv.executeStatement(sess, "DROP TABLE nonexistent_table"); err == nil {
		t.Fatalf("DROP TABLE on non-existent table should error, but succeeded")
	}
}

func TestServerCreateTableIfNotExists(t *testing.T) {
	tmp := t.TempDir()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	srv := New(engine)
	sess := &connSession{status: 'I'}

	// Create table with IF NOT EXISTS (should succeed)
	if _, err := srv.executeStatement(sess, "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE IF NOT EXISTS on new table error = %v", err)
	}

	// Create same table with IF NOT EXISTS again (should succeed, not error)
	if _, err := srv.executeStatement(sess, "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE IF NOT EXISTS on existing table should not error, got = %v", err)
	}

	// Create same table WITHOUT IF NOT EXISTS (should error)
	if _, err := srv.executeStatement(sess, "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"); err == nil {
		t.Fatalf("CREATE TABLE on existing table should error, but succeeded")
	}

	// Verify the table exists and is usable
	if _, err := srv.executeStatement(sess, "INSERT INTO users (id, name) VALUES ('test-1', 'Test User')"); err != nil {
		t.Fatalf("INSERT into created table error = %v", err)
	}
}

func TestServerDropIndexIfExists(t *testing.T) {
	tmp := t.TempDir()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	srv := New(engine)
	sess := &connSession{status: 'I'}

	// Create a table and index
	if _, err := srv.executeStatement(sess, "CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "CREATE INDEX email_idx ON users (email)"); err != nil {
		t.Fatalf("CREATE INDEX error = %v", err)
	}

	// Drop existing index with IF EXISTS (should succeed)
	if _, err := srv.executeStatement(sess, "DROP INDEX IF EXISTS email_idx"); err != nil {
		t.Fatalf("DROP INDEX IF EXISTS on existing index error = %v", err)
	}

	// Drop non-existent index with IF EXISTS (should succeed, not error)
	if _, err := srv.executeStatement(sess, "DROP INDEX IF EXISTS nonexistent_idx"); err != nil {
		t.Fatalf("DROP INDEX IF EXISTS on non-existent index should not error, got = %v", err)
	}

	// Drop non-existent index WITHOUT IF EXISTS (should error)
	if _, err := srv.executeStatement(sess, "DROP INDEX nonexistent_idx"); err == nil {
		t.Fatalf("DROP INDEX on non-existent index should error, but succeeded")
	}
}

func TestServerCreateIndexIfNotExists(t *testing.T) {
	tmp := t.TempDir()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	srv := New(engine)
	sess := &connSession{status: 'I'}

	// Create a table
	if _, err := srv.executeStatement(sess, "CREATE TABLE products (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	// Create index with IF NOT EXISTS (should succeed)
	if _, err := srv.executeStatement(sess, "CREATE INDEX IF NOT EXISTS name_idx ON products (name)"); err != nil {
		t.Fatalf("CREATE INDEX IF NOT EXISTS on new index error = %v", err)
	}

	// Create same index with IF NOT EXISTS again (should succeed, not error)
	if _, err := srv.executeStatement(sess, "CREATE INDEX IF NOT EXISTS name_idx ON products (name)"); err != nil {
		t.Fatalf("CREATE INDEX IF NOT EXISTS on existing index should not error, got = %v", err)
	}

	// Create same index WITHOUT IF NOT EXISTS (should error)
	if _, err := srv.executeStatement(sess, "CREATE INDEX name_idx ON products (name)"); err == nil {
		t.Fatalf("CREATE INDEX on existing index should error, but succeeded")
	}
}
