package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/txn/synchronization"
)

func newTestNodeWithDB(t *testing.T) *Node {
	t.Helper()
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
	t.Cleanup(func() {
		_ = engine.Close()
	})

	n := NewNode()
	n.db = engine
	n.txnManager = engine.IsolationManager()
	return n
}

func newTestRaftNodeWithDB(t *testing.T, localNodeID, leaderNodeID string, term uint64) *Node {
	t.Helper()
	tmp := t.TempDir()
	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:     filepath.Join(tmp, "db.db"),
		LSMDataDir: filepath.Join(tmp, "lsm"),
		WALPath:    filepath.Join(tmp, "wal.log"),
		ReplicationConfig: synchronization.ReplicationConfig{
			Strategy:     synchronization.SyncStrategyRaft,
			Backend:      synchronization.ReplicationBackendMaelstrom,
			ReadPolicy:   synchronization.SyncPolicySynchronous,
			WritePolicy:  synchronization.SyncPolicyQuorum,
			MinWriteAcks: 1,
			LocalNodeID:  localNodeID,
			LeaderNodeID: leaderNodeID,
			CurrentTerm:  term,
		},
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() raft error = %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Close()
	})

	n := NewNode()
	n.nodeID = localNodeID
	n.db = engine
	n.txnManager = engine.IsolationManager()
	return n
}

func TestExecuteTxn_UsesDatabaseValueForReadAfterWrite(t *testing.T) {
	n := newTestNodeWithDB(t)

	ops := []any{
		[]any{"w", 1, 42},
		[]any{"r", 1, nil},
	}
	results := n.executeTxn(ops)
	if len(results) != 2 {
		t.Fatalf("executeTxn() returned %d ops, want 2", len(results))
	}

	writeRes, ok := results[0].([]any)
	if !ok || len(writeRes) != 3 {
		t.Fatalf("unexpected write result shape: %#v", results[0])
	}
	if writeRes[0] != "w" || writeRes[1] != 1 || writeRes[2] != 42 {
		t.Fatalf("write result = %#v, want [w 1 42]", writeRes)
	}

	readRes, ok := results[1].([]any)
	if !ok || len(readRes) != 3 {
		t.Fatalf("unexpected read result shape: %#v", results[1])
	}
	if readRes[0] != "r" || readRes[1] != 1 {
		t.Fatalf("read result head = %#v, want [r 1 ...]", readRes)
	}
	if got, ok := readRes[2].(float64); !ok || got != 42 {
		t.Fatalf("read result value = %#v, want float64(42)", readRes[2])
	}
}

func TestExecuteTxn_ReadMissingKeyReturnsNil(t *testing.T) {
	n := newTestNodeWithDB(t)

	results := n.executeTxn([]any{[]any{"r", "missing", nil}})
	if len(results) != 1 {
		t.Fatalf("executeTxn() returned %d ops, want 1", len(results))
	}

	readRes, ok := results[0].([]any)
	if !ok || len(readRes) != 3 {
		t.Fatalf("unexpected read result shape: %#v", results[0])
	}
	if readRes[0] != "r" || readRes[1] != "missing" {
		t.Fatalf("read result head = %#v, want [r missing ...]", readRes)
	}
	if readRes[2] != nil {
		t.Fatalf("read missing value = %#v, want nil", readRes[2])
	}
}

func TestExecuteTxn_PersistsAcrossTransactions(t *testing.T) {
	n := newTestNodeWithDB(t)

	_ = n.executeTxn([]any{[]any{"w", "k", map[string]any{"x": 7}}})
	results := n.executeTxn([]any{[]any{"r", "k", nil}})
	if len(results) != 1 {
		t.Fatalf("executeTxn() returned %d ops, want 1", len(results))
	}

	readRes, ok := results[0].([]any)
	if !ok || len(readRes) != 3 {
		t.Fatalf("unexpected read result shape: %#v", results[0])
	}
	obj, ok := readRes[2].(map[string]any)
	if !ok {
		t.Fatalf("read result value type = %T, want map[string]any", readRes[2])
	}
	if got, ok := obj["x"].(float64); !ok || got != 7 {
		t.Fatalf("read result value x = %#v, want float64(7)", obj["x"])
	}
}

func TestApplyRaftLeadershipUpdate_DynamicFailover(t *testing.T) {
	n := newTestRaftNodeWithDB(t, "n2", "n2", 2)

	if err := n.applyRaftLeadershipUpdate(map[string]any{
		"leader_node_id": "",
		"term":           float64(3),
	}); err != nil {
		t.Fatalf("applyRaftLeadershipUpdate(leader crash) error = %v", err)
	}

	txnID := n.db.BeginTransaction(1, "", nil)
	if err := n.db.Write(txnID, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Write() during election should remain local, got = %v", err)
	}
	err := n.db.Commit(txnID)
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Commit() during election error = %v, want leadership-not-configured", err)
	}

	if err := n.applyRaftLeadershipUpdate(map[string]any{
		"leader_node_id": "n3",
		"term":           float64(4),
	}); err != nil {
		t.Fatalf("applyRaftLeadershipUpdate(new leader) error = %v", err)
	}

	txnID = n.db.BeginTransaction(1, "", nil)
	if err := n.db.Write(txnID, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Write() after failover should remain local, got = %v", err)
	}
	err = n.db.Commit(txnID)
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Fatalf("Commit() after failover error = %v, want not-leader", err)
	}

	err = n.applyRaftLeadershipUpdate(map[string]any{
		"leader_node_id": "n2",
		"term":           float64(3),
	})
	if err == nil || !strings.Contains(err.Error(), "stale raft term") {
		t.Fatalf("applyRaftLeadershipUpdate(stale term) error = %v, want stale-term", err)
	}
}

func TestParseUint64Field_ValidatesInput(t *testing.T) {
	if _, err := parseUint64Field(nil); err == nil {
		t.Fatal("parseUint64Field(nil) expected error")
	}
	if _, err := parseUint64Field(-1); err == nil {
		t.Fatal("parseUint64Field(-1) expected error")
	}
	if _, err := parseUint64Field(1.5); err == nil {
		t.Fatal("parseUint64Field(1.5) expected error")
	}
	term, err := parseUint64Field(float64(7))
	if err != nil {
		t.Fatalf("parseUint64Field(7) error = %v", err)
	}
	if term != 7 {
		t.Fatalf("parseUint64Field(7) = %d, want 7", term)
	}
}

func TestForwardTxnToLeader_AndCompleteProxyTxn(t *testing.T) {
	n := NewNodeWithOptions(NodeOptions{
		ReplicationConfig: synchronization.ReplicationConfig{
			Strategy:     synchronization.SyncStrategyRaft,
			Backend:      synchronization.ReplicationBackendMaelstrom,
			LocalNodeID:  "n1",
			LeaderNodeID: "n0",
		},
	})

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = oldStdout
		_ = r.Close()
		_ = w.Close()
	})

	if err := n.forwardTxnToLeader("client-1", float64(11), []any{[]any{"w", 1, 1}}); err != nil {
		t.Fatalf("forwardTxnToLeader() error = %v", err)
	}
	_ = w.Close()

	var forwarded MaelstromMessage
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatalf("reading forwarded message error = %v", err)
	}
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &forwarded); err != nil {
		t.Fatalf("json.Unmarshal(forwarded) error = %v", err)
	}
	if forwarded.Dest != "n0" {
		t.Fatalf("forwarded Dest = %q, want n0", forwarded.Dest)
	}
	if forwarded.Body["type"] != "txn_proxy" {
		t.Fatalf("forwarded type = %v, want txn_proxy", forwarded.Body["type"])
	}

	n.pendingMu.Lock()
	if len(n.pendingProxies) != 1 {
		t.Fatalf("pendingProxies = %d, want 1", len(n.pendingProxies))
	}
	n.pendingMu.Unlock()

	proxyID, ok := forwarded.Body["msg_id"].(float64)
	if !ok {
		t.Fatalf("forwarded msg_id type = %T, want float64", forwarded.Body["msg_id"])
	}

	// capture proxy completion response
	r2, w2, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() completion error = %v", err)
	}
	os.Stdout = w2
	n.completeProxyTxn(map[string]any{
		"in_reply_to": proxyID,
		"txn":         []any{[]any{"w", 1, 1}},
	}, false)
	_ = w2.Close()
	os.Stdout = oldStdout

	buf.Reset()
	if _, err := buf.ReadFrom(r2); err != nil {
		t.Fatalf("reading completion message error = %v", err)
	}
	var completed MaelstromMessage
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &completed); err != nil {
		t.Fatalf("json.Unmarshal(completed) error = %v", err)
	}
	if completed.Dest != "client-1" {
		t.Fatalf("completed Dest = %q, want client-1", completed.Dest)
	}
	if completed.Body["type"] != "txn_ok" {
		t.Fatalf("completed type = %v, want txn_ok", completed.Body["type"])
	}

	n.pendingMu.Lock()
	if len(n.pendingProxies) != 0 {
		t.Fatalf("pendingProxies after completion = %d, want 0", len(n.pendingProxies))
	}
	n.pendingMu.Unlock()
}

func TestChooseInitialLeaderFromInitNodeIDs_PrefersN0(t *testing.T) {
	leader := chooseInitialLeaderFromInitNodeIDs([]any{"n2", "n0", "n1"})
	if leader != "n0" {
		t.Fatalf("chooseInitialLeaderFromInitNodeIDs() = %q, want n0", leader)
	}
}

func TestBootstrapRaftLeadershipFromInit_ConfiguresLeaderAndProxyState(t *testing.T) {
	n := newTestRaftNodeWithDB(t, "", "", 0)
	n.nodeID = "n1"
	n.replicationConfig = synchronization.ReplicationConfig{
		Strategy:     synchronization.SyncStrategyRaft,
		Backend:      synchronization.ReplicationBackendMaelstrom,
		ReadPolicy:   synchronization.SyncPolicySynchronous,
		WritePolicy:  synchronization.SyncPolicyQuorum,
		MinWriteAcks: 1,
	}

	if err := n.bootstrapRaftLeadershipFromInit(map[string]any{"node_ids": []any{"n0", "n1", "n2"}}); err != nil {
		t.Fatalf("bootstrapRaftLeadershipFromInit() error = %v", err)
	}

	if n.replicationConfig.LocalNodeID != "n1" {
		t.Fatalf("LocalNodeID = %q, want n1", n.replicationConfig.LocalNodeID)
	}
	if n.replicationConfig.LeaderNodeID != "n0" {
		t.Fatalf("LeaderNodeID = %q, want n0", n.replicationConfig.LeaderNodeID)
	}
	if n.replicationConfig.CurrentTerm != 1 {
		t.Fatalf("CurrentTerm = %d, want 1", n.replicationConfig.CurrentTerm)
	}
	if !n.shouldProxyTxn() {
		t.Fatal("shouldProxyTxn() = false, want true for follower node")
	}
}

func TestApplyRaftLeadershipUpdate_SyncsNodeReplicationConfig(t *testing.T) {
	n := newTestRaftNodeWithDB(t, "", "", 0)
	n.nodeID = "n2"
	n.replicationConfig = synchronization.ReplicationConfig{
		Strategy: synchronization.SyncStrategyRaft,
		Backend:  synchronization.ReplicationBackendMaelstrom,
	}

	if err := n.applyRaftLeadershipUpdate(map[string]any{
		"leader_node_id": "n2",
		"term":           float64(5),
	}); err != nil {
		t.Fatalf("applyRaftLeadershipUpdate() error = %v", err)
	}

	if n.replicationConfig.LocalNodeID != "n2" {
		t.Fatalf("LocalNodeID = %q, want n2", n.replicationConfig.LocalNodeID)
	}
	if n.replicationConfig.LeaderNodeID != "n2" {
		t.Fatalf("LeaderNodeID = %q, want n2", n.replicationConfig.LeaderNodeID)
	}
	if n.replicationConfig.CurrentTerm != 5 {
		t.Fatalf("CurrentTerm = %d, want 5", n.replicationConfig.CurrentTerm)
	}
	if n.shouldProxyTxn() {
		t.Fatal("shouldProxyTxn() = true, want false for leader node")
	}
}

