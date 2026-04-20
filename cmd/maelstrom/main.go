package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/synchronization"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

// MaelstromMessage represents a Maelstrom protocol message
type MaelstromMessage struct {
	Src  string         `json:"src"`
	Dest string         `json:"dest"`
	Body map[string]any `json:"body"`
}

// Node represents the Maelstrom node
type Node struct {
	nodeID  string
	msgID   int
	msgIDMu sync.Mutex
	dataDir string

	pendingMu       sync.Mutex
	pendingProxies   map[int]pendingProxyTxn

	replicationConfig synchronization.ReplicationConfig

	db         database.Database
	txnManager txn.IIsolationManager
}

type pendingProxyTxn struct {
	clientSrc string
	clientMsg any
}

type NodeOptions struct {
	DataDir           string
	ReplicationConfig synchronization.ReplicationConfig
}

// NewNode creates a new Maelstrom node
func NewNode() *Node {
	return NewNodeWithOptions(NodeOptions{})
}

func NewNodeWithOptions(opts NodeOptions) *Node {
	replCfg := opts.ReplicationConfig
	if replCfg.Strategy == "" {
		replCfg = synchronization.DefaultReplicationConfig()
	}
	if replCfg.Backend == "" {
		replCfg.Backend = synchronization.ReplicationBackendNoop
	}
	return &Node{
		msgID:             0,
		dataDir:           opts.DataDir,
		pendingProxies:    make(map[int]pendingProxyTxn),
		replicationConfig: replCfg,
	}
}

// Start begins listening for messages
func (n *Node) Start() error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var msg MaelstromMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}

		msgType, ok := msg.Body["type"].(string)
		if !ok {
			continue
		}

		if msgType == "init" {
			nodeID, _ := msg.Body["node_id"].(string)
			n.nodeID = nodeID
			if n.dataDir == "" {
				tmpDir, err := os.MkdirTemp("", fmt.Sprintf("omag-maelstrom-%s-*", n.nodeID))
				if err != nil {
					continue
				}
				n.dataDir = tmpDir
			}

			engine, err := database.OpenMVCCLSM(database.Options{
				DBPath:            filepath.Join(n.dataDir, "test.db"),
				LSMDataDir:        filepath.Join(n.dataDir, "lsm_data"),
				WALPath:           filepath.Join(n.dataDir, "test.wal"),
				ReplicationConfig: n.replicationConfig,
			})
			if err != nil {
				continue
			}
			n.db = engine
			n.txnManager = engine.IsolationManager()
			if err := n.bootstrapRaftLeadershipFromInit(msg.Body); err != nil {
				continue
			}

			response := MaelstromMessage{
				Src:  n.nodeID,
				Dest: msg.Src,
				Body: map[string]any{
					"type":        "init_ok",
					"in_reply_to": msg.Body["msg_id"],
				},
			}
			n.send(response)
		} else if msgType == "txn" {
			txnRaw, ok := msg.Body["txn"].([]any)
			if !ok {
				continue
			}

			if n.shouldProxyTxn() {
				if err := n.forwardTxnToLeader(msg.Src, msg.Body["msg_id"], txnRaw); err != nil {
					n.sendTxnError(msg.Src, msg.Body["msg_id"], err.Error())
				}
				continue
			}

			resultTxn, err := n.executeTxnWithError(txnRaw)
			if err != nil {
				n.sendTxnError(msg.Src, msg.Body["msg_id"], err.Error())
				continue
			}
			n.sendTxnOK(msg.Src, msg.Body["msg_id"], resultTxn)
		} else if msgType == "txn_proxy" {
			txnRaw, ok := msg.Body["txn"].([]any)
			if !ok {
				continue
			}
			resultTxn, err := n.executeTxnWithError(txnRaw)
			if err != nil {
				n.sendTxnProxyError(msg.Src, msg.Body["msg_id"], err.Error())
				continue
			}
			n.sendTxnProxyOK(msg.Src, msg.Body["msg_id"], resultTxn)
		} else if msgType == "txn_proxy_ok" {
			n.completeProxyTxn(msg.Body, false)
		} else if msgType == "txn_proxy_error" {
			n.completeProxyTxn(msg.Body, true)
		} else if msgType == "raft_leadership_update" {
			err := n.applyRaftLeadershipUpdate(msg.Body)
			if err != nil {
				n.send(MaelstromMessage{
					Src:  n.nodeID,
					Dest: msg.Src,
					Body: map[string]any{
						"type":        "error",
						"in_reply_to": msg.Body["msg_id"],
						"code":        13,
						"text":        err.Error(),
					},
				})
				continue
			}
			n.send(MaelstromMessage{
				Src:  n.nodeID,
				Dest: msg.Src,
				Body: map[string]any{
					"type":        "raft_leadership_update_ok",
					"in_reply_to": msg.Body["msg_id"],
				},
			})
		}
	}

	return scanner.Err()
}

func (n *Node) executeTxn(txnOps []any) []any {
	result, _ := n.executeTxnWithError(txnOps)
	return result
}

func (n *Node) executeTxnWithError(txnOps []any) ([]any, error) {
	results := make([]any, 0, len(txnOps))

	if n.db == nil {
		return results, fmt.Errorf("database is not initialized")
	}

	txnID := n.db.BeginTransaction(txn_unit.SERIALIZABLE, "", nil)
	committed := false
	defer func() {
		if !committed {
			_ = n.db.Abort(txnID)
		}
	}()

	for _, opAny := range txnOps {
		op, ok := opAny.([]any)
		if !ok || len(op) < 3 {
			continue
		}

		f, ok := op[0].(string)
		if !ok {
			continue
		}

		k := fmt.Sprint(op[1])
		v := op[2]

		switch f {
		case "r":
			val, err := n.db.Read(txnID, []byte(k))
			if err != nil {
				results = append(results, []any{"r", op[1], nil})
				continue
			}

			var decoded any
			if err := json.Unmarshal(val, &decoded); err != nil {
				results = append(results, []any{"r", op[1], nil})
				continue
			}
			results = append(results, []any{"r", op[1], decoded})
		case "w":
			byteData, err := json.Marshal(v)
			if err != nil {
				results = append(results, []any{"w", op[1], nil})
				continue
			}
			err = n.db.Write(txnID, []byte(k), byteData)
			if err != nil {
				results = append(results, []any{"w", op[1], nil})
				continue
			}
			results = append(results, []any{"w", op[1], v})
		default:
			results = append(results, []any{f, op[1], nil})
		}
	}
	if err := n.db.Commit(txnID); err != nil {
		return []any{}, err
	}
	committed = true

	return results, nil
}

func (n *Node) shouldProxyTxn() bool {
	if n.replicationConfig.Strategy != synchronization.SyncStrategyRaft {
		return false
	}
	return n.replicationConfig.LocalNodeID != "" && n.replicationConfig.LeaderNodeID != "" && n.replicationConfig.LocalNodeID != n.replicationConfig.LeaderNodeID
}

func (n *Node) forwardTxnToLeader(clientSrc string, clientMsg any, txnRaw []any) error {
	leaderID := n.replicationConfig.LeaderNodeID
	if leaderID == "" {
		return fmt.Errorf("raft leader is not configured")
	}

	proxyMsg := MaelstromMessage{
		Src:  n.nodeID,
		Dest: leaderID,
		Body: map[string]any{
			"type":        "txn_proxy",
			"client_src":  clientSrc,
			"client_msg":  clientMsg,
			"txn":         txnRaw,
		},
	}
	proxyID := n.send(proxyMsg)

	n.pendingMu.Lock()
	n.pendingProxies[proxyID] = pendingProxyTxn{clientSrc: clientSrc, clientMsg: clientMsg}
	n.pendingMu.Unlock()
	return nil
}

func (n *Node) completeProxyTxn(body map[string]any, isErr bool) {
	inReply, ok := body["in_reply_to"]
	if !ok {
		return
	}
	proxyID, ok := toInt(inReply)
	if !ok {
		return
	}

	n.pendingMu.Lock()
	pending, exists := n.pendingProxies[proxyID]
	if exists {
		delete(n.pendingProxies, proxyID)
	}
	n.pendingMu.Unlock()
	if !exists {
		return
	}

	if isErr {
		txt, _ := body["text"].(string)
		n.sendTxnError(pending.clientSrc, pending.clientMsg, txt)
		return
	}

	txnRaw, _ := body["txn"].([]any)
	n.sendTxnOK(pending.clientSrc, pending.clientMsg, txnRaw)
}

func (n *Node) sendTxnOK(dest string, inReplyTo any, txn []any) {
	n.send(MaelstromMessage{
		Src:  n.nodeID,
		Dest: dest,
		Body: map[string]any{
			"type":        "txn_ok",
			"in_reply_to": inReplyTo,
			"txn":         txn,
		},
	})
}

func (n *Node) sendTxnError(dest string, inReplyTo any, text string) {
	n.send(MaelstromMessage{
		Src:  n.nodeID,
		Dest: dest,
		Body: map[string]any{
			"type":        "error",
			"in_reply_to": inReplyTo,
			"code":        13,
			"text":        text,
		},
	})
}

func (n *Node) sendTxnProxyOK(dest string, inReplyTo any, txn []any) {
	n.send(MaelstromMessage{
		Src:  n.nodeID,
		Dest: dest,
		Body: map[string]any{
			"type":        "txn_proxy_ok",
			"in_reply_to": inReplyTo,
			"txn":         txn,
		},
	})
}

func (n *Node) sendTxnProxyError(dest string, inReplyTo any, text string) {
	n.send(MaelstromMessage{
		Src:  n.nodeID,
		Dest: dest,
		Body: map[string]any{
			"type":        "txn_proxy_error",
			"in_reply_to": inReplyTo,
			"text":        text,
		},
	})
}

// send sends a message to stdout
func (n *Node) send(msg MaelstromMessage) int {
	n.msgIDMu.Lock()
	n.msgID++
	id := n.msgID
	n.msgIDMu.Unlock()

	if msg.Body == nil {
		msg.Body = make(map[string]any)
	}
	msg.Body["msg_id"] = id

	data, err := json.Marshal(msg)
	if err != nil {
		return id
	}

	fmt.Println(string(data))
	os.Stdout.Sync()
	return id
}

func toInt(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case float64:
		return int(x), true
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return 0, false
		}
		return int(n), true
	default:
		return 0, false
	}
}

func main() {
	node := NewNodeWithOptions(parseNodeOptionsFromFlags())
	_ = node.Start()
}

func (n *Node) applyRaftLeadershipUpdate(body map[string]any) error {
	if n == nil || n.db == nil {
		return fmt.Errorf("raft leadership update rejected: database is not initialized")
	}

	localNodeID, _ := body["local_node_id"].(string)
	leaderNodeID, _ := body["leader_node_id"].(string)
	term, err := parseUint64Field(body["term"])
	if err != nil {
		return fmt.Errorf("raft leadership update rejected: %w", err)
	}

	if localNodeID == "" {
		localNodeID = n.nodeID
	}
	if err := n.db.UpdateRaftLeadership(localNodeID, leaderNodeID, term); err != nil {
		return err
	}
	n.replicationConfig.LocalNodeID = localNodeID
	n.replicationConfig.LeaderNodeID = leaderNodeID
	if term > 0 {
		n.replicationConfig.CurrentTerm = term
	}
	return nil
}

func (n *Node) bootstrapRaftLeadershipFromInit(body map[string]any) error {
	if n == nil || n.db == nil || n.replicationConfig.Strategy != synchronization.SyncStrategyRaft {
		return nil
	}

	localNodeID := n.nodeID
	leaderNodeID := n.replicationConfig.LeaderNodeID
	if leaderNodeID == "" {
		leaderNodeID = chooseInitialLeaderFromInitNodeIDs(body["node_ids"])
	}
	if leaderNodeID == "" {
		leaderNodeID = localNodeID
	}

	term := n.replicationConfig.CurrentTerm
	if term == 0 {
		term = 1
	}

	if err := n.db.UpdateRaftLeadership(localNodeID, leaderNodeID, term); err != nil {
		return err
	}
	n.replicationConfig.LocalNodeID = localNodeID
	n.replicationConfig.LeaderNodeID = leaderNodeID
	n.replicationConfig.CurrentTerm = term
	return nil
}

func chooseInitialLeaderFromInitNodeIDs(raw any) string {
	nodeIDs, ok := raw.([]any)
	if !ok {
		return ""
	}

	for _, candidate := range nodeIDs {
		nodeID, ok := candidate.(string)
		if !ok {
			continue
		}
		if nodeID == "n0" {
			return nodeID
		}
	}

	for _, candidate := range nodeIDs {
		nodeID, ok := candidate.(string)
		if !ok || nodeID == "" {
			continue
		}
		return nodeID
	}

	return ""
}

func parseUint64Field(raw any) (uint64, error) {
	switch v := raw.(type) {
	case uint64:
		return v, nil
	case int:
		if v < 0 {
			return 0, errors.New("term must be non-negative")
		}
		return uint64(v), nil
	case float64:
		if v < 0 || math.Trunc(v) != v {
			return 0, errors.New("term must be a non-negative integer")
		}
		return uint64(v), nil
	default:
		return 0, errors.New("term is required")
	}
}

func parseNodeOptionsFromFlags() NodeOptions {
	dataDir := flag.String("data-dir", "", "base directory for db/wal/lsm files (defaults to temp dir per node)")
	strategy := flag.String("replication-strategy", string(synchronization.SyncStrategyStandalone), "replication strategy (standalone, raft)")
	backend := flag.String("replication-backend", string(synchronization.ReplicationBackendNoop), "replication backend (noop, maelstrom, grpc)")
	readPolicy := flag.String("replication-read-policy", string(synchronization.SyncPolicyLocal), "read sync policy (local, asynchronous, synchronous, quorum)")
	writePolicy := flag.String("replication-write-policy", string(synchronization.SyncPolicyLocal), "write sync policy (local, asynchronous, synchronous, quorum)")
	minAcks := flag.Int("replication-min-write-acks", 1, "minimum acknowledgements for quorum policy")
	flag.Parse()

	config := synchronization.ReplicationConfig{
		Backend:      synchronization.ReplicationBackend(*backend),
		Strategy:     synchronization.SyncStrategy(*strategy),
		ReadPolicy:   synchronization.SyncPolicy(*readPolicy),
		WritePolicy:  synchronization.SyncPolicy(*writePolicy),
		MinWriteAcks: *minAcks,
	}

	if err := config.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid replication config: %v\n", err)
		os.Exit(2)
	}

	return NodeOptions{
		DataDir:           *dataDir,
		ReplicationConfig: config,
	}
}
