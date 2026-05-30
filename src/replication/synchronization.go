package replication

import (
	"context"
	"fmt"
	"sort"
	"sync"

	log "github.com/rodrigo0345/omag/src/engine/wal"
)

// SyncStrategy defines the high-level replication topology.
type SyncStrategy string

const (
	SyncStrategyStandalone SyncStrategy = "standalone"
	SyncStrategyRaft       SyncStrategy = "raft"
)

// SyncPolicy defines how strictly reads/writes synchronize across nodes.
type SyncPolicy string

const (
	SyncPolicyLocal        SyncPolicy = "local"
	SyncPolicyAsynchronous SyncPolicy = "asynchronous"
	SyncPolicySynchronous  SyncPolicy = "synchronous"
	SyncPolicyQuorum       SyncPolicy = "quorum"
)

// ReplicationBackend selects the node-to-node communication protocol.
type ReplicationBackend string

const (
	ReplicationBackendNoop      ReplicationBackend = "noop"
	ReplicationBackendMaelstrom ReplicationBackend = "maelstrom"
)

// NodeEndpoint describes an internal database node in the replication group.
type NodeEndpoint struct {
	NodeID   string
	Address  string
	Metadata map[string]string
}

// ReplicationConfig controls synchronization behavior.
type ReplicationConfig struct {
	Backend      ReplicationBackend
	Strategy     SyncStrategy
	ReadPolicy   SyncPolicy
	WritePolicy  SyncPolicy
	MinWriteAcks int
	LocalNodeID  string
	LeaderNodeID string
	CurrentTerm  uint64
}

func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		Backend:      ReplicationBackendNoop,
		Strategy:     SyncStrategyStandalone,
		ReadPolicy:   SyncPolicyLocal,
		WritePolicy:  SyncPolicyLocal,
		MinWriteAcks: 1,
	}
}

func (c ReplicationConfig) Validate() error {
	switch c.Backend {
	case "", ReplicationBackendNoop, ReplicationBackendMaelstrom:
	default:
		return fmt.Errorf("invalid replication backend %q", c.Backend)
	}

	switch c.Strategy {
	case SyncStrategyStandalone, SyncStrategyRaft:
	default:
		return fmt.Errorf("invalid sync strategy %q", c.Strategy)
	}

	switch c.ReadPolicy {
	case SyncPolicyLocal, SyncPolicyAsynchronous, SyncPolicySynchronous, SyncPolicyQuorum:
	default:
		return fmt.Errorf("invalid read policy %q", c.ReadPolicy)
	}

	switch c.WritePolicy {
	case SyncPolicyLocal, SyncPolicyAsynchronous, SyncPolicySynchronous, SyncPolicyQuorum:
	default:
		return fmt.Errorf("invalid write policy %q", c.WritePolicy)
	}

	if c.MinWriteAcks <= 0 {
		return fmt.Errorf("min write acks must be greater than zero")
	}

	if c.LeaderNodeID != "" && c.LocalNodeID == "" {
		return fmt.Errorf("local node id is required when leader node id is set")
	}

	if c.CurrentTerm > 0 && c.LocalNodeID == "" {
		return fmt.Errorf("local node id is required when raft current term is set")
	}

	return nil
}

// NodeConnector manages cluster membership for internal database nodes.
type NodeConnector interface {
	ConnectNode(ctx context.Context, endpoint NodeEndpoint) error
	DisconnectNode(ctx context.Context, nodeID string) error
	ConnectedNodes() []NodeEndpoint
}

// ReplicationCoordinator synchronizes reads/writes across internal nodes.
type ReplicationCoordinator interface {
	NodeConnector

	Strategy() SyncStrategy
	Configure(config ReplicationConfig) error

	SynchronizeRead(ctx context.Context, txnID int64, tableName string, key []byte) error
	ReplicateWrite(ctx context.Context, txnID int64, tableName string, key []byte, value []byte) error
	ReplicateDelete(ctx context.Context, txnID int64, tableName string, key []byte) error
	Commit(ctx context.Context, txnID int64, operations []log.RecoveryOperation) error
	Abort(ctx context.Context, txnID int64) error
	Close() error
}

// NoopReplicationCoordinator preserves current single-node behavior.
type NoopReplicationCoordinator struct {
	mu     sync.RWMutex
	config ReplicationConfig
	nodes  map[string]NodeEndpoint
}

func NewNoopReplicationCoordinator(config ReplicationConfig) *NoopReplicationCoordinator {
	if config.Strategy == "" {
		config = DefaultReplicationConfig()
	}

	return &NoopReplicationCoordinator{
		config: config,
		nodes:  make(map[string]NodeEndpoint),
	}
}

func (n *NoopReplicationCoordinator) Strategy() SyncStrategy {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.config.Strategy
}

func (n *NoopReplicationCoordinator) Configure(config ReplicationConfig) error {
	if config.Strategy == "" {
		config = DefaultReplicationConfig()
	} else if config.Backend == "" {
		config.Backend = ReplicationBackendNoop
	}
	if err := config.Validate(); err != nil {
		return err
	}

	n.mu.Lock()
	n.config = config
	n.mu.Unlock()
	return nil
}

func (n *NoopReplicationCoordinator) ConnectNode(ctx context.Context, endpoint NodeEndpoint) error {
	_ = ctx
	if endpoint.NodeID == "" {
		return fmt.Errorf("node id cannot be empty")
	}

	n.mu.Lock()
	n.nodes[endpoint.NodeID] = endpoint
	n.mu.Unlock()
	return nil
}

func (n *NoopReplicationCoordinator) DisconnectNode(ctx context.Context, nodeID string) error {
	_ = ctx
	n.mu.Lock()
	delete(n.nodes, nodeID)
	n.mu.Unlock()
	return nil
}

func (n *NoopReplicationCoordinator) ConnectedNodes() []NodeEndpoint {
	n.mu.RLock()
	defer n.mu.RUnlock()

	ids := make([]string, 0, len(n.nodes))
	for id := range n.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]NodeEndpoint, 0, len(ids))
	for _, id := range ids {
		out = append(out, n.nodes[id])
	}
	return out
}

func (n *NoopReplicationCoordinator) SynchronizeRead(ctx context.Context, txnID int64, tableName string, key []byte) error {
	return nil
}

func (n *NoopReplicationCoordinator) ReplicateWrite(ctx context.Context, txnID int64, tableName string, key []byte, value []byte) error {
	return nil
}

func (n *NoopReplicationCoordinator) ReplicateDelete(ctx context.Context, txnID int64, tableName string, key []byte) error {
	return nil
}

func (n *NoopReplicationCoordinator) Commit(ctx context.Context, txnID int64, operations []log.RecoveryOperation) error {
	_, _ = txnID, operations
	return nil
}

func (n *NoopReplicationCoordinator) Abort(ctx context.Context, txnID int64) error {
	return nil
}

func (n *NoopReplicationCoordinator) Close() error {
	return nil
}

// ReplicationOperation identifies a protocol-agnostic replication action.
type ReplicationOperation string

const (
	ReplicationOpReadSync ReplicationOperation = "read_sync"
	ReplicationOpWrite    ReplicationOperation = "write"
	ReplicationOpDelete   ReplicationOperation = "delete"
	ReplicationOpCommit   ReplicationOperation = "commit"
	ReplicationOpAbort    ReplicationOperation = "abort"
)

// ReplicationEnvelope is the transport payload between internal nodes.
type ReplicationEnvelope struct {
	TxnID     int64
	Op        ReplicationOperation
	TableName string
	Key       []byte
	Value     []byte
	Operations []log.RecoveryOperation
}

// NodeCommunicator defines how replication messages move between nodes.
type NodeCommunicator interface {
	Backend() ReplicationBackend
	Send(ctx context.Context, endpoint NodeEndpoint, envelope ReplicationEnvelope) error
	Close() error
}

// NoopCommunicator drops all messages and is used for standalone/local mode.
type NoopCommunicator struct{}

func (n *NoopCommunicator) Backend() ReplicationBackend { return ReplicationBackendNoop }
func (n *NoopCommunicator) Send(ctx context.Context, endpoint NodeEndpoint, envelope ReplicationEnvelope) error {
	_, _, _ = ctx, endpoint, envelope
	return nil
}
func (n *NoopCommunicator) Close() error { return nil }

// MaelstromHandler receives replication envelopes for a node id.
type MaelstromHandler func(ctx context.Context, envelope ReplicationEnvelope) error

// MaelstromCommunicator provides an in-process adapter matching Maelstrom-style messaging.
type MaelstromCommunicator struct {
	mu       sync.RWMutex
	handlers map[string]MaelstromHandler
}

func NewMaelstromCommunicator() *MaelstromCommunicator {
	return &MaelstromCommunicator{handlers: make(map[string]MaelstromHandler)}
}

func (m *MaelstromCommunicator) Backend() ReplicationBackend { return ReplicationBackendMaelstrom }

func (m *MaelstromCommunicator) RegisterHandler(nodeID string, handler MaelstromHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nodeID == "" || handler == nil {
		return
	}
	m.handlers[nodeID] = handler
}

func (m *MaelstromCommunicator) Send(ctx context.Context, endpoint NodeEndpoint, envelope ReplicationEnvelope) error {
	m.mu.RLock()
	h := m.handlers[endpoint.NodeID]
	m.mu.RUnlock()
	if h == nil {
		return fmt.Errorf("maelstrom handler for node %q not found", endpoint.NodeID)
	}
	return h(ctx, envelope)
}

func (m *MaelstromCommunicator) Close() error {
	m.mu.Lock()
	m.handlers = make(map[string]MaelstromHandler)
	m.mu.Unlock()
	return nil
}

