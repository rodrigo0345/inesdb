package synchronization

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/rodrigo0345/omag/internal/txn/log"
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
	ReplicationBackendGRPC      ReplicationBackend = "grpc"
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
	case "", ReplicationBackendNoop, ReplicationBackendMaelstrom, ReplicationBackendGRPC:
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

// GRPCUnaryInvoker abstracts a unary gRPC call without hard-wiring protobuf dependencies here.
type GRPCUnaryInvoker interface {
	Invoke(ctx context.Context, endpoint NodeEndpoint, envelope ReplicationEnvelope) error
}

// GRPCCommunicator is a protocol adapter for real gRPC-based node communication.
type GRPCCommunicator struct {
	invoker GRPCUnaryInvoker
}

func NewGRPCCommunicator(invoker GRPCUnaryInvoker) *GRPCCommunicator {
	return &GRPCCommunicator{invoker: invoker}
}

func (g *GRPCCommunicator) Backend() ReplicationBackend { return ReplicationBackendGRPC }

func (g *GRPCCommunicator) Send(ctx context.Context, endpoint NodeEndpoint, envelope ReplicationEnvelope) error {
	if g.invoker == nil {
		return fmt.Errorf("grpc communicator invoker is not configured")
	}
	return g.invoker.Invoke(ctx, endpoint, envelope)
}

func (g *GRPCCommunicator) Close() error {
	if g.invoker == nil {
		return nil
	}
	closer, ok := g.invoker.(io.Closer)
	if !ok {
		return nil
	}
	return closer.Close()
}

// TransportReplicationCoordinator uses a NodeCommunicator so strategy and protocol are decoupled.
type TransportReplicationCoordinator struct {
	mu           sync.RWMutex
	config       ReplicationConfig
	nodes        map[string]NodeEndpoint
	communicator NodeCommunicator
}

func NewTransportReplicationCoordinator(config ReplicationConfig, communicator NodeCommunicator) *TransportReplicationCoordinator {
	if config.Strategy == "" {
		config = DefaultReplicationConfig()
	}
	if config.Backend == "" && communicator != nil {
		config.Backend = communicator.Backend()
	}
	if config.Backend == "" {
		config.Backend = ReplicationBackendNoop
	}

	return &TransportReplicationCoordinator{
		config:       config,
		nodes:        make(map[string]NodeEndpoint),
		communicator: communicator,
	}
}

func (t *TransportReplicationCoordinator) Strategy() SyncStrategy {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.config.Strategy
}

func (t *TransportReplicationCoordinator) Configure(config ReplicationConfig) error {
	if config.Strategy == "" {
		config = DefaultReplicationConfig()
	}
	if config.Backend == "" && t.communicator != nil {
		config.Backend = t.communicator.Backend()
	}
	if config.Backend == "" {
		config.Backend = ReplicationBackendNoop
	}
	if err := config.Validate(); err != nil {
		return err
	}

	t.mu.Lock()
	t.config = config
	t.mu.Unlock()
	return nil
}

func (t *TransportReplicationCoordinator) ConnectNode(ctx context.Context, endpoint NodeEndpoint) error {
	_ = ctx
	if endpoint.NodeID == "" {
		return fmt.Errorf("node id cannot be empty")
	}
	t.mu.Lock()
	t.nodes[endpoint.NodeID] = endpoint
	t.mu.Unlock()
	return nil
}

func (t *TransportReplicationCoordinator) DisconnectNode(ctx context.Context, nodeID string) error {
	_ = ctx
	t.mu.Lock()
	delete(t.nodes, nodeID)
	t.mu.Unlock()
	return nil
}

func (t *TransportReplicationCoordinator) ConnectedNodes() []NodeEndpoint {
	t.mu.RLock()
	defer t.mu.RUnlock()

	ids := make([]string, 0, len(t.nodes))
	for id := range t.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]NodeEndpoint, 0, len(ids))
	for _, id := range ids {
		out = append(out, t.nodes[id])
	}
	return out
}

func (t *TransportReplicationCoordinator) SynchronizeRead(ctx context.Context, txnID int64, tableName string, key []byte) error {
	return t.replicate(ctx, ReplicationEnvelope{TxnID: txnID, Op: ReplicationOpReadSync, TableName: tableName, Key: key}, t.config.ReadPolicy)
}

func (t *TransportReplicationCoordinator) ReplicateWrite(ctx context.Context, txnID int64, tableName string, key []byte, value []byte) error {
	return t.replicate(ctx, ReplicationEnvelope{TxnID: txnID, Op: ReplicationOpWrite, TableName: tableName, Key: key, Value: value}, t.config.WritePolicy)
}

func (t *TransportReplicationCoordinator) ReplicateDelete(ctx context.Context, txnID int64, tableName string, key []byte) error {
	return t.replicate(ctx, ReplicationEnvelope{TxnID: txnID, Op: ReplicationOpDelete, TableName: tableName, Key: key}, t.config.WritePolicy)
}

func (t *TransportReplicationCoordinator) Commit(ctx context.Context, txnID int64, operations []log.RecoveryOperation) error {
	return t.replicate(ctx, ReplicationEnvelope{TxnID: txnID, Op: ReplicationOpCommit, Operations: operations}, t.config.WritePolicy)
}

func (t *TransportReplicationCoordinator) Abort(ctx context.Context, txnID int64) error {
	return t.replicate(ctx, ReplicationEnvelope{TxnID: txnID, Op: ReplicationOpAbort}, t.config.WritePolicy)
}

func (t *TransportReplicationCoordinator) Close() error {
	if t.communicator != nil {
		return t.communicator.Close()
	}
	return nil
}

func (t *TransportReplicationCoordinator) replicate(ctx context.Context, envelope ReplicationEnvelope, policy SyncPolicy) error {
	if policy == SyncPolicyLocal || t.communicator == nil {
		return nil
	}

	nodes := t.ConnectedNodes()
	if len(nodes) == 0 {
		return nil
	}

	if policy == SyncPolicyAsynchronous {
		for _, endpoint := range nodes {
			ep := endpoint
			go func() { _ = t.communicator.Send(context.Background(), ep, envelope) }()
		}
		return nil
	}

	required := len(nodes)
	if policy == SyncPolicyQuorum {
		required = t.config.MinWriteAcks
		if required > len(nodes) {
			required = len(nodes)
		}
	}

	acks := 0
	var lastErr error
	for _, endpoint := range nodes {
		if err := t.communicator.Send(ctx, endpoint, envelope); err != nil {
			lastErr = err
			continue
		}
		acks++
		if acks >= required {
			return nil
		}
	}

	if lastErr != nil {
		return fmt.Errorf("replication failed (acks=%d required=%d): %w", acks, required, lastErr)
	}
	return fmt.Errorf("replication failed (acks=%d required=%d)", acks, required)
}

func newNodeCommunicatorFromBackend(config ReplicationConfig) (NodeCommunicator, error) {
	switch config.Backend {
	case ReplicationBackendNoop:
		return &NoopCommunicator{}, nil
	case ReplicationBackendMaelstrom:
		return NewMaelstromCommunicator(), nil
	case ReplicationBackendGRPC:
		return NewGRPCCommunicator(nil), nil
	default:
		return nil, fmt.Errorf("unsupported replication backend %q", config.Backend)
	}
}

// NewReplicationCoordinatorFromConfig selects a coordinator implementation by strategy/backend.
func NewReplicationCoordinatorFromConfig(config ReplicationConfig) (ReplicationCoordinator, error) {
	if config.Strategy == "" {
		config = DefaultReplicationConfig()
	}
	if config.Backend == "" {
		config.Backend = ReplicationBackendNoop
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if config.Strategy == SyncStrategyStandalone && config.Backend == ReplicationBackendNoop {
		return NewNoopReplicationCoordinator(config), nil
	}

	communicator, err := newNodeCommunicatorFromBackend(config)
	if err != nil {
		return nil, err
	}

	switch config.Strategy {
	case SyncStrategyRaft:
		return NewRaftReplicationCoordinator(config, communicator), nil
	case SyncStrategyStandalone:
		return NewTransportReplicationCoordinator(config, communicator), nil
	default:
		return nil, fmt.Errorf("unsupported sync strategy %q", config.Strategy)
	}
}
