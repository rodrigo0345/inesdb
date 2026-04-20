package synchronization

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestReplicationConfigValidate_Backend(t *testing.T) {
	cfg := DefaultReplicationConfig()
	cfg.Backend = ReplicationBackend("invalid")
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid backend validation error")
	}
}

func TestReplicationConfigValidate_StrategyVariants(t *testing.T) {
	strategies := []SyncStrategy{
		SyncStrategyStandalone,
		SyncStrategyRaft,
	}

	for _, strategy := range strategies {
		t.Run(string(strategy), func(t *testing.T) {
			cfg := DefaultReplicationConfig()
			cfg.Strategy = strategy
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestReplicationConfigValidate_RaftTermRequiresLocalNode(t *testing.T) {
	cfg := DefaultReplicationConfig()
	cfg.CurrentTerm = 2
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when raft term is set without local node id")
	}
}

func TestNewReplicationCoordinatorFromConfig_SelectsBackend(t *testing.T) {
	tests := []struct {
		name    string
		backend ReplicationBackend
	}{
		{name: "noop", backend: ReplicationBackendNoop},
		{name: "maelstrom", backend: ReplicationBackendMaelstrom},
		{name: "grpc", backend: ReplicationBackendGRPC},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultReplicationConfig()
			cfg.Backend = tc.backend
			coord, err := NewReplicationCoordinatorFromConfig(cfg)
			if err != nil {
				t.Fatalf("NewReplicationCoordinatorFromConfig() error = %v", err)
			}
			if coord == nil {
				t.Fatal("expected non-nil coordinator")
			}
			if err := coord.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		})
	}
}

func TestMaelstromCommunicator_DispatchesToRegisteredHandler(t *testing.T) {
	comm := NewMaelstromCommunicator()
	called := false
	comm.RegisterHandler("node-1", func(ctx context.Context, envelope ReplicationEnvelope) error {
		called = envelope.Op == ReplicationOpWrite && envelope.TxnID == 42
		return nil
	})

	err := comm.Send(context.Background(), NodeEndpoint{NodeID: "node-1"}, ReplicationEnvelope{TxnID: 42, Op: ReplicationOpWrite})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if !called {
		t.Fatal("expected registered handler to be called")
	}
}

func TestGRPCCommunicator_SendFailsWhenInvokerMissing(t *testing.T) {
	comm := NewGRPCCommunicator(nil)
	err := comm.Send(context.Background(), NodeEndpoint{NodeID: "node-1"}, ReplicationEnvelope{Op: ReplicationOpWrite})
	if err == nil {
		t.Fatal("expected error when grpc invoker is not configured")
	}
}

type failingCommunicator struct{}

func (f *failingCommunicator) Backend() ReplicationBackend { return ReplicationBackendMaelstrom }
func (f *failingCommunicator) Send(ctx context.Context, endpoint NodeEndpoint, envelope ReplicationEnvelope) error {
	_, _, _ = ctx, endpoint, envelope
	return errors.New("send failed")
}
func (f *failingCommunicator) Close() error { return nil }

type captureCommunicator struct {
	endpoints []NodeEndpoint
}

func (c *captureCommunicator) Backend() ReplicationBackend { return ReplicationBackendGRPC }
func (c *captureCommunicator) Send(ctx context.Context, endpoint NodeEndpoint, envelope ReplicationEnvelope) error {
	_, _ = ctx, envelope
	c.endpoints = append(c.endpoints, endpoint)
	return nil
}
func (c *captureCommunicator) Close() error { return nil }

func TestTransportReplicationCoordinator_QuorumFailsWithoutAcks(t *testing.T) {
	cfg := DefaultReplicationConfig()
	cfg.Backend = ReplicationBackendMaelstrom
	cfg.WritePolicy = SyncPolicyQuorum
	cfg.MinWriteAcks = 2

	coord := NewTransportReplicationCoordinator(cfg, &failingCommunicator{})
	if err := coord.ConnectNode(context.Background(), NodeEndpoint{NodeID: "n1"}); err != nil {
		t.Fatalf("ConnectNode(n1) error = %v", err)
	}
	if err := coord.ConnectNode(context.Background(), NodeEndpoint{NodeID: "n2"}); err != nil {
		t.Fatalf("ConnectNode(n2) error = %v", err)
	}

	err := coord.ReplicateWrite(context.Background(), 1, "users", []byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected quorum replication error")
	}
}

func TestNewReplicationCoordinatorFromConfig_SelectsRaftAndStrong(t *testing.T) {
	tests := []struct {
		name     string
		strategy SyncStrategy
	}{
		{name: "raft", strategy: SyncStrategyRaft},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultReplicationConfig()
			cfg.Backend = ReplicationBackendMaelstrom
			cfg.Strategy = tc.strategy
			coord, err := NewReplicationCoordinatorFromConfig(cfg)
			if err != nil {
				t.Fatalf("NewReplicationCoordinatorFromConfig() error = %v", err)
			}
			if coord.Strategy() != tc.strategy {
				t.Fatalf("Strategy() = %q, want %q", coord.Strategy(), tc.strategy)
			}
			if err := coord.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		})
	}
}

func TestRaftReplicationCoordinator_RejectsNonLeaderWrites(t *testing.T) {
	coord := NewRaftReplicationCoordinator(DefaultReplicationConfig(), &NoopCommunicator{})
	coord.SetLeadership("node-a", "node-b", 1)

	err := coord.ReplicateWrite(context.Background(), 10, "users", []byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected raft write rejection for non-leader")
	}
}

func TestRaftReplicationCoordinator_AllowsFollowerWritesWithGRPCBackend(t *testing.T) {
	cfg := DefaultReplicationConfig()
	cfg.Backend = ReplicationBackendGRPC
	cfg.WritePolicy = SyncPolicySynchronous

	comm := &captureCommunicator{}
	coord := NewRaftReplicationCoordinator(cfg, comm)
	if err := coord.ConnectNode(context.Background(), NodeEndpoint{NodeID: "node-b", Address: "leader:7000"}); err != nil {
		t.Fatalf("ConnectNode() error = %v", err)
	}
	coord.SetLeadership("node-a", "node-b", 1)

	if err := coord.ReplicateWrite(context.Background(), 10, "users", []byte("k"), []byte("v")); err != nil {
		t.Fatalf("ReplicateWrite() follower grpc error = %v", err)
	}
	if err := coord.Commit(context.Background(), 10, nil); err != nil {
		t.Fatalf("Commit() follower grpc error = %v", err)
	}
	if len(comm.endpoints) == 0 {
		t.Fatal("expected follower write/commit to use replication transport")
	}
}

func TestRaftReplicationCoordinator_RejectsNonLeaderReadsAndCommit(t *testing.T) {
	coord := NewRaftReplicationCoordinator(DefaultReplicationConfig(), &NoopCommunicator{})
	coord.SetLeadership("node-a", "node-b", 2)

	if err := coord.SynchronizeRead(context.Background(), 11, "users", []byte("k")); err == nil {
		t.Fatal("expected raft read rejection for non-leader")
	}
	if err := coord.Commit(context.Background(), 11, nil); err == nil {
		t.Fatal("expected raft commit rejection for non-leader")
	}
}

func TestRaftReplicationCoordinator_RejectsWritesWhenLeadershipIsNotConfigured(t *testing.T) {
	coord := NewRaftReplicationCoordinator(DefaultReplicationConfig(), &NoopCommunicator{})

	if err := coord.ReplicateWrite(context.Background(), 12, "users", []byte("k"), []byte("v")); err == nil {
		t.Fatal("expected raft write rejection when leadership is missing")
	}
}

func TestRaftReplicationCoordinator_RejectsStaleLeadershipTerm(t *testing.T) {
	coord := NewRaftReplicationCoordinator(DefaultReplicationConfig(), &NoopCommunicator{})
	if err := coord.UpdateLeadership("n1", "n1", 4); err != nil {
		t.Fatalf("UpdateLeadership(term=4) error = %v", err)
	}

	err := coord.UpdateLeadership("n1", "n2", 3)
	if err == nil {
		t.Fatal("expected stale term error")
	}
	if !errors.Is(err, ErrStaleRaftTerm) {
		t.Fatalf("UpdateLeadership(term=3) error = %v, want ErrStaleRaftTerm", err)
	}
}

func TestRaftReplicationCoordinator_AllowsLeaderCrashThenHigherTermFailover(t *testing.T) {
	coord := NewRaftReplicationCoordinator(DefaultReplicationConfig(), &NoopCommunicator{})
	if err := coord.UpdateLeadership("n2", "n2", 2); err != nil {
		t.Fatalf("UpdateLeadership(initial leader) error = %v", err)
	}

	if err := coord.ReplicateWrite(context.Background(), 21, "users", []byte("k"), []byte("v")); err != nil {
		t.Fatalf("ReplicateWrite() as leader error = %v", err)
	}

	if err := coord.UpdateLeadership("n2", "", 3); err != nil {
		t.Fatalf("UpdateLeadership(leader crash) error = %v", err)
	}
	err := coord.ReplicateWrite(context.Background(), 22, "users", []byte("k"), []byte("v"))
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("ReplicateWrite() during election error = %v, want leadership-not-configured", err)
	}

	if err := coord.UpdateLeadership("n2", "n3", 4); err != nil {
		t.Fatalf("UpdateLeadership(new leader) error = %v", err)
	}
	err = coord.ReplicateWrite(context.Background(), 23, "users", []byte("k"), []byte("v"))
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Fatalf("ReplicateWrite() as follower error = %v, want not-leader", err)
	}
}
