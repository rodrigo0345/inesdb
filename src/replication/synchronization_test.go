package replication

import (
	"context"
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
	for _, strategy := range []SyncStrategy{SyncStrategyStandalone, SyncStrategyRaft} {
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

func TestNoopReplicationCoordinator_ConnectAndList(t *testing.T) {
	coord := NewNoopReplicationCoordinator(DefaultReplicationConfig())
	if err := coord.ConnectNode(context.Background(), NodeEndpoint{NodeID: "n1"}); err != nil {
		t.Fatalf("ConnectNode() error = %v", err)
	}
	if err := coord.ConnectNode(context.Background(), NodeEndpoint{NodeID: "n2"}); err != nil {
		t.Fatalf("ConnectNode() error = %v", err)
	}
	nodes := coord.ConnectedNodes()
	if len(nodes) != 2 {
		t.Fatalf("ConnectedNodes() = %d, want 2", len(nodes))
	}
}
