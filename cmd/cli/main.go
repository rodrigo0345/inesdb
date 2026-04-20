package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/pgserver"
	"github.com/rodrigo0345/omag/internal/txn/synchronization"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	applog "github.com/rodrigo0345/omag/pkg/log"
)

const (
	defaultDBPath     = "./test.db"
	defaultLSMDataDir = "./lsm_data"
	defaultWALPath    = "./test.wal"
)

func main() {
	listenAddr := flag.String("listen", ":5432", "TCP address to listen on for psql connections")
	dbPath := flag.String("db", defaultDBPath, "path to the database file")
	lsmDataDir := flag.String("lsm-data-dir", defaultLSMDataDir, "directory for LSM table data")
	walPath := flag.String("wal", defaultWALPath, "path to the WAL file")
	replicationStrategy := flag.String("replication-strategy", string(synchronization.SyncStrategyStandalone), "replication strategy (standalone|leader-follower|multi-leader|quorum|raft|strong-consistency)")
	replicationBackend := flag.String("replication-backend", string(synchronization.ReplicationBackendNoop), "replication backend (noop|maelstrom|grpc)")
	replicationMinWriteAcks := flag.Int("replication-min-write-acks", 1, "minimum acknowledgements required for quorum policies")
	localNodeID := flag.String("replication-local-node-id", "", "local replication node id")
	leaderNodeID := flag.String("replication-leader-node-id", "", "leader replication node id (raft)")
	replicationCurrentTerm := flag.Uint64("replication-current-term", 0, "current raft term")
	peerNodes := flag.String("replication-peer-nodes", "", "comma-separated peer list: nodeA=host:port,nodeB=host:port")
	grpcListen := flag.String("replication-grpc-listen", "", "address for incoming gRPC replication envelopes, e.g. :7000")
	pprofListen := flag.String("pprof-listen", "", "optional net/http/pprof listen address, e.g. :6060")
	debug := flag.Bool("debug", false, "enable debug logs for pgwire server and engine")
	flag.Parse()

	if *debug {
		applog.SetLevel(applog.LevelDebug)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := synchronization.DefaultReplicationConfig()
	cfg.Strategy = synchronization.SyncStrategy(*replicationStrategy)
	cfg.Backend = synchronization.ReplicationBackend(*replicationBackend)
	cfg.MinWriteAcks = *replicationMinWriteAcks
	cfg.LocalNodeID = *localNodeID
	cfg.LeaderNodeID = *leaderNodeID
	cfg.CurrentTerm = *replicationCurrentTerm
	if cfg.Strategy == synchronization.SyncStrategyRaft && cfg.LocalNodeID == "" {
		inferredNodeID, inferErr := inferNodeIDFromListenAddress(*grpcListen)
		if inferErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "invalid raft local node id inference: %v\n", inferErr)
			os.Exit(1)
		}
		cfg.LocalNodeID = inferredNodeID
	}

	peers, err := parsePeerNodes(*peerNodes)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid --replication-peer-nodes: %v\n", err)
		os.Exit(1)
	}

	replicator, err := buildReplicationCoordinator(cfg)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to configure replication coordinator: %v\n", err)
		os.Exit(1)
	}
	for _, peer := range peers {
		if err := replicator.ConnectNode(ctx, peer); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to connect replication peer %q: %v\n", peer.NodeID, err)
			os.Exit(1)
		}
	}

	applog.Info("[PGSERVER] starting OMAG pgwire server listen=%s db=%s lsm=%s wal=%s replication=%s/%s local=%s peers=%d debug=%v", *listenAddr, *dbPath, *lsmDataDir, *walPath, cfg.Strategy, cfg.Backend, cfg.LocalNodeID, len(peers), *debug)

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:                 *dbPath,
		LSMDataDir:             *lsmDataDir,
		WALPath:                *walPath,
		ReplicationConfig:      cfg,
		ReplicationCoordinator: replicator,
	})
	if err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, "failed to open database:", err); writeErr != nil {
			_ = writeErr
		}
		os.Exit(1)
	}
	defer func() {
		if closeErr := engine.Close(); closeErr != nil {
			_, _ = fmt.Fprintln(os.Stderr, "close error:", closeErr)
		}
	}()

	if cfg.Backend == synchronization.ReplicationBackendGRPC && *grpcListen != "" {
		applier := newReplicatedTxnApplier(engine)
		go func() {
			err := synchronization.ListenAndServeGRPCReplication(ctx, *grpcListen, applier.ApplyEnvelope)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "replication grpc server error: %v\n", err)
				stop()
			}
		}()
	}

	if strings.TrimSpace(*pprofListen) != "" {
		pprofServer := &http.Server{Addr: strings.TrimSpace(*pprofListen), Handler: http.DefaultServeMux}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = pprofServer.Shutdown(shutdownCtx)
		}()
		go func() {
			if err := pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				_, _ = fmt.Fprintf(os.Stderr, "pprof server error: %v\n", err)
				stop()
			}
		}()
	}

	srv := pgserver.New(engine)

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, "failed to listen:", err); writeErr != nil {
			_ = writeErr
		}
		os.Exit(1)
	}

	if err := srv.Serve(ctx, ln); err != nil && err != context.Canceled {
		if _, writeErr := fmt.Fprintln(os.Stderr, "server error:", err); writeErr != nil {
			_ = writeErr
		}
		os.Exit(1)
	}
}

func parsePeerNodes(raw string) ([]synchronization.NodeEndpoint, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]synchronization.NodeEndpoint, 0, len(parts))
	for _, part := range parts {
		segment := strings.TrimSpace(part)
		if segment == "" {
			continue
		}
		nodeID := ""
		address := segment
		if strings.Contains(segment, "=") {
			tokens := strings.SplitN(segment, "=", 2)
			nodeID = strings.TrimSpace(tokens[0])
			address = strings.TrimSpace(tokens[1])
		}
		if address == "" {
			return nil, fmt.Errorf("invalid segment %q, expected non-empty address", segment)
		}
		if nodeID == "" {
			inferredNodeID, err := inferNodeIDFromListenAddress(address)
			if err != nil {
				return nil, fmt.Errorf("invalid segment %q: %w", segment, err)
			}
			nodeID = inferredNodeID
		}
		out = append(out, synchronization.NodeEndpoint{NodeID: nodeID, Address: address})
	}
	return out, nil
}

func inferNodeIDFromListenAddress(listenAddress string) (string, error) {
	if strings.TrimSpace(listenAddress) == "" {
		return "", fmt.Errorf("replication grpc listen address is required for node id inference")
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(listenAddress))
	if err != nil {
		return "", fmt.Errorf("invalid host:port address %q", listenAddress)
	}
	if strings.TrimSpace(port) == "" {
		return "", fmt.Errorf("missing port in address %q", listenAddress)
	}
	if strings.TrimSpace(host) == "" {
		host = "127.0.0.1"
	}
	normalized := net.JoinHostPort(strings.ToLower(strings.TrimSpace(host)), strings.TrimSpace(port))
	sum := sha256.Sum256([]byte(normalized))
	return "node-" + hex.EncodeToString(sum[:8]), nil
}

func buildReplicationCoordinator(cfg synchronization.ReplicationConfig) (synchronization.ReplicationCoordinator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if cfg.Backend != synchronization.ReplicationBackendGRPC {
		return synchronization.NewReplicationCoordinatorFromConfig(cfg)
	}

	communicator := synchronization.NewGRPCCommunicatorFromOptions(synchronization.GRPCUnaryInvokerOptions{})
	switch cfg.Strategy {
	case synchronization.SyncStrategyRaft:
		return synchronization.NewRaftReplicationCoordinator(cfg, communicator), nil
	case synchronization.SyncStrategyStrongConsistency:
		return synchronization.NewStrongConsistencyReplicationCoordinator(cfg, communicator), nil
	case synchronization.SyncStrategyStandalone, synchronization.SyncStrategyLeaderFollower, synchronization.SyncStrategyMultiLeader, synchronization.SyncStrategyQuorum:
		return synchronization.NewTransportReplicationCoordinator(cfg, communicator), nil
	default:
		return nil, fmt.Errorf("unsupported replication strategy %q", cfg.Strategy)
	}
}

type replicatedTxnApplier struct {
	db database.Database

	mu      sync.Mutex
	txnByID map[int64]int64
}

func newReplicatedTxnApplier(db database.Database) *replicatedTxnApplier {
	return &replicatedTxnApplier{
		db:      db,
		txnByID: make(map[int64]int64),
	}
}

func (a *replicatedTxnApplier) ApplyEnvelope(ctx context.Context, envelope synchronization.ReplicationEnvelope) error {
	_ = ctx
	iso := a.db.IsolationManager()

	switch envelope.Op {
	case synchronization.ReplicationOpWrite:
		localTxn := a.ensureTxn(envelope.TxnID)
		return iso.Write(localTxn, envelope.Key, envelope.Value)
	case synchronization.ReplicationOpDelete:
		localTxn := a.ensureTxn(envelope.TxnID)
		return iso.Delete(localTxn, envelope.Key)
	case synchronization.ReplicationOpReadSync:
		localTxn := a.ensureTxn(envelope.TxnID)
		_, err := iso.Read(localTxn, envelope.Key)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	case synchronization.ReplicationOpCommit:
		localTxn, ok := a.takeTxn(envelope.TxnID)
		if !ok {
			return nil
		}
		return iso.Commit(localTxn)
	case synchronization.ReplicationOpAbort:
		localTxn, ok := a.takeTxn(envelope.TxnID)
		if !ok {
			return nil
		}
		return iso.Abort(localTxn)
	default:
		return fmt.Errorf("unsupported replication op %q", envelope.Op)
	}
}

func (a *replicatedTxnApplier) ensureTxn(remoteTxnID int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	if localTxn, ok := a.txnByID[remoteTxnID]; ok {
		return localTxn
	}
	localTxn := a.db.BeginTransaction(txn_unit.SERIALIZABLE, "", nil)
	a.txnByID[remoteTxnID] = localTxn
	return localTxn
}

func (a *replicatedTxnApplier) takeTxn(remoteTxnID int64) (int64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	localTxn, ok := a.txnByID[remoteTxnID]
	if ok {
		delete(a.txnByID, remoteTxnID)
	}
	return localTxn, ok
}
