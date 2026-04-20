package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"reflect"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/pgserver"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/log"
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
	replicationStrategy := flag.String("replication-strategy", string(synchronization.SyncStrategyStandalone), "replication strategy (standalone|raft)")
	replicationBackend := flag.String("replication-backend", string(synchronization.ReplicationBackendNoop), "replication backend (noop|maelstrom|grpc)")
	replicationMinWriteAcks := flag.Int("replication-min-write-acks", 1, "minimum acknowledgements required for quorum policies")
	replicationLocalNodeID := flag.String("replication-local-node-id", "", "local node id for raft startup bootstrap")
	replicationLeaderNodeID := flag.String("replication-leader-node-id", "", "leader node id for raft startup bootstrap")
	replicationCurrentTerm := flag.Uint64("replication-current-term", 0, "raft term for startup bootstrap")
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
	cfg.LocalNodeID = strings.TrimSpace(*replicationLocalNodeID)
	cfg.LeaderNodeID = strings.TrimSpace(*replicationLeaderNodeID)
	cfg.CurrentTerm = *replicationCurrentTerm
	if cfg.Strategy != synchronization.SyncStrategyStandalone && cfg.Strategy != synchronization.SyncStrategyRaft {
		_, _ = fmt.Fprintf(os.Stderr, "only standalone and raft replication strategies are available for now\n")
		os.Exit(2)
	}
	if err := resolveRaftBootstrapConfig(&cfg, *grpcListen); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid raft bootstrap config: %v\n", err)
		os.Exit(2)
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

	if cfg.Strategy == synchronization.SyncStrategyRaft {
		if err := engine.UpdateRaftLeadership(cfg.LocalNodeID, cfg.LeaderNodeID, cfg.CurrentTerm); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to bootstrap raft leadership: %v\n", err)
			os.Exit(1)
		}
	}

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

func resolveRaftBootstrapConfig(cfg *synchronization.ReplicationConfig, grpcListen string) error {
	if cfg == nil || cfg.Strategy != synchronization.SyncStrategyRaft {
		return nil
	}
	if cfg.ReadPolicy == "" {
		cfg.ReadPolicy = synchronization.SyncPolicySynchronous
	}
	if cfg.WritePolicy == "" {
		cfg.WritePolicy = synchronization.SyncPolicyQuorum
	}
	if cfg.MinWriteAcks <= 0 {
		cfg.MinWriteAcks = 1
	}

	if cfg.LocalNodeID == "" {
		inferredNodeID, err := inferNodeIDFromListenAddress(grpcListen)
		if err != nil {
			return fmt.Errorf("local node id is required for raft (%w)", err)
		}
		cfg.LocalNodeID = inferredNodeID
	}
	if cfg.LeaderNodeID == "" {
		cfg.LeaderNodeID = cfg.LocalNodeID
	}
	if cfg.CurrentTerm == 0 {
		cfg.CurrentTerm = 1
	}

	return cfg.Validate()
}

func buildReplicationCoordinator(cfg synchronization.ReplicationConfig) (synchronization.ReplicationCoordinator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Strategy != synchronization.SyncStrategyStandalone && cfg.Strategy != synchronization.SyncStrategyRaft {
		return nil, fmt.Errorf("only standalone and raft replication strategies are available for now")
	}

	if cfg.Backend != synchronization.ReplicationBackendGRPC {
		return synchronization.NewReplicationCoordinatorFromConfig(cfg)
	}

	communicator := synchronization.NewGRPCCommunicatorFromOptions(synchronization.GRPCUnaryInvokerOptions{})
	switch cfg.Strategy {
	case synchronization.SyncStrategyRaft:
		return synchronization.NewRaftReplicationCoordinator(cfg, communicator), nil
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
		localTxn, err := a.ensureTxn(envelope.TxnID, envelope.TableName)
		if err != nil {
			return err
		}
		return iso.Write(localTxn, envelope.Key, envelope.Value)
	case synchronization.ReplicationOpDelete:
		localTxn, err := a.ensureTxn(envelope.TxnID, envelope.TableName)
		if err != nil {
			return err
		}
		return iso.Delete(localTxn, envelope.Key)
	case synchronization.ReplicationOpReadSync:
		localTxn, err := a.ensureTxn(envelope.TxnID, envelope.TableName)
		if err != nil {
			return err
		}
		_, err = iso.Read(localTxn, envelope.Key)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	case synchronization.ReplicationOpAbort:
		localTxn, ok := a.takeTxn(envelope.TxnID)
		if !ok {
			return nil
		}
		return iso.Abort(localTxn)
	case synchronization.ReplicationOpCommit:
		for _, op := range envelope.Operations {
			if err := a.applyCommittedOperation(op); err != nil {
				if localTxn, ok := a.takeTxn(envelope.TxnID); ok {
					_ = iso.Abort(localTxn)
				}
				return err
			}
		}
		localTxn, ok := a.takeTxn(envelope.TxnID)
		if !ok {
			return nil
		}
		return iso.Commit(localTxn)
	default:
		return fmt.Errorf("unsupported replication op %q", envelope.Op)
	}
}

type replicatedIndexOp struct {
	IndexName string   `json:"indexName"`
	IndexType string   `json:"indexType"`
	Columns   []string `json:"columns"`
	Unique    bool     `json:"unique"`
}

func (a *replicatedTxnApplier) applyCommittedOperation(op log.RecoveryOperation) error {
	if a == nil || a.db == nil {
		return fmt.Errorf("replication applier is not configured")
	}
	switch op.Type {
	case log.PUT:
		localTxn, err := a.ensureTxn(int64(op.TxnID), op.TableName)
		if err != nil {
			return err
		}
		return a.db.Write(localTxn, op.Key, op.Value)
	case log.DELETE:
		localTxn, err := a.ensureTxn(int64(op.TxnID), op.TableName)
		if err != nil {
			return err
		}
		return a.db.Delete(localTxn, op.Key)
	case log.CREATE_TABLE:
		ts, err := schema.FromJSON(op.Value)
		if err != nil {
			return err
		}
		if existing, err := a.db.GetTableSchema(ts.Name); err == nil {
			if reflect.DeepEqual(existing, ts) {
				return nil
			}
			return fmt.Errorf("table %q already exists with different schema", ts.Name)
		}
		if applier, ok := a.db.(interface{ ApplyCreateTable(*schema.TableSchema) error }); ok {
			return applier.ApplyCreateTable(ts)
		}
		return a.db.CreateTable(ts)
	case log.DROP_TABLE:
		if _, err := a.db.GetTableSchema(op.TableName); err != nil {
			return nil
		}
		if applier, ok := a.db.(interface{ ApplyDropTable(string) error }); ok {
			return applier.ApplyDropTable(op.TableName)
		}
		return a.db.DropTable(op.TableName)
	case log.CREATE_INDEX:
		var idx replicatedIndexOp
		if err := json.Unmarshal(op.Value, &idx); err != nil {
			return err
		}
		indexType := schema.IndexType(idx.IndexType)
		if idx.IndexType == "" {
			indexType = schema.IndexTypeSecondary
		}
		if ts, err := a.db.GetTableSchema(op.TableName); err == nil {
			if _, exists := ts.Indexes[idx.IndexName]; exists {
				return nil
			}
		}
		return a.db.CreateIndex(op.TableName, idx.IndexName, indexType, idx.Columns, idx.Unique)
	case log.DROP_INDEX:
		if ts, err := a.db.GetTableSchema(op.TableName); err == nil {
			if _, exists := ts.Indexes[string(op.Key)]; !exists {
				return nil
			}
		}
		return a.db.DropIndex(op.TableName, string(op.Key))
	default:
		return fmt.Errorf("unsupported committed operation type %d", op.Type)
	}
}

func (a *replicatedTxnApplier) ensureTxn(remoteTxnID int64, tableName string) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if localTxn, ok := a.txnByID[remoteTxnID]; ok {
		return localTxn, nil
	}
	var tableSchema *schema.TableSchema
	if strings.TrimSpace(tableName) != "" {
		var err error
		tableSchema, err = a.db.GetTableSchema(tableName)
		if err != nil {
			return 0, fmt.Errorf("load table schema for %q: %w", tableName, err)
		}
	}
	localTxn := a.db.BeginTransaction(txn_unit.SERIALIZABLE, tableName, tableSchema)
	a.txnByID[remoteTxnID] = localTxn
	return localTxn, nil
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
