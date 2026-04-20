package synchronization

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"

	"github.com/rodrigo0345/omag/internal/txn/log"
)

const (
	grpcReplicationServiceName  = "inesdb.sync.ReplicationService"
	grpcReplicateMethodName     = "/inesdb.sync.ReplicationService/Replicate"
	grpcRaftServiceName         = "inesdb.sync.RaftConsensusService"
	grpcRequestVoteMethodName   = "/inesdb.sync.RaftConsensusService/RequestVote"
	grpcAppendEntriesMethodName = "/inesdb.sync.RaftConsensusService/AppendEntries"
)

// GRPCAddressResolver maps a replication endpoint into a gRPC dial target.
type GRPCAddressResolver func(endpoint NodeEndpoint) (string, error)

// GRPCUnaryInvokerOptions configures the default gRPC invoker.
type GRPCUnaryInvokerOptions struct {
	AddressResolver GRPCAddressResolver
	DialOptions     []grpc.DialOption
}

// DefaultGRPCAddressResolver uses NodeEndpoint.Address.
func DefaultGRPCAddressResolver(endpoint NodeEndpoint) (string, error) {
	if endpoint.Address == "" {
		return "", fmt.Errorf("grpc endpoint address is empty for node %q", endpoint.NodeID)
	}
	return endpoint.Address, nil
}

// DefaultGRPCDialOptions uses insecure credentials and JSON codec for easy interop.
func DefaultGRPCDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(grpcJSONCodec{})),
	}
}

// DefaultGRPCUnaryInvoker is a reusable production invoker for GRPCCommunicator.
type DefaultGRPCUnaryInvoker struct {
	mu          sync.Mutex
	resolver    GRPCAddressResolver
	dialOptions []grpc.DialOption
	connections map[string]*grpc.ClientConn
	closed      bool
}

func NewDefaultGRPCUnaryInvoker(opts GRPCUnaryInvokerOptions) *DefaultGRPCUnaryInvoker {
	resolver := opts.AddressResolver
	if resolver == nil {
		resolver = DefaultGRPCAddressResolver
	}
	dialOptions := opts.DialOptions
	if len(dialOptions) == 0 {
		dialOptions = DefaultGRPCDialOptions()
	}
	return &DefaultGRPCUnaryInvoker{
		resolver:    resolver,
		dialOptions: dialOptions,
		connections: make(map[string]*grpc.ClientConn),
	}
}

// NewGRPCCommunicatorFromOptions creates a GRPCCommunicator with the default reusable invoker.
func NewGRPCCommunicatorFromOptions(opts GRPCUnaryInvokerOptions) *GRPCCommunicator {
	return NewGRPCCommunicator(NewDefaultGRPCUnaryInvoker(opts))
}

func (i *DefaultGRPCUnaryInvoker) Invoke(ctx context.Context, endpoint NodeEndpoint, envelope ReplicationEnvelope) error {
	address, err := i.resolver(endpoint)
	if err != nil {
		return err
	}
	conn, err := i.connectionFor(ctx, address)
	if err != nil {
		return err
	}

	request := grpcReplicationRequest{
		TxnID: envelope.TxnID,
		Op:    string(envelope.Op),
		TableName: envelope.TableName,
		Key:   envelope.Key,
		Value: envelope.Value,
		Operations: envelope.Operations,
	}
	var response grpcReplicationResponse
	if err := conn.Invoke(ctx, grpcReplicateMethodName, &request, &response, grpc.ForceCodec(grpcJSONCodec{})); err != nil {
		return err
	}
	if response.Error != "" {
		return fmt.Errorf("grpc replication error: %s", response.Error)
	}
	return nil
}

func (i *DefaultGRPCUnaryInvoker) RequestVote(ctx context.Context, endpoint NodeEndpoint, req RaftRequestVoteRequest) (RaftRequestVoteResponse, error) {
	address, err := i.resolver(endpoint)
	if err != nil {
		return RaftRequestVoteResponse{}, err
	}
	conn, err := i.connectionFor(ctx, address)
	if err != nil {
		return RaftRequestVoteResponse{}, err
	}

	request := grpcRaftRequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
	var response grpcRaftRequestVoteResponse
	if err := conn.Invoke(ctx, grpcRequestVoteMethodName, &request, &response, grpc.ForceCodec(grpcJSONCodec{})); err != nil {
		return RaftRequestVoteResponse{}, err
	}
	if response.Error != "" {
		return RaftRequestVoteResponse{}, fmt.Errorf("grpc raft request vote error: %s", response.Error)
	}
	return RaftRequestVoteResponse{Term: response.Term, VoteGranted: response.VoteGranted}, nil
}

func (i *DefaultGRPCUnaryInvoker) AppendEntries(ctx context.Context, endpoint NodeEndpoint, req RaftAppendEntriesRequest) (RaftAppendEntriesResponse, error) {
	address, err := i.resolver(endpoint)
	if err != nil {
		return RaftAppendEntriesResponse{}, err
	}
	conn, err := i.connectionFor(ctx, address)
	if err != nil {
		return RaftAppendEntriesResponse{}, err
	}

	entries := make([]grpcRaftLogEntry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, grpcRaftLogEntry{Index: entry.Index, Term: entry.Term, Envelope: entry.Envelope})
	}
	request := grpcRaftAppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}
	var response grpcRaftAppendEntriesResponse
	if err := conn.Invoke(ctx, grpcAppendEntriesMethodName, &request, &response, grpc.ForceCodec(grpcJSONCodec{})); err != nil {
		return RaftAppendEntriesResponse{}, err
	}
	if response.Error != "" {
		return RaftAppendEntriesResponse{}, fmt.Errorf("grpc raft append entries error: %s", response.Error)
	}
	return RaftAppendEntriesResponse{
		Term:          response.Term,
		Success:       response.Success,
		ConflictIndex: response.ConflictIndex,
		ConflictTerm:  response.ConflictTerm,
	}, nil
}

func (i *DefaultGRPCUnaryInvoker) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.closed = true
	var firstErr error
	for addr, conn := range i.connections {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close grpc connection %q: %w", addr, err)
		}
	}
	i.connections = make(map[string]*grpc.ClientConn)
	return firstErr
}

func (i *DefaultGRPCUnaryInvoker) connectionFor(ctx context.Context, address string) (*grpc.ClientConn, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed {
		return nil, fmt.Errorf("grpc invoker is closed")
	}
	if conn, ok := i.connections[address]; ok {
		return conn, nil
	}
	conn, err := grpc.DialContext(ctx, address, i.dialOptions...)
	if err != nil {
		return nil, err
	}
	i.connections[address] = conn
	return conn, nil
}

// GRPCReplicationHandler processes incoming replication envelopes on a gRPC server.
type GRPCReplicationHandler func(ctx context.Context, envelope ReplicationEnvelope) error

// GRPCRaftConsensusHandler processes incoming Raft consensus RPCs.
type GRPCRaftConsensusHandler interface {
	RequestVote(ctx context.Context, req RaftRequestVoteRequest) (RaftRequestVoteResponse, error)
	AppendEntries(ctx context.Context, req RaftAppendEntriesRequest) (RaftAppendEntriesResponse, error)
}

// RaftRequestVoteRequest mirrors RequestVote RPC fields.
type RaftRequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RaftRequestVoteResponse mirrors RequestVote response fields.
type RaftRequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// RaftLogEntry carries Raft log index/term and command payload.
type RaftLogEntry struct {
	Index    uint64               `json:"index"`
	Term     uint64               `json:"term"`
	Envelope *ReplicationEnvelope `json:"envelope,omitempty"`
}

// RaftAppendEntriesRequest carries heartbeat/append payload.
type RaftAppendEntriesRequest struct {
	Term         uint64         `json:"term"`
	LeaderID     string         `json:"leader_id"`
	PrevLogIndex uint64         `json:"prev_log_index"`
	PrevLogTerm  uint64         `json:"prev_log_term"`
	Entries      []RaftLogEntry `json:"entries,omitempty"`
	LeaderCommit uint64         `json:"leader_commit"`
}

// RaftAppendEntriesResponse mirrors AppendEntries response fields.
type RaftAppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
}

// RegisterGRPCReplicationService wires replication handling into a grpc.Server.
func RegisterGRPCReplicationService(server grpc.ServiceRegistrar, handler GRPCReplicationHandler) {
	server.RegisterService(&grpcReplicationServiceDesc, &grpcReplicationService{handler: handler})
}

// RegisterGRPCRaftConsensusService wires Raft consensus handling into a grpc.Server.
func RegisterGRPCRaftConsensusService(server grpc.ServiceRegistrar, handler GRPCRaftConsensusHandler) {
	server.RegisterService(&grpcRaftServiceDesc, &grpcRaftService{handler: handler})
}

type grpcReplicationService struct {
	handler GRPCReplicationHandler
}

type grpcRaftService struct {
	handler GRPCRaftConsensusHandler
}

type grpcReplicationServiceAPI interface {
	Replicate(ctx context.Context, req *grpcReplicationRequest) (*grpcReplicationResponse, error)
}

type grpcRaftServiceAPI interface {
	RequestVote(ctx context.Context, req *grpcRaftRequestVoteRequest) (*grpcRaftRequestVoteResponse, error)
	AppendEntries(ctx context.Context, req *grpcRaftAppendEntriesRequest) (*grpcRaftAppendEntriesResponse, error)
}

type grpcReplicationRequest struct {
	TxnID int64  `json:"txn_id"`
	Op    string `json:"op"`
	TableName string `json:"table_name,omitempty"`
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
	Operations []log.RecoveryOperation `json:"operations,omitempty"`
}

type grpcReplicationResponse struct {
	Error string `json:"error,omitempty"`
}

type grpcRaftRequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type grpcRaftRequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	Error       string `json:"error,omitempty"`
}

type grpcRaftLogEntry struct {
	Index    uint64               `json:"index"`
	Term     uint64               `json:"term"`
	Envelope *ReplicationEnvelope `json:"envelope,omitempty"`
}

type grpcRaftAppendEntriesRequest struct {
	Term         uint64             `json:"term"`
	LeaderID     string             `json:"leader_id"`
	PrevLogIndex uint64             `json:"prev_log_index"`
	PrevLogTerm  uint64             `json:"prev_log_term"`
	Entries      []grpcRaftLogEntry `json:"entries,omitempty"`
	LeaderCommit uint64             `json:"leader_commit"`
}

type grpcRaftAppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
	Error         string `json:"error,omitempty"`
}

func (s *grpcReplicationService) Replicate(ctx context.Context, req *grpcReplicationRequest) (*grpcReplicationResponse, error) {
	if req == nil {
		return &grpcReplicationResponse{Error: "empty replication request"}, nil
	}
	if s.handler == nil {
		return &grpcReplicationResponse{Error: "grpc replication handler is not configured"}, nil
	}
	envelope := ReplicationEnvelope{
		TxnID: req.TxnID,
		Op:    ReplicationOperation(req.Op),
		TableName: req.TableName,
		Key:   req.Key,
		Value: req.Value,
		Operations: req.Operations,
	}
	if err := s.handler(ctx, envelope); err != nil {
		return &grpcReplicationResponse{Error: err.Error()}, nil
	}
	return &grpcReplicationResponse{}, nil
}

func (s *grpcRaftService) RequestVote(ctx context.Context, req *grpcRaftRequestVoteRequest) (*grpcRaftRequestVoteResponse, error) {
	if req == nil {
		return &grpcRaftRequestVoteResponse{Error: "empty request vote request"}, nil
	}
	if s.handler == nil {
		return &grpcRaftRequestVoteResponse{Error: "grpc raft handler is not configured"}, nil
	}
	resp, err := s.handler.RequestVote(ctx, RaftRequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	if err != nil {
		return &grpcRaftRequestVoteResponse{Error: err.Error()}, nil
	}
	return &grpcRaftRequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (s *grpcRaftService) AppendEntries(ctx context.Context, req *grpcRaftAppendEntriesRequest) (*grpcRaftAppendEntriesResponse, error) {
	if req == nil {
		return &grpcRaftAppendEntriesResponse{Error: "empty append entries request"}, nil
	}
	if s.handler == nil {
		return &grpcRaftAppendEntriesResponse{Error: "grpc raft handler is not configured"}, nil
	}
	entries := make([]RaftLogEntry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, RaftLogEntry{Index: entry.Index, Term: entry.Term, Envelope: entry.Envelope})
	}
	resp, err := s.handler.AppendEntries(ctx, RaftAppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		return &grpcRaftAppendEntriesResponse{Error: err.Error()}, nil
	}
	return &grpcRaftAppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

func grpcReplicateHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := new(grpcReplicationRequest)
	if err := dec(request); err != nil {
		return nil, err
	}
	service := srv.(*grpcReplicationService)
	if interceptor == nil {
		return service.Replicate(ctx, request)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: grpcReplicateMethodName,
	}
	handler := func(innerCtx context.Context, req any) (any, error) {
		return service.Replicate(innerCtx, req.(*grpcReplicationRequest))
	}
	return interceptor(ctx, request, info, handler)
}

func grpcRequestVoteHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := new(grpcRaftRequestVoteRequest)
	if err := dec(request); err != nil {
		return nil, err
	}
	service := srv.(*grpcRaftService)
	if interceptor == nil {
		return service.RequestVote(ctx, request)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcRequestVoteMethodName}
	handler := func(innerCtx context.Context, req any) (any, error) {
		return service.RequestVote(innerCtx, req.(*grpcRaftRequestVoteRequest))
	}
	return interceptor(ctx, request, info, handler)
}

func grpcAppendEntriesHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := new(grpcRaftAppendEntriesRequest)
	if err := dec(request); err != nil {
		return nil, err
	}
	service := srv.(*grpcRaftService)
	if interceptor == nil {
		return service.AppendEntries(ctx, request)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: grpcAppendEntriesMethodName}
	handler := func(innerCtx context.Context, req any) (any, error) {
		return service.AppendEntries(innerCtx, req.(*grpcRaftAppendEntriesRequest))
	}
	return interceptor(ctx, request, info, handler)
}

var grpcReplicationServiceDesc = grpc.ServiceDesc{
	ServiceName: grpcReplicationServiceName,
	HandlerType: (*grpcReplicationServiceAPI)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Replicate", Handler: grpcReplicateHandler},
	},
}

var grpcRaftServiceDesc = grpc.ServiceDesc{
	ServiceName: grpcRaftServiceName,
	HandlerType: (*grpcRaftServiceAPI)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "RequestVote", Handler: grpcRequestVoteHandler},
		{MethodName: "AppendEntries", Handler: grpcAppendEntriesHandler},
	},
}

type grpcJSONCodec struct{}

func (grpcJSONCodec) Name() string { return "json" }

func (grpcJSONCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (grpcJSONCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func init() {
	encoding.RegisterCodec(grpcJSONCodec{})
}

// ListenAndServeGRPCReplication is a helper for running replication server processes.
func ListenAndServeGRPCReplication(ctx context.Context, address string, handler GRPCReplicationHandler, serverOptions ...grpc.ServerOption) error {
	return ListenAndServeGRPCReplicationWithRaft(ctx, address, handler, nil, serverOptions...)
}

// ListenAndServeGRPCReplicationWithRaft starts a server that can handle replication and Raft RPCs.
func ListenAndServeGRPCReplicationWithRaft(ctx context.Context, address string, replicationHandler GRPCReplicationHandler, raftHandler GRPCRaftConsensusHandler, serverOptions ...grpc.ServerOption) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer lis.Close()

	serverOptions = append([]grpc.ServerOption{grpc.ForceServerCodec(grpcJSONCodec{})}, serverOptions...)
	server := grpc.NewServer(serverOptions...)
	RegisterGRPCReplicationService(server, replicationHandler)
	if raftHandler != nil {
		RegisterGRPCRaftConsensusService(server, raftHandler)
	}

	shutdown := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			server.GracefulStop()
		case <-shutdown:
		}
	}()
	defer close(shutdown)

	if err := server.Serve(lis); err != nil {
		select {
		case <-ctx.Done():
			return nil
		default:
			return err
		}
	}
	return nil
}
