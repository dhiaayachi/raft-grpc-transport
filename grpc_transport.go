package raftgrpc

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/dhiaayachi/raft-grpc-transport/conn/pool"
	raftv1 "github.com/dhiaayachi/raft-grpc-transport/gen/go/proto/raft/v1"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

var _ raft.Transport = (*GrpcTransport)(nil)

// GrpcTransport implements raft.Transport using gRPC.
type GrpcTransport struct {
	// localAddr is the address of the local node.
	localAddr raft.ServerAddress

	// consumerCh is the channel to consume RPC requests.
	consumerCh chan raft.RPC

	// peers is a map of peer address to connection pool.
	peers map[raft.ServerAddress]*pool.Pool
	// peersMutex protects the peers map.
	peersMutex sync.RWMutex

	// timeout is the timeout for RPCs.
	timeout time.Duration
	// poolSize is the number of connections to maintain per peer.
	poolSize int
}

// grpcServer implements the generated RaftTransportServiceServer interface.
type grpcServer struct {
	raftv1.UnimplementedRaftTransportServiceServer
	trans *GrpcTransport
}

// TransportOption is a functional option for GrpcTransport.
type TransportOption func(*GrpcTransport)

// WithTimeout sets the timeout for RPCs.
func WithTimeout(d time.Duration) TransportOption {
	return func(t *GrpcTransport) {
		t.timeout = d
	}
}

// WithPoolSize sets the number of connections per peer.
func WithPoolSize(n int) TransportOption {
	return func(t *GrpcTransport) {
		if n > 0 {
			t.poolSize = n
		}
	}
}

// NewGrpcTransport creates a new GrpcTransport and registers it with the given gRPC server.
// The user is responsible for starting and serving the gRPC server.
func NewGrpcTransport(localAddr string, server *grpc.Server, opts ...TransportOption) (*GrpcTransport, error) {
	t := &GrpcTransport{
		localAddr:  raft.ServerAddress(localAddr),
		consumerCh: make(chan raft.RPC),
		peers:      make(map[raft.ServerAddress]*pool.Pool),
		timeout:    10 * time.Second, // Default timeout
		poolSize:   3,                // Default pool size
	}

	for _, opt := range opts {
		opt(t)
	}

	// Register the server implementation
	srv := &grpcServer{trans: t}
	raftv1.RegisterRaftTransportServiceServer(server, srv)

	return t, nil
}

// Consumer returns a channel that can be used to consume upcoming RPCs.
func (t *GrpcTransport) Consumer() <-chan raft.RPC {
	return t.consumerCh
}

// LocalAddr returns the local address of the transport.
func (t *GrpcTransport) LocalAddr() raft.ServerAddress {
	return t.localAddr
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (t *GrpcTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	conn, err := t.getPeer(target)
	if err != nil {
		return nil, err
	}
	client := raftv1.NewRaftTransportServiceClient(conn)

	stream, err := client.AppendEntriesPipeline(context.Background())
	if err != nil {
		return nil, err
	}

	pipeline := &grpcPipeline{
		stream:     stream,
		inflight:   make(chan *appendFuture, 64),
		readyCh:    make(chan raft.AppendFuture, 64),
		shutdownCh: make(chan struct{}),
	}

	go pipeline.recvLoop()

	return pipeline, nil
}

// grpcPipeline implements raft.AppendPipeline
type grpcPipeline struct {
	stream     raftv1.RaftTransportService_AppendEntriesPipelineClient
	inflight   chan *appendFuture
	readyCh    chan raft.AppendFuture
	shutdownCh chan struct{}
	mu         sync.Mutex
	closed     bool
}

func (p *grpcPipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, raft.ErrPipelineShutdown
	}

	req := encodeAppendEntriesPipelineRequest(args)
	if err := p.stream.Send(req); err != nil {
		return nil, err
	}

	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
		done:  make(chan struct{}),
	}

	select {
	case p.inflight <- future:
		return future, nil
	case <-p.shutdownCh:
		return nil, raft.ErrPipelineShutdown
	}
}

func (p *grpcPipeline) Consumer() <-chan raft.AppendFuture {
	return p.readyCh
}

func (p *grpcPipeline) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true
	close(p.shutdownCh)
	return p.stream.CloseSend()
}

func (p *grpcPipeline) recvLoop() {
	defer close(p.readyCh)

	for {
		resp, err := p.stream.Recv()
		if err != nil {
			// Close all inflight futures and error them
			// as this is the most we can do
			for future := range p.inflight {
				future.err = err
				close(future.done)
				p.readyCh <- future
			}
			return
		}

		select {
		case future := <-p.inflight:
			decodeAppendEntriesPipelineResponse(resp, future.resp)
			future.err = nil
			close(future.done)
			p.readyCh <- future
		case <-p.shutdownCh:
			return
		}
	}
}

type appendFuture struct {
	start time.Time
	args  *raft.AppendEntriesRequest
	resp  *raft.AppendEntriesResponse
	err   error
	done  chan struct{}
}

func (f *appendFuture) Error() error {
	<-f.done
	return f.err
}

func (f *appendFuture) Start() time.Time {
	return f.start
}

func (f *appendFuture) Request() *raft.AppendEntriesRequest {
	return f.args
}

func (f *appendFuture) Response() *raft.AppendEntriesResponse {
	return f.resp
}

// ... existing AppendEntries etc ...

// ...

// Server Side Implementation (grpcServer)

// ...

// AppendEntries sends an AppendEntries RPC to the given target.
func (t *GrpcTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	conn, err := t.getPeer(target)
	if err != nil {
		return err
	}
	client := raftv1.NewRaftTransportServiceClient(conn)

	req := encodeAppendEntriesRequest(args)

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	rpcResp, err := client.AppendEntries(ctx, req)
	if err != nil {
		return err
	}

	decodeAppendEntriesResponse(rpcResp, resp)
	return nil
}

// RequestVote sends a RequestVote RPC to the given target.
func (t *GrpcTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	conn, err := t.getPeer(target)
	if err != nil {
		return err
	}
	client := raftv1.NewRaftTransportServiceClient(conn)

	req := encodeRequestVoteRequest(args)

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	rpcResp, err := client.RequestVote(ctx, req)
	if err != nil {
		return err
	}

	decodeRequestVoteResponse(rpcResp, resp)
	return nil
}

// RequestPreVote sends a RequestPreVote RPC to the given target.
func (t *GrpcTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	conn, err := t.getPeer(target)
	if err != nil {
		return err
	}
	client := raftv1.NewRaftTransportServiceClient(conn)

	req := encodeRequestPreVoteRequest(args)

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	rpcResp, err := client.RequestPreVote(ctx, req)
	if err != nil {
		return err
	}

	decodeRequestPreVoteResponse(rpcResp, resp)
	return nil
}

// InstallSnapshot sends an InstallSnapshot RPC to the given target.
func (t *GrpcTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	conn, err := t.getPeer(target)
	if err != nil {
		return err
	}
	client := raftv1.NewRaftTransportServiceClient(conn)

	stream, err := client.InstallSnapshot(context.Background())
	if err != nil {
		return err
	}

	// First send the arguments
	req := encodeInstallSnapshotRequest(args)
	if err := stream.Send(req); err != nil {
		return err
	}

	// Then stream the data
	buf := make([]byte, 32*1024)
	for {
		n, err := data.Read(buf)
		if n > 0 {
			// Reuse the request structure but only populate payload
			chunk := &raftv1.InstallSnapshotRequest{
				Data: buf[:n],
			}
			if err := stream.Send(chunk); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	rpcResp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	decodeInstallSnapshotResponse(rpcResp, resp)
	return nil
}

// EncodePeer is used to serialize a peer's address.
func (t *GrpcTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

// DecodePeer is used to deserialize a peer's address.
func (t *GrpcTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return raft.ServerAddress(buf)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is optional.
func (t *GrpcTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	// Optional, not implemented
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (t *GrpcTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	conn, err := t.getPeer(target)
	if err != nil {
		return err
	}
	client := raftv1.NewRaftTransportServiceClient(conn)

	req := encodeTimeoutNowRequest(args)

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	rpcResp, err := client.TimeoutNow(ctx, req)
	if err != nil {
		return err
	}

	decodeTimeoutNowResponse(rpcResp, resp)
	return nil
}

// Close closes the transport connections to peers. The gRPC server and listener are NOT closed.
func (t *GrpcTransport) Close() error {
	t.peersMutex.Lock()
	defer t.peersMutex.Unlock()

	for _, p := range t.peers {
		p.Close()
	}
	t.peers = make(map[raft.ServerAddress]*pool.Pool)

	return nil
}

// Server Side Implementation (grpcServer)

func (s *grpcServer) AppendEntries(ctx context.Context, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	args := decodeAppendEntriesRequest(req)

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  args,
		RespChan: respChan,
	}

	s.trans.consumerCh <- rpc

	resp := <-respChan
	if resp.Error != nil {
		return nil, resp.Error
	}

	return encodeAppendEntriesResponse(resp.Response.(*raft.AppendEntriesResponse)), nil
}

func (s *grpcServer) RequestVote(ctx context.Context, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	args := decodeRequestVoteRequest(req)

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  args,
		RespChan: respChan,
	}

	s.trans.consumerCh <- rpc

	resp := <-respChan
	if resp.Error != nil {
		return nil, resp.Error
	}

	return encodeRequestVoteResponse(resp.Response.(*raft.RequestVoteResponse)), nil
}

func (s *grpcServer) RequestPreVote(ctx context.Context, req *raftv1.RequestPreVoteRequest) (*raftv1.RequestPreVoteResponse, error) {
	args := decodeRequestPreVoteRequest(req)

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  args,
		RespChan: respChan,
	}

	s.trans.consumerCh <- rpc

	resp := <-respChan
	if resp.Error != nil {
		return nil, resp.Error
	}

	return encodeRequestPreVoteResponse(resp.Response.(*raft.RequestPreVoteResponse)), nil
}

func (s *grpcServer) InstallSnapshot(stream raftv1.RaftTransportService_InstallSnapshotServer) error {
	// Read first message to get args
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	args := decodeInstallSnapshotRequest(req)

	// Create a reader for the remaining stream data
	reader := &snapshotReader{stream: stream}

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  args,
		Reader:   reader,
		RespChan: respChan,
	}

	s.trans.consumerCh <- rpc

	resp := <-respChan
	if resp.Error != nil {
		return resp.Error
	}

	return stream.SendAndClose(encodeInstallSnapshotResponse(resp.Response.(*raft.InstallSnapshotResponse)))
}

func (s *grpcServer) TimeoutNow(ctx context.Context, req *raftv1.TimeoutNowRequest) (*raftv1.TimeoutNowResponse, error) {
	args := decodeTimeoutNowRequest(req)

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  args,
		RespChan: respChan,
	}

	s.trans.consumerCh <- rpc

	resp := <-respChan
	if resp.Error != nil {
		return nil, resp.Error
	}

	return encodeTimeoutNowResponse(resp.Response.(*raft.TimeoutNowResponse)), nil
}

func (s *grpcServer) AppendEntriesPipeline(stream raftv1.RaftTransportService_AppendEntriesPipelineServer) error {
	// Channel to ensure responses are sent in order
	// We use a channel of channels to coordinate
	sendCh := make(chan chan raft.RPCResponse, 64)

	// Sender goroutine
	errCh := make(chan error, 1)
	go func() {
		for respChan := range sendCh {
			select {
			case resp := <-respChan:
				if resp.Error != nil {
					errCh <- resp.Error
					return
				}
				rpcResp := encodeAppendEntriesPipelineResponse(resp.Response.(*raft.AppendEntriesResponse))
				if err := stream.Send(rpcResp); err != nil {
					errCh <- err
					return
				}
			case <-stream.Context().Done():
				return
			}
		}
	}()

	defer close(sendCh)

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		args := decodeAppendEntriesPipelineRequest(req)
		respChan := make(chan raft.RPCResponse, 1) // Buffered to avoid blocking raft when sending responses

		// Dispatch
		select {
		case s.trans.consumerCh <- raft.RPC{Command: args, RespChan: respChan}:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}

		// Enqueue response waiter
		select {
		case sendCh <- respChan:
		case err := <-errCh:
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// Helpers

func (t *GrpcTransport) getPeer(addr raft.ServerAddress) (grpc.ClientConnInterface, error) {
	t.peersMutex.RLock()
	p, ok := t.peers[addr]
	t.peersMutex.RUnlock()
	if ok {
		return p, nil
	}

	t.peersMutex.Lock()
	defer t.peersMutex.Unlock()

	// Double check
	if p, ok := t.peers[addr]; ok {
		return p, nil
	}

	p, err := pool.New(string(addr), t.poolSize)
	if err != nil {
		return nil, err
	}

	t.peers[addr] = p
	return p, nil
}

type snapshotReader struct {
	stream raftv1.RaftTransportService_InstallSnapshotServer
	buf    []byte
}

func (s *snapshotReader) Read(p []byte) (n int, err error) {
	if len(s.buf) > 0 {
		n = copy(p, s.buf)
		s.buf = s.buf[n:]
		return n, nil
	}

	req, err := s.stream.Recv()
	if err != nil {
		return 0, err
	}

	if len(req.Data) == 0 {
		return 0, io.EOF
	}

	s.buf = req.Data
	n = copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

// Converters

func encodeRPCHeader(h raft.RPCHeader) *raftv1.RPCHeader {
	return &raftv1.RPCHeader{
		ProtocolVersion: int64(h.ProtocolVersion),
		Id:              h.ID,
		Addr:            h.Addr,
	}
}

func decodeRPCHeader(h *raftv1.RPCHeader) raft.RPCHeader {
	if h == nil {
		return raft.RPCHeader{}
	}
	return raft.RPCHeader{
		ProtocolVersion: raft.ProtocolVersion(h.ProtocolVersion),
		ID:              h.Id,
		Addr:            h.Addr,
	}
}

func encodeLog(l *raft.Log) *raftv1.Log {
	return &raftv1.Log{
		Index:      l.Index,
		Term:       l.Term,
		Type:       uint32(l.Type),
		Data:       l.Data,
		Extensions: l.Extensions,
	}
}

func decodeLog(l *raftv1.Log) *raft.Log {
	return &raft.Log{
		Index:      l.Index,
		Term:       l.Term,
		Type:       raft.LogType(l.Type),
		Data:       l.Data,
		Extensions: l.Extensions,
	}
}

func encodeAppendEntriesRequest(r *raft.AppendEntriesRequest) *raftv1.AppendEntriesRequest {
	logs := make([]*raftv1.Log, len(r.Entries))
	for i, l := range r.Entries {
		logs[i] = encodeLog(l)
	}
	return &raftv1.AppendEntriesRequest{
		Header: encodeRPCHeader(r.RPCHeader),
		Term:   r.Term,
		// Leader:            r.Leader, // Deprecated
		PrevLogEntry:      r.PrevLogEntry,
		PrevLogTerm:       r.PrevLogTerm,
		Entries:           logs,
		LeaderCommitIndex: r.LeaderCommitIndex,
	}
}

func decodeAppendEntriesRequest(r *raftv1.AppendEntriesRequest) *raft.AppendEntriesRequest {
	logs := make([]*raft.Log, len(r.Entries))
	for i, l := range r.Entries {
		logs[i] = decodeLog(l)
	}
	return &raft.AppendEntriesRequest{
		RPCHeader:         decodeRPCHeader(r.Header),
		Term:              r.Term,
		PrevLogEntry:      r.PrevLogEntry,
		PrevLogTerm:       r.PrevLogTerm,
		Entries:           logs,
		LeaderCommitIndex: r.LeaderCommitIndex,
	}
}

func encodeAppendEntriesResponse(r *raft.AppendEntriesResponse) *raftv1.AppendEntriesResponse {
	return &raftv1.AppendEntriesResponse{
		Header:         encodeRPCHeader(r.RPCHeader),
		Term:           r.Term,
		LastLog:        r.LastLog,
		Success:        r.Success,
		NoRetryBackoff: r.NoRetryBackoff,
	}
}

func decodeAppendEntriesResponse(src *raftv1.AppendEntriesResponse, dst *raft.AppendEntriesResponse) {
	dst.RPCHeader = decodeRPCHeader(src.Header)
	dst.Term = src.Term
	dst.LastLog = src.LastLog
	dst.Success = src.Success
	dst.NoRetryBackoff = src.NoRetryBackoff
}

func encodeAppendEntriesPipelineRequest(r *raft.AppendEntriesRequest) *raftv1.AppendEntriesPipelineRequest {
	logs := make([]*raftv1.Log, len(r.Entries))
	for i, l := range r.Entries {
		logs[i] = encodeLog(l)
	}
	return &raftv1.AppendEntriesPipelineRequest{
		Header:            encodeRPCHeader(r.RPCHeader),
		Term:              r.Term,
		PrevLogEntry:      r.PrevLogEntry,
		PrevLogTerm:       r.PrevLogTerm,
		Entries:           logs,
		LeaderCommitIndex: r.LeaderCommitIndex,
	}
}

func decodeAppendEntriesPipelineRequest(r *raftv1.AppendEntriesPipelineRequest) *raft.AppendEntriesRequest {
	logs := make([]*raft.Log, len(r.Entries))
	for i, l := range r.Entries {
		logs[i] = decodeLog(l)
	}
	return &raft.AppendEntriesRequest{
		RPCHeader:         decodeRPCHeader(r.Header),
		Term:              r.Term,
		PrevLogEntry:      r.PrevLogEntry,
		PrevLogTerm:       r.PrevLogTerm,
		Entries:           logs,
		LeaderCommitIndex: r.LeaderCommitIndex,
	}
}

func encodeAppendEntriesPipelineResponse(r *raft.AppendEntriesResponse) *raftv1.AppendEntriesPipelineResponse {
	return &raftv1.AppendEntriesPipelineResponse{
		Header:         encodeRPCHeader(r.RPCHeader),
		Term:           r.Term,
		LastLog:        r.LastLog,
		Success:        r.Success,
		NoRetryBackoff: r.NoRetryBackoff,
	}
}

func decodeAppendEntriesPipelineResponse(src *raftv1.AppendEntriesPipelineResponse, dst *raft.AppendEntriesResponse) {
	dst.RPCHeader = decodeRPCHeader(src.Header)
	dst.Term = src.Term
	dst.LastLog = src.LastLog
	dst.Success = src.Success
	dst.NoRetryBackoff = src.NoRetryBackoff
}

func encodeRequestVoteRequest(r *raft.RequestVoteRequest) *raftv1.RequestVoteRequest {
	return &raftv1.RequestVoteRequest{
		Header:             encodeRPCHeader(r.RPCHeader),
		Term:               r.Term,
		LastLogIndex:       r.LastLogIndex,
		LastLogTerm:        r.LastLogTerm,
		LeadershipTransfer: r.LeadershipTransfer,
	}
}

func decodeRequestVoteRequest(r *raftv1.RequestVoteRequest) *raft.RequestVoteRequest {
	return &raft.RequestVoteRequest{
		RPCHeader:          decodeRPCHeader(r.Header),
		Term:               r.Term,
		LastLogIndex:       r.LastLogIndex,
		LastLogTerm:        r.LastLogTerm,
		LeadershipTransfer: r.LeadershipTransfer,
	}
}

func encodeRequestVoteResponse(r *raft.RequestVoteResponse) *raftv1.RequestVoteResponse {
	return &raftv1.RequestVoteResponse{
		Header:  encodeRPCHeader(r.RPCHeader),
		Term:    r.Term,
		Peers:   r.Peers,
		Granted: r.Granted,
	}
}

func decodeRequestVoteResponse(src *raftv1.RequestVoteResponse, dst *raft.RequestVoteResponse) {
	dst.RPCHeader = decodeRPCHeader(src.Header)
	dst.Term = src.Term
	dst.Peers = src.Peers
	dst.Granted = src.Granted
}

func encodeRequestPreVoteRequest(r *raft.RequestPreVoteRequest) *raftv1.RequestPreVoteRequest {
	return &raftv1.RequestPreVoteRequest{
		Header:       encodeRPCHeader(r.RPCHeader),
		Term:         r.Term,
		LastLogIndex: r.LastLogIndex,
		LastLogTerm:  r.LastLogTerm,
	}
}

func decodeRequestPreVoteRequest(r *raftv1.RequestPreVoteRequest) *raft.RequestPreVoteRequest {
	return &raft.RequestPreVoteRequest{
		RPCHeader:    decodeRPCHeader(r.Header),
		Term:         r.Term,
		LastLogIndex: r.LastLogIndex,
		LastLogTerm:  r.LastLogTerm,
	}
}

func encodeRequestPreVoteResponse(r *raft.RequestPreVoteResponse) *raftv1.RequestPreVoteResponse {
	return &raftv1.RequestPreVoteResponse{
		Header:  encodeRPCHeader(r.RPCHeader),
		Term:    r.Term,
		Granted: r.Granted,
	}
}

func decodeRequestPreVoteResponse(src *raftv1.RequestPreVoteResponse, dst *raft.RequestPreVoteResponse) {
	dst.RPCHeader = decodeRPCHeader(src.Header)
	dst.Term = src.Term
	dst.Granted = src.Granted
}

func encodeInstallSnapshotRequest(r *raft.InstallSnapshotRequest) *raftv1.InstallSnapshotRequest {
	return &raftv1.InstallSnapshotRequest{
		Header:             encodeRPCHeader(r.RPCHeader),
		Term:               r.Term,
		LastLogIndex:       r.LastLogIndex,
		LastLogTerm:        r.LastLogTerm,
		Peers:              r.Peers,
		Configuration:      r.Configuration,
		ConfigurationIndex: r.ConfigurationIndex,
		Size:               r.Size,
	}
}

func decodeInstallSnapshotRequest(r *raftv1.InstallSnapshotRequest) *raft.InstallSnapshotRequest {
	return &raft.InstallSnapshotRequest{
		RPCHeader:          decodeRPCHeader(r.Header),
		Term:               r.Term,
		LastLogIndex:       r.LastLogIndex,
		LastLogTerm:        r.LastLogTerm,
		Peers:              r.Peers,
		Configuration:      r.Configuration,
		ConfigurationIndex: r.ConfigurationIndex,
		Size:               r.Size,
	}
}

func encodeInstallSnapshotResponse(r *raft.InstallSnapshotResponse) *raftv1.InstallSnapshotResponse {
	return &raftv1.InstallSnapshotResponse{
		Header:  encodeRPCHeader(r.RPCHeader),
		Term:    r.Term,
		Success: r.Success,
	}
}

func decodeInstallSnapshotResponse(src *raftv1.InstallSnapshotResponse, dst *raft.InstallSnapshotResponse) {
	dst.RPCHeader = decodeRPCHeader(src.Header)
	dst.Term = src.Term
	dst.Success = src.Success
}

func encodeTimeoutNowRequest(r *raft.TimeoutNowRequest) *raftv1.TimeoutNowRequest {
	return &raftv1.TimeoutNowRequest{
		Header: encodeRPCHeader(r.RPCHeader),
	}
}

func decodeTimeoutNowRequest(r *raftv1.TimeoutNowRequest) *raft.TimeoutNowRequest {
	return &raft.TimeoutNowRequest{
		RPCHeader: decodeRPCHeader(r.Header),
	}
}

func encodeTimeoutNowResponse(r *raft.TimeoutNowResponse) *raftv1.TimeoutNowResponse {
	return &raftv1.TimeoutNowResponse{
		Header: encodeRPCHeader(r.RPCHeader),
	}
}

func decodeTimeoutNowResponse(src *raftv1.TimeoutNowResponse, dst *raft.TimeoutNowResponse) {
	dst.RPCHeader = decodeRPCHeader(src.Header)
}
