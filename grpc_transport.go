package raftgrpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	raftv1 "github.com/dhiaayachi/raft-grpc-transport/gen/go/proto/raft/v1"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GrpcTransport implements raft.Transport using gRPC.
type GrpcTransport struct {
	// localAddr is the address of the local node.
	localAddr raft.ServerAddress

	// ip is the public IP of the local node.
	ip string

	// consumerCh is the channel to consume RPC requests.
	consumerCh chan raft.RPC

	// peers is a map of peer address to gRPC client connection.
	peers      map[raft.ServerAddress]*grpc.ClientConn
	peersMutex sync.RWMutex
}

// grpcServer implements the generated RaftTransportServer interface.
type grpcServer struct {
	raftv1.UnimplementedRaftTransportServer
	trans *GrpcTransport
}

// NewGrpcTransport creates a new GrpcTransport and registers it with the given gRPC server.
// The user is responsible for starting and serving the gRPC server.
func NewGrpcTransport(localAddr string, server *grpc.Server) (*GrpcTransport, error) {
	t := &GrpcTransport{
		localAddr:  raft.ServerAddress(localAddr),
		ip:         localAddr, // Storing raw address for now, might need resolution
		consumerCh: make(chan raft.RPC),
		peers:      make(map[raft.ServerAddress]*grpc.ClientConn),
	}

	// Register the server implementation
	srv := &grpcServer{trans: t}
	raftv1.RegisterRaftTransportServer(server, srv)

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
	// Pipelining not yet implemented.
	return nil, fmt.Errorf("pipeline replication not supported")
}

// AppendEntries sends an AppendEntries RPC to the given target.
func (t *GrpcTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	conn, err := t.getPeer(target)
	if err != nil {
		return err
	}
	client := raftv1.NewRaftTransportClient(conn)

	// Encode args
	payload, err := encode(args)
	if err != nil {
		return err
	}

	req := &raftv1.AppendEntriesRequest{
		Payload: payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // TODO: Make configurable
	defer cancel()

	rpcResp, err := client.AppendEntries(ctx, req)
	if err != nil {
		return err
	}

	// Decode resp
	if err := decode(rpcResp.Payload, resp); err != nil {
		return err
	}

	return nil
}

// RequestVote sends a RequestVote RPC to the given target.
func (t *GrpcTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	conn, err := t.getPeer(target)
	if err != nil {
		return err
	}
	client := raftv1.NewRaftTransportClient(conn)

	payload, err := encode(args)
	if err != nil {
		return err
	}

	req := &raftv1.RequestVoteRequest{
		Payload: payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rpcResp, err := client.RequestVote(ctx, req)
	if err != nil {
		return err
	}

	if err := decode(rpcResp.Payload, resp); err != nil {
		return err
	}

	return nil
}

// InstallSnapshot sends an InstallSnapshot RPC to the given target.
func (t *GrpcTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	conn, err := t.getPeer(target)
	if err != nil {
		return err
	}
	client := raftv1.NewRaftTransportClient(conn)

	stream, err := client.InstallSnapshot(context.Background())
	if err != nil {
		return err
	}

	// First send the arguments
	argsPayload, err := encode(args)
	if err != nil {
		return err
	}
	if err := stream.Send(&raftv1.InstallSnapshotRequest{Payload: argsPayload}); err != nil {
		return err
	}

	// Then stream the data
	buf := make([]byte, 32*1024)
	for {
		n, err := data.Read(buf)
		if n > 0 {
			if err := stream.Send(&raftv1.InstallSnapshotRequest{Payload: buf[:n]}); err != nil {
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

	if err := decode(rpcResp.Payload, resp); err != nil {
		return err
	}

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
	client := raftv1.NewRaftTransportClient(conn)

	payload, err := encode(args)
	if err != nil {
		return err
	}

	req := &raftv1.TimeoutNowRequest{
		Payload: payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rpcResp, err := client.TimeoutNow(ctx, req)
	if err != nil {
		return err
	}

	if err := decode(rpcResp.Payload, resp); err != nil {
		return err
	}

	return nil
}

// Close closes the transport connections to peers. The gRPC server and listener are NOT closed.
func (t *GrpcTransport) Close() error {
	t.peersMutex.Lock()
	defer t.peersMutex.Unlock()

	for _, conn := range t.peers {
		conn.Close()
	}
	t.peers = make(map[raft.ServerAddress]*grpc.ClientConn)

	return nil
}

// Server Side Implementation (grpcServer)

func (s *grpcServer) AppendEntries(ctx context.Context, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	var args raft.AppendEntriesRequest
	if err := decode(req.Payload, &args); err != nil {
		return nil, err
	}

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  &args,
		RespChan: respChan,
	}

	s.trans.consumerCh <- rpc

	resp := <-respChan
	if resp.Error != nil {
		return nil, resp.Error
	}

	payload, err := encode(resp.Response)
	if err != nil {
		return nil, err
	}

	return &raftv1.AppendEntriesResponse{
		Payload: payload,
	}, nil
}

func (s *grpcServer) RequestVote(ctx context.Context, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	var args raft.RequestVoteRequest
	if err := decode(req.Payload, &args); err != nil {
		return nil, err
	}

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  &args,
		RespChan: respChan,
	}

	s.trans.consumerCh <- rpc

	resp := <-respChan
	if resp.Error != nil {
		return nil, resp.Error
	}

	payload, err := encode(resp.Response)
	if err != nil {
		return nil, err
	}

	return &raftv1.RequestVoteResponse{
		Payload: payload,
	}, nil
}

func (s *grpcServer) InstallSnapshot(stream raftv1.RaftTransport_InstallSnapshotServer) error {
	// Read first message to get args
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	var args raft.InstallSnapshotRequest
	if err := decode(req.Payload, &args); err != nil {
		return err
	}

	// Create a reader for the remaining stream data
	reader := &snapshotReader{stream: stream}

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  &args,
		Reader:   reader,
		RespChan: respChan,
	}

	s.trans.consumerCh <- rpc

	resp := <-respChan
	if resp.Error != nil {
		return resp.Error
	}

	payload, err := encode(resp.Response)
	if err != nil {
		return err
	}

	return stream.SendAndClose(&raftv1.InstallSnapshotResponse{
		Payload: payload,
	})
}

func (s *grpcServer) TimeoutNow(ctx context.Context, req *raftv1.TimeoutNowRequest) (*raftv1.TimeoutNowResponse, error) {
	var args raft.TimeoutNowRequest
	if err := decode(req.Payload, &args); err != nil {
		return nil, err
	}

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  &args,
		RespChan: respChan,
	}

	s.trans.consumerCh <- rpc

	resp := <-respChan
	if resp.Error != nil {
		return nil, resp.Error
	}

	payload, err := encode(resp.Response)
	if err != nil {
		return nil, err
	}

	return &raftv1.TimeoutNowResponse{
		Payload: payload,
	}, nil
}

// Helpers

func (t *GrpcTransport) getPeer(addr raft.ServerAddress) (*grpc.ClientConn, error) {
	t.peersMutex.RLock()
	conn, ok := t.peers[addr]
	t.peersMutex.RUnlock()
	if ok {
		return conn, nil
	}

	t.peersMutex.Lock()
	defer t.peersMutex.Unlock()

	// Double check
	if conn, ok := t.peers[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(string(addr), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	t.peers[addr] = conn
	return conn, nil
}

type snapshotReader struct {
	stream raftv1.RaftTransport_InstallSnapshotServer
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

	s.buf = req.Payload
	n = copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

// encode serializes the value using msgpack.
func encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decode deserializes the value using msgpack.
func decode(data []byte, v interface{}) error {
	dec := codec.NewDecoderBytes(data, &codec.MsgpackHandle{})
	return dec.Decode(v)
}
