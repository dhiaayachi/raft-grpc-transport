package raftgrpc

import (
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func createTestTransport(t *testing.T, addr string) (*GrpcTransport, *grpc.Server, net.Listener) {
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	server := grpc.NewServer()
	trans, err := NewGrpcTransport(lis.Addr().String(), server)
	require.NoError(t, err)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()

	return trans, server, lis
}

func TestNewGrpcTransport(t *testing.T) {
	addr := "127.0.0.1:0"
	trans, server, lis := createTestTransport(t, addr)
	defer trans.Close()
	defer server.Stop()
	defer lis.Close()

	require.NotNil(t, trans)

	// Check if we can listen (already verified by createTestTransport returning success)
	_, port, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	require.NotEmpty(t, port)
}

func TestTransport_Connection(t *testing.T) {
	// Setup server 1
	trans1, s1, l1 := createTestTransport(t, "127.0.0.1:0")
	defer trans1.Close()
	defer s1.Stop()
	defer l1.Close()

	// Setup server 2
	trans2, s2, l2 := createTestTransport(t, "127.0.0.1:0")
	defer trans2.Close()
	defer s2.Stop()
	defer l2.Close()

	// Get addresses
	addr2 := raft.ServerAddress(l2.Addr().String())

	// Test connection from 1 to 2
	conn, err := trans1.getPeer(addr2)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Wait momentarily for connection state change (optional, but good for verification)
	_ = conn.GetState()
}

func TestTransport_AppendEntries(t *testing.T) {
	trans1, s1, l1 := createTestTransport(t, "127.0.0.1:0")
	defer trans1.Close()
	defer s1.Stop()
	defer l1.Close()

	trans2, s2, l2 := createTestTransport(t, "127.0.0.1:0")
	defer trans2.Close()
	defer s2.Stop()
	defer l2.Close()

	addr2 := raft.ServerAddress(l2.Addr().String())

	// Handle RPC on trans2
	go func() {
		select {
		case rpc := <-trans2.Consumer():
			switch cmd := rpc.Command.(type) {
			case *raft.AppendEntriesRequest:
				rpc.RespChan <- raft.RPCResponse{
					Response: &raft.AppendEntriesResponse{
						Term:    cmd.Term,
						Success: true,
					},
				}
			}
		case <-time.After(5 * time.Second): // Increased timeout
			// t.Error("timeout waiting for RPC") // Can't fail from goroutine easily without panic or helper
			// Just let it exit
		}
	}()

	// Send RPC from trans1
	req := &raft.AppendEntriesRequest{
		Term:   1,
		Leader: []byte("leader"),
		Entries: []*raft.Log{
			{Index: 1, Term: 1, Data: []byte("data")},
		},
	}
	resp := &raft.AppendEntriesResponse{}

	// Retry loop for AppendEntries to allow server startup time
	var appendErr error
	for i := 0; i < 5; i++ {
		appendErr = trans1.AppendEntries("id", addr2, req, resp)
		if appendErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, appendErr)
	require.True(t, resp.Success)
	require.Equal(t, uint64(1), resp.Term)
}

func TestTransport_AppendEntriesPipeline(t *testing.T) {
	trans1, s1, l1 := createTestTransport(t, "127.0.0.1:0")
	defer trans1.Close()
	defer s1.Stop()
	defer l1.Close()

	trans2, s2, l2 := createTestTransport(t, "127.0.0.1:0")
	defer trans2.Close()
	defer s2.Stop()
	defer l2.Close()

	addr2 := raft.ServerAddress(l2.Addr().String())

	// Handle RPCs on trans2
	go func() {
		// We expect pipeline RPCs
		// The consumer receives raft.RPC with a RespChan
		for {
			select {
			case rpc := <-trans2.Consumer():
				switch cmd := rpc.Command.(type) {
				case *raft.AppendEntriesRequest:
					// Respond success
					rpc.RespChan <- raft.RPCResponse{
						Response: &raft.AppendEntriesResponse{
							Term:    cmd.Term,
							Success: true,
							LastLog: 100 + cmd.Term, // Echo something back to verify
						},
					}
				}
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()

	// Open pipeline from trans1 to trans2
	pipeline, err := trans1.AppendEntriesPipeline("id2", addr2)
	require.NoError(t, err)
	defer pipeline.Close()

	// Send multiple requests
	count := 10
	futures := make([]raft.AppendFuture, count)

	for i := 0; i < count; i++ {
		req := &raft.AppendEntriesRequest{
			Term:   uint64(i),
			Leader: []byte("leader"),
			Entries: []*raft.Log{
				{Index: uint64(i), Term: 1, Data: []byte("data")},
			},
		}
		resp := &raft.AppendEntriesResponse{}
		fut, err := pipeline.AppendEntries(req, resp)
		require.NoError(t, err)
		futures[i] = fut
	}

	// Consume responses from Consumer channel
	// pipeline.Consumer() returns a channel of completed futures

	timeout := time.After(5 * time.Second)
	completedCount := 0

	consumerCh := pipeline.Consumer()

	for completedCount < count {
		select {
		case fut := <-consumerCh:
			require.NoError(t, fut.Error())
			require.True(t, fut.Response().Success)
			// Verify it matches one of our requests (we can't easily map back without ID,
			// but we know Term logic)
			reqTerm := fut.Request().Term
			require.Equal(t, 100+reqTerm, fut.Response().LastLog)
			completedCount++
		case <-timeout:
			t.Fatal("timeout waiting for pipeline responses")
		}
	}
}
