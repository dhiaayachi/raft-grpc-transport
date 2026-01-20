//go:build integration

package raftgrpc

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestIntegration_Migration_PrimaryUp(t *testing.T) {
	runMigrationTest(t, true)
}

func TestIntegration_Migration_PrimaryDown(t *testing.T) {
	runMigrationTest(t, false)
}

func runMigrationTest(t *testing.T, primaryUp bool) {
	// Create a cluster of 3 nodes
	count := 3
	nodes := make([]*raft.Raft, count)

	// Transports
	grpcTransports := make([]*GrpcTransport, count)
	inmemTransports := make([]raft.LoopbackTransport, count) // Use Loopback interface for Inmem
	migTransports := make([]*MigrationTransport, count)

	stores := make([]*raft.InmemStore, count)
	snaps := make([]*raft.InmemSnapshotStore, count)
	addrs := make([]string, count)
	servers := make([]*grpc.Server, count)
	listeners := make([]net.Listener, count)

	// Setup configuration
	conf := raft.DefaultConfig()
	conf.HeartbeatTimeout = 100 * time.Millisecond
	conf.ElectionTimeout = 100 * time.Millisecond
	conf.LeaderLeaseTimeout = 100 * time.Millisecond
	conf.CommitTimeout = 50 * time.Millisecond

	// 1. Prepare listeners and addresses first
	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = lis
		addrs[i] = lis.Addr().String()
	}

	for i := 0; i < count; i++ {
		// Prepare gRPC server (Primary)
		servers[i] = grpc.NewServer()
		trans, err := NewGrpcTransport(addrs[i], servers[i])
		require.NoError(t, err)
		grpcTransports[i] = trans

		// Prepare InmemTransport (Secondary)
		// We use NewInmemTransportWithAddr to match the gRPC address
		_, inmem := raft.NewInmemTransport(raft.ServerAddress(addrs[i]))
		inmemTransports[i] = inmem

		// Helper to close
		defer func(idx int) {
			servers[idx].Stop()
			listeners[idx].Close()
			grpcTransports[idx].Close()
			inmemTransports[idx].Close()
			// nodes[idx].Shutdown() // Shutdown in loop below
		}(i)

		// Create MigrationTransport
		shouldFallback := func(err error) bool {
			// Always fallback on error for this test
			return true
		}
		migTransports[i] = NewMigrationTransport(grpcTransports[i], inmemTransports[i], shouldFallback)
	}

	// 2. Wire InmemTransports together
	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if i != j {
				inmemTransports[i].Connect(raft.ServerAddress(addrs[j]), inmemTransports[j])
			}
		}
	}

	// 3. Start Servers (only if Primary Up)
	for i := 0; i < count; i++ {
		if primaryUp {
			go servers[i].Serve(listeners[i])
		} else {
			// If Primary Down, we close the listener immediately so Dial fails
			listeners[i].Close()
		}
	}

	// 4. Create and start Raft nodes
	for i := 0; i < count; i++ {
		stores[i] = raft.NewInmemStore()
		snaps[i] = raft.NewInmemSnapshotStore()
		fsm := &testFSM{}

		nodeConf := *conf
		nodeConf.LocalID = raft.ServerID(fmt.Sprintf("node-%d", i))
		// Disable PreVote as it might cause issues if transport isn't fully ready?
		// Actually GrpcTransport doesn't support PreVote?
		// "raft: pre-vote is disabled because it is not supported by the Transport"
		// This warning is expected.

		// Bootstrap configuration?
		// We can bootstrap explicitly later.

		var err error
		nodes[i], err = raft.NewRaft(
			&nodeConf,
			fsm,
			stores[i],
			stores[i],
			snaps[i],
			migTransports[i],
		)
		require.NoError(t, err)
	}

	// Shutdown nodes at end
	defer func() {
		for _, n := range nodes {
			n.Shutdown()
		}
	}()

	// 5. Bootstrap Cluster
	var configuration raft.Configuration
	for i := 0; i < count; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("node-%d", i)),
			Address: raft.ServerAddress(addrs[i]),
		})
	}
	future := nodes[0].BootstrapCluster(configuration)
	require.NoError(t, future.Error())

	// 6. Verify Leader Election
	t.Log("Waiting for leader...")
	leaderID, err := waitForLeader(nodes, 10*time.Second)
	require.NoError(t, err)
	t.Logf("Leader elected: %s", leaderID)

	// 7. Verify Replication
	t.Log("Applying log...")
	leaderNode := getNode(nodes, leaderID)
	require.NotNil(t, leaderNode)

	applyFuture := leaderNode.Apply([]byte("test-data"), 5*time.Second)
	require.NoError(t, applyFuture.Error())

	t.Log("Verifying replication...")
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if node.AppliedIndex() < applyFuture.Index() {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)
}
