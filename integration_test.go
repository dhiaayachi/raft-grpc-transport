package raftgrpc

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestIntegration_LeaderElectionAndReplication(t *testing.T) {
	// Create a cluster of 3 nodes
	nodes := make([]*raft.Raft, 3)
	transports := make([]*GrpcTransport, 3)
	stores := make([]*raft.InmemStore, 3)
	snaps := make([]*raft.InmemSnapshotStore, 3)
	addrs := make([]string, 3)
	servers := make([]*grpc.Server, 3)
	listeners := make([]net.Listener, 3)

	// Setup configuration
	conf := raft.DefaultConfig()
	conf.HeartbeatTimeout = 100 * time.Millisecond
	conf.ElectionTimeout = 100 * time.Millisecond
	conf.LeaderLeaseTimeout = 100 * time.Millisecond
	conf.CommitTimeout = 50 * time.Millisecond

	for i := 0; i < 3; i++ {
		// Prepare listener and server
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = lis
		servers[i] = grpc.NewServer()

		// Transport
		trans, err := NewGrpcTransport(lis.Addr().String(), servers[i])
		require.NoError(t, err)
		transports[i] = trans
		addrs[i] = lis.Addr().String()

		// Start server
		go servers[i].Serve(lis)

		// Helper to properly close
		defer servers[i].Stop()
		defer lis.Close()
		defer trans.Close()

		// Stores
		stores[i] = raft.NewInmemStore()
		snaps[i] = raft.NewInmemSnapshotStore()

		// Generic FSM
		fsm := &testFSM{}

		// Config with unique ID
		nodeConf := *conf
		nodeConf.LocalID = raft.ServerID(fmt.Sprintf("node-%d", i))

		nodes[i], err = raft.NewRaft(
			&nodeConf,
			fsm,
			stores[i],
			stores[i],
			snaps[i],
			transports[i],
		)
		require.NoError(t, err)
		defer nodes[i].Shutdown()
	}

	// Bootstrap the cluster configuration
	var configuration raft.Configuration
	for i := 0; i < 3; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("node-%d", i)),
			Address: raft.ServerAddress(addrs[i]),
		})
	}

	// Bootstrap on the first node (or any, but usually one initiates)
	// But `BootstrapCluster` expects a clean state.
	future := nodes[0].BootstrapCluster(configuration)
	require.NoError(t, future.Error())

	// Wait for leader
	t.Log("Waiting for leader...")
	leaderID, err := waitForLeader(nodes, 10*time.Second)
	require.NoError(t, err)
	t.Logf("Leader elected: %s", leaderID)

	// Apply a log
	t.Log("Applying log...")
	leaderNode := getNode(nodes, leaderID)
	require.NotNil(t, leaderNode)

	applyFuture := leaderNode.Apply([]byte("test-data"), 5*time.Second)
	require.NoError(t, applyFuture.Error())

	// Verify replication
	t.Log("Verifying replication...")
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			// simplistic check: ensure index is > 0 and FSM applied
			// (Assuming FSM applies are instant and queryable,
			// but generic FSM helper might be needed)
			// Actually we can check AppliedIndex
			if node.AppliedIndex() < applyFuture.Index() {
				return false
			}
		}
		return true
	}, 5*time.Second, 100*time.Millisecond)
}

func waitForLeader(nodes []*raft.Raft, timeout time.Duration) (raft.ServerID, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, node := range nodes {
			if node.State() == raft.Leader {
				return raft.ServerID(fmt.Sprintf("node-%d", i)), nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("timeout waiting for leader")
}

func getNode(nodes []*raft.Raft, id raft.ServerID) *raft.Raft {
	for i, node := range nodes {
		if raft.ServerID(fmt.Sprintf("node-%d", i)) == id {
			return node
		}
	}
	return nil
}

type testFSM struct{}

func (f *testFSM) Apply(l *raft.Log) interface{} {
	return nil
}

func (f *testFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &testSnapshot{}, nil
}

func (f *testFSM) Restore(rc io.ReadCloser) error {
	return nil
}

type testSnapshot struct{}

func (s *testSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *testSnapshot) Release() {}
