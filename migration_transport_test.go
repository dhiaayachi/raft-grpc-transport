package raftgrpc

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// MockTransport is a minimal mock for raft.Transport
type MockTransport struct {
	consumerCh chan raft.RPC
	localAddr  raft.ServerAddress

	appendEntriesErr    error
	appendEntriesCalled bool
}

func NewMockTransport(addr raft.ServerAddress) *MockTransport {
	return &MockTransport{
		consumerCh: make(chan raft.RPC),
		localAddr:  addr,
	}
}

func (m *MockTransport) Consumer() <-chan raft.RPC     { return m.consumerCh }
func (m *MockTransport) LocalAddr() raft.ServerAddress { return m.localAddr }
func (m *MockTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, nil
}
func (m *MockTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	m.appendEntriesCalled = true
	return m.appendEntriesErr
}
func (m *MockTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return nil
}
func (m *MockTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return nil
}
func (m *MockTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}
func (m *MockTransport) DecodePeer(buf []byte) raft.ServerAddress  { return raft.ServerAddress(buf) }
func (m *MockTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {}
func (m *MockTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return nil
}
func (m *MockTransport) Close() error { close(m.consumerCh); return nil }

func TestMigrationTransport_Fallback(t *testing.T) {
	primary := NewMockTransport("primary")
	secondary := NewMockTransport("secondary")

	fallbackErr := errors.New("fallback please")
	primary.appendEntriesErr = fallbackErr

	// Create migration transport that falls back only on fallbackErr
	mig := NewMigrationTransport(primary, secondary, func(err error) bool {
		return err == fallbackErr
	})
	defer mig.Close()

	// Test AppendEntries
	err := mig.AppendEntries("id", "target", &raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})

	// Should satisfy:
	// 1. Primary called (failed)
	// 2. Secondary called (success)
	// 3. Overall success (nil error)
	require.True(t, primary.appendEntriesCalled)
	require.True(t, secondary.appendEntriesCalled)
	require.NoError(t, err)
}

func TestMigrationTransport_NoFallback(t *testing.T) {
	primary := NewMockTransport("primary")
	secondary := NewMockTransport("secondary")

	otherErr := errors.New("fatal error")
	primary.appendEntriesErr = otherErr

	mig := NewMigrationTransport(primary, secondary, func(err error) bool {
		return err.Error() == "fallback please" // won't match
	})
	defer mig.Close()

	err := mig.AppendEntries("id", "target", &raft.AppendEntriesRequest{}, &raft.AppendEntriesResponse{})

	// Should satisfy:
	// 1. Primary called
	// 2. Secondary NOT called
	// 3. Error returned
	require.True(t, primary.appendEntriesCalled)
	require.False(t, secondary.appendEntriesCalled)
	require.Equal(t, otherErr, err)
}

func TestMigrationTransport_ConsumerMerge(t *testing.T) {
	primary := NewMockTransport("primary")
	secondary := NewMockTransport("secondary")

	mig := NewMigrationTransport(primary, secondary, func(err error) bool { return false })
	defer mig.Close()

	// Send to primary
	go func() {
		primary.consumerCh <- raft.RPC{Command: &raft.AppendEntriesRequest{Term: 1}}
	}()

	// Send to secondary
	go func() {
		secondary.consumerCh <- raft.RPC{Command: &raft.AppendEntriesRequest{Term: 2}}
	}()

	// Expect both
	count := 0
	timeout := time.After(1 * time.Second)

loop:
	for {
		select {
		case rpc := <-mig.Consumer():
			count++
			if cmd, ok := rpc.Command.(*raft.AppendEntriesRequest); ok {
				if cmd.Term != 1 && cmd.Term != 2 {
					t.Errorf("unexpected term: %d", cmd.Term)
				}
			}
			if count == 2 {
				break loop
			}
		case <-timeout:
			t.Fatal("timeout waiting for RPCS")
		}
	}
}
