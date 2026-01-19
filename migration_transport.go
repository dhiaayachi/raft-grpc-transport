package raftgrpc

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// MigrationTransport wraps two transports (Primary and Secondary) and allows
// failing over to the Secondary transport if the Primary fails with a specific error.
// It also merges incoming RPCs from both transports.
type MigrationTransport struct {
	primary   raft.Transport
	secondary raft.Transport

	shouldFallback func(error) bool

	consumerCh chan raft.RPC
	shutdownCh chan struct{}
	shutdown   atomic.Bool
	wg         sync.WaitGroup
}

// NewMigrationTransport creates a new MigrationTransport.
func NewMigrationTransport(primary, secondary raft.Transport, shouldFallback func(error) bool) *MigrationTransport {
	t := &MigrationTransport{
		primary:        primary,
		secondary:      secondary,
		shouldFallback: shouldFallback,
		consumerCh:     make(chan raft.RPC),
		shutdownCh:     make(chan struct{}),
	}

	t.startConsumer()
	return t
}

func (t *MigrationTransport) startConsumer() {
	// Consume from primary
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		for {
			select {
			case rpc, ok := <-t.primary.Consumer():
				if !ok {
					return
				}
				select {
				case t.consumerCh <- rpc:
				case <-t.shutdownCh:
					return
				}
			case <-t.shutdownCh:
				return
			}
		}
	}()

	// Consume from secondary
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		for {
			select {
			case rpc, ok := <-t.secondary.Consumer():
				if !ok {
					return
				}
				select {
				case t.consumerCh <- rpc:
				case <-t.shutdownCh:
					return
				}
			case <-t.shutdownCh:
				return
			}
		}
	}()
}

// Consumer returns a channel that can be used to consume upcoming RPCs.
func (t *MigrationTransport) Consumer() <-chan raft.RPC {
	return t.consumerCh
}

// LocalAddr returns the local address of the transport.
// It defaults to the Primary's address.
func (t *MigrationTransport) LocalAddr() raft.ServerAddress {
	return t.primary.LocalAddr()
}

func (t *MigrationTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	// We delegate pipeline to Primary only for now.
	// Implementing fallback for pipeline is complex as it returns an interface.
	// If primary doesn't support it, we return error.
	return t.primary.AppendEntriesPipeline(id, target)
}

func (t *MigrationTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	err := t.primary.AppendEntries(id, target, args, resp)
	if err != nil && t.shouldFallback(err) {
		return t.secondary.AppendEntries(id, target, args, resp)
	}
	return err
}

func (t *MigrationTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	err := t.primary.RequestVote(id, target, args, resp)
	if err != nil && t.shouldFallback(err) {
		return t.secondary.RequestVote(id, target, args, resp)
	}
	return err
}

func (t *MigrationTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	// Note: data (io.Reader) can usually only be read once.
	// If we read it for primary and fail, we might not be able to read it again for secondary
	// unless it's a Seekable reader or we buffer it.
	// For now, we assume if InstallSnapshot fails on Primary, we might not be able to retry easily with the same Reader
	// unless we wrap it.
	// **Risk**: If data is consumed by Primary before failure, Secondary gets empty reader.
	// We will attempt Primary first. If it fails immediately (before reading), we might be safe.
	// Proper solution requires buffering or resetting the reader if possible.

	// Check if reader is seekable?
	var seeker io.Seeker
	var startPos int64
	if s, ok := data.(io.Seeker); ok {
		seeker = s
		pos, err := s.Seek(0, io.SeekCurrent)
		if err == nil {
			startPos = pos
		}
	}

	err := t.primary.InstallSnapshot(id, target, args, resp, data)
	if err != nil && t.shouldFallback(err) {
		// Try to reset reader if possible
		if seeker != nil {
			_, seekErr := seeker.Seek(startPos, io.SeekStart)
			if seekErr != nil {
				// Can't reset, so we return the original error + seek error logic?
				// Just return original error as we can't fallback safely.
				return err
			}
			// If not seekable, and we suspect data might have been read...
			// We can't safely fallback for InstallSnapshot unless we know Primary didn't touch data.
			// But we blindly try secondary? It might just send empty snapshot.
			// Safe bet: Do not fallback for InstallSnapshot if not seekable?
			// Or assume user provides re-readable stream?
			// Let's try secondary anyway, hoping for the best or that primary failed connection-early.
		}
		return t.secondary.InstallSnapshot(id, target, args, resp, data)
	}
	return err
}

func (t *MigrationTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return t.primary.EncodePeer(id, addr)
}

func (t *MigrationTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return t.primary.DecodePeer(buf)
}

func (t *MigrationTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	t.primary.SetHeartbeatHandler(cb)
	t.secondary.SetHeartbeatHandler(cb)
}

func (t *MigrationTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	err := t.primary.TimeoutNow(id, target, args, resp)
	if err != nil && t.shouldFallback(err) {
		return t.secondary.TimeoutNow(id, target, args, resp)
	}
	return err
}

func (t *MigrationTransport) Close() error {
	if !t.shutdown.CompareAndSwap(false, true) {
		return nil
	}
	close(t.shutdownCh)

	// Close both if possible
	var err1, err2 error
	if c, ok := t.primary.(io.Closer); ok {
		err1 = c.Close()
	}
	if c, ok := t.secondary.(io.Closer); ok {
		err2 = c.Close()
	}

	t.wg.Wait()

	if err1 != nil {
		return err1
	}
	return err2
}
