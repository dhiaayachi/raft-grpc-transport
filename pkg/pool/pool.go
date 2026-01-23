package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// Pool implements grpc.ClientConnInterface to provide a pool of connections.
type Pool struct {
	conns []*grpc.ClientConn
	next  uint64
	mu    sync.RWMutex
}

// New creates a new connection pool.
func New(addr string, poolSize int, opts ...grpc.DialOption) (*Pool, error) {
	if poolSize <= 0 {
		poolSize = 1
	}

	p := &Pool{
		conns: make([]*grpc.ClientConn, poolSize),
	}

	// Add insecure credentials if not present, similar to original logic.
	// However, it's better if the caller provides opts.
	// For compatibility with the previous inline logic which used insecure by default:
	// Add insecure credentials if not present, similar to original logic.
	// We merge default options with user options below.

	// Helper to create connections
	for i := 0; i < poolSize; i++ {
		// Merge default options with user options
		finalOpts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
		finalOpts = append(finalOpts, opts...)

		conn, err := grpc.NewClient(addr, finalOpts...)
		if err != nil {
			// Cleanup
			for j := 0; j < i; j++ {
				p.conns[j].Close()
			}
			return nil, fmt.Errorf("failed to create pool connection %d: %w", i, err)
		}
		p.conns[i] = conn
	}

	return p, nil
}

// get returns a connection from the pool using round-robin.
func (p *Pool) get() *grpc.ClientConn {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := uint64(len(p.conns))
	if n == 0 {
		return nil
	}

	idx := atomic.AddUint64(&p.next, 1) % n
	conn := p.conns[idx]

	// Check for fitness?
	// If state is TransientFailure or Shutdown, we might retry?
	// But mostly gRPC handles this.
	// If Shutdown, it's bad.
	if conn.GetState() == connectivity.Shutdown {
		// Try to find a healthy one?
		// For now simple round robin is efficient.
	}

	return conn
}

// Invoke performs a unary RPC.
func (p *Pool) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	conn := p.get()
	if conn == nil {
		return fmt.Errorf("connection pool is closed or empty")
	}
	return conn.Invoke(ctx, method, args, reply, opts...)
}

// NewStream begins a streaming RPC.
func (p *Pool) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn := p.get()
	if conn == nil {
		return nil, fmt.Errorf("connection pool is closed or empty")
	}
	return conn.NewStream(ctx, desc, method, opts...)
}

// Close closes all connections in the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for _, conn := range p.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil // Ensure pool is empty
	return firstErr
}
