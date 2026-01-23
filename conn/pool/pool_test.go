package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestNew(t *testing.T) {
	// Test default size (should enforce minimum 1 if passed 0 or negative, though implementation says <=0 -> 1)
	p, err := New("target", 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(p.conns))
	p.Close()

	// Test specific size
	p, err = New("target", 5)
	require.NoError(t, err)
	require.Equal(t, 5, len(p.conns))
	p.Close()
}

func TestPool_Rotation(t *testing.T) {
	// We don't need a real server for this test as we are just checking the rotation logic
	// of the client connections returned. grpc.NewClient is non-blocking by default.

	poolSize := 3
	p, err := New("localhost:1234", poolSize, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer p.Close()

	// Helper to collect connections
	conns := make([]*grpc.ClientConn, 0, poolSize+1)

	for i := 0; i < poolSize+1; i++ {
		conns = append(conns, p.get())
	}

	// Verify we got 3 distinct pointers for the first 3
	require.NotSame(t, conns[0], conns[1])
	require.NotSame(t, conns[1], conns[2])
	require.NotSame(t, conns[0], conns[2])

	// Verify wrapping around
	require.Same(t, conns[0], conns[3])
}

func TestPool_Close(t *testing.T) {
	p, err := New("target", 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(p.conns))

	err = p.Close()
	require.NoError(t, err)
	require.Nil(t, p.conns)
}

func TestPool_Get_Empty(t *testing.T) {
	// Manual construction to test empty edge case if possible,
	// though New() prevents it.
	p := &Pool{}
	conn := p.get()
	require.Nil(t, conn)
}
