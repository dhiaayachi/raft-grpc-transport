# Raft gRPC Transport

A [gRPC](https://grpc.io/)-based `Transport` implementation for HashiCorp's [Raft](https://github.com/hashicorp/raft) consensus library.

This library allows you to use gRPC for communication between Raft nodes, leveraging HTTP/2 performance, streaming capabilities, and the rich ecosystem of gRPC middleware (auth, logging, tracing, etc.).

## Features

- **Standard gRPC**: Uses Protocol Buffers for efficient serialization (via `buf`).
- **Streamed Snapshots**: Supports streaming large snapshots over gRPC.
- **Connection Management**: Lazily establishes and maintains persistent connections to peers.
- **Connection Pooling**: Uses a configurable round-robin pool of connections (default 3) for high concurrency.
- **Drop-in Replacement**: Implements the standard `raft.Transport` interface.

## Installation

```bash
go get github.com/dhiaayachi/raft-grpc-transport
```

## Usage

Here is a basic example of how to configure and use the gRPC transport with Raft.

```go
package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"time"

	raftgrpc "github.com/dhiaayachi/raft-grpc-transport"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

func main() {
	// 1. Setup network listener
	addr := "127.0.0.1:50051"
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}

	// 2. Create gRPC server
	s := grpc.NewServer()

	// 3. Create the Raft transport and register it with the server
	transport, err := raftgrpc.NewGrpcTransport(lis.Addr().String(), s)
	if err != nil {
		panic(fmt.Sprintf("failed to create transport: %v", err))
	}
	defer transport.Close()

	// 4. Start the server (in a goroutine so it doesn't block)
	go func() {
		if err := s.Serve(lis); err != nil {
			panic(fmt.Sprintf("server error: %v", err))
		}
	}()

	// 5. Setup Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID("node-1")

	// 6. Setup other Raft dependencies (stores, snapshots, fsm)
	// For example purposes, using in-memory stores
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapStore := raft.NewInmemSnapshotStore()
	fsm := &MyFSM{} // Your state machine implementation

	// 7. Create the Raft node
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		panic(fmt.Sprintf("failed to create raft node: %v", err))
	}

	// 8. Bootstrap the cluster (if this is the first node)
	// ONLY do this for the very first node in a new cluster
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	r.BootstrapCluster(configuration)

	fmt.Println("Raft node started successfully!")
	
	// Keep the main goroutine running
	select {}
}

// Minimal FSM implementation
type MyFSM struct{}
func (f *MyFSM) Apply(l *raft.Log) interface{} { return nil }
func (f *MyFSM) Snapshot() (raft.FSMSnapshot, error) { return &MySnapshot{}, nil }
func (f *MyFSM) Restore(rc io.ReadCloser) error { return nil }

type MySnapshot struct{}
func (s *MySnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (s *MySnapshot) Release() {}
```

## Development

### Prerequisites

- Go 1.22+
- [buf](https://buf.build/docs/installation) (for protobuf generation)

### Generating Code

If you make changes to the `.proto` files, regenerate the Go code:

```bash
buf generate
```

### Running Tests


## Migration Transport

The `MigrationTransport` allows you to migrate from one transport (e.g., TCP) to another (e.g., gRPC) with zero downtime. It wraps two transports (Primary and Secondary) and attempts to use the Primary first. If the Primary fails with a specific error (controlled by a predicate function), it falls back to the Secondary.

Incoming RPCs from both transports are handled indiscriminately.

### Usage

```go
// 1. Create your two transports
oldTransport := ... // e.g., raft.NewTCPTransport(...)
newTransport, _ := raftgrpc.NewGrpcTransport(...)

// 2. Define a fallback predicate
// e.g. Fallback if the connection is refused or unavailable
shouldFallback := func(err error) bool {
    // Check error type/content
    return true
}

// 3. Create the migration transport
transport := raftgrpc.NewMigrationTransport(newTransport, oldTransport, shouldFallback)

// 4. Use 'transport' when creating your Raft node
```

## Benchmarks

This library includes benchmarks comparing `raft-grpc-transport` against the default `raft.NetworkTransport` (TCP + MsgPack).

### Results Summary

![Benchmark Results](/docs/benchmark_comparison.svg)

### Running Benchmarks

You can run the benchmarks and generate a comparison graph locally:

```bash
go test -benchtime=10000x -count=10 -tags benchmark -v -bench=. -benchmem -timeout 20m -args -plot
```

This will generate a `benchmark_comparison.svg` file with visual results.
