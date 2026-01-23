//go:build benchmark

package raftgrpc

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

var plot = flag.Bool("plot", false, "Generate benchmark plots (benchmark_comparison.svg)")

func setupGrpcTransport(b *testing.B, addr string) (*GrpcTransport, *grpc.Server, net.Listener) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	trans, err := NewGrpcTransport(lis.Addr().String(), server)
	if err != nil {
		b.Fatalf("failed to create transport: %v", err)
	}

	go func() {
		if err := server.Serve(lis); err != nil {
			// ignore error
		}
	}()

	return trans, server, lis
}

// BenchmarkGrpc_AppendEntries benchmarks the RTT of AppendEntries using GrpcTransport
func BenchmarkGrpc_AppendEntries(b *testing.B) {
	trans, server, lis := setupGrpcTransport(b, "127.0.0.1:0")
	defer trans.Close()
	defer server.Stop()
	defer lis.Close()

	clientTrans, clientServer, clientLis := setupGrpcTransport(b, "127.0.0.1:0")
	defer clientTrans.Close()
	defer clientServer.Stop()
	defer clientLis.Close()

	// Connect client to server manually to ensure connection is ready?
	// The transport dials on demand.

	target := raft.ServerAddress(lis.Addr().String())

	// Consumer loop to handle request
	go func() {
		for {
			select {
			case rpc := <-trans.Consumer():
				switch rpc.Command.(type) {
				case *raft.AppendEntriesRequest:
					rpc.RespChan <- raft.RPCResponse{
						Response: &raft.AppendEntriesResponse{
							Success: true,
							Term:    1,
						},
					}
				}
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()

	args := &raft.AppendEntriesRequest{
		Term:   1,
		Leader: []byte("leader"),
		Entries: []*raft.Log{
			{Index: 1, Term: 1, Data: make([]byte, 1024*10)}, // 10KB payload
		},
	}
	resp := &raft.AppendEntriesResponse{}

	// Warmup/Ensure connection
	if err := clientTrans.AppendEntries("server", target, args, resp); err != nil {
		b.Fatalf("Warmup RPC failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := clientTrans.AppendEntries("server", target, args, resp); err != nil {
			b.Fatalf("RPC failed: %v", err)
		}
	}
}

// BenchmarkNet_AppendEntries benchmarks the RTT of AppendEntries using raft.NetworkTransport
func BenchmarkNet_AppendEntries(b *testing.B) {
	// Server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	serverTrans, err := raft.NewTCPTransport(addr, nil, 3, 10*time.Second, io.Discard)
	if err != nil {
		b.Fatalf("failed to create server transport: %v", err)
	}
	defer serverTrans.Close()

	// Client
	clientTrans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 3, 10*time.Second, io.Discard)
	if err != nil {
		b.Fatalf("failed to create client transport: %v", err)
	}
	defer clientTrans.Close()

	target := raft.ServerAddress(serverTrans.LocalAddr())

	// Consumer loop
	go func() {
		for {
			select {
			case rpc := <-serverTrans.Consumer():
				switch rpc.Command.(type) {
				case *raft.AppendEntriesRequest:
					rpc.RespChan <- raft.RPCResponse{
						Response: &raft.AppendEntriesResponse{
							Success: true,
							Term:    1,
						},
					}
				}
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()

	args := &raft.AppendEntriesRequest{
		Term:   1,
		Leader: []byte("leader"),
		Entries: []*raft.Log{
			{Index: 1, Term: 1, Data: make([]byte, 1024)}, // 1KB payload
		},
	}
	resp := &raft.AppendEntriesResponse{}

	// Warmup
	if err := clientTrans.AppendEntries("server", target, args, resp); err != nil {
		b.Fatalf("Warmup RPC failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := clientTrans.AppendEntries("server", target, args, resp); err != nil {
			b.Fatalf("RPC failed: %v", err)
		}
	}
}

// BenchmarkGrpc_Pipeline benchmarks throughput of Pipeline using GrpcTransport
func BenchmarkGrpc_Pipeline(b *testing.B) {
	trans, server, lis := setupGrpcTransport(b, "127.0.0.1:0")
	defer trans.Close()
	defer server.Stop()
	defer lis.Close()

	clientTrans, clientServer, clientLis := setupGrpcTransport(b, "127.0.0.1:0")
	defer clientTrans.Close()
	defer clientServer.Stop()
	defer clientLis.Close()

	target := raft.ServerAddress(lis.Addr().String())

	// Consumer loop
	go func() {
		for {
			select {
			case rpc := <-trans.Consumer():
				switch rpc.Command.(type) {
				case *raft.AppendEntriesRequest:
					rpc.RespChan <- raft.RPCResponse{
						Response: &raft.AppendEntriesResponse{
							Success: true,
							Term:    1,
						},
					}
				}
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()

	pipeline, err := clientTrans.AppendEntriesPipeline("server", target)
	if err != nil {
		b.Fatalf("failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	args := &raft.AppendEntriesRequest{
		Term:   1,
		Leader: []byte("leader"),
		Entries: []*raft.Log{
			{Index: 1, Term: 1, Data: make([]byte, 1024)}, // 1KB payload
		},
	}
	resp := &raft.AppendEntriesResponse{}

	// Drain consumer to keep it flowing
	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		ch := pipeline.Consumer()
		for {
			select {
			case <-ch:
			case <-stopCh:
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pipeline.AppendEntries(args, resp)
		if err != nil {
			b.Fatalf("pipeline failed: %v", err)
		}
	}
}

// BenchmarkNet_Pipeline benchmarks throughput of Pipeline using raft.NetworkTransport
func BenchmarkNet_Pipeline(b *testing.B) {
	// Server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	serverTrans, err := raft.NewTCPTransport(addr, nil, 3, 10*time.Second, io.Discard)
	if err != nil {
		b.Fatalf("failed to create server transport: %v", err)
	}
	defer serverTrans.Close()

	// Client
	clientTrans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 3, 10*time.Second, io.Discard)
	if err != nil {
		b.Fatalf("failed to create client transport: %v", err)
	}
	defer clientTrans.Close()

	target := raft.ServerAddress(serverTrans.LocalAddr())

	// Consumer loop
	go func() {
		for {
			select {
			case rpc := <-serverTrans.Consumer():
				switch rpc.Command.(type) {
				case *raft.AppendEntriesRequest:
					rpc.RespChan <- raft.RPCResponse{
						Response: &raft.AppendEntriesResponse{
							Success: true,
							Term:    1,
						},
					}
				}
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()

	pipeline, err := clientTrans.AppendEntriesPipeline("server", target)
	if err != nil {
		b.Fatalf("failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	args := &raft.AppendEntriesRequest{
		Term:   1,
		Leader: []byte("leader"),
		Entries: []*raft.Log{
			{Index: 1, Term: 1, Data: make([]byte, 1024)}, // 1KB payload
		},
	}
	resp := &raft.AppendEntriesResponse{}

	b.ResetTimer()

	// Drain consumer
	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		ch := pipeline.Consumer()
		for {
			select {
			case <-ch:
			case <-stopCh:
				return
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		_, err := pipeline.AppendEntries(args, resp)
		if err != nil {
			b.Fatalf("pipeline failed: %v", err)
		}
	}
}

// TestMetrics plots the results if -plot flag is provided
func TestMetrics(t *testing.T) {
	if !*plot {
		t.Skip("skipping plot generation, use -plot to run")
	}

	results := make(map[string]testing.BenchmarkResult)

	benchmarks := []struct {
		Name string
		Func func(*testing.B)
	}{
		{"Grpc_AppendEntries", BenchmarkGrpc_AppendEntries},
		{"Net_AppendEntries", BenchmarkNet_AppendEntries},
		{"Grpc_Pipeline", BenchmarkGrpc_Pipeline},
		{"Net_Pipeline", BenchmarkNet_Pipeline},
	}

	for _, bm := range benchmarks {
		res := testing.Benchmark(bm.Func)
		results[bm.Name] = res
		fmt.Printf("%s: %d ns/op\n", bm.Name, res.NsPerOp())
	}

	generatePlot(t, results)
}

func generatePlot(t *testing.T, results map[string]testing.BenchmarkResult) {
	f, err := os.Create("benchmark_comparison.svg")
	if err != nil {
		t.Fatalf("failed to create plot file: %v", err)
	}
	defer f.Close()

	grpcLat := float64(results["Grpc_AppendEntries"].NsPerOp())
	netLat := float64(results["Net_AppendEntries"].NsPerOp())

	grpcOps := float64(results["Grpc_Pipeline"].N) / results["Grpc_Pipeline"].T.Seconds()
	netOps := float64(results["Net_Pipeline"].N) / results["Net_Pipeline"].T.Seconds()

	// Simple SVG Bar Chart
	// Width: 800, Height: 400
	// Two charts: Latency (Left), Throughput (Right)

	svg := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<svg width="800" height="400" xmlns="http://www.w3.org/2000/svg">
  <style>
    .text { font-family: sans-serif; font-size: 12px; }
    .title { font-family: sans-serif; font-size: 16px; font-weight: bold; }
    .bar-grpc { fill: rgba(54, 162, 235, 0.8); }
    .bar-net { fill: rgba(255, 99, 132, 0.8); }
  </style>

  <!-- Title -->
  <text x="400" y="30" text-anchor="middle" class="title">Transport Benchmark Comparison</text>

  <!-- Latency Chart (Left) -->
  <g transform="translate(50, 60)">
    <text x="150" y="-10" text-anchor="middle" class="title" font-size="14">Latency (ns/op)</text>
    <text x="150" y="5" text-anchor="middle" class="text" font-size="10">(Lower is better)</text>
    
    <!-- Y Axis -->
    <line x1="0" y1="0" x2="0" y2="300" stroke="black" width="2" />
    <line x1="0" y1="300" x2="300" y2="300" stroke="black" width="2" />

    <!-- Bars -->
    <!-- Scale: Max approx 60000ns. 300px height. Scale = 300/60000 = 0.005 -->
    <!-- Grpc Bar -->
    <rect x="50" y="%f" width="80" height="%f" class="bar-grpc" />
    <text x="90" y="%f" text-anchor="middle" class="text">%.0f</text>
    <text x="90" y="315" text-anchor="middle" class="text">gRPC</text>

    <!-- Net Bar -->
    <rect x="170" y="%f" width="80" height="%f" class="bar-net" />
    <text x="210" y="%f" text-anchor="middle" class="text">%.0f</text>
    <text x="210" y="315" text-anchor="middle" class="text">Network</text>
  </g>

  <!-- Throughput Chart (Right) -->
  <g transform="translate(450, 60)">
    <text x="150" y="-10" text-anchor="middle" class="title" font-size="14">Throughput (ops/sec)</text>
    <text x="150" y="5" text-anchor="middle" class="text" font-size="10">(Higher is better)</text>

    <!-- Y Axis -->
    <line x1="0" y1="0" x2="0" y2="300" stroke="black" width="2" />
    <line x1="0" y1="300" x2="300" y2="300" stroke="black" width="2" />

    <!-- Bars -->
    <!-- Scale: Max approx 300000 ops. 300px height. Scale = 300/300000 = 0.001 -->
    <!-- Grpc Bar -->
    <rect x="50" y="%f" width="80" height="%f" class="bar-grpc" />
    <text x="90" y="%f" text-anchor="middle" class="text">%.0f</text>
    <text x="90" y="315" text-anchor="middle" class="text">gRPC</text>

    <!-- Net Bar -->
    <rect x="170" y="%f" width="80" height="%f" class="bar-net" />
    <text x="210" y="%f" text-anchor="middle" class="text">%.0f</text>
    <text x="210" y="315" text-anchor="middle" class="text">Network</text>
  </g>
</svg>`,
		// Latency Calc
		300-(grpcLat*0.005), grpcLat*0.005, 300-(grpcLat*0.005)-5, grpcLat,
		300-(netLat*0.005), netLat*0.005, 300-(netLat*0.005)-5, netLat,

		// Throughput Calc
		300-(grpcOps*0.001), grpcOps*0.001, 300-(grpcOps*0.001)-5, grpcOps,
		300-(netOps*0.001), netOps*0.001, 300-(netOps*0.001)-5, netOps,
	)

	fmt.Fprint(f, svg)
	t.Logf("Generated benchmark_comparison.svg")
}
