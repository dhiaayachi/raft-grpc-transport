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

var plot = flag.Bool("plot", false, "Generate benchmark plots (benchmark_comparison.html)")

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
	f, err := os.Create("benchmark_comparison.html")
	if err != nil {
		t.Fatalf("failed to create plot file: %v", err)
	}
	defer f.Close()

	// Simple Chart.js template
	html := `<!DOCTYPE html>
<html>
<head>
    <title>Benchmark Comparison</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: sans-serif; padding: 20px; }
        .chart-container { width: 800px; margin: 20px auto; }
    </style>
</head>
<body>
    <h1>Transport Benchmark Comparison</h1>
    <div class="chart-container">
        <canvas id="latencyChart"></canvas>
    </div>
    <div class="chart-container">
        <canvas id="throughputChart"></canvas>
    </div>

    <script>
        const ctxLatency = document.getElementById('latencyChart').getContext('2d');
        const ctxThroughput = document.getElementById('throughputChart').getContext('2d');

        const latencyData = {
            labels: ['Grpc', 'Network'],
            datasets: [{
                label: 'AppendEntries Latency (ns/op) - Lower is better',
                data: [%d, %d],
                backgroundColor: ['rgba(54, 162, 235, 0.5)', 'rgba(255, 99, 132, 0.5)'],
                borderColor: ['rgba(54, 162, 235, 1)', 'rgba(255, 99, 132, 1)'],
                borderWidth: 1
            }]
        };

        // Estimating throughput as 1e9 / nsPerOp * (1KB payload) -> MB/s roughly, or just Ops/sec
        // Or actually, testing.BenchmarkResult doesn't give throughput direct unless SetBytes called.
        // We didn't call SetBytes. Let's use Ops/Sec for throughput comparison for Pipeline.
        const throughputData = {
            labels: ['Grpc', 'Network'],
            datasets: [{
                label: 'Pipeline Throughput (Ops/sec) - Higher is better',
                data: [%f, %f],
                backgroundColor: ['rgba(54, 162, 235, 0.5)', 'rgba(255, 99, 132, 0.5)'],
                borderColor: ['rgba(54, 162, 235, 1)', 'rgba(255, 99, 132, 1)'],
                borderWidth: 1
            }]
        };

        new Chart(ctxLatency, {
            type: 'bar',
            data: latencyData,
            options: { scales: { y: { beginAtZero: true } } }
        });

        new Chart(ctxThroughput, {
            type: 'bar',
            data: throughputData,
            options: { scales: { y: { beginAtZero: true } } }
        });
    </script>
</body>
</html>`

	grpcLat := results["Grpc_AppendEntries"].NsPerOp()
	netLat := results["Net_AppendEntries"].NsPerOp()

	grpcOps := float64(results["Grpc_Pipeline"].N) / results["Grpc_Pipeline"].T.Seconds()
	netOps := float64(results["Net_Pipeline"].N) / results["Net_Pipeline"].T.Seconds()

	fmt.Fprintf(f, html, grpcLat, netLat, grpcOps, netOps)
	t.Logf("Generated benchmark_comparison.html")
}
