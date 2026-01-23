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

var payloadSizes = []int{1024, 10 * 1024, 100 * 1024, 1000 * 1024}

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

func runBenchmarkGrpc_AppendEntries(b *testing.B, size int) {
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

	args := &raft.AppendEntriesRequest{
		Term:   1,
		Leader: []byte("leader"),
		Entries: []*raft.Log{
			{Index: 1, Term: 1, Data: make([]byte, size)},
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

func runBenchmarkNet_AppendEntries(b *testing.B, size int) {
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
			{Index: 1, Term: 1, Data: make([]byte, size)},
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

func runBenchmarkGrpc_Pipeline(b *testing.B, size int) {
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
			{Index: 1, Term: 1, Data: make([]byte, size)},
		},
	}
	resp := &raft.AppendEntriesResponse{}

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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pipeline.AppendEntries(args, resp)
		if err != nil {
			b.Fatalf("pipeline failed: %v", err)
		}
	}
}

func runBenchmarkNet_Pipeline(b *testing.B, size int) {
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
			{Index: 1, Term: 1, Data: make([]byte, size)},
		},
	}
	resp := &raft.AppendEntriesResponse{}

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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pipeline.AppendEntries(args, resp)
		if err != nil {
			b.Fatalf("pipeline failed: %v", err)
		}
	}
}

// ResultKey identifies a specific benchmark result
type ResultKey struct {
	Name string
	Size int
}

// TestMetrics plots the results if -plot flag is provided
func TestMetrics(t *testing.T) {
	if !*plot {
		t.Skip("skipping plot generation, use -plot to run")
	}

	results := make(map[ResultKey]testing.BenchmarkResult)

	for _, size := range payloadSizes {
		// Latency - gRPC
		res := testing.Benchmark(func(b *testing.B) { runBenchmarkGrpc_AppendEntries(b, size) })
		results[ResultKey{"Grpc_AppendEntries", size}] = res
		fmt.Printf("Grpc_AppendEntries (%d bytes): %d ns/op\n", size, res.NsPerOp())

		// Latency - Net
		res = testing.Benchmark(func(b *testing.B) { runBenchmarkNet_AppendEntries(b, size) })
		results[ResultKey{"Net_AppendEntries", size}] = res
		fmt.Printf("Net_AppendEntries (%d bytes): %d ns/op\n", size, res.NsPerOp())

		// Throughput - gRPC
		res = testing.Benchmark(func(b *testing.B) { runBenchmarkGrpc_Pipeline(b, size) })
		results[ResultKey{"Grpc_Pipeline", size}] = res
		fmt.Printf("Grpc_Pipeline (%d bytes): %d ns/op\n", size, res.NsPerOp())

		// Throughput - Net
		res = testing.Benchmark(func(b *testing.B) { runBenchmarkNet_Pipeline(b, size) })
		results[ResultKey{"Net_Pipeline", size}] = res
		fmt.Printf("Net_Pipeline (%d bytes): %d ns/op\n", size, res.NsPerOp())
	}

	generateSVG(t, results)
}

func generateSVG(t *testing.T, results map[ResultKey]testing.BenchmarkResult) {
	f, err := os.Create("docs/benchmark_comparison.svg")
	if err != nil {
		t.Fatalf("failed to create plot file: %v", err)
	}
	defer f.Close()

	// Constants for layout
	width, height := 800.0, 600.0
	margin := 60.0
	chartHeight := (height - 3*margin) / 2
	chartWidth := width - 2*margin

	// Helper to scale Y values
	getMaxLatency := func() float64 {
		max := 0.0
		for _, r := range results {
			// Filtering just latency tests implicitly by magnitude or name check?
			// But wait, we have both latency and throughput.
			// Let's just find max of observed latency tests.
			l := float64(r.NsPerOp())
			if l > max {
				max = l
			}
		}
		return max * 1.1 // Add 10% headroom
	}

	getMaxOps := func() float64 {
		max := 0.0
		for k, r := range results {
			if k.Name == "Grpc_Pipeline" || k.Name == "Net_Pipeline" {
				ops := float64(r.N) / r.T.Seconds()
				if ops > max {
					max = ops
				}
			}
		}
		return max * 1.1
	}

	maxLat := getMaxLatency()
	maxOps := getMaxOps()

	// Helper to get X position for ordinal payload sizes
	getX := func(idx int) float64 {
		// spread points evenly across chartWidth
		// 4 points -> 0, 1/3, 2/3, 1 (relative)
		if len(payloadSizes) == 1 {
			return margin + chartWidth/2
		}
		step := chartWidth / float64(len(payloadSizes)-1)
		return margin + float64(idx)*step
	}

	header := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<svg width="%.0f" height="%.0f" xmlns="http://www.w3.org/2000/svg">
  <style>
    .text { font-family: sans-serif; font-size: 12px; }
    .title { font-family: sans-serif; font-size: 16px; font-weight: bold; }
    .axis { stroke: black; stroke-width: 1; }
    .grid { stroke: #ddd; stroke-width: 1; stroke-dasharray: 4; }
    .line-grpc { fill: none; stroke: rgba(54, 162, 235, 1); stroke-width: 2; }
    .line-net { fill: none; stroke: rgba(255, 99, 132, 1); stroke-width: 2; }
    .dot-grpc { fill: rgba(54, 162, 235, 1); }
    .dot-net { fill: rgba(255, 99, 132, 1); }
  </style>
  <text x="%.0f" y="30" text-anchor="middle" class="title">Transport Benchmark Comparison</text>
`, width, height, width/2)

	fmt.Fprint(f, header)

	// Draw Grid & Axis for Chart 1 (Latency)
	// Y from margin+10 (title) -> margin+chartHeight
	// Base Y = margin + chartHeight
	baseY1 := margin + chartHeight

	// Draw Latency Title
	fmt.Fprintf(f, `<text x="%.0f" y="%.0f" text-anchor="middle" class="title" font-size="14">Latency (ns/op) - Lower is better</text>`, width/2, margin-10)

	// Draw Axes 1
	fmt.Fprintf(f, `<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" class="axis" />`, margin, baseY1, width-margin, baseY1) // X Axis
	fmt.Fprintf(f, `<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" class="axis" />`, margin, margin, margin, baseY1)       // Y Axis

	// Draw X Labels 1
	for i, size := range payloadSizes {
		x := getX(i)
		label := fmt.Sprintf("%dKB", size/1024)
		if size >= 1024*1024 {
			label = fmt.Sprintf("%dMB", size/(1024*1024))
		}
		fmt.Fprintf(f, `<text x="%.0f" y="%.0f" text-anchor="middle" class="text">%s</text>`, x, baseY1+15, label)
		fmt.Fprintf(f, `<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" class="axis" />`, x, baseY1, x, baseY1+5)
	}

	// Draw Lines 1
	drawSeries := func(name string, cssLine, cssDot string, scaleY func(float64) float64) {
		points := ""
		for i, size := range payloadSizes {
			res := results[ResultKey{name, size}]
			val := float64(res.NsPerOp())
			if name == "Grpc_Pipeline" || name == "Net_Pipeline" {
				val = float64(res.N) / res.T.Seconds()
			}

			x := getX(i)
			y := scaleY(val)
			points += fmt.Sprintf("%.2f,%.2f ", x, y)

			// Draw Dot
			fmt.Fprintf(f, `<circle cx="%.2f" cy="%.2f" r="4" class="%s" />`, x, y, cssDot)
			// Draw Value
			fmt.Fprintf(f, `<text x="%.2f" y="%.2f" text-anchor="middle" class="text" dy="-8">%.0f</text>`, x, y, val)
		}
		fmt.Fprintf(f, `<polyline points="%s" class="%s" />`, points, cssLine)
	}

	scaleYLat := func(val float64) float64 {
		return baseY1 - (val/maxLat)*chartHeight
	}

	drawSeries("Grpc_AppendEntries", "line-grpc", "dot-grpc", scaleYLat)
	drawSeries("Net_AppendEntries", "line-net", "dot-net", scaleYLat)

	// Draw Grid & Axis for Chart 2 (Throughput)
	baseY2 := baseY1 + margin + chartHeight
	topY2 := baseY1 + margin

	// Draw Throughput Title
	fmt.Fprintf(f, `<text x="%.0f" y="%.0f" text-anchor="middle" class="title" font-size="14">Throughput (ops/sec) - Higher is better</text>`, width/2, topY2-10)

	// Draw Axes 2
	fmt.Fprintf(f, `<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" class="axis" />`, margin, baseY2, width-margin, baseY2) // X Axis
	fmt.Fprintf(f, `<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" class="axis" />`, margin, topY2, margin, baseY2)        // Y Axis

	// Draw X Labels 2
	for i, size := range payloadSizes {
		x := getX(i)
		label := fmt.Sprintf("%dKB", size/1024)
		if size >= 1024*1024 {
			label = fmt.Sprintf("%dMB", size/(1024*1024))
		}
		fmt.Fprintf(f, `<text x="%.0f" y="%.0f" text-anchor="middle" class="text">%s</text>`, x, baseY2+15, label)
		fmt.Fprintf(f, `<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" class="axis" />`, x, baseY2, x, baseY2+5)
	}

	scaleYOps := func(val float64) float64 {
		return baseY2 - (val/maxOps)*chartHeight
	}

	drawSeries("Grpc_Pipeline", "line-grpc", "dot-grpc", scaleYOps)
	drawSeries("Net_Pipeline", "line-net", "dot-net", scaleYOps)

	// Legend (Bottom Center)
	fmt.Fprintf(f, `
<g transform="translate(%.0f, %.0f)">
  <rect x="0" y="0" width="20" height="20" class="bar-grpc" />
  <text x="25" y="15" class="text">gRPC</text>
  <rect x="100" y="0" width="20" height="20" class="bar-net" />
  <text x="125" y="15" class="text">Network</text>
</g>`, width/2-70, height-30)

	fmt.Fprint(f, "</svg>")
	t.Logf("Generated benchmark_comparison.svg")
}
