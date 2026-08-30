// Benchmark harness for gRPC-Go, matching examples/mux_compare.rs.
//
// Both endpoints live in one process talking over loopback TCP, exactly as the
// Rust harness does with --transport tcp, so the two are comparable when both
// run inside the same tc-shaped network namespace. The stdout event format is
// identical to the Rust harness, so one summariser reads both.
//
// Each "stream" is one bidirectional streaming RPC carrying raw bytes, so no
// protobuf compiler is needed.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

// ---------------------------------------------------------------------------
// raw bytes codec, registered on both sides (they share this process)
// ---------------------------------------------------------------------------

const codecName = "bench-bytes"

type bytesCodec struct{}

func (bytesCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("bytesCodec: want *[]byte, got %T", v)
	}
	return *b, nil
}

func (bytesCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("bytesCodec: want *[]byte, got %T", v)
	}
	*b = append((*b)[:0], data...)
	return nil
}

func (bytesCodec) Name() string { return codecName }

// ---------------------------------------------------------------------------

type metrics struct {
	delivered atomic.Uint64
	started   time.Time
}

func (m *metrics) echoSample(d time.Duration) {
	fmt.Printf("%.3f,client,echo,rtt_ms=%.2f\n", time.Since(m.started).Seconds(),
		float64(d.Microseconds())/1000.0)
}

// cpuMs returns utime and stime from /proc/self/stat in milliseconds (USER_HZ=100).
func cpuMs() (uint64, uint64) {
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, 0
	}
	// comm can contain spaces and parens; fields are counted after the last ')'.
	s := string(raw)
	if i := strings.LastIndex(s, ")"); i >= 0 && i+2 <= len(s) {
		s = s[i+2:]
	}
	f := strings.Fields(s)
	// With pid and comm removed, state is f[0], so utime is f[11], stime f[12].
	if len(f) < 13 {
		return 0, 0
	}
	u, _ := strconv.ParseUint(f[11], 10, 64)
	k, _ := strconv.ParseUint(f[12], 10, 64)
	return u * 10, k * 10
}

var (
	workload     = flag.String("workload", "bulk", "bulk|echo|jobs")
	streams      = flag.Int("streams", 8, "concurrent bulk streams")
	durationS    = flag.Float64("duration-s", 22, "run duration, or deadline for jobs")
	bytesPerKB   = flag.Int64("bytes-per-stream-kb", 1024, "payload per stream for jobs")
	chunkKB      = flag.Int("chunk-kb", 64, "application write size")
	sampleMS     = flag.Int("sample-ms", 500, "goodput sample interval")
	cpuSampleMS  = flag.Int("cpu-sample-ms", 50, "cpu sample interval")
	initWindowKB = flag.Int("init-window-kb", 0, "static stream window; 0 keeps gRPC's BDP autotuning")
	initConnKB   = flag.Int("init-conn-window-kb", 0, "static connection window; 0 keeps BDP autotuning")
	writeBufKB   = flag.Int("write-buffer-kb", 32, "grpc write buffer")
	readBufKB    = flag.Int("read-buffer-kb", 32, "grpc read buffer")
	serverPort   = flag.Int("server-port", 0, "bind the server on this port (0 = ephemeral)")
	dial         = flag.String("dial", "", "dial this address instead of the server's own, to route through emu_proxy")
)

const echoChunk = 1024

func main() {
	flag.Parse()
	encoding.RegisterCodec(bytesCodec{})
	fmt.Printf("# grpc-go workload=%s streams=%d duration=%.1f init_window_kb=%d init_conn_kb=%d\n",
		*workload, *streams, *durationS, *initWindowKB, *initConnKB)

	m := &metrics{started: time.Now()}
	echo := *workload == "echo"
	jobs := *workload == "jobs"

	var target int64 = 1 << 62
	if jobs {
		target = *bytesPerKB * 1024
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *serverPort))
	if err != nil {
		fail(m, err)
	}
	srvOpts := []grpc.ServerOption{
		grpc.WriteBufferSize(*writeBufKB * 1024),
		grpc.ReadBufferSize(*readBufKB * 1024),
		grpc.MaxRecvMsgSize(64 << 20),
	}
	if *initWindowKB > 0 {
		srvOpts = append(srvOpts, grpc.InitialWindowSize(int32(*initWindowKB*1024)))
	}
	if *initConnKB > 0 {
		srvOpts = append(srvOpts, grpc.InitialConnWindowSize(int32(*initConnKB*1024)))
	}
	srv := grpc.NewServer(srvOpts...)
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "bench.Bench",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    "Duplex",
			Handler:       func(_ any, ss grpc.ServerStream) error { return serve(ss, m) },
			ServerStreams: true,
			ClientStreams: true,
		}},
		Metadata: "bench",
	}, nil)
	go func() { _ = srv.Serve(lis) }()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.CallContentSubtype(codecName),
			grpc.MaxCallRecvMsgSize(64<<20),
			grpc.MaxCallSendMsgSize(64<<20),
		),
		grpc.WithWriteBufferSize(*writeBufKB * 1024),
		grpc.WithReadBufferSize(*readBufKB * 1024),
	}
	if *initWindowKB > 0 {
		dialOpts = append(dialOpts, grpc.WithInitialWindowSize(int32(*initWindowKB*1024)))
	}
	if *initConnKB > 0 {
		dialOpts = append(dialOpts, grpc.WithInitialConnWindowSize(int32(*initConnKB*1024)))
	}
	addr := lis.Addr().String()
	if *dial != "" {
		addr = *dial
	}
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		fail(m, err)
	}
	defer conn.Close()

	go sampleGoodput(m)
	go sampleCPU(m)

	total := *streams
	if echo {
		total++
	}
	chunk := make([]byte, *chunkKB*1024)

	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		isEcho := echo && i == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			runClientStream(conn, isEcho, target, chunk, m)
		}()
	}

	deadline := time.After(time.Duration(*durationS * float64(time.Second)))
	done := make(chan struct{})
	if jobs {
		go func() { wg.Wait(); close(done) }()
	}
	select {
	case <-done:
		fmt.Printf("%.3f,both,done,elapsed_s=%.3f\n",
			time.Since(m.started).Seconds(), time.Since(m.started).Seconds())
	case <-deadline:
		fmt.Printf("%.3f,both,deadline,\n", time.Since(m.started).Seconds())
	}
}

func fail(m *metrics, err error) {
	fmt.Printf("%.3f,both,failed,error=%s\n", time.Since(m.started).Seconds(), err)
	os.Exit(1)
}

// runClientStream drives one RPC: an echo ping-pong, or a bulk writer that
// stops at `target` bytes.
func runClientStream(conn *grpc.ClientConn, isEcho bool, target int64, chunk []byte, m *metrics) {
	desc := &grpc.StreamDesc{StreamName: "Duplex", ServerStreams: true, ClientStreams: true}
	cs, err := conn.NewStream(context.Background(), desc, "/bench.Bench/Duplex")
	if err != nil {
		return
	}
	marker := []byte("D")
	if isEcho {
		marker = []byte("E")
	}
	if err := cs.SendMsg(&marker); err != nil {
		return
	}
	if isEcho {
		payload := make([]byte, echoChunk)
		for {
			sent := time.Now()
			if err := cs.SendMsg(&payload); err != nil {
				return
			}
			var back []byte
			if err := cs.RecvMsg(&back); err != nil {
				return
			}
			m.echoSample(time.Since(sent))
		}
	}
	// The marker counts toward the stream's payload, matching the Rust harness.
	written := int64(1)
	for written < target {
		take := int64(len(chunk))
		if target-written < take {
			take = target - written
		}
		part := chunk[:take]
		if err := cs.SendMsg(&part); err != nil {
			return
		}
		written += take
	}
	_ = cs.CloseSend()
	var sink []byte
	for cs.RecvMsg(&sink) == nil {
	}
}

// serve handles one inbound RPC: echo it back, or count the bytes.
func serve(ss grpc.ServerStream, m *metrics) error {
	var first []byte
	if err := ss.RecvMsg(&first); err != nil {
		return err
	}
	isEcho := len(first) > 0 && first[0] == 'E'
	if !isEcho {
		m.delivered.Add(uint64(len(first)))
	}
	for {
		var msg []byte
		if err := ss.RecvMsg(&msg); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if isEcho {
			if err := ss.SendMsg(&msg); err != nil {
				return err
			}
		} else {
			m.delivered.Add(uint64(len(msg)))
		}
	}
}

func sampleGoodput(m *metrics) {
	tick := time.NewTicker(time.Duration(*sampleMS) * time.Millisecond)
	defer tick.Stop()
	var last uint64
	prev := time.Now()
	for range tick.C {
		now := m.delivered.Load()
		dt := time.Since(prev).Seconds()
		prev = time.Now()
		fmt.Printf("%.3f,link,goodput,to_server_mbps=%.2f\n",
			time.Since(m.started).Seconds(), float64(now-last)*8/dt/1e6)
		last = now
	}
}

func sampleCPU(m *metrics) {
	tick := time.NewTicker(time.Duration(*cpuSampleMS) * time.Millisecond)
	defer tick.Stop()
	for range tick.C {
		u, s := cpuMs()
		fmt.Printf("%.3f,proc,cpu,utime_ms=%d,stime_ms=%d,delivered=%d\n",
			time.Since(m.started).Seconds(), u, s, m.delivered.Load())
	}
}
