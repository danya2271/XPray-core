//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
	"golang.org/x/sys/unix"
)

func processCPU() int64 {
	var usage unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &usage) != nil {
		return 0
	}
	return usage.Utime.Nano() + usage.Stime.Nano()
}

func benchmarkModes(b *testing.B, run func(*testing.B, *core.Instance, xnet.Destination)) {
	for _, mode := range []struct{ name, native, bridge, pool string }{
		{"go", "0", "socket", "0"},
		{"socket_perflow", "1", "socket", "0"},
		{"socket_pool2", "1", "socket", "2"},
		{"shared_perflow", "1", "shared", "0"},
		{"shared_pool2", "1", "shared", "2"},
	} {
		b.Run(mode.name, func(b *testing.B) {
			masked := os.Getenv("XHYS_BENCH_SALAMANDER") != "0"
			b.Setenv("XRAY_HYSTERIA_NATIVE", "0")
			if os.Getenv("XHYS_BENCH_NATIVE_SERVER") == "1" {
				b.Setenv("XRAY_HYSTERIA_NATIVE", "1")
			}
			server, caPath := xrayServer(b, masked)
			ca, err := os.ReadFile(caPath)
			if err != nil {
				b.Fatal(err)
			}
			b.Setenv("XRAY_HYSTERIA_NATIVE", mode.native)
			b.Setenv("XRAY_HYSTERIA_BRIDGE", mode.bridge)
			b.Setenv("XRAY_HYSTERIA_POOL_SIZE", mode.pool)
			client := embeddedClient(b, server, ca, masked)
			b.Cleanup(func() {
				var usage unix.Rusage
				if unix.Getrusage(unix.RUSAGE_SELF, &usage) == nil {
					fmt.Printf("process_peak_rss_kib=%d native_stats=%+v\n", usage.Maxrss, native.Stats())
				}
			})
			tcp, _ := echoTargets(b)
			target, _ := xnet.ParseDestination("tcp:" + tcp)
			run(b, client, target)
		})
	}
}

func benchmarkDial(b *testing.B, client *core.Instance, target xnet.Destination) net.Conn {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := core.Dial(ctx, client, target)
	if err != nil {
		cancel()
		b.Fatal(err)
	}
	return &benchmarkConn{Conn: conn, cancel: cancel}
}

type benchmarkConn struct {
	net.Conn
	cancel context.CancelFunc
}

func (c *benchmarkConn) Close() error { c.cancel(); return c.Conn.Close() }

func benchmarkExchange(b *testing.B, conn net.Conn, data, received []byte) {
	b.Helper()
	if _, err := conn.Write(data); err != nil {
		b.Fatal(err)
	}
	if _, err := io.ReadFull(conn, received); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkNativeOpenClose(b *testing.B) {
	benchmarkModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		if os.Getenv("XRAY_HYSTERIA_NATIVE") == "0" && os.Getenv("XHYS_BENCH_GO_OPENCLOSE") != "1" {
			b.Skip("Go baseline returns closed pipe under sustained rapid open/close; no valid result")
		}
		data, received := []byte("ping"), make([]byte, 4)
		// Warm pools/cert caches outside the measured interval.
		conn := benchmarkDial(b, client, target)
		benchmarkExchange(b, conn, data, received)
		conn.Close()
		before := native.Stats().StartedEngines
		trace := os.Getenv("XHYS_BENCH_TRACE") == "1"
		b.ReportAllocs()
		b.ResetTimer()
		cpu := processCPU()
		iteration := 0
		for b.Loop() {
			iteration++
			var started, opened, exchanged time.Time
			if trace {
				started = time.Now()
			}
			conn := benchmarkDial(b, client, target)
			if trace {
				opened = time.Now()
			}
			benchmarkExchange(b, conn, data, received)
			if trace {
				exchanged = time.Now()
			}
			conn.Close()
			if trace && time.Since(started) > 100*time.Millisecond {
				b.Logf("slow short flow %d: open=%s exchange=%s close=%s", iteration, opened.Sub(started), exchanged.Sub(opened), time.Since(exchanged))
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(processCPU()-cpu)/float64(b.N), "process-cpu-ns/op")
		b.ReportMetric(float64(native.Stats().StartedEngines-before)/float64(b.N), "native-starts/op")
	})
}

func BenchmarkNativeTransfer(b *testing.B) {
	benchmarkModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		conn := benchmarkDial(b, client, target)
		defer conn.Close()
		data, received := make([]byte, 65536), make([]byte, 65536)
		benchmarkExchange(b, conn, data, received)
		b.SetBytes(int64(2 * len(data)))
		b.ReportAllocs()
		b.ResetTimer()
		cpu := processCPU()
		for b.Loop() {
			benchmarkExchange(b, conn, data, received)
		}
		b.StopTimer()
		b.ReportMetric(float64(processCPU()-cpu)/float64(b.N), "process-cpu-ns/op")
	})
}

func BenchmarkNativeParallel(b *testing.B) {
	benchmarkModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		b.SetBytes(2 * 65536)
		b.ReportAllocs()
		b.ResetTimer()
		cpu := processCPU()
		b.RunParallel(func(pb *testing.PB) {
			conn := benchmarkDial(b, client, target)
			defer conn.Close()
			data, received := make([]byte, 65536), make([]byte, 65536)
			conn.SetDeadline(time.Now().Add(time.Minute))
			for pb.Next() {
				benchmarkExchange(b, conn, data, received)
			}
		})
		b.StopTimer()
		b.ReportMetric(float64(processCPU()-cpu)/float64(b.N), "process-cpu-ns/op")
	})
}

func BenchmarkNativeUpload(b *testing.B) {
	benchmarkModes(b, func(b *testing.B, client *core.Instance, _ xnet.Destination) {
		benchmarkStreaming(b, client, true)
	})
}

func BenchmarkNativeDownload(b *testing.B) {
	benchmarkModes(b, func(b *testing.B, client *core.Instance, _ xnet.Destination) {
		benchmarkStreaming(b, client, false)
	})
}

func benchmarkStreaming(b *testing.B, client *core.Instance, upload bool) {
	b.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()
	completed := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			completed <- err
			return
		}
		defer conn.Close()
		var header [8]byte
		_, err = io.ReadFull(conn, header[:])
		if err == nil {
			total := int64(binary.BigEndian.Uint64(header[:]))
			if upload {
				_, err = io.CopyN(io.Discard, conn, total)
			} else {
				_, err = io.CopyN(conn, zeroReader{}, total)
			}
		}
		if err == nil {
			_, err = conn.Write([]byte{1})
		}
		completed <- err
	}()
	target, _ := xnet.ParseDestination("tcp:" + listener.Addr().String())
	conn := benchmarkDial(b, client, target)
	defer conn.Close()
	// Conventional b.N includes the receiver's final acknowledgement in the
	// measured interval, not just data queued into local socket/bridge buffers.
	const size = 65536
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], uint64(b.N)*size)
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	cpu := processCPU()
	if _, err := conn.Write(header[:]); err != nil {
		b.Fatal(err)
	}
	data := make([]byte, size)
	for range b.N {
		if upload {
			_, err = conn.Write(data)
		} else {
			_, err = io.ReadFull(conn, data)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	if _, err := io.ReadFull(conn, header[:1]); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	b.ReportMetric(float64(processCPU()-cpu)/float64(b.N), "process-cpu-ns/op")
	if err := <-completed; err != nil {
		b.Fatal(err)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func BenchmarkNativeLatency(b *testing.B) {
	benchmarkModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		benchmarkLatency(b, client, target, false)
	})
}

func BenchmarkNativeLoadedLatency(b *testing.B) {
	benchmarkModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		benchmarkLatency(b, client, target, true)
	})
}

func BenchmarkNativePacedLatency(b *testing.B) {
	benchmarkModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		benchmarkLatencyWithLoad(b, client, target, true, true)
	})
}

func BenchmarkNativeUDPLatency(b *testing.B) {
	benchmarkModes(b, func(b *testing.B, client *core.Instance, _ xnet.Destination) {
		_, udp := echoTargets(b)
		target, _ := xnet.ParseDestination("udp:" + udp)
		benchmarkLatency(b, client, target, false)
	})
}

func benchmarkLatency(b *testing.B, client *core.Instance, target xnet.Destination, loaded bool) {
	b.Helper()
	benchmarkLatencyWithLoad(b, client, target, loaded, false)
}

func benchmarkLatencyWithLoad(b *testing.B, client *core.Instance, target xnet.Destination, loaded, paced bool) {
	b.Helper()
	var backgroundBytes atomic.Int64
	pacedMiBPS := 64
	if value := os.Getenv("XHYS_BENCH_PACED_MIBPS"); paced && value != "" {
		var err error
		pacedMiBPS, err = strconv.Atoi(value)
		if err != nil || pacedMiBPS < 1 || pacedMiBPS > 4096 {
			b.Fatal("XHYS_BENCH_PACED_MIBPS must be 1..4096")
		}
	}
	if loaded {
		var workers sync.WaitGroup
		var load []net.Conn
		stop := make(chan struct{})
		b.Cleanup(func() {
			close(stop)
			for _, conn := range load {
				conn.Close()
			}
			workers.Wait()
		})
		for range 4 {
			conn := benchmarkDial(b, client, target)
			load = append(load, conn)
			data, got := make([]byte, 65536), make([]byte, 65536)
			benchmarkExchange(b, conn, data, got)
			workers.Go(func() {
				var ticks <-chan time.Time
				if paced {
					ticker := time.NewTicker(time.Second / time.Duration(2*pacedMiBPS))
					defer ticker.Stop()
					ticks = ticker.C
				}
				report := func(err error) {
					select {
					case <-stop:
					default:
						b.Errorf("background transfer stopped: %v", err)
					}
				}
				for {
					select {
					case <-stop:
						return
					default:
					}
					if paced {
						select {
						case <-stop:
							return
						case <-ticks:
						}
					}
					if _, err := conn.Write(data); err != nil {
						report(err)
						return
					}
					if _, err := io.ReadFull(conn, got); err != nil {
						report(err)
						return
					}
					backgroundBytes.Add(int64(2 * len(data)))
				}
			})
		}
	}
	conn := benchmarkDial(b, client, target)
	defer conn.Close()
	data, got := make([]byte, 64), make([]byte, 64)
	benchmarkExchange(b, conn, data, got)
	samples := make([]int64, 0, 16384)
	b.ReportAllocs()
	b.ResetTimer()
	started := time.Now()
	before := backgroundBytes.Load()
	for b.Loop() {
		start := time.Now()
		benchmarkExchange(b, conn, data, got)
		samples = append(samples, time.Since(start).Nanoseconds())
	}
	b.StopTimer()
	if loaded {
		b.ReportMetric(float64(backgroundBytes.Load()-before)/time.Since(started).Seconds()/1024/1024, "background-MiB/s")
	}
	slices.Sort(samples)
	for _, percentile := range []int{50, 95, 99} {
		index := min(len(samples)-1, len(samples)*percentile/100)
		b.ReportMetric(float64(samples[index])/1000, fmt.Sprintf("p%d-us", percentile))
	}
}
