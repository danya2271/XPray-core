//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
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
			b.Setenv("XRAY_HYSTERIA_NATIVE", "0")
			if os.Getenv("XHYS_BENCH_NATIVE_SERVER") == "1" {
				b.Setenv("XRAY_HYSTERIA_NATIVE", "1")
			}
			server, caPath := xrayServer(b, true)
			ca, err := os.ReadFile(caPath)
			if err != nil {
				b.Fatal(err)
			}
			b.Setenv("XRAY_HYSTERIA_NATIVE", mode.native)
			b.Setenv("XRAY_HYSTERIA_BRIDGE", mode.bridge)
			b.Setenv("XRAY_HYSTERIA_POOL_SIZE", mode.pool)
			client := embeddedClient(b, server, ca, true)
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
		if os.Getenv("XRAY_HYSTERIA_NATIVE") == "0" {
			b.Skip("Go baseline returns closed pipe under sustained rapid open/close; no valid result")
		}
		data, received := []byte("ping"), make([]byte, 4)
		// Warm pools/cert caches outside the measured interval.
		conn := benchmarkDial(b, client, target)
		benchmarkExchange(b, conn, data, received)
		conn.Close()
		before := native.Stats().StartedEngines
		b.ReportAllocs()
		b.ResetTimer()
		cpu := processCPU()
		for b.Loop() {
			conn := benchmarkDial(b, client, target)
			benchmarkExchange(b, conn, data, received)
			conn.Close()
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
