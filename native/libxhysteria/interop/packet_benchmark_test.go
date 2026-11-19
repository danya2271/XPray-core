//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func benchmarkPacketModes(b *testing.B, run func(*testing.B, *core.Instance, xnet.Destination)) {
	benchmarkPacketModesWithPool(b, "1", 1, run)
}

func benchmarkPacketModesWithPool(b *testing.B, pool string, parallelism int, run func(*testing.B, *core.Instance, xnet.Destination)) {
	for _, mode := range []struct{ name, backend string }{{"go", "0"}, {"native", "1"}} {
		b.Run(mode.name, func(b *testing.B) {
			b.Setenv("XRAY_HYSTERIA_NATIVE", mode.backend)
			b.Setenv("XRAY_HYSTERIA_POOL_SIZE", pool)
			b.Setenv("XRAY_HYSTERIA_BRIDGE", "shared")
			before := native.Stats().StartedEngines
			masks := packetMasks(b, "header")
			server, caPath, _ := xrayServerWithConfig(b, false, func(config *core.Config) {
				message, err := config.Inbound[0].ReceiverSettings.GetInstance()
				if err != nil {
					b.Fatal(err)
				}
				receiver := message.(*proxyman.ReceiverConfig)
				receiver.StreamSettings.Udpmasks = masks
				config.Inbound[0].ReceiverSettings = serial.ToTypedMessage(receiver)
			})
			ca, err := os.ReadFile(caPath)
			if err != nil {
				b.Fatal(err)
			}
			config := embeddedClientConfig(b, server, ca, false, auth)
			message, err := config.Outbound[0].SenderSettings.GetInstance()
			if err != nil {
				b.Fatal(err)
			}
			sender := message.(*proxyman.SenderConfig)
			sender.StreamSettings.Udpmasks = masks
			config.Outbound[0].SenderSettings = serial.ToTypedMessage(sender)
			client, err := core.New(config)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { client.Close() })
			if err := client.Start(); err != nil {
				b.Fatal(err)
			}
			tcp, _ := echoTargets(b)
			target, err := xnet.ParseDestination("tcp:" + tcp)
			if err != nil {
				b.Fatal(err)
			}
			b.SetParallelism(parallelism)
			run(b, client, target)
			if mode.backend == "1" {
				engines := native.Stats().StartedEngines - before
				if engines < 2 || (pool != "0" && engines != 2) {
					b.Fatal("packet benchmark fell back to Go or opened extra engines")
				}
				if pool == "0" {
					b.ReportMetric(float64(engines-1), "native-client-engines")
				}
			}
		})
	}
}

func BenchmarkNativePacketTransfer(b *testing.B) {
	benchmarkPacketModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		connection := benchmarkDial(b, client, target)
		defer connection.Close()
		payload, reply := make([]byte, 65536), make([]byte, 65536)
		benchmarkExchange(b, connection, payload, reply)
		b.SetBytes(int64(2 * len(payload)))
		b.ReportAllocs()
		b.ResetTimer()
		cpu := processCPU()
		for b.Loop() {
			benchmarkExchange(b, connection, payload, reply)
		}
		b.StopTimer()
		b.ReportMetric(float64(processCPU()-cpu)/float64(b.N), "process-cpu-ns/op")
	})
}

func BenchmarkNativePacketUpload(b *testing.B) {
	benchmarkPacketModes(b, func(b *testing.B, client *core.Instance, _ xnet.Destination) {
		benchmarkStreaming(b, client, true)
	})
}

func BenchmarkNativePacketParallel(b *testing.B) {
	benchmarkPacketModes(b, benchmarkParallel)
}

func BenchmarkNativePacketParallelConnections(b *testing.B) {
	benchmarkPacketModesWithPool(b, "0", 4, benchmarkParallel)
}

func BenchmarkNativePacketParallelLatency(b *testing.B) {
	benchmarkConcurrentPacketModes(b, "1")
}

func BenchmarkNativePacketParallelConnectionLatency(b *testing.B) {
	benchmarkConcurrentPacketModes(b, "0")
}

func benchmarkConcurrentPacketModes(b *testing.B, pool string) {
	parallelism := 4
	if value := os.Getenv("XHYS_BENCH_PARALLELISM"); value != "" {
		var err error
		parallelism, err = strconv.Atoi(value)
		if err != nil || parallelism < 1 || parallelism > 64 {
			b.Fatal("XHYS_BENCH_PARALLELISM must be 1..64")
		}
	}
	benchmarkPacketModesWithPool(b, pool, parallelism, benchmarkConcurrentLatency)
}

func benchmarkConcurrentLatency(b *testing.B, client *core.Instance, target xnet.Destination) {
	var mutex sync.Mutex
	var samples []int64
	b.ReportAllocs()
	b.ResetTimer()
	cpu := processCPU()
	b.RunParallel(func(iterations *testing.PB) {
		connection := benchmarkDial(b, client, target)
		defer connection.Close()
		connection.SetDeadline(time.Now().Add(time.Minute))
		payload, reply := make([]byte, 64), make([]byte, 64)
		benchmarkExchange(b, connection, payload, reply)
		local := make([]int64, 0, 1024)
		for iterations.Next() {
			started := time.Now()
			benchmarkExchange(b, connection, payload, reply)
			local = append(local, time.Since(started).Nanoseconds())
		}
		mutex.Lock()
		samples = append(samples, local...)
		mutex.Unlock()
	})
	b.StopTimer()
	b.ReportMetric(float64(processCPU()-cpu)/float64(b.N), "process-cpu-ns/op")
	slices.Sort(samples)
	for _, percentile := range []int{50, 95, 99} {
		index := min(len(samples)-1, len(samples)*percentile/100)
		b.ReportMetric(float64(samples[index])/1000, fmt.Sprintf("p%d-us", percentile))
	}
}

func BenchmarkNativePacketDownload(b *testing.B) {
	benchmarkPacketModes(b, func(b *testing.B, client *core.Instance, _ xnet.Destination) {
		benchmarkStreaming(b, client, false)
	})
}

func BenchmarkNativePacketLatency(b *testing.B) {
	benchmarkPacketModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		benchmarkLatency(b, client, target, false)
	})
}

func BenchmarkNativePacketLoadedLatency(b *testing.B) {
	benchmarkPacketModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		benchmarkLatency(b, client, target, true)
	})
}

func BenchmarkNativePacketPacedLatency(b *testing.B) {
	benchmarkPacketModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		benchmarkLatencyWithLoad(b, client, target, true, true)
	})
}

func BenchmarkNativePacketUDPLatency(b *testing.B) {
	benchmarkPacketModes(b, func(b *testing.B, client *core.Instance, _ xnet.Destination) {
		_, address := echoTargets(b)
		target, err := xnet.ParseDestination("udp:" + address)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkLatency(b, client, target, false)
	})
}

func BenchmarkNativePacketUDPTransfer(b *testing.B) {
	benchmarkPacketModes(b, func(b *testing.B, client *core.Instance, _ xnet.Destination) {
		_, address := echoTargets(b)
		target, err := xnet.ParseDestination("udp:" + address)
		if err != nil {
			b.Fatal(err)
		}
		connection := benchmarkDial(b, client, target)
		defer connection.Close()
		payload, reply := make([]byte, 4096), make([]byte, 4096)
		benchmarkExchange(b, connection, payload, reply)
		b.SetBytes(int64(2 * len(payload)))
		b.ReportAllocs()
		b.ResetTimer()
		cpu := processCPU()
		for b.Loop() {
			benchmarkExchange(b, connection, payload, reply)
		}
		b.StopTimer()
		b.ReportMetric(float64(processCPU()-cpu)/float64(b.N), "process-cpu-ns/op")
	})
}
