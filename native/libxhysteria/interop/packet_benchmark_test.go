//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"os"
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func benchmarkPacketModes(b *testing.B, run func(*testing.B, *core.Instance, xnet.Destination)) {
	for _, mode := range []struct{ name, backend string }{{"go", "0"}, {"native", "1"}} {
		b.Run(mode.name, func(b *testing.B) {
			b.Setenv("XRAY_HYSTERIA_NATIVE", mode.backend)
			b.Setenv("XRAY_HYSTERIA_POOL_SIZE", "1")
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
			run(b, client, target)
			if mode.backend == "1" && native.Stats().StartedEngines-before != 2 {
				b.Fatal("packet benchmark fell back to Go or opened extra engines")
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
