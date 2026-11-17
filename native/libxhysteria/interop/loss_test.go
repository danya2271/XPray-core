//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func processUDPDrops(t *testing.T) map[string]uint64 {
	t.Helper()
	descriptors, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	owned := make(map[string]bool)
	for _, descriptor := range descriptors {
		target, err := os.Readlink("/proc/self/fd/" + descriptor.Name())
		if err == nil && strings.HasPrefix(target, "socket:[") {
			owned[strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")] = true
		}
	}
	drops := make(map[string]uint64)
	for _, table := range []string{"udp", "udp6"} {
		data, err := os.ReadFile("/proc/self/net/" + table)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n")[1:] {
			fields := strings.Fields(line)
			if len(fields) < 10 || !owned[fields[9]] {
				continue
			}
			count, err := strconv.ParseUint(fields[len(fields)-1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			drops[fields[9]] = count
		}
	}
	return drops
}

func TestHysteriaPacketLossUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("packet-loss sampling takes twelve seconds per scenario")
	}
	for _, backend := range []string{"native", "go"} {
		for _, cascade := range []bool{false, true} {
			for _, size := range []int{256, 4096} {
				t.Run(fmt.Sprintf("%s/cascade_%t/size_%d", backend, cascade, size), func(t *testing.T) {
					maskKind := os.Getenv("XHYS_LOSS_PACKET_MASK")
					configureMasks := func(config *core.Config) {
						if maskKind == "" {
							return
						}
						masks := packetMasks(t, maskKind)
						for _, inbound := range config.Inbound {
							message, err := inbound.ReceiverSettings.GetInstance()
							if err != nil {
								t.Fatal(err)
							}
							receiver := message.(*proxyman.ReceiverConfig)
							receiver.StreamSettings.Udpmasks = masks
							inbound.ReceiverSettings = serial.ToTypedMessage(receiver)
						}
						for _, outbound := range config.Outbound {
							if outbound.SenderSettings == nil {
								continue
							}
							message, err := outbound.SenderSettings.GetInstance()
							if err != nil {
								t.Fatal(err)
							}
							sender := message.(*proxyman.SenderConfig)
							if sender.StreamSettings == nil {
								continue
							}
							sender.StreamSettings.Udpmasks = masks
							outbound.SenderSettings = serial.ToTypedMessage(sender)
						}
					}
					t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
					if backend == "go" {
						t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
					}
					t.Setenv("XRAY_HYSTERIA_BRIDGE", "shared")
					t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "1")
					before := native.Stats().StartedEngines
					exit, caPath, _ := xrayServerWithConfig(t, true, configureMasks)
					ca, err := os.ReadFile(caPath)
					if err != nil {
						t.Fatal(err)
					}
					server := exit
					if cascade {
						server, caPath, _ = xrayServerWithConfig(t, true, func(config *core.Config) {
							config.Outbound = embeddedClientConfig(t, exit, ca, true, auth).Outbound
							configureMasks(config)
						})
						ca, err = os.ReadFile(caPath)
						if err != nil {
							t.Fatal(err)
						}
					}
					clientConfig := embeddedClientConfig(t, server, ca, true, auth)
					configureMasks(clientConfig)
					client, err := core.New(clientConfig)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { client.Close() })
					if err := client.Start(); err != nil {
						t.Fatal(err)
					}
					tcp, udp := echoTargetsWithTimeout(t, 30*time.Second)
					ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
					defer cancel()
					open := func(network, address string) net.Conn {
						target, err := xnet.ParseDestination(network + ":" + address)
						if err != nil {
							t.Fatal(err)
						}
						conn, err := core.Dial(ctx, client, target)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { conn.Close() })
						return conn
					}
					conn := open("udp", udp)
					stopRead := context.AfterFunc(ctx, func() { conn.Close() })
					defer stopRead()
					warm := bytes.Repeat([]byte{0xa5}, size)
					if _, err := conn.Write(warm); err != nil {
						t.Fatal(err)
					}
					if _, err := io.ReadFull(conn, make([]byte, size)); err != nil {
						t.Fatal(err)
					}
					var backgroundBytes atomic.Int64
					var workers sync.WaitGroup
					var load []net.Conn
					stopped := make(chan struct{})
					defer workers.Wait()
					defer func() {
						close(stopped)
						conn.Close()
						for _, bulk := range load {
							bulk.Close()
						}
					}()
					if os.Getenv("XHYS_LOSS_LOAD") != "0" {
						for range 4 {
							load = append(load, open("tcp", tcp))
						}
					}
					for _, bulk := range load {
						workers.Go(func() {
							reportError := func(err error) {
								select {
								case <-stopped:
								default:
									t.Errorf("background TCP load stopped: %v", err)
								}
							}
							payload, reply := make([]byte, 65536), make([]byte, 65536)
							for {
								if _, err := bulk.Write(payload); err != nil {
									reportError(err)
									return
								}
								if _, err := io.ReadFull(bulk, reply); err != nil {
									reportError(err)
									return
								}
								backgroundBytes.Add(int64(2 * len(payload)))
							}
						})
					}
					minimumEngines := int64(2)
					if cascade {
						minimumEngines = 4
					}
					if backend == "native" && native.Stats().StartedEngines-before < minimumEngines {
						t.Fatal("native packet-loss test silently fell back to Go")
					}
					const packets = 2000
					received := make(chan int, 1)
					workers.Go(func() {
						buffer := make([]byte, size)
						seen := make([]bool, packets)
						count := 0
						defer func() { received <- count }()
						for {
							if _, err := io.ReadFull(conn, buffer); err != nil {
								return
							}
							sequence := int(binary.BigEndian.Uint32(buffer))
							if sequence >= packets || !bytes.Equal(buffer[4:], bytes.Repeat([]byte{0xa5}, size-4)) {
								t.Error("UDP payload corrupted")
								return
							}
							if !seen[sequence] {
								seen[sequence] = true
								count++
							}
						}
					})
					kernelBefore := processUDPDrops(t)
					started := time.Now()
					ticker := time.NewTicker(5 * time.Millisecond)
					payload := bytes.Repeat([]byte{0xa5}, size)
					sent := 0
					for sequence := range packets {
						select {
						case <-ctx.Done():
							t.Errorf("load sampling timed out: %v", ctx.Err())
						case <-ticker.C:
						}
						if ctx.Err() != nil {
							break
						}
						binary.BigEndian.PutUint32(payload, uint32(sequence))
						if _, err := conn.Write(payload); err != nil {
							t.Errorf("UDP send: %v", err)
							break
						}
						sent++
					}
					ticker.Stop()
					time.Sleep(2 * time.Second)
					var kernelDropped uint64
					for inode, after := range processUDPDrops(t) {
						if before, ok := kernelBefore[inode]; ok && after >= before {
							kernelDropped += after - before
						}
					}
					conn.Close()
					count := <-received
					t.Logf("backend=%s packet_mask=%s cascade=%t payload=%d sent=%d received=%d loss_pct=%.3f background_mibps=%.2f kernel_udp_dropped=%d", backend, maskKind, cascade, size, sent, count, 100*float64(sent-count)/float64(max(1, sent)), float64(backgroundBytes.Load())/(1<<20)/time.Since(started).Seconds(), kernelDropped)
					if count == 0 {
						t.Fatal("all application UDP packets lost")
					}
				})
			}
		}
	}
}
