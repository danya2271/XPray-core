//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
	"golang.org/x/sys/unix"
)

const scalingFlows = 32
const scalingBlock = 65536

func benchmarkScalingModes(b *testing.B, run func(*testing.B, *core.Instance, xnet.Destination, map[string]uint64)) {
	pool := os.Getenv("XHYS_SCALING_POOL")
	if pool == "" {
		pool = "8"
	}
	n, err := strconv.Atoi(pool)
	if err != nil || n < 1 || n > 8 {
		b.Fatal("XHYS_SCALING_POOL must be 1..8")
	}
	var existing map[string]uint64
	benchmarkPacketModesWithSetup(b, pool, 1, func(b *testing.B) {
		// Pool cleanup is asynchronous. A previous calibration's engines must
		// finish before setup, so they cannot consume this sample's CPU budget.
		until := time.Now().Add(3 * time.Second)
		for native.Stats().LiveEngines != 0 && time.Now().Before(until) {
			time.Sleep(10 * time.Millisecond)
		}
		if native.Stats().LiveEngines != 0 {
			b.Fatal("previous benchmark native engines did not stop")
		}
		existing = scalingSocketDrops(b)
	}, func(b *testing.B, client *core.Instance, target xnet.Destination) {
		run(b, client, target, existing)
	})
}

func scalingConnections(b *testing.B, client *core.Instance, target xnet.Destination, count int) []net.Conn {
	b.Helper()
	connections := make([]net.Conn, count)
	for index := range connections {
		connection := benchmarkDial(b, client, target)
		connections[index] = connection
		b.Cleanup(func() { connection.Close() })
		if err := connection.SetDeadline(time.Now().Add(110 * time.Second)); err != nil {
			b.Fatal(err)
		}
		benchmarkExchange(b, connection, []byte{0xa7}, make([]byte, 1))
	}
	return connections
}

func scalingExchange(connection net.Conn, payload, reply []byte) error {
	for remaining := payload; len(remaining) != 0; {
		n, err := connection.Write(remaining)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		remaining = remaining[n:]
	}
	if _, err := io.ReadFull(connection, reply); err != nil {
		return err
	}
	if !bytes.Equal(payload, reply) {
		return fmt.Errorf("scaling echo changed payload")
	}
	return nil
}

func reportScalingCPU(b *testing.B, started time.Time, cpu int64) {
	b.Helper()
	elapsed, used := time.Since(started), processCPU()-cpu
	b.ReportMetric(float64(used)/float64(b.N), "process-cpu-ns/op")
	b.ReportMetric(float64(used)/float64(elapsed), "process-cores")
	b.ReportMetric(scalingFlows, "tcp-flows")
}

func scalingSocketDrops(b *testing.B) map[string]uint64 {
	b.Helper()
	owned := make(map[string]bool)
	files, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		b.Fatal(err)
	}
	for _, file := range files {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", file.Name()))
		if err == nil && strings.HasPrefix(target, "socket:[") {
			owned[strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")] = true
		}
	}
	drops := make(map[string]uint64)
	for _, path := range []string{"/proc/self/net/udp", "/proc/self/net/udp6"} {
		data, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 13 || !owned[fields[9]] {
				continue
			}
			dropped, err := strconv.ParseUint(fields[len(fields)-1], 10, 64)
			if err != nil {
				b.Fatal(err)
			}
			drops[fields[9]] = dropped
			if os.Getenv("XHYS_SCALING_SOCKET_DIAG") == "1" {
				b.Logf("scaling_udp inode=%s local=%s peer=%s rx_queue=%s drops=%d",
					fields[9], fields[1], fields[2], fields[4], dropped)
			}
		}
	}
	return drops
}

func scalingStartDrops(b *testing.B, existing map[string]uint64) map[string]uint64 {
	b.Helper()
	drops := scalingSocketDrops(b)
	// A prior benchmark's endpoint shutdown may still be finishing in this
	// process. Only this fixture's newly created sockets belong to its gate.
	for inode := range existing {
		delete(drops, inode)
	}
	if len(drops) == 0 {
		b.Fatal("scaling fixture has no new owned UDP sockets")
	}
	// A completed flow can retire its UDP endpoint before the final snapshot.
	// Hold one duplicate per socket only for observation, without reading or
	// writing it, so the kernel drop counter remains available until cleanup.
	files, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		b.Fatal(err)
	}
	pinned := make(map[string]bool)
	for _, file := range files {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", file.Name()))
		if err != nil || !strings.HasPrefix(target, "socket:[") {
			continue
		}
		inode := strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")
		if _, present := drops[inode]; !present || pinned[inode] {
			continue
		}
		fd, err := strconv.Atoi(file.Name())
		if err != nil {
			b.Fatal(err)
		}
		if os.Getenv("XHYS_SCALING_SOCKET_DIAG") == "1" {
			receive, _ := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF)
			send, _ := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF)
			b.Logf("scaling_udp_buffers inode=%s recv=%d send=%d", inode, receive, send)
		}
		duplicate, err := unix.Dup(fd)
		if err != nil {
			b.Fatal(err)
		}
		unix.CloseOnExec(duplicate)
		b.Cleanup(func() { unix.Close(duplicate) })
		pinned[inode] = true
	}
	if len(pinned) != len(drops) {
		b.Fatal("could not retain every measured UDP socket counter")
	}
	return drops
}

func reportScalingDrops(b *testing.B, before map[string]uint64) {
	b.Helper()
	after := scalingSocketDrops(b)
	var total uint64
	for inode, count := range before {
		current, exists := after[inode]
		if !exists || current < count {
			b.Fatal("scaling fixture UDP identity changed")
		}
		total += current - count
	}
	b.ReportMetric(float64(total), "kernel-udp-drops")
}

func benchmarkScalingExchange(b *testing.B, size int) {
	benchmarkScalingModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination, existing map[string]uint64) {
		connections := scalingConnections(b, client, target, scalingFlows)
		start := make(chan struct{})
		errors := make(chan error, scalingFlows)
		samples := make([][]int64, scalingFlows)
		var group sync.WaitGroup
		for index, connection := range connections {
			iterations := b.N / scalingFlows
			if index < b.N%scalingFlows {
				iterations++
			}
			group.Add(1)
			go func() {
				defer group.Done()
				payload, reply := bytes.Repeat([]byte{0xa7}, size), make([]byte, size)
				local := make([]int64, 0, iterations)
				<-start
				for range iterations {
					at := time.Now()
					if err := scalingExchange(connection, payload, reply); err != nil {
						errors <- err
						return
					}
					local = append(local, time.Since(at).Nanoseconds())
				}
				samples[index] = local
			}()
		}
		if size > 64 {
			b.SetBytes(int64(size * 2))
		}
		b.ReportAllocs()
		drops := scalingStartDrops(b, existing)
		b.ResetTimer()
		at, cpu := time.Now(), processCPU()
		close(start)
		group.Wait()
		b.StopTimer()
		reportScalingCPU(b, at, cpu)
		reportScalingDrops(b, drops)
		close(errors)
		for err := range errors {
			b.Fatal(err)
		}
		latencies := make([]int64, 0, b.N)
		for _, local := range samples {
			latencies = append(latencies, local...)
		}
		if len(latencies) != b.N {
			b.Fatal("scaling exchange count changed")
		}
		slices.Sort(latencies)
		for _, percentile := range []int{50, 99} {
			b.ReportMetric(float64(latencies[min(len(latencies)-1, len(latencies)*percentile/100)])/1000,
				fmt.Sprintf("p%d-us", percentile))
		}
	})
}

func BenchmarkNativeScalingDuplex(b *testing.B) { benchmarkScalingExchange(b, scalingBlock) }
func BenchmarkNativeScalingSmall(b *testing.B)  { benchmarkScalingExchange(b, 64) }

func benchmarkScalingStreaming(b *testing.B, upload bool) {
	benchmarkScalingModes(b, func(b *testing.B, client *core.Instance, _ xnet.Destination, existing map[string]uint64) {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { listener.Close() })
		release := make(chan struct{})
		b.Cleanup(func() { close(release) })
		completed := make(chan error, scalingFlows)
		go func() {
			for range scalingFlows {
				connection, err := listener.Accept()
				if err != nil {
					completed <- err
					continue
				}
				go func() {
					defer connection.Close()
					connection.SetDeadline(time.Now().Add(110 * time.Second))
					if _, err := connection.Write([]byte{1}); err != nil {
						completed <- err
						return
					}
					var header [8]byte
					_, err := io.ReadFull(connection, header[:])
					if err == nil {
						total := int64(binary.BigEndian.Uint64(header[:]))
						if upload {
							_, err = io.CopyN(io.Discard, connection, total)
						} else {
							_, err = io.CopyN(connection, zeroReader{}, total)
						}
					}
					if err == nil {
						_, err = connection.Write([]byte{1})
					}
					completed <- err
					// Preserve the same established flows through the counter
					// snapshot, including the zero-length calibration flows.
					<-release
				}()
			}
		}()
		target, err := xnet.ParseDestination("tcp:" + listener.Addr().String())
		if err != nil {
			b.Fatal(err)
		}
		connections := make([]net.Conn, scalingFlows)
		for index := range connections {
			connection := benchmarkDial(b, client, target)
			connections[index] = connection
			b.Cleanup(func() { connection.Close() })
			connection.SetDeadline(time.Now().Add(110 * time.Second))
			var ready [1]byte
			if _, err := io.ReadFull(connection, ready[:]); err != nil || ready[0] != 1 {
				b.Fatalf("scaling target startup: %v", err)
			}
		}
		start := make(chan struct{})
		errors := make(chan error, scalingFlows)
		var group sync.WaitGroup
		for index, connection := range connections {
			iterations := b.N / scalingFlows
			if index < b.N%scalingFlows {
				iterations++
			}
			group.Add(1)
			go func() {
				defer group.Done()
				payload := make([]byte, scalingBlock)
				var header [8]byte
				binary.BigEndian.PutUint64(header[:], uint64(iterations)*scalingBlock)
				<-start
				if _, err := connection.Write(header[:]); err != nil {
					errors <- err
					return
				}
				for range iterations {
					var err error
					if upload {
						_, err = connection.Write(payload)
					} else {
						_, err = io.ReadFull(connection, payload)
					}
					if err != nil {
						errors <- err
						return
					}
				}
				if _, err := io.ReadFull(connection, header[:1]); err != nil || header[0] != 1 {
					errors <- fmt.Errorf("receiver completion marker: %v", err)
				}
			}()
		}
		b.SetBytes(scalingBlock)
		b.ReportAllocs()
		drops := scalingStartDrops(b, existing)
		b.ResetTimer()
		at, cpu := time.Now(), processCPU()
		close(start)
		group.Wait()
		b.StopTimer()
		reportScalingCPU(b, at, cpu)
		reportScalingDrops(b, drops)
		close(errors)
		for err := range errors {
			b.Fatal(err)
		}
		for range scalingFlows {
			if err := <-completed; err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkNativeScalingUpload(b *testing.B)   { benchmarkScalingStreaming(b, true) }
func BenchmarkNativeScalingDownload(b *testing.B) { benchmarkScalingStreaming(b, false) }

func BenchmarkNativeScalingPacedLatency(b *testing.B) {
	benchmarkScalingModes(b, func(b *testing.B, client *core.Instance, target xnet.Destination, existing map[string]uint64) {
		connections := scalingConnections(b, client, target, scalingFlows)
		probe := scalingConnections(b, client, target, 1)[0]
		stop := make(chan struct{})
		errors := make(chan error, scalingFlows)
		var group sync.WaitGroup
		stopBackground := sync.OnceFunc(func() {
			close(stop)
			group.Wait()
		})
		defer stopBackground()
		var transferred atomic.Int64
		// The same offered 64 MiB/s duplex background across all CPU budgets.
		// Each flow exchanges 16 KiB in both directions every 15.625 ms.
		for _, connection := range connections {
			group.Add(1)
			go func() {
				defer group.Done()
				payload := bytes.Repeat([]byte{0xa7}, 16384)
				reply := make([]byte, len(payload))
				ticker := time.NewTicker(time.Second / 64)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						if err := scalingExchange(connection, payload, reply); err != nil {
							errors <- err
							return
						}
						transferred.Add(int64(2 * len(payload)))
					}
				}
			}()
		}
		time.Sleep(100 * time.Millisecond)
		payload, reply := bytes.Repeat([]byte{0xa7}, 64), make([]byte, 64)
		samples := make([]int64, 0, b.N)
		b.ReportAllocs()
		drops := scalingStartDrops(b, existing)
		b.ResetTimer()
		at, cpu, before := time.Now(), processCPU(), transferred.Load()
		var probeError error
		for range b.N {
			began := time.Now()
			if probeError = scalingExchange(probe, payload, reply); probeError != nil {
				break
			}
			samples = append(samples, time.Since(began).Nanoseconds())
		}
		b.StopTimer()
		elapsed, background := time.Since(at), transferred.Load()-before
		reportScalingCPU(b, at, cpu)
		reportScalingDrops(b, drops)
		b.ReportMetric(float64(background)/elapsed.Seconds()/(1024*1024),
			"background-MiB/s")
		stopBackground()
		close(errors)
		for err := range errors {
			b.Fatal(err)
		}
		if probeError != nil {
			b.Fatal(probeError)
		}
		slices.Sort(samples)
		for _, percentile := range []int{50, 99} {
			b.ReportMetric(float64(samples[min(len(samples)-1, len(samples)*percentile/100)])/1000,
				fmt.Sprintf("p%d-us", percentile))
		}
	})
}
