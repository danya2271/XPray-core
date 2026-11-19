//go:build xhysteria_interop && xhysteria_native && cgo && linux

package interop_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
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
	"golang.org/x/sys/unix"
)

type isolatedServer struct {
	Server string
	CA     []byte
	TCP    string
	UDP    string
}

func TestIsolatedClientServerProcess(t *testing.T) {
	if os.Getenv("XHYS_ISOLATED_SERVER") != "1" {
		t.Skip("subprocess fixture for client benchmarks")
	}
	server, caPath, _ := xrayServerWithConfig(t, false, func(config *core.Config) {
		message, err := config.Inbound[0].ReceiverSettings.GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		receiver := message.(*proxyman.ReceiverConfig)
		receiver.StreamSettings.Udpmasks = packetMasks(t, "header")
		config.Inbound[0].ReceiverSettings = serial.ToTypedMessage(receiver)
	})
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	tcp, udp := echoTargetsWithTimeout(t, 2*time.Minute)
	data, err := json.Marshal(isolatedServer{Server: server, CA: ca, TCP: tcp, UDP: udp})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("isolated_server=%s\n", data)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func isolatedClient(b *testing.B) (*core.Instance, xnet.Destination) {
	b.Helper()
	executable, err := os.Executable()
	if err != nil {
		b.Fatal(err)
	}
	cpus := os.Getenv("XHYS_CLIENT_SERVER_CPUS")
	if cpus == "" {
		b.Fatal("XHYS_CLIENT_SERVER_CPUS must select CPUs separate from the client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	command := exec.CommandContext(ctx, "taskset", "-c", cpus, executable, "-test.run=^TestIsolatedClientServerProcess$", "-test.timeout=120s")
	command.Env = append(os.Environ(), "XHYS_ISOLATED_SERVER=1", "GOMAXPROCS=4", "XRAY_HYSTERIA_NATIVE=1", "XRAY_HYSTERIA_SERVER_WORKERS=4", "XRAY_NET_DIAG=0")
	input, err := command.StdinPipe()
	if err != nil {
		cancel()
		b.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		input.Close()
		cancel()
		b.Fatal(err)
	}
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	if err := command.Start(); err != nil {
		input.Close()
		cancel()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		input.Close()
		stopped := make(chan error, 1)
		go func() { stopped <- command.Wait() }()
		select {
		case err := <-stopped:
			if err != nil {
				b.Errorf("isolated server: %v\n%s", err, diagnostics.String())
			}
		case <-time.After(5 * time.Second):
			cancel()
			b.Errorf("isolated server did not stop: %v", <-stopped)
		}
		cancel()
	})
	var server isolatedServer
	scanner := bufio.NewScanner(output)
	for scanner.Scan() {
		if data, found := strings.CutPrefix(scanner.Text(), "isolated_server="); found {
			if err := json.Unmarshal([]byte(data), &server); err != nil {
				b.Fatal(err)
			}
			break
		}
	}
	if server.Server == "" || server.TCP == "" || len(server.CA) == 0 {
		b.Fatalf("isolated server did not publish endpoints: %v", scanner.Err())
	}
	go func() {
		for scanner.Scan() {
		}
	}()
	config := embeddedClientConfig(b, server.Server, server.CA, false, auth)
	message, err := config.Outbound[0].SenderSettings.GetInstance()
	if err != nil {
		b.Fatal(err)
	}
	sender := message.(*proxyman.SenderConfig)
	sender.StreamSettings.Udpmasks = packetMasks(b, "header")
	config.Outbound[0].SenderSettings = serial.ToTypedMessage(sender)
	before := native.Stats()
	client, err := core.New(config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		client.Close()
		deadline := time.Now().Add(3 * time.Second)
		for native.Stats().LiveEngines > before.LiveEngines && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if native.Stats().LiveEngines != before.LiveEngines {
			b.Error("isolated client left native engines running")
		}
	})
	if err := client.Start(); err != nil {
		b.Fatal(err)
	}
	target, err := xnet.ParseDestination("tcp:" + server.TCP)
	if err != nil {
		b.Fatal(err)
	}
	return client, target
}

func clientWorkerCPU() map[string]uint64 {
	result := make(map[string]uint64)
	paths, _ := filepath.Glob("/proc/self/task/*/comm")
	for _, path := range paths {
		name, err := os.ReadFile(path)
		if err != nil || !strings.HasPrefix(string(name), "xhysteria-share") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(filepath.Dir(path), "schedstat"))
		if err != nil {
			continue
		}
		fields := strings.Fields(string(data))
		if len(fields) == 0 {
			continue
		}
		if elapsed, err := strconv.ParseUint(fields[0], 10, 64); err == nil {
			result[filepath.Base(filepath.Dir(path))] = elapsed
		}
	}
	return result
}

func benchmarkIsolatedClient(b *testing.B, size int, parallel bool) {
	backend := os.Getenv("XHYS_CLIENT_BACKEND")
	if backend == "" {
		backend = "native"
	}
	if backend != "native" && backend != "go" {
		b.Fatal("XHYS_CLIENT_BACKEND must be native or go")
	}
	b.Setenv("XRAY_HYSTERIA_NATIVE", "1")
	if backend == "go" {
		b.Setenv("XRAY_HYSTERIA_NATIVE", "0")
	}
	b.Setenv("XRAY_HYSTERIA_BRIDGE", "shared")
	parallelism := 1
	if value := os.Getenv("XHYS_BENCH_PARALLELISM"); value != "" {
		var err error
		parallelism, err = strconv.Atoi(value)
		if err != nil || parallelism < 1 || parallelism > 16 {
			b.Fatal("XHYS_BENCH_PARALLELISM must be 1..16")
		}
	}
	flows := 1
	if parallel {
		flows = parallelism * runtime.GOMAXPROCS(0)
	}
	before := native.Stats().StartedEngines
	client, target := isolatedClient(b)
	connections := make([]*benchmarkConn, flows)
	for index := range connections {
		connections[index] = benchmarkDial(b, client, target).(*benchmarkConn)
		b.Cleanup(func() { connections[index].Close() })
		connections[index].SetDeadline(time.Now().Add(time.Minute))
		benchmarkExchange(b, connections[index], make([]byte, size), make([]byte, size))
	}
	engines := native.Stats().StartedEngines - before
	if backend == "native" && engines == 0 || backend == "go" && engines != 0 {
		b.Fatal("isolated benchmark used an unexpected client backend")
	}
	b.SetParallelism(parallelism)
	var mutex sync.Mutex
	var samples []int64
	var next atomic.Uint32
	threads := clientWorkerCPU()
	b.ReportAllocs()
	if size > 64 {
		b.SetBytes(int64(2 * size))
	}
	b.ResetTimer()
	cpu := processCPU()
	started := time.Now()
	if parallel {
		b.RunParallel(func(iterations *testing.PB) {
			connection := connections[int(next.Add(1))-1]
			payload, reply := make([]byte, size), make([]byte, size)
			local := make([]int64, 0, 1024)
			for iterations.Next() {
				exchange := time.Now()
				benchmarkExchange(b, connection, payload, reply)
				local = append(local, time.Since(exchange).Nanoseconds())
			}
			mutex.Lock()
			samples = append(samples, local...)
			mutex.Unlock()
		})
	} else {
		payload, reply := make([]byte, size), make([]byte, size)
		for b.Loop() {
			exchange := time.Now()
			benchmarkExchange(b, connections[0], payload, reply)
			samples = append(samples, time.Since(exchange).Nanoseconds())
		}
	}
	b.StopTimer()
	elapsed := time.Since(started)
	used := processCPU() - cpu
	var hottest uint64
	active := 0
	after := clientWorkerCPU()
	for thread, total := range after {
		if before, found := threads[thread]; found && total >= before {
			delta := total - before
			hottest = max(hottest, delta)
			if delta > uint64(elapsed.Nanoseconds()/100) {
				active++
			}
		}
	}
	b.ReportMetric(float64(used)/float64(b.N), "client-cpu-ns/op")
	b.ReportMetric(float64(used)/float64(elapsed), "client-cpu-cores")
	b.ReportMetric(float64(engines), "native-client-engines")
	b.ReportMetric(float64(len(after)), "rust-workers")
	b.ReportMetric(float64(active), "rust-active-workers")
	b.ReportMetric(100*float64(hottest)/float64(elapsed), "rust-max-core-pct")
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(usage.Maxrss)/1024, "client-peak-rss-MiB")
	slices.Sort(samples)
	for _, percentile := range []int{50, 95, 99} {
		index := min(len(samples)-1, len(samples)*percentile/100)
		b.ReportMetric(float64(samples[index])/1000, fmt.Sprintf("p%d-us", percentile))
	}
}

func BenchmarkIsolatedClientLatency(b *testing.B) {
	benchmarkIsolatedClient(b, 64, false)
}

func BenchmarkIsolatedClientParallelLatency(b *testing.B) {
	benchmarkIsolatedClient(b, 64, true)
}

func BenchmarkIsolatedClientParallelTransfer(b *testing.B) {
	benchmarkIsolatedClient(b, 65536, true)
}
