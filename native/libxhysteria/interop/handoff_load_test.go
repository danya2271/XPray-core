//go:build linux && xhysteria_native && xhysteria_interop && cgo

package interop_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/handoff"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

type handoffLoadSnapshot struct {
	SupervisorCPU int64
	ServerCPU     int64
	SupervisorRSS int64
	ServerRSS     int64
	Drops         uint64
	Counts        []uint64
	Ingress       native.NativeIngressStats
}

type handoffLoadInfo struct {
	Server string
	CA     []byte
	TCP    string
	UDP    string
}

func TestHandoffLoadServerProcess(t *testing.T) {
	if os.Getenv("XHYS_HANDOFF_FIXTURE") != "1" {
		t.Skip("server/supervisor subprocess for handoff measurements")
	}
	root, err := os.MkdirTemp("", "xh-load-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
	certificatePEM, keyPEM := certificate.ToPEM()
	certPath, keyPath := filepath.Join(root, "cert.pem"), filepath.Join(root, "key.pem")
	for path, data := range map[string][]byte{certPath: certificatePEM, keyPath: keyPEM} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	local := *public.LocalAddr().(*net.UDPAddr)
	managed := os.Getenv("XHYS_HANDOFF_MODE") == "managed"
	key := [16]byte{4, 7, 1, 9}
	var ingress interface {
		Add(byte) (*os.File, error)
		Activate(byte) error
		Dropped() uint64
		Close() error
	}
	if managed {
		if os.Getenv("XHYS_HANDOFF_TUNE_PUBLIC") == "1" {
			native.TunePacketSocket(public, true, false)
		}
		if os.Getenv("XHYS_HANDOFF_NATIVE_INGRESS") == "1" {
			if os.Getenv("XHYS_HANDOFF_PACKET_BATCH") != "1" {
				t.Fatal("native ingress requires segmented-frame-capable generations")
			}
			ingress, err = native.NewNativeIngress(public, key, 0, []native.UDPMaskConfig{
				{Kind: "wireguard"}, {Kind: "salamander", Password: psk},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer public.Close()
		} else {
			var masks []finalmask.UDPMask
			for _, mask := range packetMasks(t, "header") {
				value, err := mask.GetInstance()
				if err != nil {
					t.Fatal(err)
				}
				masks = append(masks, value.(finalmask.UDPMask))
			}
			wrapped := finalmask.NewFinalMask(nil, masks, nil, nil, nil,
				func(context.Context, xnet.Addr) (xnet.PacketConn, error) { return public, nil })
			masked, err := wrapped.ListenPacket(context.Background(), &local)
			if err != nil {
				t.Fatal(err)
			}
			ingress, err = handoff.NewUDPIngress(masked, key, 0, 4096, 30*time.Second)
			if err != nil {
				t.Fatal(err)
			}
		}
		defer ingress.Close()
	} else {
		public.Close()
	}
	tcp, udp := echoTargetsWithTimeout(t, 3*time.Minute)
	ticksRaw, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		t.Fatal(err)
	}
	ticks, err := strconv.ParseInt(strings.TrimSpace(string(ticksRaw)), 10, 64)
	if err != nil || ticks <= 0 {
		t.Fatal("invalid process CPU clock tick frequency")
	}
	var processes []*exec.Cmd
	var controls []handoff.ControlSocket
	start := func(slot byte) {
		directory := filepath.Join(root, strconv.Itoa(int(slot)))
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
		config := fmt.Sprintf(`{"log":{"loglevel":"warning"},"inbounds":[{"tag":"hy","listen":"127.0.0.1","port":%d,"protocol":"hysteria","settings":{"users":[{"auth":%q}]},"streamSettings":{"network":"hysteria","security":"tls","hysteriaSettings":{"native":%t},"tlsSettings":{"certificates":[{"certificateFile":%q,"keyFile":%q,"oneTimeLoading":true}]},"finalmask":{"quicParams":{"disableChromeParrot":true},"udp":[{"type":"salamander","settings":{"password":%q}},{"type":"mkcp-legacy","settings":{"header":"wireguard"}}]}}}],"outbounds":[{"protocol":"freedom","settings":{"finalRules":[{"action":"allow","ip":["127.0.0.1/32"]}]}}]}`,
			local.Port, auth, os.Getenv("XHYS_HANDOFF_BACKEND") != "go", certPath, keyPath, psk)
		configPath := filepath.Join(directory, "config.json")
		if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		arguments := []string{"run", "-config", configPath}
		var file *os.File
		var transmit *os.File
		control := handoff.ControlSocket(filepath.Join(directory, "control.sock"))
		if managed {
			file, err = ingress.Add(slot)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			manifest := handoff.Manifest{Control: string(control), UDP: []handoff.UDPBinding{{Tag: "hy", Address: "127.0.0.1", Port: local.Port, FD: 3, Key: key, Slot: slot,
				PacketBatch: os.Getenv("XHYS_HANDOFF_PACKET_BATCH") == "1"}}}
			if os.Getenv("XHYS_HANDOFF_DIRECT_TX") == "1" {
				if os.Getenv("XHYS_HANDOFF_NATIVE_INGRESS") != "1" || os.Getenv("XHYS_HANDOFF_BACKEND") == "go" {
					t.Fatal("direct transmit requires native ingress and server")
				}
				transmit, err = public.File()
				if err != nil {
					t.Fatal(err)
				}
				defer transmit.Close()
				manifest.UDP[0].TransmitFD = 4
			}
			data, _ := json.Marshal(manifest)
			manifestPath := filepath.Join(directory, "manifest.json")
			if err := os.WriteFile(manifestPath, data, 0600); err != nil {
				t.Fatal(err)
			}
			arguments = append(arguments, "-handoff-manifest", manifestPath)
		}
		command := exec.Command(os.Getenv("XHYS_HANDOFF_SERVER_BINARY"), arguments...)
		command.Env = append(os.Environ(), "XRAY_HYSTERIA_NATIVE=0", "XRAY_NET_DIAG=0")
		if os.Getenv("XHYS_HANDOFF_BACKEND") != "go" {
			command.Env = append(command.Env, "XRAY_HYSTERIA_NATIVE=1")
		}
		log, err := os.Create(filepath.Join(directory, "server.log"))
		if err != nil {
			t.Fatal(err)
		}
		command.Stdout, command.Stderr = log, log
		if file != nil {
			command.ExtraFiles = []*os.File{file}
			if transmit != nil {
				command.ExtraFiles = append(command.ExtraFiles, transmit)
			}
		}
		if err := command.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		processes = append(processes, command)
		controls = append(controls, control)
		t.Cleanup(func() {
			command.Process.Kill()
			command.Wait()
			log.Close()
			if t.Failed() {
				data, _ := os.ReadFile(log.Name())
				t.Log(string(data))
			}
		})
		deadline := time.Now().Add(5 * time.Second)
		for {
			if managed {
				if response, err := control.Command(t.Context(), "status"); err == nil && response.State == "paused" {
					break
				}
			} else {
				probe, err := net.ListenUDP("udp4", &local)
				if errors.Is(err, syscall.EADDRINUSE) {
					break
				}
				if err == nil {
					probe.Close()
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("load server did not become ready")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	start(0)
	if managed {
		if _, err := controls[0].Command(t.Context(), "resume"); err != nil {
			t.Fatal(err)
		}
	}
	encoder := json.NewEncoder(os.Stdout)
	if err := encoder.Encode(handoffLoadInfo{Server: local.String(), CA: certificatePEM, TCP: tcp, UDP: udp}); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if scanner.Text() == "switch" {
			if !managed || len(processes) != 1 {
				t.Fatal("switch requires one managed generation")
			}
			start(1)
			if _, err := controls[0].Command(t.Context(), "pause"); err != nil {
				t.Fatal(err)
			}
			if _, err := controls[1].Command(t.Context(), "resume"); err != nil {
				t.Fatal(err)
			}
			if err := ingress.Activate(1); err != nil {
				t.Fatal(err)
			}
		}
		var snapshot handoffLoadSnapshot
		snapshot.SupervisorCPU = processCPU()
		_, snapshot.SupervisorRSS = handoffProcessUsage(t, os.Getpid(), ticks)
		for index, process := range processes {
			cpu, rss := handoffProcessUsage(t, process.Process.Pid, ticks)
			snapshot.ServerCPU += cpu
			snapshot.ServerRSS += rss
			if managed {
				response, err := controls[index].Command(t.Context(), "quic-status")
				if err != nil {
					t.Fatal(err)
				}
				status := response.UDP[fmt.Sprintf("hy:%d", local.Port)]
				if !status.Known {
					t.Fatal("generation connection count is unknown")
				}
				snapshot.Counts = append(snapshot.Counts, status.Count)
			}
		}
		if ingress != nil {
			snapshot.Drops = ingress.Dropped()
			if value, ok := ingress.(*native.NativeIngress); ok {
				snapshot.Ingress = value.Stats()
			}
		}
		if err := encoder.Encode(snapshot); err != nil {
			t.Fatal(err)
		}
	}
}

func handoffProcessUsage(t testing.TB, pid int, ticks int64) (int64, int64) {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+2:])
	user, userErr := strconv.ParseInt(fields[11], 10, 64)
	system, systemErr := strconv.ParseInt(fields[12], 10, 64)
	if userErr != nil || systemErr != nil {
		t.Fatal("cannot parse process CPU usage")
	}
	data, err = os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			rss, err := strconv.ParseInt(strings.Fields(line)[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return (user + system) * int64(time.Second) / ticks, rss
		}
	}
	t.Fatal("process RSS high-water mark unavailable")
	return 0, 0
}

type handoffLoadFixture struct {
	info    handoffLoadInfo
	input   io.WriteCloser
	decoder *json.Decoder
}

func newHandoffLoadFixture(t testing.TB) *handoffLoadFixture {
	t.Helper()
	if os.Getenv("XHYS_HANDOFF_SERVER_BINARY") == "" {
		t.Skip("set XHYS_HANDOFF_SERVER_BINARY to the Xray version under measurement")
	}
	if mode := os.Getenv("XHYS_HANDOFF_MODE"); mode != "" && mode != "direct" && mode != "managed" {
		t.Fatal("XHYS_HANDOFF_MODE must be direct or managed")
	}
	if backend := os.Getenv("XHYS_HANDOFF_BACKEND"); backend != "" && backend != "native" && backend != "go" {
		t.Fatal("XHYS_HANDOFF_BACKEND must be native or go")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cpus := os.Getenv("XHYS_HANDOFF_SERVER_CPUS")
	if cpus == "" {
		t.Fatal("XHYS_HANDOFF_SERVER_CPUS must separate server/supervisor from client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	command := exec.CommandContext(ctx, "taskset", "-c", cpus, executable, "-test.run=^TestHandoffLoadServerProcess$", "-test.timeout=170s")
	processors := os.Getenv("XHYS_HANDOFF_SERVER_PROCS")
	if processors == "" {
		processors = "4"
	}
	command.Env = append(os.Environ(), "XHYS_HANDOFF_FIXTURE=1", "GOMAXPROCS="+processors, "XRAY_HYSTERIA_SERVER_WORKERS="+processors)
	input, err := command.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		input.Close()
		cancel()
		t.Fatal(err)
	}
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	if err := command.Start(); err != nil {
		input.Close()
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		input.Close()
		stopped := make(chan error, 1)
		go func() { stopped <- command.Wait() }()
		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("handoff fixture: %v: %s", err, diagnostics.String())
			}
		case <-time.After(5 * time.Second):
			cancel()
			t.Errorf("handoff fixture failed to stop: %v", <-stopped)
		}
		cancel()
	})
	fixture := &handoffLoadFixture{input: input, decoder: json.NewDecoder(output)}
	if err := fixture.decoder.Decode(&fixture.info); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture *handoffLoadFixture) snapshot(t testing.TB, command string) handoffLoadSnapshot {
	t.Helper()
	if _, err := fmt.Fprintln(fixture.input, command); err != nil {
		t.Fatal(err)
	}
	var result handoffLoadSnapshot
	if err := fixture.decoder.Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func (fixture *handoffLoadFixture) client(t testing.TB) *core.Instance {
	t.Helper()
	config := embeddedClientConfig(t, fixture.info.Server, fixture.info.CA, false, auth)
	message, err := config.Outbound[0].SenderSettings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	sender := message.(*proxyman.SenderConfig)
	sender.StreamSettings.Udpmasks = packetMasks(t, "header")
	config.Outbound[0].SenderSettings = serial.ToTypedMessage(sender)
	client, err := core.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	return client
}

func benchmarkHandoffLoad(b *testing.B, size int) {
	if runtime.GOMAXPROCS(0) != 8 {
		b.Fatal("handoff load benchmarks require -test.cpu 8")
	}
	fixture := newHandoffLoadFixture(b)
	engines := native.Stats().StartedEngines
	client := fixture.client(b)
	target, err := xnet.ParseDestination("tcp:" + fixture.info.TCP)
	if err != nil {
		b.Fatal(err)
	}
	const flows = 32
	connections := make([]net.Conn, flows)
	for index := range connections {
		connections[index] = benchmarkDial(b, client, target)
		b.Cleanup(func() { connections[index].Close() })
		connections[index].SetDeadline(time.Now().Add(90 * time.Second))
		benchmarkExchange(b, connections[index], make([]byte, size), make([]byte, size))
	}
	startedEngines := native.Stats().StartedEngines - engines
	if backend := os.Getenv("XRAY_HYSTERIA_NATIVE"); backend == "1" && startedEngines == 0 || backend == "0" && startedEngines != 0 {
		b.Fatal("load generator used an unexpected Hysteria backend")
	}
	before := fixture.snapshot(b, "stats")
	var next atomic.Uint32
	var mutex sync.Mutex
	var samples []int64
	b.SetParallelism(flows / 8)
	if size > 64 {
		b.SetBytes(int64(2 * size))
	}
	b.ReportAllocs()
	b.ResetTimer()
	started, cpu := time.Now(), processCPU()
	b.RunParallel(func(iterations *testing.PB) {
		connection := connections[next.Add(1)-1]
		payload, reply := bytes.Repeat([]byte{0xa7}, size), make([]byte, size)
		local := make([]int64, 0, 1024)
		for iterations.Next() {
			at := time.Now()
			benchmarkExchange(b, connection, payload, reply)
			local = append(local, time.Since(at).Nanoseconds())
		}
		mutex.Lock()
		samples = append(samples, local...)
		mutex.Unlock()
	})
	b.StopTimer()
	elapsed, clientCPU := time.Since(started), processCPU()-cpu
	after := fixture.snapshot(b, "stats")
	serverCPU := after.ServerCPU - before.ServerCPU
	supervisorCPU := after.SupervisorCPU - before.SupervisorCPU
	b.ReportMetric(float64(serverCPU)/float64(b.N), "server-cpu-ns/op")
	b.ReportMetric(float64(supervisorCPU)/float64(b.N), "supervisor-echo-cpu-ns/op")
	b.ReportMetric(float64(serverCPU+supervisorCPU)/float64(elapsed), "server-total-cores")
	b.ReportMetric(float64(clientCPU)/float64(b.N), "client-cpu-ns/op")
	b.ReportMetric(float64(after.ServerRSS)/1024, "server-peak-rss-MiB")
	b.ReportMetric(float64(after.SupervisorRSS)/1024, "supervisor-echo-peak-rss-MiB")
	b.ReportMetric(float64(after.Drops-before.Drops), "ingress-drops")
	b.ReportMetric(float64(after.Ingress.Backpressure-before.Ingress.Backpressure), "ingress-queued")
	b.ReportMetric(float64(after.Ingress.QueueDrops-before.Ingress.QueueDrops), "ingress-queue-drops")
	b.ReportMetric(float64(after.Ingress.SendDrops-before.Ingress.SendDrops), "ingress-send-drops")
	b.ReportMetric(float64(after.Ingress.PeakQueuedBytes)/1024, "ingress-peak-queue-KiB")
	slices.Sort(samples)
	for _, percentile := range []int{50, 95, 99} {
		b.ReportMetric(float64(samples[min(len(samples)-1, len(samples)*percentile/100)])/1000, fmt.Sprintf("p%d-us", percentile))
	}
	if after.Drops != before.Drops {
		b.Logf("managed ingress dropped %d datagrams during TCP echo", after.Drops-before.Drops)
	}
}

func BenchmarkHandoffServerLatency(b *testing.B)  { benchmarkHandoffLoad(b, 64) }
func BenchmarkHandoffServerTransfer(b *testing.B) { benchmarkHandoffLoad(b, 65536) }

func TestHandoffUnderLoad(t *testing.T) {
	fixture := newHandoffLoadFixture(t)
	client := fixture.client(t)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	open := func(client *core.Instance, network, address string) net.Conn {
		target, err := xnet.ParseDestination(network + ":" + address)
		if err != nil {
			t.Fatal(err)
		}
		connection, err := core.Dial(ctx, client, target)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { connection.Close() })
		connection.SetDeadline(time.Now().Add(35 * time.Second))
		return connection
	}
	udp := open(client, "udp", fixture.info.UDP)
	defer udp.Close()
	warm := bytes.Repeat([]byte{0xa5}, 4096)
	if _, err := udp.Write(warm); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(udp, make([]byte, len(warm))); err != nil {
		t.Fatal(err)
	}
	var total atomic.Uint64
	stop := make(chan struct{})
	errors := make(chan error, 4)
	var workers sync.WaitGroup
	for range 4 {
		connection := open(client, "tcp", fixture.info.TCP)
		workers.Go(func() {
			payload, reply := bytes.Repeat([]byte{0x71}, 65536), make([]byte, 65536)
			ticker := time.NewTicker(time.Second / 256)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				_, err := connection.Write(payload)
				if err == nil {
					_, err = io.ReadFull(connection, reply)
				}
				if err != nil || !bytes.Equal(payload, reply) {
					errors <- fmt.Errorf("background echo failed: %v", err)
					return
				}
				total.Add(uint64(2 * len(payload)))
			}
		})
	}
	defer workers.Wait()
	defer close(stop)
	const packets = 2000
	seen := make([]bool, packets)
	sentAt := make([]atomic.Int64, packets)
	epoch := time.Now()
	latencies := make(chan []int64, 1)
	go func() {
		var samples []int64
		payload := make([]byte, 4096)
		for {
			if _, err := io.ReadFull(udp, payload); err != nil {
				break
			}
			sequence := int(binary.BigEndian.Uint32(payload))
			if sequence >= packets || !bytes.Equal(payload[4:], warm[4:]) || seen[sequence] {
				t.Error("invalid or duplicate UDP reply")
				break
			}
			seen[sequence] = true
			samples = append(samples, time.Since(epoch).Nanoseconds()-sentAt[sequence].Load())
		}
		latencies <- samples
	}()
	before := fixture.snapshot(t, "stats")
	started := time.Now()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for sequence := range packets {
		<-ticker.C
		if sequence == 600 && os.Getenv("XHYS_HANDOFF_MODE") == "managed" {
			fixture.snapshot(t, "switch")
			next := fixture.client(t)
			probe := open(next, "tcp", fixture.info.TCP)
			if _, err := probe.Write(warm); err != nil {
				t.Fatal(err)
			}
			reply := make([]byte, len(warm))
			if _, err := io.ReadFull(probe, reply); err != nil || !bytes.Equal(reply, warm) {
				t.Fatalf("new generation probe: %v", err)
			}
			status := fixture.snapshot(t, "stats")
			if len(status.Counts) != 2 || status.Counts[0] == 0 || status.Counts[1] == 0 {
				t.Fatalf("old and new QUIC connections not both alive: %+v", status)
			}
		}
		binary.BigEndian.PutUint32(warm, uint32(sequence))
		sentAt[sequence].Store(time.Since(epoch).Nanoseconds())
		if _, err := udp.Write(warm); err != nil {
			t.Fatal(err)
		}
	}
	seconds := time.Since(started).Seconds()
	background := float64(total.Load()) / (1 << 20) / seconds
	time.Sleep(time.Second)
	udp.Close()
	samples := <-latencies
	after := fixture.snapshot(t, "stats")
	var missing []int
	missingBefore, missingAfter := 0, 0
	for sequence, received := range seen {
		if !received {
			if len(missing) < 32 {
				missing = append(missing, sequence)
			}
			if sequence < 600 {
				missingBefore++
			} else {
				missingAfter++
			}
		}
	}
	slices.Sort(samples)
	if len(samples) == 0 {
		t.Fatal("no UDP replies")
	}
	t.Logf("received=%d/%d background_mibps=%.2f udp_p50_us=%.1f udp_p99_us=%.1f udp_max_us=%.1f ingress_drops=%d generations=%v server_cpu_seconds=%.3f supervisor_echo_cpu_seconds=%.3f server_peak_rss_mib=%.2f supervisor_echo_peak_rss_mib=%.2f", len(samples), packets, background, float64(samples[len(samples)/2])/1000, float64(samples[(len(samples)-1)*99/100])/1000, float64(samples[len(samples)-1])/1000, after.Drops-before.Drops, after.Counts, float64(after.ServerCPU-before.ServerCPU)/1e9, float64(after.SupervisorCPU-before.SupervisorCPU)/1e9, float64(after.ServerRSS)/1024, float64(after.SupervisorRSS)/1024)
	t.Logf("missing_before_sequence_600=%d missing_after_sequence_600=%d missing_first_32=%v", missingBefore, missingAfter, missing)
	t.Logf("native_ingress=%+v", after.Ingress)
	if len(samples) != packets || after.Drops != before.Drops {
		t.Error("UDP loss under load")
	}
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
}
