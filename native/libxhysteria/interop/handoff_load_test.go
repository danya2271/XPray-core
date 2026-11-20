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
	Drain         string
	PublicUDP     handoffSocketStats
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
			create := native.NewNativeIngress
			if os.Getenv("XHYS_HANDOFF_DIRECT_RX") == "1" {
				if os.Getenv("XHYS_HANDOFF_SHARED_RX") != "1" || os.Getenv("XHYS_HANDOFF_DIRECT_TX") != "1" || os.Getenv("XHYS_HANDOFF_BACKEND") == "go" {
					t.Fatal("direct receive requires shared RX, direct TX and native server")
				}
				create = native.NewDormantNativeIngress
			}
			ingress, err = create(public, key, 0, []native.UDPMaskConfig{
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
	stopped := make(map[int]bool)
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
		var inherited []*os.File
		control := handoff.ControlSocket(filepath.Join(directory, "control.sock"))
		if managed {
			if os.Getenv("XHYS_HANDOFF_SHARED_RX") == "1" {
				relay, ok := ingress.(*native.NativeIngress)
				if !ok || os.Getenv("XHYS_HANDOFF_PACKET_BATCH") != "1" || os.Getenv("XHYS_HANDOFF_BACKEND") == "go" {
					t.Fatal("shared receive requires native ingress, batch and native server")
				}
				if os.Getenv("XHYS_HANDOFF_DIRECT_TX") == "1" {
					inherited, err = relay.AddDirectRing(slot)
				} else {
					inherited, err = relay.AddRing(slot)
				}
				if err == nil {
					file = inherited[0]
				}
			} else {
				file, err = ingress.Add(slot)
				inherited = []*os.File{file}
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				for _, file := range inherited {
					file.Close()
				}
			}()
			manifest := handoff.Manifest{Control: string(control), UDP: []handoff.UDPBinding{{Tag: "hy", Address: "127.0.0.1", Port: local.Port, FD: 3, Key: key, Slot: slot,
				PacketBatch: os.Getenv("XHYS_HANDOFF_PACKET_BATCH") == "1"}}}
			if len(inherited) == 4 {
				manifest.UDP[0].RingFDs = []int{4, 5, 6}
			}
			manifest.UDP[0].DirectReceive = slot == 0 && os.Getenv("XHYS_HANDOFF_DIRECT_RX") == "1"
			if os.Getenv("XHYS_HANDOFF_DIRECT_TX") == "1" {
				if os.Getenv("XHYS_HANDOFF_NATIVE_INGRESS") != "1" || os.Getenv("XHYS_HANDOFF_BACKEND") == "go" {
					t.Fatal("direct transmit requires native ingress and server")
				}
				transmit, err = handoff.DuplicatePacketSocket(public)
				if err != nil {
					t.Fatal(err)
				}
				manifest.UDP[0].TransmitFD = 3 + len(inherited)
				inherited = append(inherited, transmit)
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
		if os.Getenv("XHYS_HANDOFF_DIAGNOSTIC") == "1" {
			command.Env = append(command.Env, "XRAY_NET_DIAG=1")
		}
		if os.Getenv("XHYS_HANDOFF_BACKEND") != "go" {
			command.Env = append(command.Env, "XRAY_HYSTERIA_NATIVE=1")
		}
		log, err := os.Create(filepath.Join(directory, "server.log"))
		if err != nil {
			t.Fatal(err)
		}
		command.Stdout, command.Stderr = log, log
		if os.Getenv("XHYS_HANDOFF_DIAGNOSTIC") == "1" {
			command.Stdout, command.Stderr = io.MultiWriter(log, os.Stderr), io.MultiWriter(log, os.Stderr)
		}
		if file != nil {
			command.ExtraFiles = inherited
		}
		if err := command.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		processes = append(processes, command)
		controls = append(controls, control)
		t.Cleanup(func() {
			if !stopped[int(slot)] {
				command.Process.Kill()
				command.Wait()
			}
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
	publicStats, err := readHandoffSocketStats(local, "")
	if err != nil {
		t.Fatal(err)
	}
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
	var drainToken [16]byte
	copy(drainToken[:], []byte("test-drain-token"))
	for scanner.Scan() {
		if scanner.Text() == "switch" {
			if !managed || len(processes) != 1 {
				t.Fatal("switch requires one managed generation")
			}
			start(1)
			if os.Getenv("XHYS_HANDOFF_DIRECT_RX") == "1" {
				target := fmt.Sprintf("release-receive hy:%d", local.Port)
				released, err := controls[0].Command(t.Context(), target)
				if err != nil || len(released.Receive) == 0 {
					t.Fatalf("releasing direct receive owner: %v", err)
				}
				retry, err := controls[0].Command(t.Context(), target)
				if err != nil || !bytes.Equal(released.Receive, retry.Receive) {
					t.Fatalf("receive release retry changed snapshot: %v", err)
				}
				relay := ingress.(*native.NativeIngress)
				if err := relay.AdoptReceive(released.Receive); err != nil {
					t.Fatal(err)
				}
				if err := relay.AdoptReceive(released.Receive); err != nil {
					t.Fatalf("receive adoption retry: %v", err)
				}
			}
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
		snapshot.PublicUDP, err = readHandoffSocketStats(local, publicStats.Inode)
		if err != nil {
			t.Fatal(err)
		}
		if scanner.Text() == "return-direct" {
			relay, ok := ingress.(*native.NativeIngress)
			if !ok || len(processes) != 2 {
				t.Fatal("return-direct requires native managed generations")
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				retired, err := relay.Retire(0, drainToken)
				if err != nil {
					t.Fatalf("retiring old ingress route: %v", err)
				}
				if retired {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("old ingress route did not settle")
				}
				time.Sleep(10 * time.Millisecond)
			}
			var token [16]byte
			copy(token[:], []byte("return-rx-token!"))
			deadline = time.Now().Add(5 * time.Second)
			for {
				ready, err := relay.ReleasePublic(1, token)
				if err != nil {
					t.Fatal(err)
				}
				if ready {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("ingress did not release public receive")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if ready, err := relay.ReleasePublic(1, token); err != nil || !ready {
				t.Fatalf("release retry: %v", err)
			}
			for {
				response, err := controls[1].Command(t.Context(), fmt.Sprintf("acquire-receive hy:%d %x", local.Port, token))
				if err != nil {
					t.Fatal(err)
				}
				if response.State == "receive-direct" {
					snapshot.Drain = response.State
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("new generation did not acquire public receive")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if os.Getenv("XHYS_HANDOFF_BENCH_AFTER_RETIRE") == "1" {
				state, err := controls[0].Command(t.Context(), "status")
				if err != nil || state.State != "drained" {
					t.Fatalf("refusing to stop uncertified generation: %+v, %v", state, err)
				}
				if err := processes[0].Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
				stopped[0] = true // Wait below owns process exit, including timeout.
				done := make(chan error, 1)
				go func() { done <- processes[0].Wait() }()
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("certified old generation shutdown: %v", err)
					}
				case <-time.After(5 * time.Second):
					processes[0].Process.Kill()
					<-done
					t.Fatal("certified old generation did not stop")
				}
			}
		}
		if scanner.Text() == "drain" {
			if !managed || len(processes) != 2 {
				t.Fatal("drain requires two managed generations")
			}
			var fenced bool
			var err error
			if relay, ok := ingress.(*native.NativeIngress); ok {
				fenced, err = relay.FenceGeneration(0, drainToken)
			} else {
				fenced, err = ingress.(*handoff.UDPIngress).FenceGeneration(t.Context(), 0, drainToken)
			}
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Drain = "ingress-waiting"
			if fenced {
				response, err := controls[0].Command(t.Context(), fmt.Sprintf("drain-udp hy:%d %x", local.Port, drainToken))
				if err != nil {
					t.Fatal(err)
				}
				snapshot.Drain = response.State
				if response.State == "udp-drained" {
					response, err = controls[0].Command(t.Context(), "drain")
					if err != nil {
						t.Fatal(err)
					}
					snapshot.Drain = response.State
				}
			}
		}
		snapshot.SupervisorCPU = processCPU()
		_, snapshot.SupervisorRSS = handoffProcessUsage(t, os.Getpid(), ticks)
		for index, process := range processes {
			if stopped[index] {
				snapshot.Counts = append(snapshot.Counts, 0)
				continue
			}
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
					// Ordinary drain closes the old listener only after the
					// engine/queue certificate; its diagnostic hook is then gone.
					state, stateErr := controls[index].Command(t.Context(), "status")
					if stateErr != nil || state.State != "drained" || status.Count != 0 {
						t.Fatalf("generation %d connection count is unknown outside confirmed drain: %+v, %v", index, state, stateErr)
					}
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
	output  io.Reader
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
		if t.Failed() || os.Getenv("XHYS_HANDOFF_DIAGNOSTIC") == "1" {
			t.Logf("handoff server diagnostics: %s", diagnostics.String())
		}
	})
	fixture := &handoffLoadFixture{input: input, decoder: json.NewDecoder(output), output: output}
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
		diagnostic, _ := io.ReadAll(io.LimitReader(io.MultiReader(fixture.decoder.Buffered(), fixture.output), 8192))
		t.Fatalf("fixture %s: %v: %s", command, err, diagnostic)
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
	benchAfterDrain := os.Getenv("XHYS_HANDOFF_BENCH_AFTER_DRAIN") == "1" || os.Getenv("XHYS_HANDOFF_BENCH_AFTER_RETIRE") == "1"
	benchNewGeneration := os.Getenv("XHYS_HANDOFF_BENCH_NEW_GENERATION") == "1" || benchAfterDrain
	benchMixed := os.Getenv("XHYS_HANDOFF_BENCH_MIXED_GENERATIONS") == "1"
	if benchMixed && benchNewGeneration {
		b.Fatal("mixed overlap cannot also replace/drain the previous client")
	}
	benchSwitch := os.Getenv("XHYS_HANDOFF_BENCH_SWITCH") == "1" || benchNewGeneration || benchMixed
	if benchSwitch {
		if os.Getenv("XHYS_HANDOFF_MODE") != "managed" {
			b.Fatal("benchmark switch requires managed mode")
		}
		fixture.snapshot(b, "switch")
		previousClient := client
		nextClient := fixture.client(b)
		if benchNewGeneration {
			client = nextClient
			for index := range connections {
				connections[index].Close()
				connections[index] = benchmarkDial(b, client, target)
				connections[index].SetDeadline(time.Now().Add(90 * time.Second))
				benchmarkExchange(b, connections[index], make([]byte, size), make([]byte, size))
			}
		} else if benchMixed {
			for index := len(connections) / 2; index < len(connections); index++ {
				connections[index].Close()
				connections[index] = benchmarkDial(b, nextClient, target)
				connections[index].SetDeadline(time.Now().Add(90 * time.Second))
				benchmarkExchange(b, connections[index], make([]byte, size), make([]byte, size))
			}
		} else {
			probe := benchmarkDial(b, nextClient, target)
			b.Cleanup(func() { probe.Close() })
			probe.SetDeadline(time.Now().Add(90 * time.Second))
			benchmarkExchange(b, probe, make([]byte, 4096), make([]byte, 4096))
		}
		status := fixture.snapshot(b, "stats")
		if len(status.Counts) != 2 || status.Counts[0] == 0 || status.Counts[1] == 0 {
			b.Fatalf("benchmark generations are not both alive: %+v", status)
		}
		if benchAfterDrain {
			if os.Getenv("XHYS_HANDOFF_SHARED_RX") != "1" || os.Getenv("XHYS_HANDOFF_DIRECT_TX") != "1" {
				b.Fatal("post-drain direct receive requires shared RX and direct TX")
			}
			previousClient.Close()
			deadline := time.Now().Add(50 * time.Second)
			for {
				// Initial-pin expiry must not make B's measured streams idle.
				for _, connection := range connections {
					benchmarkExchange(b, connection, []byte{1}, make([]byte, 1))
				}
				state := fixture.snapshot(b, "drain").Drain
				if state == "drained" {
					break
				}
				if (state != "ingress-waiting" && state != "udp-waiting" && state != "draining") || time.Now().After(deadline) {
					b.Fatalf("old generation did not drain before measurement: %s", state)
				}
				time.Sleep(100 * time.Millisecond)
			}
			if state := fixture.snapshot(b, "return-direct").Drain; state != "receive-direct" {
				b.Fatalf("new generation did not reacquire direct receive: %s", state)
			}
			for _, connection := range connections {
				benchmarkExchange(b, connection, make([]byte, size), make([]byte, size))
			}
		}
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
	if after.PublicUDP.Inode != before.PublicUDP.Inode || after.PublicUDP.Drops < before.PublicUDP.Drops || after.Drops < before.Drops {
		b.Fatal("public UDP identity changed or a drop counter regressed")
	}
	b.ReportMetric(float64(after.PublicUDP.Drops-before.PublicUDP.Drops), "public-udp-drops")
	b.ReportMetric(float64(after.PublicUDP.QueueBytes), "public-udp-queue-bytes")
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
	if os.Getenv("XHYS_HANDOFF_REQUIRE_NO_DROPS") == "1" && (after.Drops != before.Drops || after.PublicUDP.Drops != before.PublicUDP.Drops) {
		b.Fatalf("strict loss gate failed: ingress_drops=%d public_udp_drops=%d", after.Drops-before.Drops, after.PublicUDP.Drops-before.PublicUDP.Drops)
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
	udpClients := []net.Conn{udp}
	activeUDP := udp
	defer func() {
		for _, connection := range udpClients {
			connection.Close()
		}
	}()
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
	seen := make([]atomic.Bool, packets)
	sentAt := make([]atomic.Int64, packets)
	epoch := time.Now()
	latencies := make(chan []int64, 2)
	readerCount := 0
	var samples []int64
	startUDPReader := func(connection net.Conn) {
		readerCount++
		go func() {
			var samples []int64
			payload := make([]byte, 4096)
			for {
				if _, err := io.ReadFull(connection, payload); err != nil {
					break
				}
				sequence := int(binary.BigEndian.Uint32(payload))
				if sequence >= packets || !bytes.Equal(payload[4:], warm[4:]) || !seen[sequence].CompareAndSwap(false, true) {
					t.Error("invalid or duplicate UDP reply")
					break
				}
				samples = append(samples, time.Since(epoch).Nanoseconds()-sentAt[sequence].Load())
			}
			latencies <- samples
		}()
	}
	startUDPReader(udp)
	before := fixture.snapshot(t, "stats")
	started := time.Now()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for sequence := range packets {
		<-ticker.C
		if sequence == 600 && os.Getenv("XHYS_HANDOFF_MODE") == "managed" {
			drainDeadline := time.Now().Add(100 * time.Millisecond)
			for time.Now().Before(drainDeadline) {
				allReceived := true
				for prior := range sequence {
					if !seen[prior].Load() {
						allReceived = false
						break
					}
				}
				if allReceived {
					break
				}
				time.Sleep(100 * time.Microsecond)
			}
			udp.Close()
			samples = append(samples, <-latencies...)
			readerCount--
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
			activeUDP = open(next, "udp", fixture.info.UDP)
			udpClients = append(udpClients, activeUDP)
			startUDPReader(activeUDP)
		}
		binary.BigEndian.PutUint32(warm, uint32(sequence))
		sentAt[sequence].Store(time.Since(epoch).Nanoseconds())
		if _, err := activeUDP.Write(warm); err != nil {
			t.Fatal(err)
		}
	}
	seconds := time.Since(started).Seconds()
	background := float64(total.Load()) / (1 << 20) / seconds
	time.Sleep(time.Second)
	for _, connection := range udpClients {
		connection.Close()
	}
	for range readerCount {
		samples = append(samples, <-latencies...)
	}
	after := fixture.snapshot(t, "stats")
	var missing []int
	missingBefore, missingAfter := 0, 0
	for sequence := range seen {
		if !seen[sequence].Load() {
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
	t.Logf("public_udp_drops_before=%d public_udp_drops_after=%d public_udp_queued_bytes=%d", before.PublicUDP.Drops, after.PublicUDP.Drops, after.PublicUDP.QueueBytes)
	if len(samples) != packets || after.Drops != before.Drops || after.PublicUDP.Drops != before.PublicUDP.Drops {
		t.Error("UDP loss under load")
	}
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
}

func TestHandoffDrainRetainsLiveConnections(t *testing.T) {
	if os.Getenv("XHYS_HANDOFF_MODE") != "managed" {
		t.Skip("managed drain integration")
	}
	fixture := newHandoffLoadFixture(t)
	clientA := fixture.client(t)
	open := func(client *core.Instance) net.Conn {
		t.Helper()
		target, err := xnet.ParseDestination("tcp:" + fixture.info.TCP)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := core.Dial(t.Context(), client, target)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	pulse := func(conn net.Conn) {
		t.Helper()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write([]byte("still-live")); err != nil {
			t.Fatal(err)
		}
		var reply [10]byte
		if _, err := io.ReadFull(conn, reply[:]); err != nil || string(reply[:]) != "still-live" {
			t.Fatalf("live connection interrupted: %q %v", reply, err)
		}
	}
	a := open(clientA)
	pulse(a)
	fixture.snapshot(t, "switch")
	clientB := fixture.client(t)
	b := open(clientB)
	pulse(b)
	// Let original Initial pins expire without imposing a session lifetime.
	deadline := time.Now().Add(45 * time.Second)
	for {
		state := fixture.snapshot(t, "drain").Drain
		if state == "udp-waiting" {
			break
		}
		if state != "ingress-waiting" {
			t.Fatalf("drained live A: %s", state)
		}
		if time.Now().After(deadline) {
			t.Fatal("ingress never became fenceable")
		}
		pulse(a)
		pulse(b)
		time.Sleep(250 * time.Millisecond)
	}
	pulse(a)
	pulse(b)
	a.Close()
	clientA.Close()
	deadline = time.Now().Add(15 * time.Second)
	for {
		state := fixture.snapshot(t, "drain").Drain
		pulse(b)
		if state == "drained" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A did not drain naturally: %s", state)
		}
		time.Sleep(25 * time.Millisecond)
	}
	pulse(b)
	if os.Getenv("XHYS_HANDOFF_SHARED_RX") == "1" && os.Getenv("XHYS_HANDOFF_DIRECT_TX") == "1" {
		if result := fixture.snapshot(t, "return-direct"); result.Drain != "receive-direct" {
			t.Fatalf("new generation did not return direct: %+v", result)
		}
		pulse(b)
	}
}
