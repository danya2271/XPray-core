//go:build xhysteria_interop && xhysteria_native && cgo && linux

package interop_test

import (
	"bufio"
	"bytes"
	"context"
	cryptotls "crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	hyproxy "github.com/xtls/xray-core/proxy/hysteria"
	"github.com/xtls/xray-core/proxy/hysteria/account"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
)

type capacityMaskConn struct {
	net.PacketConn
	mask   *salamander.SalamanderObfuscator
	packet [65536]byte
}

func (c *capacityMaskConn) ReadFrom(payload []byte) (int, net.Addr, error) {
	for {
		n, address, err := c.PacketConn.ReadFrom(c.packet[:])
		if err != nil {
			return 0, nil, err
		}
		if decoded := c.mask.Deobfuscate(c.packet[:n], payload); decoded != 0 {
			return decoded, address, nil
		}
	}
}

func (c *capacityMaskConn) WriteTo(payload []byte, address net.Addr) (int, error) {
	packet := make([]byte, len(payload)+8)
	length := c.mask.Obfuscate(payload, packet)
	written, err := c.PacketConn.WriteTo(packet[:length], address)
	if err == nil && written != length {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, err
	}
	return len(payload), nil
}

func TestNativeCapacityServerProcess(t *testing.T) {
	if os.Getenv("XHYS_CAPACITY_CHILD") != "1" {
		t.Skip("subprocess fixture for the capacity test")
	}
	server, caPath, _ := xrayServerWithConfig(t, os.Getenv("XHYS_CAPACITY_MASK") != "0", func(c *core.Config) {
		nativeServerConfig(t, c, false)
		var users []*protocol.User
		for index := range 655 {
			users = append(users, &protocol.User{Email: fmt.Sprintf("capacity-%d@example.test", index), Account: serial.ToTypedMessage(&account.Account{Auth: fmt.Sprintf("capacity-user-%d", index)})})
		}
		c.Inbound[0].ProxySettings = serial.ToTypedMessage(&hyproxy.ServerConfig{Users: users})
		if upstream := os.Getenv("XHYS_CAPACITY_UPSTREAM"); upstream != "" {
			ca, err := os.ReadFile(os.Getenv("XHYS_CAPACITY_UPSTREAM_CA"))
			if err != nil {
				t.Fatal(err)
			}
			c.Outbound = nil
			routes := &router.Config{}
			for index := range 8 {
				outbound := embeddedClientConfig(t, upstream, ca, true, auth).Outbound[0]
				outbound.Tag = fmt.Sprintf("capacity-upstream-%d", index)
				c.Outbound = append(c.Outbound, outbound)
				rule := &router.RoutingRule{TargetTag: &router.RoutingRule_Tag{Tag: outbound.Tag}}
				for user := index; user < len(users); user += 8 {
					rule.UserEmail = append(rule.UserEmail, users[user].Email)
				}
				routes.Rule = append(routes.Rule, rule)
			}
			c.App = append(c.App, serial.ToTypedMessage(routes))
		}
	})
	fmt.Printf("capacity_server=%s,%s,%d\n", server, caPath, os.Getpid())
	commands := bufio.NewScanner(os.Stdin)
	for commands.Scan() {
		if directory := os.Getenv("XHYS_CAPACITY_PROFILE"); directory != "" {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			fmt.Fprintf(os.Stderr, "phase=%s go_heap_mib=%.1f go_heap_inuse_mib=%.1f go_retained_mib=%.1f go_stack_mib=%.1f goroutines=%d go_gc_cycles=%d\n", commands.Text(), float64(stats.HeapAlloc)/(1<<20), float64(stats.HeapInuse)/(1<<20), float64(stats.Sys-stats.HeapReleased)/(1<<20), float64(stats.StackInuse)/(1<<20), runtime.NumGoroutine(), stats.NumGC)
			if err := os.MkdirAll(directory, 0700); err != nil {
				t.Fatal(err)
			}
			profile, err := os.Create(filepath.Join(directory, commands.Text()+".pprof"))
			if err != nil {
				t.Fatal(err)
			}
			if err := pprof.WriteHeapProfile(profile); err != nil {
				t.Error(err)
			}
			profile.Close()
		}
	}
	if err := commands.Err(); err != nil {
		t.Fatal(err)
	}
}

func capacityParallel(t *testing.T, count int, action func(int) error) {
	t.Helper()
	if err := capacityParallelError(count, action); err != nil {
		t.Fatal(err)
	}
}

func capacityParallelError(count int, action func(int) error) error {
	jobs := make(chan int)
	failures := make(chan error, 1)
	var workers sync.WaitGroup
	var failed atomic.Bool
	for range 32 {
		workers.Go(func() {
			for index := range jobs {
				if failed.Load() {
					continue
				}
				if err := action(index); err != nil {
					failed.Store(true)
					select {
					case failures <- fmt.Errorf("session %d: %w", index, err):
					default:
					}
				}
			}
		})
	}
	for index := range count {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	select {
	case err := <-failures:
		return err
	default:
		return nil
	}
}

func capacityServerMemory(t *testing.T, pid int) (uint64, uint64) {
	t.Helper()
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatal(err)
	}
	var rss, peak uint64
	for line := range strings.SplitSeq(string(status), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		value, _ := strconv.ParseUint(fields[1], 10, 64)
		switch fields[0] {
		case "VmRSS:":
			rss = value << 10
		case "VmHWM:":
			peak = value << 10
		}
	}
	if rss == 0 || peak == 0 {
		t.Fatal("server RSS counters missing")
	}
	return rss, peak
}

func capacityServerCgroup(t *testing.T, pid int) string {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if group, found := strings.CutPrefix(line, "0::"); found {
			return filepath.Join("/sys/fs/cgroup", group)
		}
	}
	t.Fatal("cgroup v2 entry missing")
	return ""
}

func TestNativeServer500Users(t *testing.T) {
	if testing.Short() {
		t.Skip("capacity test opens 1000 real QUIC connections")
	}
	backend := os.Getenv("XHYS_CAPACITY_BACKEND")
	if backend == "" {
		backend = "native"
	}
	if backend != "native" && backend != "go" {
		t.Fatal("XHYS_CAPACITY_BACKEND must be native or go")
	}
	goMemoryLimit := os.Getenv("XHYS_CAPACITY_GOMEMLIMIT")
	if goMemoryLimit == "" {
		goMemoryLimit = "128MiB"
	}
	sessions := 1000
	if configured := os.Getenv("XHYS_CAPACITY_SESSIONS"); configured != "" {
		var err error
		sessions, err = strconv.Atoi(configured)
		if err != nil || sessions < 1 || sessions > 1310 {
			t.Fatal("XHYS_CAPACITY_SESSIONS must be 1..1310")
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	arguments := []string{executable, "-test.run=^TestNativeCapacityServerProcess$", "-test.timeout=90s"}
	if cpu := os.Getenv("XHYS_CAPACITY_SERVER_CPU"); cpu != "" {
		if _, err := strconv.ParseUint(cpu, 10, 31); err != nil {
			t.Fatal("XHYS_CAPACITY_SERVER_CPU must be a single CPU number")
		}
		arguments = append([]string{"taskset", "-c", cpu}, arguments...)
	}
	if os.Getenv("XHYS_CAPACITY_SYSTEMD") == "1" {
		arguments = append([]string{"systemd-run", "--user", "--quiet", "--scope", "--property=MemoryMax=1G", "--property=CPUQuota=100%"}, arguments...)
	}
	command := exec.CommandContext(ctx, arguments[0], arguments[1:]...)
	command.Env = append(os.Environ(), "XHYS_CAPACITY_CHILD=1", "GOMAXPROCS=1", "GOMEMLIMIT="+goMemoryLimit, "XRAY_NET_DIAG="+os.Getenv("XHYS_CAPACITY_DIAG"),
		"XRAY_HYSTERIA_NATIVE=", "XRAY_HYSTERIA_BRIDGE=shared", "XRAY_HYSTERIA_SERVER_WORKERS=1",
		"XRAY_HYSTERIA_SERVER_CONNECTIONS=1310", "XRAY_HYSTERIA_SERVER_FLOWS=1966")
	if os.Getenv("XHYS_CAPACITY_CASCADE") == "1" {
		t.Setenv("XRAY_HYSTERIA_NATIVE", "")
		if backend == "go" {
			t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
		}
		upstream, caPath, _ := xrayServerWithConfig(t, true, func(c *core.Config) { nativeServerConfig(t, c, false) })
		command.Env = append(command.Env, "XRAY_HYSTERIA_NATIVE=1", "XRAY_HYSTERIA_POOL_SIZE=2", "XHYS_CAPACITY_UPSTREAM="+upstream, "XHYS_CAPACITY_UPSTREAM_CA="+caPath)
	}
	if backend == "go" {
		command.Env = append(command.Env, "XRAY_HYSTERIA_NATIVE=0")
	}
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		input.Close()
		if err := command.Wait(); err != nil {
			t.Errorf("capacity server: %v\n%s", err, diagnostics.String())
		}
		if state := command.ProcessState; state != nil {
			t.Logf("server_process_cpu_seconds=%.3f", (state.UserTime() + state.SystemTime()).Seconds())
		}
		if os.Getenv("XHYS_CAPACITY_DIAG") == "1" || os.Getenv("XHYS_CAPACITY_PROFILE") != "" {
			t.Log(diagnostics.String())
		}
	})
	scanner := bufio.NewScanner(output)
	var server, caPath string
	var serverPID int
	for scanner.Scan() {
		if configuration, found := strings.CutPrefix(scanner.Text(), "capacity_server="); found {
			fields := strings.Split(configuration, ",")
			if len(fields) != 3 {
				t.Fatal("invalid capacity server endpoint")
			}
			server, caPath = fields[0], fields[1]
			serverPID, _ = strconv.Atoi(fields[2])
			break
		}
	}
	if server == "" || caPath == "" || serverPID == 0 {
		t.Fatal("capacity server did not publish its endpoint")
	}
	go io.Copy(io.Discard, output)
	if os.Getenv("XHYS_CAPACITY_SYSTEMD") == "1" {
		group := capacityServerCgroup(t, serverPID)
		memoryLimit, err := os.ReadFile(filepath.Join(group, "memory.max"))
		if err != nil || strings.TrimSpace(string(memoryLimit)) != "1073741824" {
			t.Fatalf("server cgroup memory limit=%q error=%v", memoryLimit, err)
		}
		cpuLimit, err := os.ReadFile(filepath.Join(group, "cpu.max"))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("server_cgroup_memory_max=%s server_cgroup_cpu_max=%s", strings.TrimSpace(string(memoryLimit)), strings.TrimSpace(string(cpuLimit)))
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid capacity server certificate")
	}
	socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("XHYS_CAPACITY_MASK") != "0" {
		mask, err := salamander.NewSalamanderObfuscator([]byte(psk))
		if err != nil {
			t.Fatal(err)
		}
		socket = &capacityMaskConn{PacketConn: socket, mask: mask}
	}
	transport := &quic.Transport{Conn: socket}
	t.Cleanup(func() { transport.Close(); socket.Close() })
	address, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig := &cryptotls.Config{RootCAs: roots, ServerName: "localhost", NextProtos: []string{"h3"}}
	quicConfig := &quic.Config{EnableDatagrams: true, DisablePathManager: true, MaxIdleTimeout: time.Minute, KeepAlivePeriod: 10 * time.Second,
		InitialStreamReceiveWindow: 65536, MaxStreamReceiveWindow: 65536,
		InitialConnectionReceiveWindow: 131072, MaxConnectionReceiveWindow: 131072}
	connections := make([]*quic.Conn, sessions)
	streams := make([]*quic.Stream, sessions)
	t.Cleanup(func() {
		for _, connection := range connections {
			if connection != nil {
				connection.CloseWithError(0, "capacity test complete")
			}
		}
	})
	tcp, _ := echoTargetsWithTimeout(t, 90*time.Second)
	started := time.Now()
	capacityParallel(t, sessions, func(index int) error {
		connection, err := transport.Dial(ctx, address, tlsConfig, quicConfig)
		if err != nil {
			return fmt.Errorf("QUIC dial: %w", err)
		}
		connections[index] = connection
		httpTransport := &http3.Transport{}
		client := httpTransport.NewClientConn(connection)
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://hysteria/auth", nil)
		if err != nil {
			return err
		}
		request.Header.Set("Hysteria-Auth", fmt.Sprintf("capacity-user-%d", index/2))
		request.Header.Set("Hysteria-CC-RX", "0")
		response, err := client.RoundTrip(request)
		if err != nil {
			return fmt.Errorf("auth request: %w", err)
		}
		response.Body.Close()
		if response.StatusCode != 233 {
			return fmt.Errorf("auth status %d", response.StatusCode)
		}
		stream, err := connection.OpenStreamSync(ctx)
		if err != nil {
			return err
		}
		streams[index] = stream
		stream.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := stream.Write([]byte{0x44, 0x01}); err != nil {
			return err
		}
		if err := hyproxy.WriteTCPRequest(stream, tcp); err != nil {
			return err
		}
		accepted, message, err := hyproxy.ReadTCPResponse(stream)
		if err != nil {
			return fmt.Errorf("TCP response: %w", err)
		}
		if !accepted {
			return fmt.Errorf("TCP rejected: %s", message)
		}
		return nil
	})
	idleRSS, _ := capacityServerMemory(t, serverPID)
	fmt.Fprintln(input, "idle")
	t.Logf("backend=%s gomemlimit=%s %d users, %d authenticated QUIC sessions and TCP flows; setup=%s server_idle_rss_mib=%.1f", backend, goMemoryLimit, (sessions+1)/2, sessions, time.Since(started), float64(idleRSS)/(1<<20))
	payload := bytes.Repeat([]byte{0xa7}, 256<<10)
	started = time.Now()
	written := make(chan error, 1)
	go func() {
		written <- capacityParallelError(sessions, func(index int) error {
			streams[index].SetWriteDeadline(time.Now().Add(30 * time.Second))
			_, err := streams[index].Write(payload)
			return err
		})
	}()
	time.Sleep(2 * time.Second)
	bufferedRSS, _ := capacityServerMemory(t, serverPID)
	fmt.Fprintln(input, "buffered")
	capacityParallel(t, sessions, func(index int) error {
		streams[index].SetReadDeadline(time.Now().Add(30 * time.Second))
		reply := make([]byte, len(payload))
		if _, err := io.ReadFull(streams[index], reply); err != nil {
			return err
		}
		if !bytes.Equal(reply, payload) {
			return fmt.Errorf("echo payload changed")
		}
		return nil
	})
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	transferElapsed := time.Since(started)
	t.Logf("duplex_transfer_mib=%.0f transfer_elapsed=%s duplex_transfer_mib_per_second=%.2f",
		float64(2*sessions*len(payload))/(1<<20), transferElapsed,
		float64(2*sessions*len(payload))/(1<<20)/transferElapsed.Seconds())
	heartbeatLatency := make([]time.Duration, 12*sessions)
	for round := range 12 {
		time.Sleep(time.Second)
		capacityParallel(t, sessions, func(index int) error {
			streams[index].SetDeadline(time.Now().Add(5 * time.Second))
			heartbeatStarted := time.Now()
			if _, err := streams[index].Write([]byte{0x71}); err != nil {
				return err
			}
			var reply [1]byte
			if _, err := io.ReadFull(streams[index], reply[:]); err != nil {
				return err
			}
			if reply[0] != 0x71 {
				return fmt.Errorf("live session payload changed")
			}
			heartbeatLatency[round*sessions+index] = time.Since(heartbeatStarted)
			return nil
		})
	}
	slices.Sort(heartbeatLatency)
	t.Logf("heartbeat_p50_us=%.1f heartbeat_p99_us=%.1f",
		float64(heartbeatLatency[len(heartbeatLatency)/2])/float64(time.Microsecond),
		float64(heartbeatLatency[len(heartbeatLatency)*99/100])/float64(time.Microsecond))
	rss, peak := capacityServerMemory(t, serverPID)
	fmt.Fprintln(input, "final")
	t.Logf("server_buffered_rss_mib=%.1f server_final_rss_mib=%.1f server_peak_rss_mib=%.1f duplex_mib=%.0f elapsed=%s",
		float64(bufferedRSS)/(1<<20), float64(rss)/(1<<20), float64(peak)/(1<<20), float64(2*sessions*len(payload))/(1<<20), time.Since(started))
	if peak > 512<<20 {
		t.Fatalf("server peak RSS %d exceeds the 512 MiB allowance on a 1 GiB node", peak)
	}
	if os.Getenv("XHYS_CAPACITY_SYSTEMD") == "1" {
		group := capacityServerCgroup(t, serverPID)
		memoryPeak, err := os.ReadFile(filepath.Join(group, "memory.peak"))
		if err != nil {
			t.Fatal(err)
		}
		peak, err := strconv.ParseUint(strings.TrimSpace(string(memoryPeak)), 10, 64)
		if err != nil || peak > 512<<20 {
			t.Fatalf("server cgroup peak %q exceeds 512 MiB: %v", memoryPeak, err)
		}
		t.Logf("server_cgroup_peak_mib=%.1f", float64(peak)/(1<<20))
		cpu, err := os.ReadFile(filepath.Join(group, "cpu.stat"))
		if err != nil {
			t.Fatal(err)
		}
		for line := range strings.SplitSeq(string(cpu), "\n") {
			if value, found := strings.CutPrefix(line, "usage_usec "); found {
				microseconds, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("server_cgroup_cpu_seconds=%.3f", float64(microseconds)/1e6)
			}
		}
	}
	for index, connection := range connections {
		if connection != nil {
			connection.CloseWithError(0, "capacity teardown")
			connections[index] = nil
		}
	}
	time.Sleep(2 * time.Second)
	closedRSS, _ := capacityServerMemory(t, serverPID)
	fmt.Fprintln(input, "closed")
	t.Logf("server_after_close_rss_mib=%.1f", float64(closedRSS)/(1<<20))
}
