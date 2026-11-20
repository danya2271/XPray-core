//go:build linux && !android && xhysteria_native && cgo

package main

import (
	"bufio"
	"bytes"
	"context"
	wirebinary "encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"golang.org/x/sys/unix"
)

func TestNativeDirectBrokerRecoveryPreservesApplicationUDP(t *testing.T) {
	testBrokerApplicationUDP(t, true, false)
}

func TestBrokerControlOverlapPreservesApplicationUDP(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native-%t", native), func(t *testing.T) { testBrokerApplicationUDP(t, native, true) })
	}
}

func testBrokerApplicationUDP(t *testing.T, native, controlOverlap bool) {
	t.Helper()
	if os.Getenv("NODEX_TEST_CONTROL_DIAGNOSTIC") == "1" {
		t.Setenv("XRAY_NET_DIAG", "1")
	}
	binary := os.Getenv("XRAY_TEST_GENERATION_BINARY")
	if binary == "" {
		t.Skip("requires native Xray executable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	root := socketRoot(t)
	echo, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		buffer := make([]byte, 2048)
		for {
			n, from, err := echo.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			if _, err := echo.WriteToUDP(buffer[:n], from); err != nil {
				return
			}
		}
	}()
	freePort := func() int {
		t.Helper()
		packet, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer packet.Close()
		return packet.LocalAddr().(*net.UDPAddr).Port
	}
	port, clientPort := freePort(), freePort()
	certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
	certificatePEM, keyPEM := certificate.ToPEM()
	certPath, keyPath := filepath.Join(root, "cert.pem"), filepath.Join(root, "key.pem")
	if err := os.WriteFile(certPath, certificatePEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"log":{"loglevel":"warning"},"inbounds":[{"tag":"hy","listen":"127.0.0.1","port":%d,"protocol":"hysteria","settings":{"users":[{"auth":"test-auth"}]},"streamSettings":{"network":"hysteria","security":"tls","hysteriaSettings":{"native":%t},"tlsSettings":{"certificates":[{"certificateFile":%q,"keyFile":%q,"oneTimeLoading":true}]},"finalmask":{"quicParams":{"disableChromeParrot":true},"udp":[{"type":"salamander","settings":{"password":"test-password"}}]}}}],"outbounds":[{"protocol":"freedom","settings":{"finalRules":[{"action":"allow","ip":["127.0.0.1/32"]}]}}]}`, port, native, certPath, keyPath)
	configPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := newSupervisor(ctx, root, "/bin/true", release{binary, configPath})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); s.close() }()
	before := s.status()
	if !controlOverlap && !before.BrokerRecoveryReady {
		t.Fatalf("checkpoint unavailable: %+v", before)
	}
	clientConfig := fmt.Sprintf(`{"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":%d,"protocol":"dokodemo-door","settings":{"address":"127.0.0.1","port":%d,"network":"udp"}}],"outbounds":[{"protocol":"hysteria","settings":{"address":"127.0.0.1","port":%d},"streamSettings":{"network":"hysteria","security":"tls","hysteriaSettings":{"auth":"test-auth","native":true},"tlsSettings":{"serverName":"localhost","disableSystemRoot":true,"certificates":[{"certificateFile":%q,"usage":"verify","oneTimeLoading":true}]},"finalmask":{"quicParams":{"disableChromeParrot":true},"udp":[{"type":"salamander","settings":{"password":"test-password"}}]}}}]}`, clientPort, echo.LocalAddr().(*net.UDPAddr).Port, port, certPath)
	var group sync.WaitGroup
	var tcpBytes atomic.Uint64
	var streams []net.Conn
	stopping := make(chan struct{})
	backgroundErrors := make(chan error, 32)
	defer func() {
		close(stopping)
		for _, connection := range streams {
			connection.Close()
		}
		group.Wait()
	}()
	background := controlOverlap && os.Getenv("NODEX_TEST_CONTROL_SATURATED") == "1"
	tcpPort := 0
	if background {
		tcpEcho, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer tcpEcho.Close()
		go func() {
			for {
				connection, err := tcpEcho.Accept()
				if err != nil {
					return
				}
				go func() { defer connection.Close(); _, _ = io.Copy(connection, connection) }()
			}
		}()
		reserve, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		tcpPort = reserve.Addr().(*net.TCPAddr).Port
		reserve.Close()
		var parsed map[string]any
		if err := json.Unmarshal([]byte(clientConfig), &parsed); err != nil {
			t.Fatal(err)
		}
		parsed["inbounds"] = append(parsed["inbounds"].([]any), map[string]any{
			"listen": "127.0.0.1", "port": tcpPort, "protocol": "dokodemo-door",
			"settings": map[string]any{"address": "127.0.0.1", "port": tcpEcho.Addr().(*net.TCPAddr).Port, "network": "tcp"},
		})
		wire, _ := json.Marshal(parsed)
		clientConfig = string(wire)
	}
	startStreams := func(port int) {
		t.Helper()
		if !background {
			return
		}
		for range 16 {
			connection, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			streams = append(streams, connection)
			if err := connection.SetDeadline(time.Now().Add(50 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := connection.Write([]byte{1}); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(connection, make([]byte, 1)); err != nil {
				t.Fatal(err)
			}
			group.Add(1)
			go func(connection net.Conn) {
				defer group.Done()
				payload, reply := bytes.Repeat([]byte{0x91}, 65536), make([]byte, 65536)
				for {
					select {
					case <-stopping:
						return
					default:
					}
					if _, err := connection.Write(payload); err != nil {
						backgroundErrors <- err
						return
					}
					if _, err := io.ReadFull(connection, reply); err != nil {
						backgroundErrors <- err
						return
					}
					if !bytes.Equal(payload, reply) {
						backgroundErrors <- fmt.Errorf("TCP payload changed")
						return
					}
					tcpBytes.Add(uint64(2 * len(payload)))
				}
			}(connection)
		}
	}
	clientPath := filepath.Join(root, "client.json")
	if err := os.WriteFile(clientPath, []byte(clientConfig), 0600); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(root, "client.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	defer func() {
		if t.Failed() || os.Getenv("NODEX_TEST_CONTROL_DIAGNOSTIC") == "1" {
			data, _ := os.ReadFile(log.Name())
			t.Logf("client diagnostics: %s", data)
		}
	}()
	client := exec.CommandContext(ctx, binary, "run", "-config", clientPath)
	client.Env = append(os.Environ(), "XRAY_HYSTERIA_NATIVE=1", "XRAY_NET_DIAG=1")
	client.Stdout, client.Stderr = log, log
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { client.Process.Kill(); client.Wait() }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: clientPort})
		if err != nil {
			break
		}
		probe.Close()
		if time.Now().After(deadline) {
			t.Fatal("UDP client did not bind")
		}
		time.Sleep(10 * time.Millisecond)
	}
	connection, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: clientPort})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(45 * time.Second)); err != nil {
		t.Fatal(err)
	}
	const packets = 4000
	warm := bytes.Repeat([]byte{0x71}, 1200)
	if _, err := connection.Write(warm); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, len(warm))
	if n, err := connection.Read(reply); err != nil || !bytes.Equal(warm, reply[:n]) {
		t.Fatalf("UDP warm-up: %d, %v", n, err)
	}
	assertClientNative := func() {
		t.Helper()
		data, err := os.ReadFile(log.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(data, []byte("event=client_runtime")) || bytes.Contains(data, []byte("using Go backend")) {
			t.Fatalf("unexpected native client selection: %s", data)
		}
	}
	assertClientNative()
	startStreams(tcpPort)
	var second net.Conn
	if controlOverlap {
		candidate := filepath.Join(root, "candidate.json")
		if err := os.WriteFile(candidate, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		if err := s.upgrade(release{binary, candidate}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(8 * time.Second)
		for s.status().Phase != "draining" {
			if state := s.status(); state.Phase == "blocked" || state.Phase == "admission-unknown" {
				t.Fatalf("upgrade blocked: %+v", state)
			}
			if time.Now().After(deadline) {
				t.Fatal("candidate did not enter overlap")
			}
			time.Sleep(10 * time.Millisecond)
		}
		// A distinct native client creates a separate QUIC session pinned to B.
		var parsed map[string]any
		if err := json.Unmarshal([]byte(clientConfig), &parsed); err != nil {
			t.Fatal(err)
		}
		secondPort := freePort()
		parsed["inbounds"].([]any)[0].(map[string]any)["port"] = secondPort
		secondTCP := 0
		if background {
			reserve, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			secondTCP = reserve.Addr().(*net.TCPAddr).Port
			reserve.Close()
			parsed["inbounds"].([]any)[1].(map[string]any)["port"] = secondTCP
		}
		wire, _ := json.Marshal(parsed)
		secondPath := filepath.Join(root, "client-b.json")
		if err := os.WriteFile(secondPath, wire, 0600); err != nil {
			t.Fatal(err)
		}
		other := exec.CommandContext(ctx, binary, "run", "-config", secondPath)
		other.Env = client.Env
		other.Stdout, other.Stderr = log, log
		if err := other.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { other.Process.Kill(); other.Wait() }()
		deadline = time.Now().Add(3 * time.Second)
		for {
			probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: secondPort})
			if err != nil {
				break
			}
			probe.Close()
			if time.Now().After(deadline) {
				t.Fatal("second UDP client did not bind")
			}
			time.Sleep(10 * time.Millisecond)
		}
		second, err = net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: secondPort})
		if err != nil {
			t.Fatal(err)
		}
		defer second.Close()
		if err := second.SetDeadline(time.Now().Add(45 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := second.Write(warm); err != nil {
			t.Fatal(err)
		}
		if n, err := second.Read(reply); err != nil || !bytes.Equal(warm, reply[:n]) {
			t.Fatalf("B warmup: %d, %v", n, err)
		}
		before = s.status()
		startStreams(secondTCP)
	}
	publicBefore := controlUDPSocketStats(t, port, "")
	relayBefore, files, err := s.broker.command(ctx, brokerRequest{Operation: "stats", Tag: "hy"})
	for _, file := range files {
		file.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	credit := make(chan struct{}, 8)
	for range cap(credit) {
		credit <- struct{}{}
	}
	done := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	defer func() { connection.Close(); readers.Wait() }()
	go func() {
		defer readers.Done()
		seen := make([]bool, packets)
		for range packets {
			n, err := connection.Read(reply)
			if err != nil {
				done <- err
				return
			}
			if n != len(warm) || !bytes.Equal(reply[4:], warm[4:]) {
				done <- fmt.Errorf("UDP payload changed: %d", n)
				return
			}
			sequence := wirebinary.BigEndian.Uint32(reply)
			if sequence >= packets || seen[sequence] {
				done <- fmt.Errorf("invalid/duplicate sequence %d", sequence)
				return
			}
			seen[sequence] = true
			select {
			case credit <- struct{}{}:
			case <-ctx.Done():
				done <- ctx.Err()
				return
			}
		}
		done <- nil
	}()
	crashes := 0
	replyForSecond := make([]byte, len(warm))
	for sequence := range packets {
		select {
		case <-credit:
		case err := <-done:
			t.Fatalf("UDP stopped at %d: %v", sequence, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if sequence == 1000 || sequence == 2000 || sequence == 3000 {
			current := s.status()
			if !controlOverlap && !current.BrokerRecoveryReady {
				t.Fatalf("recovery not ready before crash: %+v", current)
			}
			pid := current.Broker.PID
			if controlOverlap {
				if current.BrokerControl == nil {
					t.Fatal("missing control process")
				}
				pid = current.BrokerControl.PID
			}
			fd, err := unix.PidfdOpen(pid, 0)
			if err != nil {
				t.Fatal(err)
			}
			err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
			unix.Close(fd)
			if err != nil {
				t.Fatal(err)
			}
			crashes++
		}
		packet := append([]byte(nil), warm...)
		wirebinary.BigEndian.PutUint32(packet, uint32(sequence))
		if _, err := connection.Write(packet); err != nil {
			t.Fatal(err)
		}
		if second != nil {
			if _, err := second.Write(packet); err != nil {
				t.Fatal(err)
			}
			n, err := second.Read(replyForSecond)
			if err != nil || !bytes.Equal(replyForSecond[:n], packet) {
				stats, files, statsErr := s.broker.command(ctx, brokerRequest{Operation: "stats", Tag: "hy"})
				for _, file := range files {
					file.Close()
				}
				socket := controlUDPSocketStats(t, port, publicBefore.inode)
				t.Fatalf("B UDP %d: %d, %v relay_drops=%d kernel_drops=%d stats_error=%v", sequence, n, err, stats.Drops-relayBefore.Drops, socket.drops-publicBefore.drops, statsErr)
			}
		}
		// Response-limited window, not application retransmission. Leave
		// enough time between injected crashes for verified reconstruction.
		time.Sleep(200 * time.Microsecond)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	deadline = time.Now().Add(5 * time.Second)
	for !controlOverlap && !s.status().BrokerRecoveryReady {
		if time.Now().After(deadline) {
			t.Fatalf("final recovery incomplete: %+v", s.status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	after := s.status()
	if after.Active.PID != before.Active.PID || after.Sequence != before.Sequence || controlOverlap && (after.Other.PID != before.Other.PID || after.Broker.PID != before.Broker.PID) {
		t.Fatal("broker recovery replaced the generation")
	}
	publicAfter := controlUDPSocketStats(t, port, publicBefore.inode)
	relayAfter, files, err := s.broker.command(ctx, brokerRequest{Operation: "stats", Tag: "hy"})
	for _, file := range files {
		file.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	if relayAfter.Drops != relayBefore.Drops || publicAfter.drops != publicBefore.drops {
		t.Fatalf("loss under controller failures: relay=%d kernel=%d", relayAfter.Drops-relayBefore.Drops, publicAfter.drops-publicBefore.drops)
	}
	select {
	case err := <-backgroundErrors:
		t.Fatal("background TCP interrupted:", err)
	default:
	}
	t.Logf("control_only=%t native=%t broker_crashes=%d udp_received=%d/%d per_flow xray_pid=%d unchanged=true", controlOverlap, native, crashes, packets, packets, after.Active.PID)
	t.Logf("background_flows=%d tcp_duplex_mibps=%.2f relay_drops=%d kernel_drops=%d", len(streams), float64(tcpBytes.Load())/(1<<20)/time.Since(started).Seconds(), relayAfter.Drops-relayBefore.Drops, publicAfter.drops-publicBefore.drops)
	assertClientNative()
}

type controlSocketStats struct {
	inode string
	drops uint64
}

func controlUDPSocketStats(t *testing.T, port int, inode string) controlSocketStats {
	t.Helper()
	file, err := os.Open("/proc/self/net/udp")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	wanted := fmt.Sprintf("0100007F:%04X", port)
	var result controlSocketStats
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 13 || fields[1] != wanted || inode != "" && fields[9] != inode {
			continue
		}
		if result.inode != "" {
			t.Fatal("ambiguous public UDP socket")
		}
		drops, err := strconv.ParseUint(fields[12], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		result = controlSocketStats{fields[9], drops}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if result.inode == "" {
		t.Fatal("original public UDP socket disappeared")
	}
	return result
}
