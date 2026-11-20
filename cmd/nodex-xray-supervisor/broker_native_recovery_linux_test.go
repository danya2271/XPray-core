//go:build linux && !android && xhysteria_native && cgo

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	wirebinary "encoding/binary"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"golang.org/x/sys/unix"
)

func TestNativeDirectBrokerRecoveryPreservesApplicationUDP(t *testing.T) {
	binary := os.Getenv("XRAY_TEST_GENERATION_BINARY")
	if binary == "" {
		t.Skip("requires native Xray executable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
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
	config := fmt.Sprintf(`{"log":{"loglevel":"warning"},"inbounds":[{"tag":"hy","listen":"127.0.0.1","port":%d,"protocol":"hysteria","settings":{"users":[{"auth":"test-auth"}]},"streamSettings":{"network":"hysteria","security":"tls","hysteriaSettings":{"native":true},"tlsSettings":{"certificates":[{"certificateFile":%q,"keyFile":%q,"oneTimeLoading":true}]},"finalmask":{"quicParams":{"disableChromeParrot":true},"udp":[{"type":"salamander","settings":{"password":"test-password"}}]}}}],"outbounds":[{"protocol":"freedom","settings":{"finalRules":[{"action":"allow","ip":["127.0.0.1/32"]}]}}]}`, port, certPath, keyPath)
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
	if !before.BrokerRecoveryReady {
		t.Fatalf("checkpoint unavailable: %+v", before)
	}
	block, _ := pem.Decode(certificatePEM)
	pin := sha256.Sum256(block.Bytes)
	clientConfig := fmt.Sprintf(`{"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":%d,"protocol":"dokodemo-door","settings":{"address":"127.0.0.1","port":%d,"network":"udp"}}],"outbounds":[{"protocol":"hysteria","settings":{"address":"127.0.0.1","port":%d},"streamSettings":{"network":"hysteria","security":"tls","hysteriaSettings":{"auth":"test-auth","native":true},"tlsSettings":{"serverName":"localhost","pinnedPeerCertSha256":"%x"},"finalmask":{"quicParams":{"disableChromeParrot":true},"udp":[{"type":"salamander","settings":{"password":"test-password"}}]}}}]}`, clientPort, echo.LocalAddr().(*net.UDPAddr).Port, port, pin)
	clientPath := filepath.Join(root, "client.json")
	if err := os.WriteFile(clientPath, []byte(clientConfig), 0600); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(root, "client.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	client := exec.CommandContext(ctx, binary, "run", "-config", clientPath)
	client.Env = append(os.Environ(), "XRAY_HYSTERIA_NATIVE=1")
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
	if err := connection.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
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
			if !current.BrokerRecoveryReady {
				t.Fatalf("recovery not ready before crash: %+v", current)
			}
			fd, err := unix.PidfdOpen(current.Broker.PID, 0)
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
	for !s.status().BrokerRecoveryReady {
		if time.Now().After(deadline) {
			t.Fatalf("final recovery incomplete: %+v", s.status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	after := s.status()
	if after.Active.PID != before.Active.PID || after.Sequence != before.Sequence {
		t.Fatal("broker recovery replaced the generation")
	}
	t.Logf("broker_crashes=%d udp_received=%d/%d xray_pid=%d unchanged=true", crashes, packets, packets, after.Active.PID)
}
