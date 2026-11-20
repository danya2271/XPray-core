//go:build linux

package hysteria_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdnet "net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	proxy "github.com/xtls/xray-core/proxy/hysteria"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"github.com/xtls/xray-core/transport/internet/handoff"
	"github.com/xtls/xray-core/transport/internet/hysteria"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type hopTestSocket struct {
	stdnet.PacketConn
	canonical *stdnet.UDPAddr
	alias     *stdnet.UDPAddr
	useAlias  atomic.Bool
	aliasRX   atomic.Uint64
}

func (s *hopTestSocket) WriteTo(payload []byte, destination stdnet.Addr) (int, error) {
	if s.useAlias.Load() {
		destination = s.alias
	}
	return s.PacketConn.WriteTo(payload, destination)
}

func (s *hopTestSocket) ReadFrom(payload []byte) (int, stdnet.Addr, error) {
	n, source, err := s.PacketConn.ReadFrom(payload)
	if udp, ok := source.(*stdnet.UDPAddr); err == nil && ok && s.alias != nil && udp.Port == s.alias.Port && udp.IP.Equal(s.alias.IP) {
		s.aliasRX.Add(1)
		return n, s.canonical, nil
	}
	return n, source, err
}

func TestManagedHysteriaProcesses(t *testing.T) {
	binary := os.Getenv("XRAY_TEST_GENERATION_BINARY")
	if binary == "" {
		t.Skip("set XRAY_TEST_GENERATION_BINARY to a built Xray binary")
	}
	t.Run("go-to-go", func(t *testing.T) { testManagedHysteria(t, binary, false, false, false, false) })
	t.Run("ipv6-go-to-go", func(t *testing.T) { testManagedHysteria(t, binary, false, false, false, true) })
	if os.Getenv("XRAY_TEST_MANAGED_NATIVE") == "1" {
		t.Run("go-to-rust-salamander", func(t *testing.T) { testManagedHysteria(t, binary, false, true, true, false) })
		t.Run("rust-to-go-salamander", func(t *testing.T) { testManagedHysteria(t, binary, true, false, true, false) })
		t.Run("rust-to-rust-salamander", func(t *testing.T) { testManagedHysteria(t, binary, true, true, true, false) })
		t.Run("ipv6-rust-to-go", func(t *testing.T) { testManagedHysteria(t, binary, true, false, false, true) })
	}
}

func testManagedHysteria(t *testing.T, binary string, nativeA, nativeB, masked, ipv6 bool) {
	t.Helper()
	t.Setenv("XRAY_HYSTERIA_NATIVE", "")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("", "hy-switch-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	key := [16]byte{8, 7, 6}
	publicAddr := &stdnet.UDPAddr{IP: stdnet.IPv4(127, 0, 0, 1)}
	network := "udp4"
	if ipv6 {
		publicAddr.IP, network = stdnet.IPv6loopback, "udp6"
	}
	var public stdnet.PacketConn
	var masks []finalmask.UDPMask
	const password = "managed-handoff-test"
	if masked {
		masks = []finalmask.UDPMask{&salamander.Config{Password: password}}
		public, err = finalmask.NewFinalMask(nil, masks, nil, nil, nil,
			func(_ context.Context, a net.Addr) (net.PacketConn, error) {
				return stdnet.ListenUDP(network, a.(*net.UDPAddr))
			}).ListenPacket(ctx, publicAddr)
	} else {
		public, err = stdnet.ListenUDP(network, publicAddr)
	}
	if err != nil {
		if ipv6 {
			t.Skipf("IPv6 loopback unavailable: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { public.Close() })
	local := public.LocalAddr().(*stdnet.UDPAddr)
	ingress, err := handoff.NewUDPIngress(public, key, 0, 4096, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ingress.Close() })
	var alias *stdnet.UDPAddr
	if !ipv6 {
		alias = addNATHop(t, ctx, local)
	}
	certificate, hash := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
	certPEM, keyPEM := certificate.ToPEM()
	certFile, keyFile := filepath.Join(root, "cert.pem"), filepath.Join(root, "key.pem")
	for path, data := range map[string][]byte{certFile: certPEM, keyFile: keyPEM} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	start := func(slot byte, native bool) handoff.ControlSocket {
		t.Helper()
		backend, err := stdnet.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { backend.Close() })
		udp, err := stdnet.ListenUDP("udp4", &stdnet.UDPAddr{IP: stdnet.IPv4(127, 0, 0, 1), Port: backend.Addr().(*stdnet.TCPAddr).Port})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { udp.Close() })
		go func() {
			var pulse [2]byte
			pulse[0] = slot
			for {
				n, addr, err := udp.ReadFromUDPAddrPort(pulse[1:])
				if err != nil {
					return
				}
				if n == 1 {
					udp.WriteToUDPAddrPort(pulse[:], addr)
				}
			}
		}()
		go func() {
			for {
				conn, err := backend.Accept()
				if err != nil {
					return
				}
				go func() {
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(15 * time.Second))
					var pulse [1]byte
					for {
						if _, err := io.ReadFull(conn, pulse[:]); err != nil {
							return
						}
						if _, err := conn.Write([]byte{slot, pulse[0]}); err != nil {
							return
						}
					}
				}()
			}
		}()
		fd, err := ingress.Add(slot)
		if err != nil {
			t.Fatal(err)
		}
		defer fd.Close()
		dir := filepath.Join(root, strconv.Itoa(int(slot)))
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		control := filepath.Join(dir, "ctl.sock")
		manifest, err := json.Marshal(handoff.Manifest{Control: control, UDP: []handoff.UDPBinding{{Tag: "hy", Address: local.IP.String(), Port: local.Port, FD: 3, Key: key, Slot: slot, PacketBatch: true}}})
		if err != nil {
			t.Fatal(err)
		}
		var configuredMask string
		if masked {
			configuredMask = fmt.Sprintf(`,"udp":[{"type":"salamander","settings":{"password":%q}}]`, password)
		}
		config := fmt.Sprintf(`{"inbounds":[{"tag":"hy","listen":%q,"port":%d,"protocol":"hysteria","settings":{"users":[{"auth":"test-auth","email":"tester"}]},"streamSettings":{"network":"hysteria","security":"tls","hysteriaSettings":{"native":%t},"tlsSettings":{"certificates":[{"certificateFile":%q,"keyFile":%q,"oneTimeLoading":true}]},"finalmask":{"quicParams":{"disableChromeParrot":true,"congestion":"reno"}%s}}}],"outbounds":[{"protocol":"freedom","settings":{"redirect":%q,"finalRules":[{"action":"allow","ip":["127.0.0.1/32"]}]}}]}`,
			local.IP.String(), local.Port, native, certFile, keyFile, configuredMask, backend.Addr().String())
		manifestFile, configFile := filepath.Join(dir, "manifest.json"), filepath.Join(dir, "config.json")
		if err := os.WriteFile(manifestFile, manifest, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configFile, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		log, err := os.Create(filepath.Join(dir, "process.log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, binary, "run", "-format=json", "-config", configFile, "-handoff-manifest", manifestFile)
		cmd.Stdout, cmd.Stderr, cmd.ExtraFiles = log, log, []*os.File{fd}
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cmd.Process.Kill()
			cmd.Wait()
			log.Close()
			if t.Failed() {
				content, _ := os.ReadFile(log.Name())
				t.Logf("generation %d: %s", slot, content)
			}
		})
		socket := handoff.ControlSocket(control)
		for attempts := 0; attempts < 300; attempts++ {
			if reply, err := socket.Command(ctx, "status"); err == nil && reply.State == "paused" {
				return socket
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("new Hysteria process never became ready")
		return ""
	}
	open := func() (stat.Connection, *hopTestSocket, *internet.MemoryStreamConfig) {
		t.Helper()
		socket, err := stdnet.ListenUDP(network, publicAddr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { socket.Close() })
		peer := &hopTestSocket{PacketConn: socket, canonical: local, alias: alias}
		clientMask := finalmask.NewFinalMask(nil, masks, nil, nil,
			func(_ context.Context, _ net.Destination) (net.PacketConn, net.Addr, error) { return peer, local, nil }, nil)
		settings := &internet.MemoryStreamConfig{
			ProtocolName: "hysteria", ProtocolSettings: &hysteria.Config{Auth: "test-auth"},
			FinalMask: clientMask, SecurityType: "tls", QuicParams: &internet.QuicParams{DisableChromeParrot: true, Congestion: "reno"},
			SecuritySettings: &tls.Config{PinnedPeerCertSha256: [][]byte{hash[:]}, ServerName: "localhost"},
		}
		conn, err := hysteria.Dial(ctx, net.UDPDestination(net.IPAddress(local.IP), net.Port(local.Port)), settings)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := proxy.WriteTCPRequest(conn, "127.0.0.1:80"); err != nil {
			t.Fatal(err)
		}
		if ok, msg, err := proxy.ReadTCPResponse(conn); err != nil || !ok {
			t.Fatalf("Hysteria stream failed: %s (%v)", msg, err)
		}
		return conn, peer, settings
	}
	command := func(socket handoff.ControlSocket, cmd, state string) {
		t.Helper()
		if reply, err := socket.Command(ctx, cmd); err != nil || reply.State != state {
			t.Fatalf("%s: %+v (%v)", cmd, reply, err)
		}
	}
	pulse := func(conn stat.Connection, slot, value byte) {
		t.Helper()
		if _, err := conn.Write([]byte{value}); err != nil {
			t.Fatal(err)
		}
		var got [2]byte
		if _, err := io.ReadFull(conn, got[:]); err != nil || got != [2]byte{slot, value} {
			t.Fatalf("got %v (%v), wanted generation %d value %c", got, err, slot, value)
		}
	}
	udpPulse := func(conn stat.Connection, slot, value byte) {
		t.Helper()
		msg := proxy.UDPMessage{FragCount: 1, Addr: "127.0.0.1:80", Data: []byte{value}}
		var outgoing [256]byte
		if _, err := conn.Write(outgoing[:msg.Serialize(outgoing[:])]); err != nil {
			t.Fatal(err)
		}
		finished := make(chan error, 1)
		go func() {
			var incoming [256]byte
			n, err := conn.Read(incoming[:])
			if err != nil {
				finished <- err
				return
			}
			response, err := proxy.ParseUDPMessage(incoming[:n])
			if err != nil || string(response.Data) != string([]byte{slot, value}) {
				finished <- fmt.Errorf("unexpected UDP relay reply: %v (%v), wanted %d %q", response, err, slot, value)
				return
			}
			finished <- nil
		}()
		select {
		case err := <-finished:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Hysteria UDP relay did not respond")
		}
	}
	a := start(0, nativeA)
	command(a, "resume", "active")
	old, hopper, oldSettings := open()
	pulse(old, 0, 'a')
	oldUDP, err := hysteria.Dial(hysteria.ContextWithDatagram(ctx, true), net.UDPDestination(net.IPAddress(local.IP), net.Port(local.Port)), oldSettings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { oldUDP.Close() })
	udpPulse(oldUDP, 0, 'u')
	b := start(1, nativeB)
	if _, err := handoff.Switch(ctx, a, b, func(context.Context) error { return nil }); err == nil {
		t.Fatal("generation with UDP accepted a switch without its ingress selector")
	}
	selectSlot := func(candidate bool) error {
		if candidate {
			return ingress.Activate(1)
		}
		return ingress.Activate(0)
	}
	probeFailed := errors.New("simulated UDP probe failure")
	var candidateFlow stat.Connection
	result, err := handoff.SwitchWithIngress(ctx, a, b, selectSlot, func(context.Context) error {
		candidateFlow, _, _ = open()
		pulse(candidateFlow, 1, 'b')
		return probeFailed
	})
	if !errors.Is(err, probeFailed) || !result.RolledBack || result.Active != "previous" {
		t.Fatalf("UDP ingress rollback: %+v (%v)", result, err)
	}
	restored, _, _ := open()
	pulse(restored, 0, 'r')
	pulse(candidateFlow, 1, 's')
	var next stat.Connection
	result, err = handoff.SwitchWithIngress(ctx, a, b, selectSlot, func(context.Context) error {
		next, _, _ = open()
		pulse(next, 1, 't')
		return nil
	})
	if err != nil || result.Active != "candidate" {
		t.Fatalf("UDP cutover: %+v (%v)", result, err)
	}
	if alias != nil {
		hopper.useAlias.Store(true)
	}
	pulse(old, 0, 'c')
	udpPulse(oldUDP, 0, 'v')
	if alias != nil && hopper.aliasRX.Load() == 0 {
		t.Fatal("old QUIC connection did not receive a packet through the REDIRECT port")
	}
	for _, socket := range []handoff.ControlSocket{a, b} {
		reply, err := socket.Command(ctx, "quic-status")
		if info := reply.UDP[fmt.Sprintf("hy:%d", local.Port)]; err != nil || !info.Known || info.Count != 2 || !info.WorkKnown || info.TransportWork < 2 {
			t.Fatalf("UDP accounting %s: %+v (%v)", socket, reply, err)
		}
	}
	if _, err := a.Command(ctx, "drain"); err == nil {
		t.Fatal("managed UDP drain incorrectly declared safe without ingress retirement")
	}
	pulse(next, 1, 'd')
}

// Optional real REDIRECT test: enable only inside a throwaway network
// namespace (e.g. unshare -Urn), never on the host network.
func addNATHop(t *testing.T, ctx context.Context, local *stdnet.UDPAddr) *stdnet.UDPAddr {
	t.Helper()
	if os.Getenv("XRAY_TEST_ISOLATED_UDP_NAT") != "1" {
		return nil
	}
	owner := os.Getenv("XRAY_TEST_UDP_NAT_HOST_NS")
	current, err := os.Readlink("/proc/self/ns/net")
	if err != nil || owner == "" || current == owner {
		t.Fatal("refusing NAT test without an isolated network namespace")
	}
	reserve, err := stdnet.ListenUDP("udp4", &stdnet.UDPAddr{IP: local.IP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reserve.Close() })
	alias := reserve.LocalAddr().(*stdnet.UDPAddr)
	rule := []string{"OUTPUT", "-d", "127.0.0.1", "-p", "udp", "--dport", strconv.Itoa(alias.Port), "-j", "REDIRECT", "--to-ports", strconv.Itoa(local.Port)}
	args := append([]string{"-t", "nat", "-A"}, rule...)
	if output, err := exec.CommandContext(ctx, "iptables", args...).CombinedOutput(); err != nil {
		t.Fatalf("test namespace REDIRECT failed: %v: %s", err, output)
	}
	t.Cleanup(func() {
		args := append([]string{"-t", "nat", "-D"}, rule...)
		if output, err := exec.Command("iptables", args...).CombinedOutput(); err != nil {
			t.Errorf("removing test REDIRECT: %v: %s", err, output)
		}
	})
	return alias
}
