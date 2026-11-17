//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"golang.org/x/sys/unix"
)

func TestTunePacketSocketMatchesNativeBufferPolicy(t *testing.T) {
	for _, server := range []bool{false, true} {
		t.Run(fmt.Sprintf("server_%t", server), func(t *testing.T) {
			open := func() *net.UDPConn {
				packet, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { packet.Close() })
				return packet
			}
			buffers := func(packet *net.UDPConn, configure bool) [2]int {
				raw, err := packet.SyscallConn()
				if err != nil {
					t.Fatal(err)
				}
				var result [2]int
				if err := raw.Control(func(descriptor uintptr) {
					for index, option := range []int{unix.SO_RCVBUF, unix.SO_SNDBUF} {
						if configure {
							requested := 256 << 10
							if server {
								requested = 1 << 20
							}
							if option == unix.SO_RCVBUF {
								requested *= 4
							}
							if err := unix.SetsockoptInt(int(descriptor), unix.SOL_SOCKET, option, requested); err != nil {
								t.Error(err)
							}
						}
						result[index], err = unix.GetsockoptInt(int(descriptor), unix.SOL_SOCKET, option)
						if err != nil {
							t.Error(err)
						}
					}
				}); err != nil {
					t.Fatal(err)
				}
				return result
			}
			packet := open()
			want := buffers(open(), true)
			for range 2 {
				TunePacketSocket(packet, server, false)
				if got := buffers(packet, false); got != want {
					t.Fatalf("UDP buffers=%v native policy=%v", got, want)
				}
			}
		})
	}
}

func TestPacketMetadataRoundTripsAndRejectsMalformedFrames(t *testing.T) {
	for _, address := range []string{"127.0.0.1:443", "[2001:db8::1]:8443"} {
		peer, err := net.ResolveUDPAddr("udp", address)
		if err != nil {
			t.Fatal(err)
		}
		wire := make([]byte, packetHeader+16)
		length, err := encodePacket(wire, peer, []byte("QUIC"))
		if err != nil {
			t.Fatal(err)
		}
		decoded, payload, err := decodePacket(wire[:length])
		if err != nil || decoded.String() != address || string(payload) != "QUIC" {
			t.Fatalf("address=%v payload=%q error=%v", decoded, payload, err)
		}
		for size := range packetHeader {
			if _, _, err := decodePacket(wire[:size]); err == nil {
				t.Fatal("short packet metadata accepted")
			}
		}
		wire[20] = 1
		if _, _, err := decodePacket(wire[:length]); err == nil {
			t.Fatal("invalid packet length accepted")
		}
	}
}

func TestPacketPeerCacheReusesAndOwnsAddresses(t *testing.T) {
	var cache packetPeerCache
	wire := make([]byte, packetHeader+16)
	for _, address := range []string{"127.0.0.1:443", "[2001:db8::1]:8443"} {
		peer, err := net.ResolveUDPAddr("udp", address)
		if err != nil {
			t.Fatal(err)
		}
		length, err := encodePacket(wire, peer, []byte("QUIC"))
		if err != nil {
			t.Fatal(err)
		}
		first, _, err := cache.decode(wire[:length])
		if err != nil {
			t.Fatal(err)
		}
		second, _, err := cache.decode(wire[:length])
		if err != nil || first != second {
			t.Fatal("packet peer cache allocated on a hit")
		}
		wire[0] = 2
		binary.BigEndian.PutUint16(wire[20:22], 2)
		binary.BigEndian.PutUint16(wire[22:24], 4)
		batched, _, err := cache.decode(wire[:length])
		if err != nil || first != batched {
			t.Fatal("IPC batch did not reuse the single-packet address")
		}
		for port := 1; port <= 128; port++ {
			length, err := encodePacket(wire, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: port}, []byte("other"))
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := cache.decode(wire[:length]); err != nil {
				t.Fatal(err)
			}
		}
		clear(wire)
		if first.String() != address {
			t.Fatal("cached packet address changed after wire reuse/eviction")
		}
	}
	if _, _, err := cache.decode([]byte("invalid")); err == nil {
		t.Fatal("cache accepted malformed frame")
	}
}

func TestPacketBatchMetadataValidatesBoundaries(t *testing.T) {
	for _, test := range []struct {
		name            string
		stride, payload int
		valid           bool
	}{
		{"single", 1200, 1200, true},
		{"short_tail", 1200, 2401, true},
		{"max_batch", 1200, 9600, true},
		{"max_batch_short_tail", 1200, 8401, true},
		{"too_many", 1200, 9601, false},
		{"zero_stride", 0, 2400, false},
		{"oversized_stride", 2401, 2400, false},
		{"empty", 0, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte{0x37}, test.payload)
			wire := make([]byte, packetHeader+len(payload))
			peer := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443}
			length, err := encodePacket(wire, peer, payload)
			if err != nil {
				t.Fatal(err)
			}
			wire[0] = 2
			binary.BigEndian.PutUint16(wire[20:22], uint16(test.stride))
			binary.BigEndian.PutUint16(wire[22:24], uint16(test.payload))
			decoded, received, err := decodePacket(wire[:length])
			if !test.valid {
				if err == nil {
					t.Fatal("malformed batch accepted")
				}
				return
			}
			if err != nil || decoded.String() != peer.String() || !bytes.Equal(received, payload) {
				t.Fatalf("batch address or payload changed: %v", err)
			}
			wire[23] ^= 1
			if _, _, err := decodePacket(wire); err == nil {
				t.Fatal("incorrect batch length accepted")
			}
		})
	}
}

func TestPacketRelaySplitsBatchesBeforeMaskingAndCountsMTUDrops(t *testing.T) {
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		packet.Close()
		t.Fatal(err)
	}
	defer remote.Close()
	remote.SetDeadline(time.Now().Add(2 * time.Second))
	limited := &mtuPacketConn{PacketConn: packet, limit: 1208}
	mask := &salamander.Config{Password: "batch-test"}
	masked, err := finalmask.NewFinalMask(nil, []finalmask.UDPMask{mask}, nil, nil, nil, func(context.Context, net.Addr) (net.PacketConn, error) { return limited, nil }).ListenPacket(context.Background(), packet.LocalAddr())
	if err != nil {
		packet.Close()
		t.Fatal(err)
	}
	relay, file, err := newPacketRelay(masked, nil)
	if err != nil {
		masked.Close()
		t.Fatal(err)
	}
	defer func() { relay.stop(net.ErrClosed); relay.wg.Wait() }()
	pipe, err := net.FileConn(file)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer pipe.Close()
	obfuscator, err := salamander.NewSalamanderObfuscator([]byte("batch-test"))
	if err != nil {
		t.Fatal(err)
	}
	wire := make([]byte, packetHeader+3601)
	for _, stride := range []int{1201, 1200} {
		payload := bytes.Repeat([]byte{0x37}, 2*stride+1)
		length, err := encodePacket(wire, remote.LocalAddr().(*net.UDPAddr), payload)
		if err != nil {
			t.Fatal(err)
		}
		wire[0] = 2
		binary.BigEndian.PutUint16(wire[20:22], uint16(stride))
		binary.BigEndian.PutUint16(wire[22:24], uint16(len(payload)))
		if _, err := pipe.Write(wire[:length]); err != nil {
			t.Fatal(err)
		}
		wanted := []int{1}
		if stride == 1200 {
			wanted = []int{1200, 1200, 1}
		}
		for _, size := range wanted {
			packet := make([]byte, 1500)
			length, _, err := remote.ReadFrom(packet)
			if err != nil || length != size+8 {
				t.Fatalf("batched masked datagram length=%d expected=%d error=%v", length, size+8, err)
			}
			decoded := make([]byte, size)
			obfuscator.Deobfuscate(packet[:length], decoded)
			if !bytes.Equal(decoded, bytes.Repeat([]byte{0x37}, size)) {
				t.Fatal("batch masking corrupted segment")
			}
		}
	}
	if relay.txDropped.Load() != 2 || relay.txMTUDropped.Load() != 2 || relay.txMaskDropped.Load() != 0 {
		t.Fatal("MTU drops were not attributed separately")
	}
	for _, stride := range []uint16{0, 1, 5000} {
		binary.BigEndian.PutUint16(wire[20:22], stride)
		if _, _, err := decodePacket(wire[:packetHeader+2401]); err == nil {
			t.Fatal("invalid batch accepted")
		}
	}
}

type mtuPacketConn struct {
	net.PacketConn
	limit int
}

func (packet *mtuPacketConn) WriteTo(payload []byte, address net.Addr) (int, error) {
	if len(payload) > packet.limit {
		return 0, &net.OpError{Op: "write", Net: "udp", Err: os.NewSyscallError("sendto", unix.EMSGSIZE)}
	}
	return packet.PacketConn.WriteTo(payload, address)
}

func BenchmarkPacketPeerDecode(b *testing.B) {
	for _, cached := range []bool{false, true} {
		b.Run(fmt.Sprintf("cached_%t", cached), func(b *testing.B) {
			wire := make([]byte, packetHeader+1200)
			length, err := encodePacket(wire, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443}, make([]byte, 1200))
			if err != nil {
				b.Fatal(err)
			}
			var cache packetPeerCache
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var address *net.UDPAddr
				var err error
				if cached {
					address, _, err = cache.decode(wire[:length])
				} else {
					address, _, err = decodePacket(wire[:length])
				}
				if err != nil || address.Port != 443 {
					b.Fatal("packet decode failed")
				}
			}
		})
	}
}

func TestPacketRelayPreservesMessagesAndShutdown(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "server", true: "client"}[fixed], func(t *testing.T) {
			packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			remote, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				packet.Close()
				t.Fatal(err)
			}
			defer remote.Close()
			remote.SetDeadline(time.Now().Add(2 * time.Second))
			var logical *net.UDPAddr
			if fixed {
				logical = &net.UDPAddr{IP: net.ParseIP("127.0.0.2"), Port: 443}
			}
			relay, file, err := newPacketRelay(packet, logical)
			if err != nil {
				packet.Close()
				t.Fatal(err)
			}
			defer func() { relay.stop(net.ErrClosed); relay.wg.Wait() }()
			connection, err := net.FileConn(file)
			file.Close()
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			connection.SetDeadline(time.Now().Add(2 * time.Second))
			wire := make([]byte, packetHeader+maxPacket)
			for _, payload := range [][]byte{[]byte("one"), bytes.Repeat([]byte{0x37}, 4096), bytes.Repeat([]byte{0x37}, 65507), {}} {
				if _, err := remote.WriteTo(payload, packet.LocalAddr()); err != nil {
					t.Fatal(err)
				}
				count, err := connection.Read(wire)
				if err != nil {
					t.Fatal(err)
				}
				address, received, err := decodePacket(wire[:count])
				if err != nil || !bytes.Equal(payload, received) {
					t.Fatalf("packet=%q error=%v", received, err)
				}
				want := remote.LocalAddr().String()
				if fixed {
					want = logical.String()
				}
				if address.String() != want {
					t.Fatal("packet relay lost the logical peer")
				}
				length, err := encodePacket(wire, remote.LocalAddr().(*net.UDPAddr), payload)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := connection.Write(wire[:length]); err != nil {
					t.Fatal(err)
				}
				count, _, err = remote.ReadFrom(wire)
				if err != nil || !bytes.Equal(payload, wire[:count]) {
					t.Fatalf("wire packet changed: %v", err)
				}
			}
			relay.stop(net.ErrClosed)
			complete := make(chan struct{})
			go func() { relay.wg.Wait(); close(complete) }()
			select {
			case <-complete:
			case <-time.After(time.Second):
				t.Fatal("packet relay goroutine leaked")
			}
		})
	}
}

func TestPacketRelayStartupFailureReleasesSocket(t *testing.T) {
	for _, failure := range []string{"cancelled", "invalid_tls", "invalid_local", "invalid_peer", "zone"} {
		t.Run(failure, func(t *testing.T) {
			packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			config := Config{Server: true, Users: []string{"test"}}
			local := packet.LocalAddr()
			switch failure {
			case "cancelled":
				cancel()
			case "invalid_local":
				local = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
			case "invalid_peer":
				config = Config{Peer: "invalid peer"}
			case "zone":
				local = &net.UDPAddr{IP: net.IPv6loopback, Zone: "eth0"}
			}
			if _, err := StartPacketConn(ctx, config, packet, local, nil, nil); err == nil {
				t.Fatal("invalid startup succeeded")
			}
			packet.SetReadDeadline(time.Now().Add(time.Second))
			if _, _, err := packet.ReadFrom(make([]byte, 1)); err == nil {
				t.Fatal("failed startup retained the packet socket")
			}
		})
	}
}

type lossyPacketConn struct {
	net.PacketConn
	fail atomic.Bool
}

func (packet *lossyPacketConn) WriteTo(payload []byte, address net.Addr) (int, error) {
	if packet.fail.Swap(false) {
		return 0, &net.OpError{Op: "write", Net: "udp", Err: os.NewSyscallError("sendto", unix.ENOBUFS)}
	}
	return packet.PacketConn.WriteTo(payload, address)
}

func TestPacketRelayContinuesAfterTransientSendFailure(t *testing.T) {
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		packet.Close()
		t.Fatal(err)
	}
	defer remote.Close()
	remote.SetDeadline(time.Now().Add(time.Second))
	lossy := &lossyPacketConn{PacketConn: packet}
	lossy.fail.Store(true)
	relay, file, err := newPacketRelay(lossy, nil)
	if err != nil {
		packet.Close()
		t.Fatal(err)
	}
	defer func() { relay.stop(net.ErrClosed); relay.wg.Wait() }()
	connection, err := net.FileConn(file)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	wire := make([]byte, packetHeader+16)
	for _, payload := range []string{"dropped", "delivered"} {
		length, err := encodePacket(wire, remote.LocalAddr().(*net.UDPAddr), []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Write(wire[:length]); err != nil {
			t.Fatal(err)
		}
	}
	count, _, err := remote.ReadFrom(wire)
	if err != nil || string(wire[:count]) != "delivered" || relay.txDropped.Load() != 1 {
		t.Fatalf("transient failure closed or corrupted relay: payload=%q drops=%d err=%v", wire[:count], relay.txDropped.Load(), err)
	}
	if packetSendRetryable(net.ErrClosed) || packetSendRetryable(errors.New("mask configuration error")) {
		t.Fatal("fatal packet error was suppressed")
	}
}

func TestPacketEnginesTransferAndCloseConcurrently(t *testing.T) {
	for _, bridge := range []string{"shared", "socket"} {
		t.Run(bridge, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
			certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
			ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate}))
			key := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: certificate.PrivateKey}))
			baseline := Stats().LiveEngines
			var engines []*Engine
			var packets []net.PacketConn
			t.Cleanup(func() {
				for _, engine := range engines {
					engine.Close()
				}
			})
			start := func(config Config, accept func(*Conn)) *Engine {
				t.Helper()
				packet, err := net.ListenPacket("udp", ":0")
				if err != nil {
					t.Fatal(err)
				}
				engine, err := StartPacketConn(context.Background(), config, packet, nil, nil, accept)
				if err != nil {
					t.Fatal(err)
				}
				engines = append(engines, engine)
				packets = append(packets, packet)
				return engine
			}
			server := start(Config{Server: true, Users: []string{"test"}, CertificatePEM: ca, KeyPEM: key}, func(conn *Conn) {
				defer conn.Close()
				io.Copy(conn, conn)
			})
			_, port, err := net.SplitHostPort(server.local.String())
			if err != nil {
				t.Fatal(err)
			}
			client := start(Config{Peer: net.JoinHostPort("localhost", port), ServerName: "localhost", Auth: "test", CAPEM: ca}, nil)
			connection, err := client.Open(context.Background(), xnet.TCPDestination(xnet.LocalHostIP, 80))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			connection.SetDeadline(time.Now().Add(3 * time.Second))
			payload := bytes.Repeat([]byte{0x37}, 64<<10)
			if _, err := connection.Write(payload); err != nil {
				t.Fatal(err)
			}
			received := make([]byte, len(payload))
			if _, err := io.ReadFull(connection, received); err != nil || !bytes.Equal(payload, received) {
				t.Fatalf("packet bridge transfer failed: %v", err)
			}
			for index, packet := range packets {
				if err := packet.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-engines[index].done:
				case <-time.After(time.Second):
					t.Fatal("packet failure did not stop engine")
				}
			}
			var workers sync.WaitGroup
			for _, engine := range engines {
				for range 2 {
					workers.Add(1)
					go func(engine *Engine) { defer workers.Done(); engine.Close() }(engine)
				}
			}
			complete := make(chan struct{})
			go func() { workers.Wait(); close(complete) }()
			select {
			case <-complete:
			case <-time.After(3 * time.Second):
				t.Fatal("concurrent packet engine close blocked")
			}
			for _, packet := range packets {
				if _, _, err := packet.ReadFrom(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
					t.Fatalf("packet engine retained socket: %v", err)
				}
			}
			if Stats().LiveEngines != baseline {
				t.Fatal("packet engine leaked native handle")
			}
		})
	}
}
