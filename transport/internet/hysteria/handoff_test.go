package hysteria_test

import (
	"context"
	cryptotls "crypto/tls"
	"io"
	stdnet "net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apernet/quic-go"
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

// Hold the server's first flight so the transport has an in-progress
// handshake but the Hysteria listener cannot have accepted it yet.
type holdHandshake struct {
	net.PacketConn
	release <-chan struct{}
}

func (c *holdHandshake) WriteTo(p []byte, addr net.Addr) (int, error) {
	select {
	case <-c.release:
		return c.PacketConn.WriteTo(p, addr)
	default:
		return len(p), nil // Simulate outbound packet loss during handshake.
	}
}

func TestManagedGoStatusCountsUnacceptedHandshake(t *testing.T) {
	t.Setenv("XRAY_HYSTERIA_NATIVE", "")
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.LocalHostIP.IP()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	local := server.LocalAddr().(*net.UDPAddr)
	release := make(chan struct{})
	certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
	credential := tls.ParseCertificate(certificate)
	credential.OneTimeLoading = true
	settings := &internet.MemoryStreamConfig{
		ProtocolName: "hysteria", ProtocolSettings: &hysteria.Config{Auth: "test-auth"},
		SecurityType: "tls", SecuritySettings: &tls.Config{Certificate: []*tls.Certificate{credential}},
	}
	l, err := hysteria.ListenGeneration(ctx, net.LocalHostIP, net.Port(local.Port), settings, func(stat.Connection) {},
		&holdHandshake{PacketConn: server, release: release}, [16]byte{8}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	status := l.(hysteria.GenerationListener).GenerationStatus
	client := make(chan *quic.Conn, 1)
	dialError := make(chan error, 1)
	go func() {
		conn, err := quic.DialAddr(ctx, local.String(), &cryptotls.Config{
			InsecureSkipVerify: true, NextProtos: []string{"h3"}, // Local ephemeral certificate.
		}, nil)
		if err != nil {
			dialError <- err
			return
		}
		client <- conn
		<-ctx.Done()
		_ = conn.CloseWithError(0, "test finished")
	}()
	wait := func(ready func(handoff.UDPConnectionStatus) bool) {
		t.Helper()
		for ctx.Err() == nil {
			if ready(status()) {
				return
			}
			select {
			case err := <-dialError:
				t.Fatal(err)
			case <-time.After(5 * time.Millisecond):
			}
		}
		t.Fatalf("unexpected QUIC work status: %+v", status())
	}
	wait(func(s handoff.UDPConnectionStatus) bool {
		return s.Known && s.WorkKnown && s.Count == 0 && s.TransportWork == 1
	})
	close(release)
	select {
	case conn := <-client:
		wait(func(s handoff.UDPConnectionStatus) bool {
			return s.Known && s.WorkKnown && s.Count == 1 && s.TransportWork == 1
		})
		if err := conn.CloseWithError(0, "test finished"); err != nil {
			t.Fatal(err)
		}
	case err := <-dialError:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	wait(func(s handoff.UDPConnectionStatus) bool {
		return s.Known && s.WorkKnown && s.Count == 0 && s.TransportWork == 0
	})
}

type testPacket struct {
	data []byte
	peer net.Addr
}

// Test double for the supervisor's per-generation datagram ingress. It reads
// real packets from the one public UDP socket and preserves peer addresses.
type testPacketView struct {
	net.PacketConn
	packets chan testPacket
	closed  chan struct{}
	once    sync.Once
	mu      sync.Mutex
	wake    chan struct{}
	readBy  time.Time
}

func (p *testPacketView) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

func (p *testPacketView) SetReadDeadline(at time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readBy = at
	close(p.wake)
	p.wake = make(chan struct{})
	return nil
}

func (p *testPacketView) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		p.mu.Lock()
		wake, deadline := p.wake, p.readBy
		p.mu.Unlock()
		var timer *time.Timer
		var expired <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			expired = timer.C
		}
		select {
		case packet := <-p.packets:
			if timer != nil {
				timer.Stop()
			}
			return copy(b, packet.data), packet.peer, nil
		case <-p.closed:
			if timer != nil {
				timer.Stop()
			}
			return 0, nil, stdnet.ErrClosed
		case <-expired:
			return 0, nil, os.ErrDeadlineExceeded
		case <-wake:
			if timer != nil {
				timer.Stop()
			}
		}
	}
}

func TestHysteriaGoHandoff(t *testing.T) {
	for _, masked := range []bool{false, true} {
		testHysteriaHandoff(t, false, false, masked)
	}
}

func testHysteriaHandoff(t *testing.T, nativeA, nativeB, masked bool) {
	t.Helper()
	t.Setenv("XRAY_HYSTERIA_NATIVE", "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := [16]byte{7, 8, 9}
	routes := handoff.NewQUICRoutes(handoff.NewQUICCIDCodec(key), 0, 1024, 10*time.Second)
	var masks *finalmask.FinalMask
	if masked {
		masks = finalmask.NewFinalMask(nil, []finalmask.UDPMask{&salamander.Config{Password: "handoff-test-mask"}},
			nil, nil,
			func(ctx context.Context, dest net.Destination) (net.PacketConn, net.Addr, error) {
				dialed, err := internet.DialSystem(ctx, dest, nil)
				if err != nil {
					return nil, nil, err
				}
				return dialed.(*net.PacketConnWrapper).PacketConn, dialed.RemoteAddr(), nil
			},
			func(ctx context.Context, address net.Addr) (net.PacketConn, error) {
				return internet.ListenSystemPacket(ctx, address, nil)
			})
	}
	address := &net.UDPAddr{IP: net.LocalHostIP.IP()}
	var public net.PacketConn
	var err error
	if masks != nil {
		public, err = masks.ListenPacket(ctx, address)
	} else {
		public, err = net.ListenUDP("udp4", address)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	local := public.LocalAddr().(*net.UDPAddr)
	views := [2]*testPacketView{}
	for slot := range views {
		views[slot] = &testPacketView{PacketConn: public, packets: make(chan testPacket, 256), closed: make(chan struct{}), wake: make(chan struct{})}
		defer views[slot].Close()
	}
	go func() {
		reader := routes.NewReader()
		var b [65535]byte
		for {
			n, peer, err := public.ReadFrom(b[:])
			if err != nil {
				return
			}
			if slot, ok := reader.Route(b[:n], time.Now()); ok && slot < 2 {
				select {
				case views[slot].packets <- testPacket{data: append([]byte(nil), b[:n]...), peer: peer}:
				case <-views[slot].closed:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	certificate, hash := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
	quicConfig := &internet.QuicParams{DisableChromeParrot: true, Congestion: "reno"}
	start := func(slot byte, native bool) internet.Listener {
		t.Helper()
		credential := tls.ParseCertificate(certificate)
		credential.OneTimeLoading = true
		settings := &internet.MemoryStreamConfig{
			ProtocolName: "hysteria", ProtocolSettings: &hysteria.Config{Auth: "test-auth", Native: native},
			QuicParams: quicConfig, SecurityType: "tls", FinalMask: masks,
			SecuritySettings: &tls.Config{Certificate: []*tls.Certificate{credential}},
		}
		l, err := hysteria.ListenGeneration(ctx, net.LocalHostIP, net.Port(local.Port), settings, func(conn stat.Connection) {
			go func() {
				defer conn.Close()
				// The native engine handles the request and response headers
				// itself, as in proxy/hysteria.Server.Process.
				if !native {
					if _, err := proxy.ReadTCPRequest(conn); err != nil {
						return
					}
					if err := proxy.WriteTCPResponse(conn, true, ""); err != nil {
						return
					}
				}
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
		}, views[slot], key, slot)
		if err != nil {
			t.Fatal(err)
		}
		if n, ok := l.(hysteria.GenerationListener).GenerationConnections(); !ok || n != 0 {
			t.Fatalf("new generation: expected a known zero, got %d (%v)", n, ok)
		}
		t.Cleanup(func() { l.Close() })
		return l
	}
	open := func() stat.Connection {
		t.Helper()
		settings := &internet.MemoryStreamConfig{
			ProtocolName: "hysteria", ProtocolSettings: &hysteria.Config{Auth: "test-auth"},
			QuicParams: quicConfig, SecurityType: "tls", FinalMask: masks,
			SecuritySettings: &tls.Config{PinnedPeerCertSha256: [][]byte{hash[:]}, ServerName: "localhost"},
		}
		conn, err := hysteria.Dial(ctx, net.UDPDestination(net.LocalHostIP, net.Port(local.Port)), settings)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := proxy.WriteTCPRequest(conn, "localhost:80"); err != nil {
			t.Fatal(err)
		}
		if ok, msg, err := proxy.ReadTCPResponse(conn); !ok || err != nil {
			t.Fatalf("Hysteria stream not ready: %s %v", msg, err)
		}
		return conn
	}
	check := func(conn stat.Connection, slot, pulse byte) {
		t.Helper()
		if _, err := conn.Write([]byte{pulse}); err != nil {
			t.Fatal(err)
		}
		var got [2]byte
		if _, err := io.ReadFull(conn, got[:]); err != nil || got != [2]byte{slot, pulse} {
			t.Fatalf("got %v, %v; expected generation %d with %q", got, err, slot, pulse)
		}
	}
	previous := start(0, nativeA).(hysteria.GenerationListener)
	a := open()
	check(a, 0, 'a')
	if n, ok := previous.GenerationConnections(); !ok || n != 1 {
		t.Fatalf("old generation: live connection not visible: %d (%v)", n, ok)
	}
	candidate := start(1, nativeB).(hysteria.GenerationListener)
	routes.Activate(1)
	b := open()
	check(b, 1, 'b')
	check(a, 0, 'c')
	check(b, 1, 'd')
	if n, ok := candidate.GenerationConnections(); !ok || n != 1 {
		t.Fatalf("new generation: live connection not visible: %d (%v)", n, ok)
	}
	// A closed stream is not a closed QUIC connection. The client can still
	// open another stream on it until the QUIC endpoint closes naturally.
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if n, ok := previous.GenerationConnections(); !ok || n != 1 {
		t.Fatalf("retirement would lose an idle QUIC connection: %d (%v)", n, ok)
	}
	// This is a forced test teardown, not a verified drain. Never report a
	// trustworthy zero after losing the listener or the worker.
	if err := previous.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := previous.GenerationConnections(); ok {
		t.Fatal("closed generation reported a trustworthy connection count")
	}
	check(b, 1, 'e')
}
