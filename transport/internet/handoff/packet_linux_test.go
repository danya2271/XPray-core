package handoff

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestManagedPacketBatchPreservesDatagrams(t *testing.T) {
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := NewUDPIngress(public, [16]byte{1}, 1, 8, time.Second)
	if err != nil {
		public.Close()
		t.Fatal(err)
	}
	defer ingress.Close()
	file, err := ingress.Add(1)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	worker, err := net.FileConn(file)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	client, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	wire := make([]byte, packetHeaderSize+2401)
	if !writePacketHeader(wire, client.LocalAddr().(*net.UDPAddr).AddrPort(), len(wire)-packetHeaderSize) {
		t.Fatal("could not encode batch peer")
	}
	wire[0] = 2
	binary.BigEndian.PutUint16(wire[20:22], 1200)
	binary.BigEndian.PutUint16(wire[22:24], 2401)
	for i := range wire[packetHeaderSize:] {
		wire[packetHeaderSize+i] = byte(i % 251)
	}
	for _, mutate := range []func([]byte){
		func(p []byte) { p[20], p[21] = 0, 0 },
		func(p []byte) { binary.BigEndian.PutUint16(p[20:22], 1) },
		func(p []byte) { binary.BigEndian.PutUint16(p[20:22], 2402) },
		func(p []byte) { p[23]++ },
		func(p []byte) { p[4] = 1 },
	} {
		bad := append([]byte(nil), wire...)
		mutate(bad)
		if _, _, ok := readPacketFrame(bad); ok {
			t.Fatal("malformed batch accepted")
		}
		if _, err := worker.Write(bad); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := worker.Write(wire); err != nil {
		t.Fatal(err)
	}
	client.SetReadDeadline(time.Now().Add(time.Second))
	var buffer [4096]byte
	for offset := packetHeaderSize; offset < len(wire); offset += 1200 {
		n, _, err := client.ReadFromUDP(buffer[:])
		if err != nil || !bytes.Equal(buffer[:n], wire[offset:min(offset+1200, len(wire))]) {
			t.Fatalf("batch segment at %d: %d bytes, %v", offset, n, err)
		}
	}
}

// Delay observation of a read timeout, letting the test change admission
// while the ingress owner is waiting for the receive-loop acknowledgement.
type delayedFenceSocket struct {
	net.PacketConn
	timedOut chan struct{}
	release  chan struct{}
}

func (s *delayedFenceSocket) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := s.PacketConn.ReadFrom(p)
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		select {
		case s.timedOut <- struct{}{}:
		default:
		}
		<-s.release
	}
	return n, addr, err
}

func TestUDPIngressFenceWaitsForPinsAndReceiveLoop(t *testing.T) {
	for _, change := range []bool{false, true} {
		name := "stable"
		if change {
			name = "reactivated"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			wrapped := &delayedFenceSocket{public, make(chan struct{}, 1), make(chan struct{})}
			key := [16]byte{7}
			ingress, err := NewUDPIngress(wrapped, key, 1, 8, time.Minute)
			if err != nil {
				public.Close()
				t.Fatal(err)
			}
			defer ingress.Close()
			defer func() {
				select {
				case <-wrapped.release:
				default:
					close(wrapped.release)
				}
			}()
			for _, slot := range []byte{1, 2} {
				file, err := ingress.Add(slot)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
			}
			if _, err := ingress.FenceInitials(ctx, 1); err == nil {
				t.Fatal("fenced an active generation")
			}
			routes := ingress.routes.NewReader()
			initial := initialPacket(1, 0, "initial-id")
			now := time.Now()
			// Simulate an expiring pin without waiting a minute in this test.
			pinnedAt := now.Add(-59 * time.Second)
			want := pinnedAt.Add(time.Minute)
			if slot, ok := routes.Route(initial, pinnedAt); !ok || slot != 1 {
				t.Fatal("could not pin old handshake")
			}
			if err := ingress.Activate(2); err != nil {
				t.Fatal(err)
			}
			if slot, ok := routes.Route(initial, now); !ok || slot != 1 {
				t.Fatal("retransmission lost original ownership")
			}
			until, err := ingress.FenceInitials(ctx, 1)
			if err != nil || !until.Equal(want) {
				t.Fatalf("fence did not defer for pin: %v, %v", until, err)
			}
			timer := time.NewTimer(time.Until(until) + time.Millisecond)
			defer timer.Stop()
			<-timer.C
			result := make(chan error, 1)
			go func() {
				until, err := ingress.FenceInitials(ctx, 1)
				if err == nil && !until.IsZero() {
					err = errors.New("fence still pinned after expiry")
				}
				result <- err
			}()
			select {
			case <-wrapped.timedOut:
			case <-ctx.Done():
				t.Fatal("receive loop was not fenced")
			}
			select {
			case err := <-result:
				t.Fatalf("fence completed without receive-loop acknowledgement: %v", err)
			default:
			}
			if change {
				// Switch away again: even an ABA transition invalidates the
				// earlier snapshot and may have admitted work in the gap.
				if err := ingress.Activate(1); err != nil {
					t.Fatal(err)
				}
				if err := ingress.Activate(2); err != nil {
					t.Fatal(err)
				}
			}
			close(wrapped.release)
			if err := <-result; (err != nil) != change {
				t.Fatalf("fence result after admission change=%t: %v", change, err)
			}
			if slot, ok := routes.Route(initial, time.Now()); !ok || slot != 2 {
				t.Fatal("expired initial was not admitted by successor")
			}
			if slot, ok := routes.Route(shortPacket(NewQUICCIDCodec(key), 1), time.Now()); !ok || slot != 1 {
				t.Fatal("fence disrupted established traffic")
			}
		})
	}
}

func TestManagedPacketHeader(t *testing.T) {
	for _, ip := range []string{"192.0.2.1", "::ffff:192.0.2.1", "2001:db8::1"} {
		addr := &net.UDPAddr{IP: net.ParseIP(ip), Port: 1234}
		var header [packetHeaderSize]byte
		if !writePacketHeader(header[:], udpAddress(addr), 1200) {
			t.Fatal("valid address rejected")
		}
		out, ok := readPacketHeader(header[:], 1200)
		if !ok || out.Addr() != addr.AddrPort().Addr().Unmap() || out.Port() != uint16(addr.Port) {
			t.Fatalf("decoded %s, %t from %s", out, ok, addr)
		}
		if _, ok := readPacketHeader(header[:], 1199); ok {
			t.Fatal("truncated datagram accepted")
		}
		header[0] = 2
		if _, ok := readPacketHeader(header[:], 1200); ok {
			t.Fatal("unexpected batched wire accepted")
		}
	}
}

func TestManagedTransmitDescriptorOwnershipAndAddress(t *testing.T) {
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	wrong, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.Close()
	ingress, err := NewUDPIngress(public, [16]byte{1}, 0, 8, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	file, err := ingress.Add(0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	view, err := ImportPacketEndpoint(file, *public.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if f, err := view.TransmitFile(); err != nil || f != nil {
		t.Fatalf("missing transmit descriptor: %v, %v", f, err)
	}
	inherited := func(socket *net.UDPConn) int {
		t.Helper()
		file, err := socket.File()
		if err != nil {
			t.Fatal(err)
		}
		fd, err := unix.Dup(int(file.Fd()))
		file.Close()
		if err != nil {
			t.Fatal(err)
		}
		unix.CloseOnExec(fd)
		return fd
	}
	if err := attachUDPTransmit(view, inherited(wrong)); err == nil {
		t.Fatal("accepted a transmit descriptor bound to another address")
	}
	// attachUDPTransmit consumes its inherited file descriptor; public
	// continues to own its original descriptor.
	if err := attachUDPTransmit(view, inherited(public)); err != nil {
		t.Fatal(err)
	}
	duplicate, err := view.TransmitFile()
	if err != nil || duplicate == nil {
		t.Fatalf("exporting transmit descriptor: %v", err)
	}
	duplicate.Close()
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if _, err := public.WriteToUDP([]byte("still live"), receiver.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatalf("closing a generation closed the public socket: %v", err)
	}
	receiver.SetReadDeadline(time.Now().Add(time.Second))
	var buffer [32]byte
	n, peer, err := receiver.ReadFromUDP(buffer[:])
	if err != nil || string(buffer[:n]) != "still live" || peer.String() != public.LocalAddr().String() {
		t.Fatalf("public socket after generation close: %q from %v: %v", buffer[:n], peer, err)
	}
}

func TestUDPIngressIsolatesBackpressure(t *testing.T) {
	local := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
	public, err := net.ListenUDP("udp4", local)
	if err != nil {
		t.Fatal(err)
	}
	key := [16]byte{1, 2, 3}
	ingress, err := NewUDPIngress(public, key, 1, 8, time.Second)
	if err != nil {
		public.Close()
		t.Fatal(err)
	}
	defer ingress.Close()
	old, err := ingress.Add(1)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close() // Deliberately never read from this endpoint.
	if _, err := ingress.Add(1); err == nil {
		t.Fatal("accepted duplicate generation slot")
	}
	if err := ingress.Activate(2); err == nil {
		t.Fatal("admitted traffic to a missing successor")
	}
	candidate, err := ingress.Add(2)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Close()
	bound := public.LocalAddr().(*net.UDPAddr)
	worker, err := ImportPacketEndpoint(candidate, *bound)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	client, err := net.DialUDP("udp4", nil, bound)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	codec := NewQUICCIDCodec(key)
	stale := shortPacket(codec, 1)
	for range 2000 {
		if _, err := client.Write(stale); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for ingress.Dropped() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ingress.Dropped() == 0 {
		t.Fatal("old worker's bounded queue did not fill")
	}
	if err := ingress.Activate(2); err != nil {
		t.Fatal(err)
	}
	initial := initialPacket(1, 0, "new-client-id")
	var data [2048]byte
	var n int
	var address net.Addr
	deadline = time.Now().Add(time.Second)
	// The real UDP socket can also drop during the flood. Retry the Initial,
	// like a client, instead of relying on arrival of the first datagram.
	for {
		if _, err = client.Write(initial); err != nil {
			t.Fatal(err)
		}
		worker.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		n, address, err = worker.ReadFrom(data[:])
		if err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil || !bytes.Equal(data[:n], initial) || address.String() != client.LocalAddr().String() {
		t.Fatalf("candidate ingress received %d bytes from %v: %v", n, address, err)
	}
	if _, err := worker.WriteTo([]byte("ok"), address); err != nil {
		t.Fatal(err)
	}
	client.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := client.Read(data[:]); err != nil || string(data[:n]) != "ok" {
		t.Fatalf("candidate egress received %q: %v", data[:n], err)
	}
}

func BenchmarkManagedUDPIngressRoundTrip(b *testing.B) {
	for _, managed := range []bool{false, true} {
		name := "direct"
		if managed {
			name = "handoff"
		}
		b.Run(name, func(b *testing.B) {
			public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				b.Fatal(err)
			}
			defer public.Close()
			bound := public.LocalAddr().(*net.UDPAddr)
			key := [16]byte{1}
			var echo net.PacketConn = public
			if managed {
				ingress, err := NewUDPIngress(public, key, 0, 8, time.Second)
				if err != nil {
					b.Fatal(err)
				}
				defer ingress.Close()
				file, err := ingress.Add(0)
				if err != nil {
					b.Fatal(err)
				}
				echo, err = ImportPacketEndpoint(file, *bound)
				file.Close()
				if err != nil {
					b.Fatal(err)
				}
				defer echo.Close()
			}
			go func() {
				var packet [1200]byte
				for {
					if direct, ok := echo.(*net.UDPConn); ok {
						n, addr, err := direct.ReadFromUDPAddrPort(packet[:])
						if err != nil {
							return
						}
						direct.WriteToUDPAddrPort(packet[:n], addr)
					} else {
						n, addr, err := echo.ReadFrom(packet[:])
						if err != nil {
							return
						}
						echo.WriteTo(packet[:n], addr)
					}
				}
			}()
			client, err := net.DialUDP("udp4", nil, bound)
			if err != nil {
				b.Fatal(err)
			}
			defer client.Close()
			client.SetDeadline(time.Now().Add(10 * time.Second))
			packet := make([]byte, 1200)
			copy(packet, shortPacket(NewQUICCIDCodec(key), 0))
			var result [1200]byte
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := client.Write(packet); err != nil {
					b.Fatal(err)
				}
				if n, err := client.Read(result[:]); n != len(packet) || err != nil {
					b.Fatalf("relay response: %d, %v", n, err)
				}
			}
		})
	}
}
