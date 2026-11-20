package handoff

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestManagedPacketRingManifestOptIn(t *testing.T) {
	raw, err := json.Marshal(UDPBinding{Tag: "hy", Port: 443, FD: 3})
	if err != nil || bytes.Contains(raw, []byte("ring_fds")) {
		t.Fatalf("legacy manifest unexpectedly requires packet rings: %s, %v", raw, err)
	}
	raw, err = json.Marshal(UDPBinding{Tag: "hy", Port: 443, FD: 3, RingFDs: []int{4, 5, 6}})
	if err != nil || !bytes.Contains(raw, []byte(`"ring_fds":[4,5,6]`)) {
		t.Fatalf("explicit packet ring descriptors not serialized: %s, %v", raw, err)
	}
}

func TestManagedPacketRingDescriptorsAndOwnership(t *testing.T) {
	const size = 192 + 32*65600
	for _, kind := range []string{"valid", "unsealed", "wrong-event", "blocking-event"} {
		t.Run(kind, func(t *testing.T) {
			fds := [3]int{}
			var err error
			fds[0], err = unix.MemfdCreate("packet-ring-test", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Ftruncate(fds[0], size); err != nil {
				t.Fatal(err)
			}
			if kind != "unsealed" {
				if _, err := unix.FcntlInt(uintptr(fds[0]), unix.F_ADD_SEALS, unix.F_SEAL_SHRINK|unix.F_SEAL_GROW|unix.F_SEAL_SEAL); err != nil {
					t.Fatal(err)
				}
			}
			for index := 1; index < 3; index++ {
				flags := unix.EFD_NONBLOCK | unix.EFD_CLOEXEC
				if kind == "blocking-event" && index == 2 {
					flags = unix.EFD_CLOEXEC
				}
				fds[index], err = unix.Eventfd(0, flags)
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind == "wrong-event" {
				unix.Close(fds[2])
				fds[2], err = unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
				if err != nil {
					t.Fatal(err)
				}
			}
			worker, peer, err := packetPair()
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			defer worker.Close()
			endpoint, err := ImportPacketEndpoint(worker, net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443})
			if err != nil {
				t.Fatal(err)
			}
			err = attachPacketRing(endpoint, fds)
			if (err == nil) != (kind == "valid") {
				t.Fatalf("descriptor validation: %v", err)
			}
			for index, file := range endpoint.RingFiles() {
				if file == nil || int(file.Fd()) != fds[index] {
					t.Fatal("endpoint lost descriptor ownership")
				}
			}
			endpoint.Close()
			for _, fd := range fds {
				if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
					t.Fatalf("ring descriptor was not closed: %v", err)
				}
			}
		})
	}
}

func packetPair() (*os.File, *os.File, error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	return os.NewFile(uintptr(fds[0]), "packet-worker-test"), os.NewFile(uintptr(fds[1]), "packet-relay-test"), nil
}

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

type observedPacketSocket struct {
	net.PacketConn
	read chan struct{}
}

func (s *observedPacketSocket) ReadFrom(payload []byte) (int, net.Addr, error) {
	n, from, err := s.PacketConn.ReadFrom(payload)
	if err == nil {
		select {
		case s.read <- struct{}{}:
		default:
		}
	}
	return n, from, err
}

func TestUDPIngressBackpressureRetainsPacketAndDefersFence(t *testing.T) {
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	observed := &observedPacketSocket{PacketConn: public, read: make(chan struct{}, 1)}
	key := [16]byte{7}
	ingress, err := NewUDPIngress(observed, key, 0, 8, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	file, err := ingress.Add(0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	worker, err := net.FileConn(file)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	forNext, err := ingress.Add(1)
	if err != nil {
		t.Fatal(err)
	}
	defer forNext.Close()
	var filled int
	if err := ingress.peers.Load()[0].raw.Control(func(fd uintptr) {
		err = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, 4096)
		if err != nil {
			return
		}
		for {
			if sendErr := unix.Send(int(fd), make([]byte, 1200), unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL); sendErr != nil {
				if !errors.Is(sendErr, unix.EAGAIN) {
					err = sendErr
				}
				break
			}
			filled++
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err != nil || filled == 0 {
		t.Fatalf("could not fill worker socket: filled=%d, %v", filled, err)
	}
	client, err := net.DialUDP("udp4", nil, public.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	packet := append(shortPacket(NewQUICCIDCodec(key), 0), 0x71)
	if _, err := client.Write(packet); err != nil {
		t.Fatal(err)
	}
	select {
	case <-observed.read:
	case <-time.After(time.Second):
		t.Fatal("public datagram not read")
	}
	if err := ingress.Activate(1); err != nil {
		t.Fatal("blocked receive prevented admission switch:", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	fenced := make(chan error, 1)
	go func() { _, err := ingress.FenceInitials(ctx, 0); fenced <- err }()
	select {
	case err := <-fenced:
		t.Fatalf("fenced before held datagram was delivered: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if ingress.Dropped() != 0 {
		t.Fatal("backpressure discarded the held datagram")
	}
	if err := worker.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 2048)
	for range filled {
		if _, err := worker.Read(buffer); err != nil {
			t.Fatal(err)
		}
	}
	n, err := worker.Read(buffer)
	if err != nil || !bytes.Equal(buffer[packetHeaderSize:n], packet) {
		t.Fatalf("held datagram lost or changed: %d, %v", n, err)
	}
	if err := <-fenced; err != nil {
		t.Fatal(err)
	}
	if ingress.Dropped() != 0 {
		t.Fatal("ingress reported loss after successful retry")
	}
}

func TestUDPIngressCloseInterruptsBackpressure(t *testing.T) {
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	observed := &observedPacketSocket{PacketConn: public, read: make(chan struct{}, 1)}
	ingress, err := NewUDPIngress(observed, [16]byte{7}, 0, 8, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	file, err := ingress.Add(0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close() // Keep the receiver alive and deliberately unread.
	if err := ingress.peers.Load()[0].raw.Control(func(fd uintptr) {
		err = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, 4096)
		if err != nil {
			return
		}
		for {
			if sendErr := unix.Send(int(fd), make([]byte, 1200), unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL); sendErr != nil {
				if !errors.Is(sendErr, unix.EAGAIN) {
					err = sendErr
				}
				return
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.DialUDP("udp4", nil, public.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write(shortPacket(NewQUICCIDCodec([16]byte{7}), 0)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-observed.read:
	case <-time.After(time.Second):
		t.Fatal("public datagram not read")
	}
	closed := make(chan error, 1)
	go func() { closed <- ingress.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("socket shutdown did not interrupt the held write")
	}
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
		file, err := DuplicatePacketSocket(socket)
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
	raw, err := public.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Control(func(fd uintptr) {
		flags, err := unix.FcntlInt(fd, unix.F_GETFL, 0)
		if err != nil || flags&unix.O_NONBLOCK == 0 {
			t.Errorf("export made the live public socket blocking: flags=%d, %v", flags, err)
		}
	}); err != nil {
		t.Fatal(err)
	}
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

func TestUDPIngressResumesAfterBackpressureWithoutDropping(t *testing.T) {
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
	defer old.Close()
	oldWorker, err := ImportPacketEndpoint(old, *public.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer oldWorker.Close()
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
	for range 128 {
		if _, err := client.Write(stale); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if ingress.Dropped() != 0 {
		t.Fatal("backpressure dropped an old-generation packet")
	}
	if err := ingress.Activate(2); err != nil {
		t.Fatal(err)
	}
	if err := oldWorker.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var received [2048]byte
	for sequence := range 128 {
		n, _, err := oldWorker.ReadFrom(received[:])
		if err != nil || !bytes.Equal(received[:n], stale) {
			t.Fatalf("old generation packet %d was lost: %d, %v", sequence, n, err)
		}
	}
	initial := initialPacket(1, 0, "new-client-id")
	var data [2048]byte
	var n int
	var address net.Addr
	deadline := time.Now().Add(time.Second)
	if _, err = client.Write(initial); err != nil {
		t.Fatal(err)
	}
	if err := worker.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	n, address, err = worker.ReadFrom(data[:])
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
	if ingress.Dropped() != 0 {
		t.Fatal("resumed ingress reported packet loss")
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
