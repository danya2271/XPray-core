package handoff

import (
	"bytes"
	"net"
	"testing"
	"time"
)

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
