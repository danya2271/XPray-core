package handoff

import (
	"bytes"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestWildcardPacketDestinationSurvivesWorkerAndSourceReply(t *testing.T) {
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	wrapped, err := PreservePacketDestination(public)
	if err != nil {
		t.Fatal(err)
	}
	key := [16]byte{7}
	ingress, err := NewUDPIngress(wrapped, key, 0, 8, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	file, err := ingress.Add(0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	endpoint, err := ImportPacketEndpoint(file, *public.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	client, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	cid := NewQUICCIDCodec(key).Generate(0)
	packet := append(append([]byte{0x40}, cid[:]...), 9)
	for _, local := range []string{"127.0.0.1", "127.0.0.2"} {
		target := &net.UDPAddr{IP: net.ParseIP(local), Port: public.LocalAddr().(*net.UDPAddr).Port}
		if _, err := client.WriteToUDP(packet, target); err != nil {
			t.Fatal(err)
		}
		endpoint.SetReadDeadline(time.Now().Add(time.Second))
		buffer := make([]byte, 2048)
		n, from, err := endpoint.ReadFrom(buffer)
		if err != nil || !bytes.Equal(buffer[:n], packet) {
			t.Fatalf("wildcard RX %d: %v", n, err)
		}
		metadata, ok := from.(*packetDestination)
		if !ok || metadata.local.String() != local {
			t.Fatalf("wrong packet destination: %v", from)
		}
		if _, err := endpoint.WritePacketTo([]byte("reply"), metadata.peer, packetSource(netip.MustParseAddr(local))); err != nil {
			t.Fatal(err)
		}
		client.SetReadDeadline(time.Now().Add(time.Second))
		n, source, err := client.ReadFromUDP(buffer)
		if err != nil || string(buffer[:n]) != "reply" || source.IP.String() != local {
			t.Fatalf("wildcard reply from %v: %v", source, err)
		}
	}
}
