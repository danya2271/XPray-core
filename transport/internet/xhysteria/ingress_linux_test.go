//go:build xhysteria_native && cgo && linux && !android

package xhysteria

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/finalmask/mkcp/header"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"github.com/xtls/xray-core/transport/internet/handoff"
)

func TestNativeIngressPinsMasksAndSegmentedReply(t *testing.T) {
	for _, masked := range []bool{false, true} {
		name := "plain"
		if masked {
			name = "salamander-wireguard"
		}
		t.Run(name, func(t *testing.T) {
			public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer public.Close()
			var config []UDPMaskConfig
			if masked {
				config = []UDPMaskConfig{{Kind: "wireguard"}, {Kind: "salamander", Password: "test-password"}}
			}
			key := [16]byte{1, 2, 3}
			ingress, err := NewNativeIngress(public, key, 0, config)
			if err != nil {
				t.Fatal(err)
			}
			defer ingress.Close()
			if err := ingress.Activate(0); err == nil {
				t.Fatal("activated missing generation")
			}
			views := make([]*handoff.PacketEndpoint, 2)
			var files []*os.File
			defer func() {
				for _, file := range files {
					file.Close()
				}
			}()
			for slot := range views {
				file, err := ingress.Add(byte(slot))
				if err != nil {
					t.Fatal(err)
				}
				files = append(files, file)
				view, err := handoff.ImportPacketEndpoint(file, *public.LocalAddr().(*net.UDPAddr))
				if err != nil {
					t.Fatal(err)
				}
				defer view.Close()
				views[slot] = view
			}
			if _, err := ingress.Add(1); err == nil {
				t.Fatal("reused generation slot")
			}
			client, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			var packet net.PacketConn = client
			if masked {
				packet, err = finalmask.NewFinalMask(nil, []finalmask.UDPMask{
					&salamander.Config{Password: "test-password"}, &header.Config{ID: int32(header.WIREGUARD)},
				}, nil, nil, nil, func(context.Context, xnet.Addr) (xnet.PacketConn, error) { return client, nil }).ListenPacket(t.Context(), client.LocalAddr())
				if err != nil {
					t.Fatal(err)
				}
			}
			defer packet.Close()
			initial := func(id string) []byte {
				p := make([]byte, 1200)
				p[0], p[4], p[5] = 0xc0, 1, byte(len(id))
				copy(p[6:], id)
				return p
			}
			check := func(data []byte, slot int) {
				t.Helper()
				if _, err := packet.WriteTo(data, public.LocalAddr()); err != nil {
					t.Fatal(err)
				}
				views[slot].SetReadDeadline(time.Now().Add(time.Second))
				buffer := make([]byte, 2048)
				n, from, err := views[slot].ReadFrom(buffer)
				if err != nil || !bytes.Equal(data, buffer[:n]) || from.String() != packet.LocalAddr().String() {
					t.Fatalf("routed to %d: %d bytes from %v, %v", slot, n, from, err)
				}
			}
			check(initial("client-old"), 0)
			if err := ingress.Activate(1); err != nil {
				t.Fatal(err)
			}
			check(initial("client-old"), 0)
			check(initial("client-new"), 1)
			cid := handoff.NewQUICCIDCodec(key).Generate(0)
			established := append([]byte{0x40}, cid[:]...)
			established = append(established, 1)
			check(established, 0)
			// Send a native-style segmented frame on the generation side.
			worker, err := net.FileConn(files[1])
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			payload := bytes.Repeat([]byte{0x37}, 2401)
			wire := make([]byte, packetHeader+len(payload))
			n, err := encodePacket(wire, packet.LocalAddr().(*net.UDPAddr), payload)
			if err != nil {
				t.Fatal(err)
			}
			wire[0] = 2
			binary.BigEndian.PutUint16(wire[20:22], 1200)
			binary.BigEndian.PutUint16(wire[22:24], uint16(len(payload)))
			if _, err := worker.Write(wire[:n]); err != nil {
				t.Fatal(err)
			}
			packet.SetReadDeadline(time.Now().Add(time.Second))
			var buffer [4096]byte
			for offset := 0; offset < len(payload); offset += 1200 {
				n, _, err := packet.ReadFrom(buffer[:])
				if err != nil || !bytes.Equal(buffer[:n], payload[offset:min(offset+1200, len(payload))]) {
					t.Fatalf("segmented reply at %d: %d, %v", offset, n, err)
				}
			}
		})
	}
}

func TestNativeIngressStalledGenerationDoesNotBlockActivation(t *testing.T) {
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	key := [16]byte{9, 4, 2}
	ingress, err := NewNativeIngress(public, key, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	stalled, err := ingress.Add(0)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	healthy, err := ingress.Add(1)
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()
	view, err := handoff.ImportPacketEndpoint(healthy, *public.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	client, err := net.DialUDP("udp4", nil, public.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	old := make([]byte, 1200)
	old[0] = 0x40
	cid := handoff.NewQUICCIDCodec(key).Generate(0)
	copy(old[1:], cid[:])
	deadline := time.Now().Add(3 * time.Second)
	for ingress.Stats().QueueDrops == 0 {
		for range 256 {
			if _, err := client.Write(old); err != nil {
				t.Fatal(err)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("could not saturate stalled generation: %+v", ingress.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	if err := ingress.Activate(1); err != nil {
		t.Fatal(err)
	}
	initial := make([]byte, 1200)
	initial[0], initial[4], initial[5] = 0xc0, 1, 10
	copy(initial[6:], "new-client")
	if _, err := client.Write(initial); err != nil {
		t.Fatal(err)
	}
	view.SetReadDeadline(time.Now().Add(time.Second))
	var buffer [2048]byte
	n, _, err := view.ReadFrom(buffer[:])
	if err != nil || !bytes.Equal(initial, buffer[:n]) {
		t.Fatalf("healthy generation blocked by stalled worker: %d, %v", n, err)
	}
	stats := ingress.Stats()
	if !stats.Running || stats.QueuedBytes == 0 || stats.PeakQueuedBytes > 512*1024 || stats.Backpressure == 0 {
		t.Fatalf("unexpected bounded queue state: %+v", stats)
	}
}

func TestDormantNativeIngressDoesNotReadUntilConfirmedAdoption(t *testing.T) {
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	key := [16]byte{5, 1, 9}
	ingress, err := NewDormantNativeIngress(public, key, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.Close()
	file, err := ingress.Add(0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	view, err := handoff.ImportPacketEndpoint(file, *public.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	client, err := net.DialUDP("udp4", nil, public.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := ingress.Activate(0); err == nil {
		t.Fatal("dormant ingress allowed activation")
	}
	// Model the sole direct reader explicitly. This synthetic fixture snapshot
	// is never a production substitute for release-receive.
	if _, err := client.Write([]byte("direct-only")); err != nil {
		t.Fatal(err)
	}
	public.SetReadDeadline(time.Now().Add(time.Second))
	var buffer [2048]byte
	n, _, err := public.ReadFromUDP(buffer[:])
	if err != nil || string(buffer[:n]) != "direct-only" {
		t.Fatalf("dormant ingress competed with direct reader: %d, %v", n, err)
	}
	public.SetReadDeadline(time.Time{})
	if ingress.Stats().Received != 0 {
		t.Fatal("dormant ingress received public traffic")
	}
	snapshot := func(key [16]byte, local string) []byte {
		data, err := json.Marshal(struct {
			Lease uint64   `json:"lease"`
			Key   [16]byte `json:"key"`
			Local string   `json:"local"`
			Slot  byte     `json:"slot"`
			Pins  []any    `json:"pins"`
		}{1, key, local, 0, []any{}})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	if err := ingress.AdoptReceive(snapshot([16]byte{6}, public.LocalAddr().String())); err == nil {
		t.Fatal("adopted receive from another CID key")
	}
	if err := ingress.AdoptReceive(snapshot(key, "127.0.0.1:1")); err == nil {
		t.Fatal("adopted receive from another socket")
	}
	data := snapshot(key, public.LocalAddr().String())
	if err := ingress.AdoptReceive(data); err != nil {
		t.Fatal(err)
	}
	if err := ingress.AdoptReceive(data); err != nil {
		t.Fatalf("same adoption retry failed: %v", err)
	}
	initial := make([]byte, 1200)
	initial[0], initial[4], initial[5] = 0xc0, 1, 8
	copy(initial[6:], "new-work")
	if _, err := client.Write(initial); err != nil {
		t.Fatal(err)
	}
	view.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err = view.ReadFrom(buffer[:])
	if err != nil || !bytes.Equal(initial, buffer[:n]) {
		t.Fatalf("adopted ingress did not receive: %d, %v", n, err)
	}
}
