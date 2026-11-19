package handoff

import (
	"encoding/binary"
	"testing"
	"time"
)

func shortPacket(codec *QUICCIDCodec, slot byte) []byte {
	cid := codec.Generate(slot)
	data := make([]byte, 64)
	data[0] = 0x40
	copy(data[1:], cid[:])
	return data
}

func initialPacket(version uint32, packetType byte, id string) []byte {
	data := make([]byte, 1200)
	data[0] = 0xc0 | packetType<<4
	binary.BigEndian.PutUint32(data[1:], version)
	data[5] = byte(len(id))
	copy(data[6:], id)
	return data
}

func TestQUICGenerationRoutes(t *testing.T) {
	codec := NewQUICCIDCodec([16]byte{1, 2, 3})
	vector := [16]byte{0x9c, 0x45, 0x94, 0xa8, 0x12, 0xf8, 0xdf, 0xfe,
		0x10, 0xb5, 0x82, 0xbb, 0xea, 0xbe, 0xfd, 0xfb}
	var plain [QUICCIDLength]byte
	if slot, ok := codec.slot(vector[:], &plain); !ok || slot != 12 {
		t.Fatal("CID emitted by the Rust backend is not routable")
	}
	routes := NewQUICRoutes(codec, 12, 3, 5*time.Second)
	reader := routes.NewReader()
	now := time.Now()
	check := func(data []byte, want byte, valid bool) {
		t.Helper()
		got, ok := reader.Route(data, now)
		if ok != valid || (ok && got != want) {
			t.Fatalf("got slot=%d ok=%t, want slot=%d ok=%t", got, ok, want, valid)
		}
	}
	first := initialPacket(1, 0, "client-A")
	check(first, 12, true)
	check(shortPacket(codec, 12), 12, true)
	routes.Activate(13)
	check(shortPacket(codec, 12), 12, true)
	check(first, 12, true)                           // Initial retransmission during the switch.
	check(initialPacket(1, 1, "client-A"), 12, true) // 0-RTT.
	check(initialPacket(1, 1, "unknown!"), 0, false)
	check(initialPacket(1, 0, "client-B"), 13, true)
	check(initialPacket(0x6b3343cf, 1, "client-C"), 13, true)
	check(initialPacket(1, 0, "overlimit"), 0, false)
	check(initialPacket(2, 0, "unknown-version"), 0, false)
	check(shortPacket(codec, 13), 13, true)
	check(shortPacket(NewQUICCIDCodec([16]byte{99}), 13), 0, false)
	check(shortPacket(codec, 12)[:QUICCIDLength], 0, false)
	check(initialPacket(1, 0, "client-D")[:500], 0, false)
	// Later CIDs issued by the same server still route to that generation.
	for range 10 {
		check(shortPacket(codec, 12), 12, true)
	}
	if routes.Retire(13) || !routes.Retire(12) {
		t.Fatal("unexpected retirement result")
	}
	check(shortPacket(codec, 12), 0, false)
	check(first, 0, false) // Don't redirect a pinned retry when A is retired.
	now = now.Add(6 * time.Second)
	check(first, 13, true) // Expired pins no longer prevent new handshakes.
}

func BenchmarkQUICEstablishedRoute(b *testing.B) {
	codec := NewQUICCIDCodec([16]byte{1})
	reader := NewQUICRoutes(codec, 12, 1024, 10*time.Second).NewReader()
	packet := shortPacket(codec, 12)
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if slot, ok := reader.Route(packet, now); !ok || slot != 12 {
			b.Fatal("established packet lost")
		}
	}
}
