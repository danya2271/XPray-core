package hysteria

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/xtls/xray-core/common/buf"
	transportHysteria "github.com/xtls/xray-core/transport/internet/hysteria"
)

func TestParseUDPMessage(t *testing.T) {
	msg := &UDPMessage{
		SessionID: 11,
		PacketID:  7,
		FragCount: 1,
		Addr:      "example.com:53",
		Data:      []byte("payload"),
	}
	packet := make([]byte, transportHysteria.MaxDatagramFrameSize)
	n := msg.Serialize(packet)
	if n < 0 {
		t.Fatal("failed to serialize UDP message")
	}

	parsed, err := ParseUDPMessage(packet[:n])
	if err != nil {
		t.Fatal(err)
	}
	if parsed.SessionID != msg.SessionID || parsed.PacketID != msg.PacketID || parsed.FragCount != msg.FragCount || parsed.Addr != msg.Addr || string(parsed.Data) != string(msg.Data) {
		t.Fatalf("unexpected parsed message: %#v", parsed)
	}
}

func TestUDPReaderReadFromFragmentedDatagram(t *testing.T) {
	packets := makeFragmentPackets(t, []byte("hello"), []byte("world"))
	reader := &UDPReader{
		reader: &datagramListReader{packets: packets},
		df:     &Defragger{},
	}

	payload := make([]byte, 2048)
	n, addr, err := reader.ReadFrom(payload)
	if err != nil {
		t.Fatal(err)
	}
	if addr == nil || addr.NetAddr() != "example.com:53" {
		t.Fatalf("unexpected destination: %v", addr)
	}
	if got := string(payload[:n]); got != "helloworld" {
		t.Fatalf("unexpected payload: %q", got)
	}
}

func TestUDPWriterFragmentsMessageLargerThanScratchBuffer(t *testing.T) {
	payload := make([]byte, 4090)
	writer := &datagramCaptureWriter{maxSize: 1200}
	udpWriter := &UDPWriter{
		writer: writer,
		addr:   "127.0.0.1:53",
	}

	packet := buf.New()
	packet.Write(payload)
	if err := udpWriter.WriteMultiBuffer(buf.MultiBuffer{packet}); err != nil {
		t.Fatal(err)
	}
	if len(writer.packets) < 2 {
		t.Fatalf("sent %d datagrams, want fragmented packet", len(writer.packets))
	}

	var (
		defragger Defragger
		assembled *UDPMessage
	)
	for _, datagram := range writer.packets {
		msg, err := ParseUDPMessage(datagram)
		if err != nil {
			t.Fatal(err)
		}
		assembled = defragger.Feed(msg)
	}
	if assembled == nil {
		t.Fatal("fragments did not reassemble")
	}
	if len(assembled.Data) != len(payload) {
		t.Fatalf("reassembled payload length = %d, want %d", len(assembled.Data), len(payload))
	}
	if !bytes.Equal(assembled.Data, payload) {
		t.Fatal("reassembled payload differs from original")
	}
}

func TestUDPWriterRejectsDatagramLargerThanMaxUDPSize(t *testing.T) {
	writer := &datagramCaptureWriter{maxSize: 1200}
	udpWriter := &UDPWriter{writer: writer, addr: "127.0.0.1:53"}
	packet := buf.FromBytes(make([]byte, transportHysteria.MaxUDPSize+1))

	if err := udpWriter.WriteMultiBuffer(buf.MultiBuffer{packet}); err == nil {
		t.Fatal("oversized UDP datagram was accepted")
	}
	if len(writer.packets) != 0 {
		t.Fatalf("sent %d datagrams, want none", len(writer.packets))
	}
}

func TestDefraggerHandlesInterleavedPackets(t *testing.T) {
	d := &Defragger{}
	if got := d.Feed(&UDPMessage{PacketID: 1, FragID: 0, FragCount: 2, Addr: "example.com:53", Data: []byte("a")}); got != nil {
		t.Fatal("first fragment unexpectedly completed")
	}
	if got := d.Feed(&UDPMessage{PacketID: 2, FragID: 0, FragCount: 2, Addr: "example.com:53", Data: []byte("c")}); got != nil {
		t.Fatal("second packet unexpectedly completed")
	}
	if got := d.Feed(&UDPMessage{PacketID: 1, FragID: 1, FragCount: 2, Addr: "example.com:53", Data: []byte("b")}); got == nil || string(got.Data) != "ab" {
		t.Fatalf("first packet = %#v", got)
	}
	if got := d.Feed(&UDPMessage{PacketID: 2, FragID: 1, FragCount: 2, Addr: "example.com:53", Data: []byte("d")}); got == nil || string(got.Data) != "cd" {
		t.Fatalf("second packet = %#v", got)
	}
}

func TestDefraggerExpiresStalePacketID(t *testing.T) {
	d := &Defragger{}
	if got := d.Feed(fragment(1, 9, 0, 2, "example.com:53", "old-")); got != nil {
		t.Fatal("incomplete packet unexpectedly completed")
	}
	d.slots[0].lastSeen = time.Now().Add(-udpFragmentTTL - time.Second)

	if got := d.Feed(fragment(1, 9, 0, 2, "example.com:53", "new-")); got != nil {
		t.Fatal("first fresh fragment unexpectedly completed")
	}
	got := d.Feed(fragment(1, 9, 1, 2, "example.com:53", "data"))
	if got == nil || string(got.Data) != "new-data" {
		t.Fatalf("reassembled packet = %#v, want fresh payload", got)
	}
}

func TestDefraggerResetsOnMetadataMismatch(t *testing.T) {
	tests := []struct {
		name       string
		second     *UDPMessage
		freshFirst *UDPMessage
		wantAddr   string
	}{
		{
			name:       "session ID",
			second:     fragment(2, 9, 1, 2, "example.com:53", "new-"),
			freshFirst: fragment(2, 9, 0, 2, "example.com:53", "fresh-"),
			wantAddr:   "example.com:53",
		},
		{
			name:       "address",
			second:     fragment(1, 9, 1, 2, "other.example:53", "new-"),
			freshFirst: fragment(1, 9, 0, 2, "other.example:53", "fresh-"),
			wantAddr:   "other.example:53",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := &Defragger{}
			if got := d.Feed(fragment(1, 9, 0, 2, "example.com:53", "stale-")); got != nil {
				t.Fatal("incomplete packet unexpectedly completed")
			}
			if got := d.Feed(test.second); got != nil {
				t.Fatal("metadata mismatch unexpectedly completed a packet")
			}
			if got := d.Feed(test.freshFirst); got == nil {
				t.Fatal("matching fragments did not reassemble")
			} else if want := "fresh-new-"; string(got.Data) != want || got.Addr != test.wantAddr || got.SessionID != test.freshFirst.SessionID {
				t.Fatalf("reassembled packet = %#v, want payload %q at %q", got, want, test.wantAddr)
			}
		})
	}
}

func TestDefraggerRejectsZeroFragmentCount(t *testing.T) {
	if got := (&Defragger{}).Feed(fragment(1, 9, 0, 0, "example.com:53", "payload")); got != nil {
		t.Fatalf("zero-fragment packet = %#v, want nil", got)
	}
}

func TestFragUDPMessageRejectsTooManyFragments(t *testing.T) {
	msg := &UDPMessage{
		Addr: "example.com:53",
		Data: make([]byte, 256*256),
	}
	if got := FragUDPMessage(msg, msg.HeaderSize()+255); got != nil {
		t.Fatalf("fragments = %d, want nil", len(got))
	}
}

func TestDefraggerRejectsOversizedAssembly(t *testing.T) {
	d := &Defragger{}
	for id := uint8(0); id < 4; id++ {
		got := d.Feed(fragment(1, 9, id, 4, "example.com:53", string(make([]byte, 1200))))
		if got != nil {
			t.Fatal("oversized assembly unexpectedly completed")
		}
	}
	if d.slots[0].fragments != nil {
		t.Fatal("oversized assembly was retained")
	}
}

type datagramCaptureWriter struct {
	maxSize int
	packets [][]byte
}

func (w *datagramCaptureWriter) Write(p []byte) (int, error) {
	if len(p) > w.maxSize {
		return 0, &quic.DatagramTooLargeError{MaxDatagramPayloadSize: int64(w.maxSize)}
	}
	w.packets = append(w.packets, append([]byte(nil), p...))
	return len(p), nil
}

func fragment(sessionID uint32, packetID uint16, fragmentID, fragmentCount uint8, addr, data string) *UDPMessage {
	return &UDPMessage{
		SessionID: sessionID,
		PacketID:  packetID,
		FragID:    fragmentID,
		FragCount: fragmentCount,
		Addr:      addr,
		Data:      []byte(data),
	}
}

func makeFragmentPackets(t *testing.T, first []byte, second []byte) [][]byte {
	t.Helper()
	messages := []UDPMessage{
		{PacketID: 9, FragID: 0, FragCount: 2, Addr: "example.com:53", Data: first},
		{PacketID: 9, FragID: 1, FragCount: 2, Addr: "example.com:53", Data: second},
	}
	packets := make([][]byte, 0, len(messages))
	for i := range messages {
		packet := make([]byte, transportHysteria.MaxDatagramFrameSize)
		n := messages[i].Serialize(packet)
		if n < 0 {
			t.Fatal("failed to serialize UDP fragment")
		}
		packets = append(packets, packet[:n])
	}
	return packets
}

type datagramListReader struct {
	packets [][]byte
	index   int
}

func (r *datagramListReader) Read(p []byte) (int, error) {
	if r.index >= len(r.packets) {
		return 0, io.EOF
	}
	packet := r.packets[r.index]
	r.index++
	return copy(p, packet), nil
}
