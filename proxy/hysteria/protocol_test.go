package hysteria

import (
	"bytes"
	"io"
	"testing"

	"github.com/apernet/quic-go"
	"github.com/xtls/xray-core/common/buf"
	transportHysteria "github.com/xtls/xray-core/transport/internet/hysteria"
)

func TestParseUDPMessage(t *testing.T) {
	msg := &UDPMessage{
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
	if parsed.PacketID != msg.PacketID || parsed.FragCount != msg.FragCount || parsed.Addr != msg.Addr || string(parsed.Data) != string(msg.Data) {
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
