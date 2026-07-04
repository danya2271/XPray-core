package hysteria

import (
	"io"
	"testing"

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
