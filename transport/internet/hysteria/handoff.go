package hysteria

import (
	"context"
	"fmt"

	"github.com/apernet/quic-go"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/handoff"
)

type generationKey struct{}

type generationIngress struct {
	packet net.PacketConn
	key    [16]byte
	slot   byte
}

// ListenGeneration uses a supervisor-owned, already demultiplexed UDP socket
// view. The caller must remove and apply any datagram masks at the shared
// ingress, and preserve the source and destination addresses in packet.
// Closing this view must not close other generations or the public socket.
func ListenGeneration(ctx context.Context, address net.Address, port net.Port, settings *internet.MemoryStreamConfig, handler internet.ConnHandler, packet net.PacketConn, key [16]byte, slot byte) (internet.Listener, error) {
	if packet == nil || key == [16]byte{} {
		return nil, fmt.Errorf("Hysteria handoff requires dedicated packet ingress and a routing key")
	}
	return Listen(context.WithValue(ctx, generationKey{}, &generationIngress{packet: packet, key: key, slot: slot}), address, port, settings, handler)
}

func generationFromContext(ctx context.Context) *generationIngress {
	ingress, _ := ctx.Value(generationKey{}).(*generationIngress)
	return ingress
}

type generationCIDGenerator struct {
	codec *handoff.QUICCIDCodec
	slot  byte
}

func (g generationCIDGenerator) GenerateConnectionID() (quic.ConnectionID, error) {
	cid := g.codec.Generate(g.slot)
	return quic.ConnectionIDFromBytes(cid[:]), nil
}

func (g generationCIDGenerator) ConnectionIDLen() int { return handoff.QUICCIDLength }
