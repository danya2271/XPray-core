package quic

import "context"

// TryRetireIdle serializes the predicate after all earlier unknown-CID packets.
// The caller must first fence old Initial admission and its packet ingress.
// A false result leaves admission unchanged. A true result prevents future
// unknown-CID work without cancelling any connection or handshake.
func (l *Listener) TryRetireIdle(ctx context.Context, idle func() bool) (bool, error) {
	ack := make(chan bool, 1)
	p := receivedPacket{retire: &idleRetirement{idle: idle, done: ack}}
	select {
	case l.baseServer.receivedPackets <- p:
	case <-l.baseServer.running:
		return false, ErrServerClosed
	case <-ctx.Done():
		return false, ctx.Err()
	}
	select {
	case result := <-ack:
		return result, nil
	case <-l.baseServer.running:
		return false, ErrServerClosed
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

type idleRetirement struct {
	idle func() bool
	done chan bool
}
