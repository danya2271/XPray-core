package quic

import (
	"context"
	"testing"
	"time"
)

func TestNodeXIdleRetirementWaitsForSendOwner(t *testing.T) {
	s := &baseServer{receivedPackets: make(chan receivedPacket, 4), running: make(chan struct{}), errorChan: make(chan struct{}), sendFence: make(chan chan struct{})}
	listener := &Listener{baseServer: s}
	go s.run()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		retired, err := listener.TryRetireIdle(ctx, func() bool { return true })
		done <- retired && err == nil
	}()
	ack := <-s.sendFence
	select {
	case <-done:
		t.Fatal("retired before send-owner acknowledgement")
	default:
	}
	close(ack)
	if !<-done {
		t.Fatal("idle retirement failed")
	}
	close(s.errorChan)
	<-s.running
}
