//go:build linux && xhysteria_native && xhysteria_interop && cgo

package interop_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
)

func TestHandoffSaturatedUDP(t *testing.T) {
	fixture := newHandoffLoadFixture(t)
	if os.Getenv("XHYS_HANDOFF_DIAGNOSTIC") == "1" {
		t.Setenv("XRAY_NET_DIAG", "1")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 70*time.Second)
	defer cancel()
	open := func(client *core.Instance, network, address string) net.Conn {
		t.Helper()
		target, err := xnet.ParseDestination(network + ":" + address)
		if err != nil {
			t.Fatal(err)
		}
		connection, err := core.Dial(ctx, client, target)
		if err != nil {
			t.Fatal(err)
		}
		if err := connection.SetDeadline(time.Now().Add(65 * time.Second)); err != nil {
			connection.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { connection.Close() })
		return connection
	}
	clientA := fixture.client(t)
	streams := make([]net.Conn, 32)
	for index := range streams {
		streams[index] = open(clientA, "tcp", fixture.info.TCP)
		if _, err := streams[index].Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(streams[index], make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
	}
	udpA := open(clientA, "udp", fixture.info.UDP)
	warm := bytes.Repeat([]byte{0x71}, 1200)
	if _, err := udpA.Write(warm); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(udpA, make([]byte, len(warm))); err != nil {
		t.Fatal(err)
	}
	before := fixture.snapshot(t, "stats")
	stop := make(chan struct{})
	errors := make(chan error, len(streams))
	var group sync.WaitGroup
	var tcpBytes atomic.Uint64
	for _, stream := range streams {
		group.Add(1)
		go func(connection net.Conn) {
			defer group.Done()
			payload := bytes.Repeat([]byte{0x91}, 65536)
			reply := make([]byte, len(payload))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := connection.Write(payload); err != nil {
					errors <- err
					return
				}
				if _, err := io.ReadFull(connection, reply); err != nil {
					errors <- err
					return
				}
				if !bytes.Equal(payload, reply) {
					errors <- fmt.Errorf("background TCP payload changed")
					return
				}
				tcpBytes.Add(uint64(2 * len(payload)))
			}
		}(stream)
	}
	defer func() {
		close(stop)
		for _, stream := range streams {
			stream.Close()
		}
		group.Wait()
	}()
	exchangeUDP := func(connection net.Conn, count int) {
		t.Helper()
		const window = 8
		credit := make(chan struct{}, window)
		for range window {
			credit <- struct{}{}
		}
		done := make(chan error, 1)
		var received atomic.Uint32
		go func() {
			seen := make([]bool, count)
			reply := make([]byte, len(warm))
			for range count {
				n, err := connection.Read(reply)
				if err != nil {
					done <- err
					return
				}
				if n != len(reply) || !bytes.Equal(reply[4:], warm[4:]) {
					done <- fmt.Errorf("UDP payload changed: %d bytes", n)
					return
				}
				sequence := binary.BigEndian.Uint32(reply)
				if sequence >= uint32(count) || seen[sequence] {
					done <- fmt.Errorf("invalid/duplicate UDP sequence %d", sequence)
					return
				}
				seen[sequence] = true
				received.Add(1)
				credit <- struct{}{}
			}
			done <- nil
		}()
		for sequence := range count {
			select {
			case <-credit:
			case err := <-done:
				t.Fatalf("UDP stopped at %d/%d: %v", sequence, count, err)
			case <-ctx.Done():
				state := fixture.snapshot(t, "stats")
				t.Fatalf("UDP stalled: sent=%d received=%d/%d ingress_drops=%d public_udp_drops=%d", sequence, received.Load(), count, state.Drops-before.Drops, state.PublicUDP.Drops-before.PublicUDP.Drops)
			}
			packet := append([]byte(nil), warm...)
			binary.BigEndian.PutUint32(packet, uint32(sequence))
			if _, err := connection.Write(packet); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			state := fixture.snapshot(t, "stats")
			t.Fatalf("UDP responses did not finish: received=%d/%d ingress_drops=%d public_udp_drops=%d", received.Load(), count, state.Drops-before.Drops, state.PublicUDP.Drops-before.PublicUDP.Drops)
		case <-time.After(15 * time.Second):
			state := fixture.snapshot(t, "stats")
			t.Fatalf("UDP tail stalled: received=%d/%d ingress_drops=%d public_udp_drops=%d", received.Load(), count, state.Drops-before.Drops, state.PublicUDP.Drops-before.PublicUDP.Drops)
		}
	}
	started := time.Now()
	exchangeUDP(udpA, 2000)
	if os.Getenv("XHYS_HANDOFF_MODE") == "managed" {
		fixture.snapshot(t, "switch")
	}
	clientB := fixture.client(t)
	udpB := open(clientB, "udp", fixture.info.UDP)
	// A remains open throughout cutover; neither application flow reconnects.
	exchangeUDP(udpA, 2000)
	exchangeUDP(udpB, 2000)
	after := fixture.snapshot(t, "stats")
	t.Logf("saturated_udp_received=6000/6000 tcp_duplex_mibps=%.2f ingress_drops=%d public_udp_drops=%d generations=%v",
		float64(tcpBytes.Load())/(1<<20)/time.Since(started).Seconds(), after.Drops-before.Drops,
		after.PublicUDP.Drops-before.PublicUDP.Drops, after.Counts)
	if after.Drops != before.Drops || after.PublicUDP.Drops != before.PublicUDP.Drops {
		t.Fatal("packet loss during saturated UDP/TCP cutover")
	}
	select {
	case err := <-errors:
		t.Fatal("background TCP failed:", err)
	default:
	}
}

func TestHandoffDrainUnderSaturatedLoad(t *testing.T) {
	if os.Getenv("XHYS_HANDOFF_MODE") != "managed" ||
		os.Getenv("XHYS_HANDOFF_SHARED_RX") != "1" ||
		os.Getenv("XHYS_HANDOFF_DIRECT_TX") != "1" {
		t.Skip("requires managed native ring with direct receive reacquisition")
	}
	fixture := newHandoffLoadFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Second)
	defer cancel()
	target, err := xnet.ParseDestination("tcp:" + fixture.info.TCP)
	if err != nil {
		t.Fatal(err)
	}
	open := func(client *core.Instance) net.Conn {
		t.Helper()
		connection, err := core.Dial(ctx, client, target)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { connection.Close() })
		if err := connection.SetDeadline(time.Now().Add(70 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(connection, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		return connection
	}
	clientA := fixture.client(t)
	old := open(clientA)
	before := fixture.snapshot(t, "stats")
	fixture.snapshot(t, "switch")
	clientB := fixture.client(t)
	streams := make([]net.Conn, 32)
	for index := range streams {
		streams[index] = open(clientB)
	}
	stop := make(chan struct{})
	errors := make(chan error, len(streams))
	var group sync.WaitGroup
	var exchanges atomic.Uint64
	for _, stream := range streams {
		group.Add(1)
		go func(connection net.Conn) {
			defer group.Done()
			payload := bytes.Repeat([]byte{0x91}, 65536)
			reply := make([]byte, len(payload))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := connection.Write(payload); err != nil {
					errors <- err
					return
				}
				if _, err := io.ReadFull(connection, reply); err != nil {
					errors <- err
					return
				}
				if !bytes.Equal(payload, reply) {
					errors <- fmt.Errorf("new generation TCP payload changed")
					return
				}
				exchanges.Add(1)
			}
		}(stream)
	}
	defer func() {
		close(stop)
		for _, connection := range streams {
			connection.Close()
		}
		group.Wait()
	}()
	wait := func(wanted string) {
		t.Helper()
		for {
			state := fixture.snapshot(t, "drain").Drain
			if state == wanted {
				return
			}
			if state != "ingress-waiting" && state != "udp-waiting" && state != "draining" {
				t.Fatalf("unexpected drain state with live B: %s", state)
			}
			select {
			case err := <-errors:
				t.Fatal("live B interrupted:", err)
			case <-ctx.Done():
				t.Fatalf("drain starved by active traffic: %s", state)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	wait("udp-waiting")
	// A must remain alive after the ingress fence, until its client closes.
	if _, err := old.Write([]byte{0x71}); err != nil {
		t.Fatal(err)
	}
	var reply [1]byte
	if _, err := io.ReadFull(old, reply[:]); err != nil || reply[0] != 0x71 {
		t.Fatalf("old generation interrupted before natural close: %x %v", reply, err)
	}
	old.Close()
	clientA.Close()
	wait("drained")
	atTransfer := exchanges.Load()
	if result := fixture.snapshot(t, "return-direct"); result.Drain != "receive-direct" {
		t.Fatalf("new generation could not reacquire receive under load: %+v", result)
	}
	deadline := time.Now().Add(3 * time.Second)
	for exchanges.Load() < atTransfer+128 {
		select {
		case err := <-errors:
			t.Fatal("direct receive interrupted live B:", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("new generation stopped progressing after receive transfer")
		}
	}
	after := fixture.snapshot(t, "stats")
	t.Logf("drain_under_load_exchanges=%d ingress_drops=%d public_udp_drops=%d generations=%v",
		exchanges.Load(), after.Drops-before.Drops, after.PublicUDP.Drops-before.PublicUDP.Drops, after.Counts)
	if after.Drops != before.Drops || after.PublicUDP.Drops != before.PublicUDP.Drops {
		t.Fatal("packet loss during drain/direct receive reacquisition under load")
	}
	select {
	case err := <-errors:
		t.Fatal("live B failed:", err)
	default:
	}
}
