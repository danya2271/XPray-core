//go:build linux && !android

package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet/handoff"
	"golang.org/x/sys/unix"
)

func TestPersistentBrokerReconnectKeepsCIDRoutesAndDescriptors(t *testing.T) {
	root := socketRoot(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := public.LocalAddr().(*net.UDPAddr).Port
	public.Close()
	config := []byte(`{"inbounds":[{"tag":"hy","streamSettings":{"network":"hysteria"}}]}`)
	s := &supervisor{ctx: ctx, root: root, bindings: []binding{{tag: "hy", address: "127.0.0.1", port: port, transport: "hysteria"}}}
	if err := s.startBroker(config); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s.brokerStop != nil {
			s.brokerStop()
		}
	}()
	first := *s.broker
	response, files, err := first.command(ctx, brokerRequest{Operation: "add", Tag: "hy", Slot: 0})
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		defer file.Close()
	}
	_, repeated, err := first.command(ctx, brokerRequest{Operation: "add", Tag: "hy", Slot: 0})
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range repeated {
		defer file.Close()
	}
	var a, b unix.Stat_t
	unix.Fstat(int(files[0].Fd()), &a)
	unix.Fstat(int(repeated[0].Fd()), &b)
	if a.Ino != b.Ino {
		t.Fatal("retry replaced generation channel")
	}
	view, err := handoff.ImportPacketEndpoint(files[0], net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	client, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	codec := handoff.NewQUICCIDCodec(response.Binding.Key)
	cid := codec.Generate(0)
	packet := append([]byte{0x40}, cid[:]...)
	packet = append(packet, 1)
	check := func() {
		t.Helper()
		client.Write(packet)
		view.SetReadDeadline(time.Now().Add(2 * time.Second))
		var buffer [2048]byte
		n, _, err := view.ReadFrom(buffer[:])
		if err != nil || !bytes.Equal(buffer[:n], packet) {
			t.Fatalf("CID forwarding: %d, %v", n, err)
		}
	}
	check()
	// A new management object reconnects to the same child and all in-memory
	// route ownership. It neither binds nor starts a replacement packet owner.
	recovered := &supervisor{root: root}
	if err := recovered.recoverBroker(ctx, s.brokerState); err != nil {
		t.Fatal(err)
	}
	if recovered.broker.pid != first.pid {
		t.Fatal("replaced persistent broker")
	}
	check()
	bad := *recovered.broker
	bad.nonce[0] ^= 1
	if _, _, err := bad.command(ctx, brokerRequest{Operation: "ping"}); err == nil {
		t.Fatal("accepted stale broker nonce")
	}
	bad = *recovered.broker
	bad.pid++
	if _, _, err := bad.command(ctx, brokerRequest{Operation: "ping"}); err == nil {
		t.Fatal("accepted different broker process")
	}
	recovered.brokerStop()
	s.brokerStop = nil
	_ = os.Remove(filepath.Join(root, "broker", "ctl.sock"))
}

func TestBrokerDeathWithoutDirectCheckpointBlocksWithoutStoppingXray(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	root := socketRoot(t)
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := public.LocalAddr().(*net.UDPAddr).Port
	public.Close()
	s := &supervisor{ctx: ctx, root: root, bindings: []binding{{tag: "hy", address: "127.0.0.1", port: port, transport: "hysteria"}}}
	if err := s.startBroker([]byte(`{"inbounds":[{"tag":"hy","streamSettings":{"network":"hysteria"}}]}`)); err != nil {
		t.Fatal(err)
	}
	var signalled bool
	active := &generation{id: "g1", done: make(chan struct{}), signal: func(syscall.Signal) error { signalled = true; return nil }}
	if err := s.record("active", active, nil, nil); err != nil {
		t.Fatal(err)
	}
	s.watchBroker()
	defer func() { cancel(); s.wg.Wait(); s.brokerStop() }()
	fd, err := unix.PidfdOpen(s.broker.pid, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	unix.Close(fd)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for s.status().Phase != "blocked" {
		if time.Now().After(deadline) {
			t.Fatalf("dead relay broker remained active: %+v", s.status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if signalled || !active.running() {
		t.Fatal("broker failure stopped live Xray")
	}
	if err := s.upgrade(release{}); err == nil {
		t.Fatal("accepted an update without the packet owner")
	}
}

func TestBrokerPidfdWaitCancelsWhileProcessIsAlive(t *testing.T) {
	ticks, err := processStart(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- waitBrokerExit(ctx, brokerClient{pid: os.Getpid(), startTicks: ticks}) }()
	select {
	case err := <-done:
		t.Fatalf("reported death of a live broker: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("broker monitor shutdown did not wake pidfd wait")
	}
}

func TestBrokerRecoveryRejectsReusedReservedSlots(t *testing.T) {
	broker := &packetBroker{nonce: [32]byte{1}, endpoints: map[string]*brokerEndpoint{
		"hy": {reservedBefore: 200, peers: make(map[byte]*brokerPeer)},
	}}
	response, files := broker.command(t.Context(), brokerRequest{Nonce: broker.nonce, Operation: "add", Tag: "hy", Slot: 199})
	if response.Error == "" || response.Ready || len(files) != 0 {
		t.Fatalf("recovered slot reuse accepted: %+v", response)
	}
}
