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

// Package-variable initialization precedes the internal child init. Production
// builds contain neither this environment switch nor its abrupt process exit.
var controlAckFailureFixture = func() bool {
	brokerControlAfterCommand = func(request brokerRequest) {
		if operation := os.Getenv("NODEX_TEST_CONTROL_EXIT_AFTER"); operation != "" && request.Operation == operation {
			os.Exit(86)
		}
	}
	return true
}()

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
	// Losing control must not lose the owner's packet channel, CID key or pins.
	previousControl := first.control.identity()
	fd, err := unix.PidfdOpen(previousControl.PID, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	unix.Close(fd)
	if err != nil {
		t.Fatal(err)
	}
	check() // No management command has recovered control yet.
	_, again, err := first.command(ctx, brokerRequest{Operation: "add", Tag: "hy", Slot: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, file := range again {
			file.Close()
		}
	}()
	if err := unix.Fstat(int(again[0].Fd()), &b); err != nil {
		t.Fatal(err)
	}
	if a.Ino != b.Ino || first.control.identity().PID == previousControl.PID {
		t.Fatal("control recovery replaced channel or did not replace controller")
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
	identity := recovered.broker.control.identity()
	fd, err = unix.PidfdOpen(identity.PID, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	unix.Close(fd)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for _, client := range []brokerClient{first, *recovered.broker} {
		go func(client brokerClient) {
			_, files, err := client.command(ctx, brokerRequest{Operation: "ping"})
			for _, file := range files {
				file.Close()
			}
			results <- err
		}(client)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if first.control.identity() != recovered.broker.control.identity() {
		t.Fatal("competing control recovery launched different controllers")
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

func TestControlRecoveryDoesNotReplayAgainstLiveUnresponsiveProcess(t *testing.T) {
	ticks, err := processStart(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	control := &brokerControl{root: socketRoot(t), target: brokerControlIdentity{PID: os.Getpid(), StartTicks: ticks, Sequence: 1}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, _, err := control.command(ctx, brokerRequest{Operation: "activate", Slot: 1}); err == nil {
		t.Fatal("unavailable control was acknowledged")
	}
	if control.target.Sequence != 1 {
		t.Fatal("live process connection failure started a competing controller")
	}
}

func TestControlLostAckReplaysExistingChannelWithoutChangingOwner(t *testing.T) {
	root := socketRoot(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := public.LocalAddr().(*net.UDPAddr).Port
	public.Close()
	s := &supervisor{ctx: ctx, root: root, bindings: []binding{{tag: "hy", address: "127.0.0.1", port: port, transport: "hysteria"}}}
	t.Setenv("NODEX_TEST_CONTROL_EXIT_AFTER", "add")
	if err := s.startBroker([]byte(`{"inbounds":[{"tag":"hy","streamSettings":{"network":"hysteria"}}]}`)); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NODEX_TEST_CONTROL_EXIT_AFTER", "")
	defer s.brokerStop()
	identity := s.broker.control.identity()
	owner := *s.broker
	response, files, err := owner.command(ctx, brokerRequest{Operation: "add", Tag: "hy", Slot: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()
	direct := owner
	direct.control = nil
	_, retained, err := direct.command(ctx, brokerRequest{Operation: "add", Tag: "hy", Slot: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, file := range retained {
			file.Close()
		}
	}()
	var first, second unix.Stat_t
	if err := unix.Fstat(int(files[0].Fd()), &first); err != nil {
		t.Fatal(err)
	}
	if err := unix.Fstat(int(retained[0].Fd()), &second); err != nil {
		t.Fatal(err)
	}
	if first.Ino != second.Ino || s.broker.control.identity().PID == identity.PID || !response.Ready {
		t.Fatal("lost ACK reconstructed channel or failed to replace control")
	}
}

func TestCorruptControlJournalRejectsRecoveryWithoutStoppingOwner(t *testing.T) {
	root := socketRoot(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
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
	defer s.brokerStop()
	if err := os.WriteFile(filepath.Join(s.broker.control.root, "state.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	recovered := &supervisor{root: root}
	if err := recovered.recoverBroker(ctx, s.brokerState); err == nil {
		t.Fatal("accepted corrupt controller journal")
	}
	owner := *s.broker
	owner.control = nil
	if response, _, err := owner.command(ctx, brokerRequest{Operation: "ping"}); err != nil || !response.Ready {
		t.Fatal("rejected management recovery stopped original packet owner", err)
	}
}

func TestControlRecoveryRetainsQueuedPacketsAndInitialPins(t *testing.T) {
	root := socketRoot(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
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
	defer s.brokerStop()
	views := make([]*handoff.PacketEndpoint, 2)
	for slot := range views {
		_, files, err := s.broker.command(ctx, brokerRequest{Operation: "add", Tag: "hy", Slot: byte(slot)})
		if err != nil {
			t.Fatal(err)
		}
		view, err := handoff.ImportPacketEndpoint(files[0], net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
		for _, file := range files {
			file.Close()
		}
		if err != nil {
			t.Fatal(err)
		}
		views[slot] = view
		defer view.Close()
	}
	client, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	initial := make([]byte, 1200)
	initial[0], initial[4], initial[5] = 0xc0, 1, 8
	copy(initial[6:14], "pinned-A")
	if _, err := client.Write(initial); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 2048)
	if err := views[0].SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, _, err := views[0].ReadFrom(buffer); err != nil || !bytes.Equal(buffer[:n], initial) {
		t.Fatal("Initial pin was not established", err)
	}
	// Keep this finite burst within the public socket capacity while A's
	// original bounded private channel fills.
	direct := *s.broker
	direct.control = nil
	const count = 128
	// Use the owner's actual CID key.
	response, _, err := direct.command(ctx, brokerRequest{Operation: "status", Tag: "hy"})
	if err != nil {
		t.Fatal(err)
	}
	cid := handoff.NewQUICCIDCodec(response.Binding.Key).Generate(0)
	packet := make([]byte, 1200)
	packet[0] = 0x40
	copy(packet[1:], cid[:])
	for sequence := range count {
		packet[24], packet[25] = byte(sequence>>8), byte(sequence)
		if _, err := client.Write(packet); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Microsecond)
	}
	identity := s.broker.control.identity()
	fd, err := unix.PidfdOpen(identity.PID, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	unix.Close(fd)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.broker.command(ctx, brokerRequest{Operation: "activate", Tag: "hy", Slot: 1}); err != nil {
		t.Fatal(err)
	}
	if err := views[0].SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for sequence := range count {
		packet[24], packet[25] = byte(sequence>>8), byte(sequence)
		n, _, err := views[0].ReadFrom(buffer)
		if err != nil || !bytes.Equal(buffer[:n], packet) {
			t.Fatalf("held A datagram lost/reordered at %d: %d %v", sequence, n, err)
		}
	}
	check := func(data []byte, slot int) {
		t.Helper()
		if _, err := client.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := views[slot].SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		n, _, err := views[slot].ReadFrom(buffer)
		if err != nil || !bytes.Equal(buffer[:n], data) {
			t.Fatalf("pin route %d lost: %d %v", slot, n, err)
		}
	}
	check(initial, 0) // A's already-started handshake must not move to B.
	copy(initial[6:14], "fresh--B")
	check(initial, 1)
}

func TestManagedPacketSocketPolicyPreservesLargerBuffers(t *testing.T) {
	open := func() *net.UDPConn {
		t.Helper()
		packet, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { packet.Close() })
		return packet
	}
	reference, packet := open(), open()
	settings := func(packet *net.UDPConn, configure bool) [2]int {
		t.Helper()
		raw, err := packet.SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		var result [2]int
		if err := raw.Control(func(fd uintptr) {
			for i, option := range []int{unix.SO_RCVBUF, unix.SO_SNDBUF} {
				if configure {
					size := 1 << 20
					if i == 0 {
						size = 4 << 20
					}
					if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, option, size); err != nil {
						t.Error(err)
					}
				}
				var err error
				result[i], err = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, option)
				if err != nil {
					t.Error(err)
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
		return result
	}
	want := settings(reference, true)
	if err := tuneManagedPacketSocket(packet); err != nil {
		t.Fatal(err)
	}
	if got := settings(packet, false); got != want {
		t.Fatalf("public policy=%v native=%v", got, want)
	}
	if err := packet.SetReadBuffer(8 << 20); err != nil {
		t.Fatal(err)
	}
	larger := settings(packet, false)
	if err := tuneManagedPacketSocket(packet); err != nil {
		t.Fatal(err)
	}
	if got := settings(packet, false); got != larger {
		t.Fatalf("larger buffers changed: before=%v after=%v", larger, got)
	}
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
