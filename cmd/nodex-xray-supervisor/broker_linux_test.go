//go:build linux && !android

package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
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
