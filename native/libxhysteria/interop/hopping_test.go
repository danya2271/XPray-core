//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet/finalmask/udphop"
)

func TestEmbeddedNativePortHopping(t *testing.T) {
	for _, serverNative := range []bool{false, true} {
		for _, masked := range []bool{false, true} {
			t.Run(fmt.Sprintf("native_server_%t_salamander_%t", serverNative, masked), func(t *testing.T) {
				t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
				if serverNative {
					t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
				}
				server, caPath := xrayServer(t, masked)
				relay := newHoppingRelay(t, server)
				ca, err := os.ReadFile(caPath)
				if err != nil {
					t.Fatal(err)
				}
				t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
				// The logical peer port is deliberately unreachable. Even the
				// first Initial must use a configured hopping port.
				client := embeddedClient(t, "127.0.0.1:1", ca, masked, &udphop.Config{
					Remote: true, IntervalMin: 5, IntervalMax: 5, RemotePorts: relay.ports(),
				})
				tcp, udp := echoTargets(t)
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				defer cancel()
				var connections []net.Conn
				for _, address := range []string{"tcp:" + tcp, "udp:" + udp} {
					target, err := xnet.ParseDestination(address)
					if err != nil {
						t.Fatal(err)
					}
					conn, err := core.Dial(ctx, client, target)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					stop := context.AfterFunc(ctx, func() { conn.Close() })
					defer stop()
					connections = append(connections, conn)
				}
				for sample := 0; ctx.Err() == nil; sample++ {
					for index, conn := range connections {
						size := 32 * 1024
						if index == 1 {
							size = 4096
						}
						payload := bytes.Repeat([]byte{byte(sample), byte(index), 0xa5, 0x5a}, size/4)
						if _, err := conn.Write(payload); err != nil {
							t.Fatal(err)
						}
						got := make([]byte, len(payload))
						if _, err := io.ReadFull(conn, got); err != nil {
							t.Fatalf("transfer during hopping failed: %v", err)
						}
						if !bytes.Equal(got, payload) {
							t.Fatal("TCP/fragmented UDP payload changed during hopping")
						}
					}
					relay.mu.Lock()
					complete := len(relay.peers) == 2
					for _, peer := range relay.peers {
						complete = complete && peer.changes >= 2
					}
					relay.mu.Unlock()
					if complete {
						t.Log("same TCP/UDP flows survived two remote port changes with stable local sockets")
						return
					}
					select {
					case err := <-relay.errors:
						t.Fatal(err)
					case <-time.After(250 * time.Millisecond):
					case <-ctx.Done():
					}
				}
				t.Fatal("did not observe two port changes on each live native flow")
			})
		}
	}
}

type hoppingRelayPeer struct {
	upstream *net.UDPConn
	address  *net.UDPAddr
	front    *net.UDPConn
	changes  int
}

// A userspace stand-in for a server's DNAT port range. Each client keeps one
// upstream source socket, irrespective of the ingress port used by its packets.
type hoppingRelay struct {
	mu      sync.Mutex
	peers   map[string]*hoppingRelayPeer
	fronts  []*net.UDPConn
	server  *net.UDPAddr
	closed  bool
	errors  chan error
	workers sync.WaitGroup
}

func newHoppingRelay(t *testing.T, server string) *hoppingRelay {
	t.Helper()
	address, err := net.ResolveUDPAddr("udp4", server)
	if err != nil {
		t.Fatal(err)
	}
	relay := &hoppingRelay{server: address, peers: make(map[string]*hoppingRelayPeer), errors: make(chan error, 1)}
	t.Cleanup(func() {
		relay.mu.Lock()
		relay.closed = true
		for _, front := range relay.fronts {
			front.Close()
		}
		for _, peer := range relay.peers {
			peer.upstream.Close()
		}
		relay.mu.Unlock()
		relay.workers.Wait()
	})
	for range 2 {
		front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		relay.fronts = append(relay.fronts, front)
		relay.workers.Add(1)
		go relay.forward(front)
	}
	return relay
}

func (r *hoppingRelay) ports() []uint32 {
	var ports []uint32
	for _, front := range r.fronts {
		ports = append(ports, uint32(front.LocalAddr().(*net.UDPAddr).Port))
	}
	return ports
}

func (r *hoppingRelay) fail(err error) {
	select {
	case r.errors <- err:
	default:
	}
}

func (r *hoppingRelay) forward(front *net.UDPConn) {
	defer r.workers.Done()
	packet := make([]byte, 65536)
	for {
		n, address, err := front.ReadFromUDP(packet)
		if err != nil {
			return
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}
		peer := r.peers[address.String()]
		if peer == nil {
			upstream, err := net.DialUDP("udp4", nil, r.server)
			if err != nil {
				r.mu.Unlock()
				r.fail(err)
				return
			}
			peer = &hoppingRelayPeer{upstream: upstream, address: address, front: front}
			r.peers[address.String()] = peer
			r.workers.Add(1)
			go r.reply(peer)
		} else if peer.front != front {
			peer.front = front
			peer.changes++
		}
		r.mu.Unlock()
		if _, err := peer.upstream.Write(packet[:n]); err != nil {
			r.fail(err)
			return
		}
	}
}

func (r *hoppingRelay) reply(peer *hoppingRelayPeer) {
	defer r.workers.Done()
	packet := make([]byte, 65536)
	for {
		n, err := peer.upstream.Read(packet)
		if err != nil {
			return
		}
		r.mu.Lock()
		front := peer.front
		r.mu.Unlock()
		if _, err := front.WriteToUDP(packet[:n], peer.address); err != nil {
			r.fail(err)
			return
		}
	}
}
