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

	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet/finalmask/udphop"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func TestNativePacketLocalHopping(t *testing.T) {
	for _, serverNative := range []bool{false, true} {
		for _, bridge := range []string{"shared", "socket"} {
			for _, mode := range []string{"local", "local_remote", "local_once"} {
				t.Run(fmt.Sprintf("server_%t/%s/%s", serverNative, bridge, mode), func(t *testing.T) {
					t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "0")
					t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
					if serverNative {
						t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
					}
					t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
					masks := packetMasks(t, "header")
					before := native.Stats().StartedEngines
					server, caPath, _ := xrayServerWithConfig(t, false, func(config *core.Config) {
						message, err := config.Inbound[0].ReceiverSettings.GetInstance()
						if err != nil {
							t.Fatal(err)
						}
						receiver := message.(*proxyman.ReceiverConfig)
						receiver.StreamSettings.Udpmasks = masks
						config.Inbound[0].ReceiverSettings = serial.ToTypedMessage(receiver)
					})
					if serverNative && native.Stats().StartedEngines <= before {
						t.Fatal("local hopping listener fell back to Go")
					}
					relay := newHoppingRelay(t, server)
					peer := relay.fronts[0].LocalAddr().String()
					hop := &udphop.Config{Local: true, IntervalMin: 5, IntervalMax: 5}
					if mode != "local" {
						peer = "127.0.0.1:1"
						hop.Remote = mode == "local_remote"
						hop.RemoteOnce = mode == "local_once"
						hop.RemotePorts = relay.ports()
						hop.RemoteIPs = []string{"::ffff:127.0.0.1/128"}
					}
					ca, err := os.ReadFile(caPath)
					if err != nil {
						t.Fatal(err)
					}
					t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
					config := embeddedClientConfig(t, peer, ca, false, auth)
					message, err := config.Outbound[0].SenderSettings.GetInstance()
					if err != nil {
						t.Fatal(err)
					}
					sender := message.(*proxyman.SenderConfig)
					sender.StreamSettings.Udpmasks = append(append([]*serial.TypedMessage(nil), masks...), serial.ToTypedMessage(hop))
					config.Outbound[0].SenderSettings = serial.ToTypedMessage(sender)
					client, err := core.New(config)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { client.Close() })
					if err := client.Start(); err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					tcp, udp := echoTargets(t)
					before = native.Stats().StartedEngines
					var connections []net.Conn
					for _, address := range []string{"tcp:" + tcp, "udp:" + udp} {
						target, err := xnet.ParseDestination(address)
						if err != nil {
							t.Fatal(err)
						}
						connection, err := core.Dial(ctx, client, target)
						if err != nil {
							t.Fatal(err)
						}
						defer connection.Close()
						stop := context.AfterFunc(ctx, func() { connection.Close() })
						defer stop()
						connections = append(connections, connection)
					}
					for sample := 0; ctx.Err() == nil; sample++ {
						for index, connection := range connections {
							size := 32 << 10
							if index == 1 {
								size = 4096
							}
							payload := bytes.Repeat([]byte{byte(sample)}, size)
							if _, err := connection.Write(payload); err != nil {
								t.Fatal(err)
							}
							received := make([]byte, size)
							if _, err := io.ReadFull(connection, received); err != nil || !bytes.Equal(payload, received) {
								t.Fatalf("flow failed during local hopping: %v", err)
							}
						}
						relay.mu.Lock()
						sources := len(relay.peers)
						relay.mu.Unlock()
						if sources >= 4 {
							if native.Stats().StartedEngines-before != 2 {
								t.Fatal("local hopping reconnected or fell back to Go")
							}
							t.Log("TCP and fragmented UDP survived local source-port replacement without reconnecting")
							return
						}
						select {
						case err := <-relay.errors:
							t.Fatal(err)
						case <-time.After(100 * time.Millisecond):
						case <-ctx.Done():
						}
					}
					t.Fatal("local source ports did not change")
				})
			}
		}
	}
}

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
				before := native.Stats().StartedEngines
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
						if native.Stats().StartedEngines-before < 2 {
							t.Fatal("port hopping silently fell back to Go")
						}
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

func TestEmbeddedNativeRemoteAddressHopping(t *testing.T) {
	for _, mode := range []string{"ip_only", "ip_and_ports", "ipv6_ports", "once_ip_and_port", "once_ports"} {
		for _, serverNative := range []bool{false, true} {
			for _, masked := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/native_server_%t_salamander_%t", mode, serverNative, masked), func(t *testing.T) {
					t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
					if serverNative {
						t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
					}
					server, caPath := xrayServer(t, masked)
					var relay *hoppingRelay
					if mode == "ipv6_ports" {
						relay = newHoppingRelayOnIPs(t, server, []string{"::1", "::1"}, false)
					} else if mode == "once_ports" {
						relay = newHoppingRelay(t, server)
					} else {
						relay = newHoppingRelayOnIPs(t, server, []string{"127.0.0.2", "127.0.0.3"}, mode != "ip_and_ports")
					}
					ca, err := os.ReadFile(caPath)
					if err != nil {
						t.Fatal(err)
					}
					hop := &udphop.Config{IntervalMin: 5, IntervalMax: 5, RemoteIPs: []string{"127.0.0.2/32", "127.0.0.3/32"}}
					logical := "127.0.0.1:1"
					if mode == "ipv6_ports" {
						hop.Remote = true
						hop.RemoteIPs = []string{"::1/128"}
						hop.RemotePorts = relay.ports()
						logical = "[::1]:1"
					} else if mode == "ip_and_ports" {
						hop.Remote = true
						hop.RemotePorts = relay.ports()
					} else if mode == "ip_only" {
						hop.Remote = true
						logical = fmt.Sprintf("127.0.0.1:%d", relay.ports()[0])
					} else {
						hop.RemoteOnce = true
						hop.RemotePorts = relay.ports()
						if mode == "once_ports" {
							hop.RemoteIPs = nil
						}
					}
					t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
					before := native.Stats().StartedEngines
					client := embeddedClient(t, logical, ca, masked, hop)
					tcp, udp := echoTargets(t)
					ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
					defer cancel()
					var connections []net.Conn
					for _, address := range []string{"tcp:" + tcp, "udp:" + udp} {
						target, err := xnet.ParseDestination(address)
						if err != nil {
							t.Fatal(err)
						}
						connection, err := core.Dial(ctx, client, target)
						if err != nil {
							t.Fatal(err)
						}
						defer connection.Close()
						stop := context.AfterFunc(ctx, func() { connection.Close() })
						defer stop()
						connections = append(connections, connection)
					}
					started := time.Now()
					for sample := 0; ctx.Err() == nil; sample++ {
						for index, connection := range connections {
							size := 32 * 1024
							if index == 1 {
								size = 4096
							}
							payload := bytes.Repeat([]byte{byte(sample), byte(index), 0xa5, 0x5a}, size/4)
							if _, err := connection.Write(payload); err != nil {
								t.Fatal(err)
							}
							received := make([]byte, size)
							if _, err := io.ReadFull(connection, received); err != nil {
								t.Fatalf("transfer during remote address hopping failed: %v", err)
							}
							if !bytes.Equal(payload, received) {
								t.Fatal("TCP/fragmented UDP payload changed during remote address hopping")
							}
						}
						if time.Since(started) >= 6*time.Second {
							if native.Stats().StartedEngines-before < 2 {
								t.Fatal("remote address hopping silently fell back to Go")
							}
							relay.mu.Lock()
							complete := len(relay.peers) == 2
							for _, peer := range relay.peers {
								if hop.Remote {
									complete = complete && peer.changes >= 1
								} else {
									complete = complete && peer.changes == 0
								}
							}
							relay.mu.Unlock()
							if !complete {
								t.Fatal("unexpected remote address changes or local socket replacement")
							}
							return
						}
						select {
						case err := <-relay.errors:
							t.Fatal(err)
						case <-time.After(100 * time.Millisecond):
						case <-ctx.Done():
						}
					}
					t.Fatal("remote address hopping test timed out")
				})
			}
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
	return newHoppingRelayOnIPs(t, server, []string{"127.0.0.1", "127.0.0.1"}, false)
}

func newHoppingRelayOnIPs(t *testing.T, server string, ips []string, sharedPort bool) *hoppingRelay {
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
	port := 0
	for _, ip := range ips {
		address := net.ParseIP(ip)
		network := "udp4"
		if address.To4() == nil {
			network = "udp6"
		}
		front, err := net.ListenUDP(network, &net.UDPAddr{IP: address, Port: port})
		if err != nil {
			t.Fatal(err)
		}
		relay.fronts = append(relay.fronts, front)
		if sharedPort {
			port = front.LocalAddr().(*net.UDPAddr).Port
		}
		relay.workers.Add(1)
		go relay.forward(front)
	}
	if !sharedPort {
		ports := relay.ports()
		for _, ip := range ips {
			address := net.ParseIP(ip)
			for _, port := range ports {
				present := false
				for _, front := range relay.fronts {
					bound := front.LocalAddr().(*net.UDPAddr)
					present = present || bound.IP.Equal(address) && bound.Port == int(port)
				}
				if present {
					continue
				}
				network := "udp4"
				if address.To4() == nil {
					network = "udp6"
				}
				front, err := net.ListenUDP(network, &net.UDPAddr{IP: address, Port: int(port)})
				if err != nil {
					t.Fatal(err)
				}
				relay.fronts = append(relay.fronts, front)
				relay.workers.Add(1)
				go relay.forward(front)
			}
		}
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
