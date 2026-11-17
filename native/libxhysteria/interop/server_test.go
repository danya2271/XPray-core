//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/app/proxyman/command"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/inbound"
	hyproxy "github.com/xtls/xray-core/proxy/hysteria"
	"github.com/xtls/xray-core/proxy/hysteria/account"
	hytransport "github.com/xtls/xray-core/transport/internet/hysteria"
	"github.com/xtls/xray-core/transport/internet/tls"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func nativeServerConfig(t *testing.T, config *core.Config, empty bool) {
	t.Helper()
	message, err := config.Inbound[0].ReceiverSettings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	receiver := message.(*proxyman.ReceiverConfig)
	receiver.StreamSettings.TransportSettings[0].Settings = serial.ToTypedMessage(&hytransport.Config{
		Native: true, Auth: auth, UdpIdleTimeout: 30, MasqType: "404",
	})
	message, err = receiver.StreamSettings.SecuritySettings[0].GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	security := message.(*tls.Config)
	security.MinVersion = "1.2" // DanVlessBot's existing QUIC/TLS lower bound.
	receiver.StreamSettings.SecuritySettings[0] = serial.ToTypedMessage(security)
	config.Inbound[0].ReceiverSettings = serial.ToTypedMessage(receiver)
	if empty {
		config.Inbound[0].ProxySettings = serial.ToTypedMessage(&hyproxy.ServerConfig{})
		receiver.StreamSettings.TransportSettings[0].Settings = serial.ToTypedMessage(&hytransport.Config{
			Native: true, UdpIdleTimeout: 30, MasqType: "404",
		})
		config.Inbound[0].ReceiverSettings = serial.ToTypedMessage(receiver)
	}
}

func TestNativeServerDynamicUsers(t *testing.T) {
	t.Setenv("XRAY_HYSTERIA_NATIVE", "")
	t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "0")
	before := native.Stats().StartedEngines
	server, caPath, instance := xrayServerWithConfig(t, true, func(c *core.Config) { nativeServerConfig(t, c, true) })
	if native.Stats().StartedEngines-before != 1 {
		t.Fatal("per-transport native selection did not start the server")
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := instance.GetFeature(inbound.ManagerType()).(inbound.Manager).GetHandler(context.Background(), "hysteria-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
	tcp, udp := echoTargets(t)
	dial := func(password, network, address string) net.Conn {
		t.Helper()
		client := embeddedClientWithAuth(t, server, ca, true, password)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		target, err := xnet.ParseDestination(network + ":" + address)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := core.Dial(ctx, client, target)
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(4 * time.Second))
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	exchange := func(conn net.Conn, size int) error {
		payload := bytes.Repeat([]byte{0xa7}, size)
		if _, err := conn.Write(payload); err != nil {
			return err
		}
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, got); err != nil {
			return err
		}
		if !bytes.Equal(payload, got) {
			return fmt.Errorf("native server payload changed")
		}
		return nil
	}
	first := "341c44a5-9b49-4830-a407-2fd997d44866"
	if err := exchange(dial(first, "tcp", tcp), 4); err == nil {
		t.Fatal("empty native validator accepted an unknown user")
	}
	add := func(password, email string) {
		t.Helper()
		op := command.AddUserOperation{User: &protocol.User{Email: email, Account: serial.ToTypedMessage(&account.Account{Auth: password})}}
		if err := op.ApplyInbound(context.Background(), handler); err != nil {
			t.Fatal(err)
		}
	}
	add(first, "first@example.test")
	existing := dial(first, "tcp", tcp)
	if err := exchange(existing, 65536); err != nil {
		t.Fatal(err)
	}
	if err := exchange(dial(first, "udp", udp), 4096); err != nil {
		t.Fatal(err)
	}
	variant := uuid.MustParse(first)
	variant[6], variant[7] = 0x12, 0x34
	if err := exchange(dial(variant.String(), "tcp", tcp), 4); err != nil {
		t.Fatalf("native auth lost the validator's UUID routing alias: %v", err)
	}
	remove := command.RemoveUserOperation{Email: "first@example.test"}
	if err := remove.ApplyInbound(context.Background(), handler); err != nil {
		t.Fatal(err)
	}
	if err := exchange(existing, 4096); err != nil {
		t.Fatalf("user removal interrupted an existing flow: %v", err)
	}
	if err := exchange(dial(first, "tcp", tcp), 4); err == nil {
		t.Fatal("removed user opened a new native session")
	}
	// Authentication must not be limited by the prototype's static 1024-user table.
	for i := range 1100 {
		add(fmt.Sprintf("dynamic-password-%d", i), fmt.Sprintf("user-%d@example.test", i))
	}
	if err := exchange(dial("dynamic-password-1099", "tcp", tcp), 4); err != nil {
		t.Fatal(err)
	}
}

func TestNativeServerBeyondPrototypeFlowCap(t *testing.T) {
	t.Setenv("XRAY_HYSTERIA_NATIVE", "")
	t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "4")
	t.Setenv("XRAY_HYSTERIA_SERVER_FLOWS", "256")
	server, caPath, _ := xrayServerWithConfig(t, true, func(c *core.Config) { nativeServerConfig(t, c, false) })
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
	client := embeddedClient(t, server, ca, true)
	tcp, _ := echoTargets(t)
	target, _ := xnet.ParseDestination("tcp:" + tcp)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range 160 {
		conn, err := core.Dial(ctx, client, target)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		var reply [1]byte
		if _, err := io.ReadFull(conn, reply[:]); err != nil || reply[0] != byte(i) {
			t.Fatalf("flow %d failed: %v", i, err)
		}
	}
}

func TestNativeShortFlowsReturnQUICStreamCapacity(t *testing.T) {
	for _, bridge := range []string{"shared", "socket"} {
		t.Run(bridge, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_NATIVE", "")
			t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
			t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "1")
			server, caPath, _ := xrayServerWithConfig(t, true, func(config *core.Config) {
				nativeServerConfig(t, config, false)
			})
			ca, err := os.ReadFile(caPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
			client := embeddedClient(t, server, ca, true)
			tcp, _ := echoTargets(t)
			target, _ := xnet.ParseDestination("tcp:" + tcp)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for index := range 300 {
				conn, err := core.Dial(ctx, client, target)
				if err != nil {
					t.Fatal(err)
				}
				conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
				_, err = conn.Write([]byte{byte(index)})
				var reply [1]byte
				if err == nil {
					_, err = io.ReadFull(conn, reply[:])
				}
				conn.Close()
				if err != nil || reply[0] != byte(index) {
					t.Fatalf("short flow %d stalled or changed payload: %v", index, err)
				}
			}
		})
	}
}

func TestNativeUDPActivityRefreshesIdleExpiry(t *testing.T) {
	t.Setenv("XRAY_HYSTERIA_NATIVE", "")
	t.Setenv("XRAY_HYSTERIA_SERVER_FLOWS", "1")
	server, caPath, _ := xrayServerWithConfig(t, true, func(config *core.Config) {
		nativeServerConfig(t, config, false)
		message, err := config.Inbound[0].ReceiverSettings.GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		receiver := message.(*proxyman.ReceiverConfig)
		receiver.StreamSettings.TransportSettings[0].Settings = serial.ToTypedMessage(&hytransport.Config{
			Native: true, Auth: auth, UdpIdleTimeout: 1, MasqType: "404",
		})
		config.Inbound[0].ReceiverSettings = serial.ToTypedMessage(receiver)
	})
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
	t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "1")
	client := embeddedClient(t, server, ca, true)
	tcp, udp := echoTargets(t)
	udpTarget, _ := xnet.ParseDestination("udp:" + udp)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	packetConn, err := core.Dial(ctx, client, udpTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer packetConn.Close()
	exchange := func(conn net.Conn) error {
		conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
		if _, err := conn.Write([]byte{0x71}); err != nil {
			return err
		}
		var reply [1]byte
		if _, err := io.ReadFull(conn, reply[:]); err != nil {
			return err
		}
		if reply[0] != 0x71 {
			return fmt.Errorf("echo payload changed")
		}
		return nil
	}
	if err := exchange(packetConn); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond)
	if err := exchange(packetConn); err != nil {
		t.Fatal(err)
	}
	tcpTarget, _ := xnet.ParseDestination("tcp:" + tcp)
	openTCP := func() error {
		conn, err := core.Dial(ctx, client, tcpTarget)
		if err != nil {
			return err
		}
		defer conn.Close()
		return exchange(conn)
	}
	time.Sleep(550 * time.Millisecond)
	if err := openTCP(); err == nil {
		t.Fatal("active UDP flow expired before its refreshed idle deadline")
	}
	time.Sleep(700 * time.Millisecond)
	if err := openTCP(); err != nil {
		t.Fatalf("idle UDP flow did not release server capacity: %v", err)
	}
}

func TestNativeServerCapacityRejectsAndRecovers(t *testing.T) {
	for _, bridge := range []string{"shared", "socket"} {
		t.Run(bridge, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_NATIVE", "")
			t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
			t.Setenv("XRAY_HYSTERIA_SERVER_CONNECTIONS", "2")
			t.Setenv("XRAY_HYSTERIA_SERVER_FLOWS", "4")
			server, caPath, _ := xrayServerWithConfig(t, true, func(c *core.Config) { nativeServerConfig(t, c, false) })
			ca, err := os.ReadFile(caPath)
			if err != nil {
				t.Fatal(err)
			}
			address, err := net.ResolveUDPAddr("udp", server)
			if err != nil {
				t.Fatal(err)
			}
			start := func() (*native.Engine, error) {
				udp, err := net.ListenUDP("udp4", nil)
				if err != nil {
					return nil, err
				}
				defer udp.Close()
				file, err := udp.File()
				if err != nil {
					return nil, err
				}
				defer file.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				engine, err := native.Start(ctx, native.Config{Peer: server, ServerName: "localhost", Auth: auth, CAPEM: string(ca), Salamander: psk}, file, udp.LocalAddr(), address, nil)
				if engine != nil {
					t.Cleanup(func() { engine.Close() })
				}
				return engine, err
			}
			first, err := start()
			if err != nil {
				t.Fatal(err)
			}
			second, err := start()
			if err != nil {
				t.Fatal(err)
			}
			began := time.Now()
			if excess, err := start(); err == nil || excess != nil || !strings.Contains(strings.ToLower(err.Error()), "refused") || time.Since(began) >= 2*time.Second {
				t.Fatalf("excess connection was not refused promptly: engine=%v error=%v elapsed=%s", excess, err, time.Since(began))
			}
			tcp, _ := echoTargets(t)
			target, _ := xnet.ParseDestination("tcp:" + tcp)
			exchange := func(conn net.Conn) error {
				conn.SetDeadline(time.Now().Add(2 * time.Second))
				if _, err := conn.Write([]byte{0xa7}); err != nil {
					return err
				}
				var reply [1]byte
				_, err := io.ReadFull(conn, reply[:])
				if err == nil && reply[0] != 0xa7 {
					return fmt.Errorf("payload changed")
				}
				return err
			}
			var flows []*native.Conn
			for _, engine := range []*native.Engine{first, second, first, second} {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				conn, err := engine.Open(ctx, target)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { conn.Close() })
				if err := exchange(conn); err != nil {
					t.Fatal(err)
				}
				flows = append(flows, conn)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			began = time.Now()
			excess, err := second.Open(ctx, target)
			cancel()
			if excess != nil {
				excess.Close()
			}
			if err == nil || time.Since(began) >= 2*time.Second {
				t.Fatalf("excess flow was not rejected promptly: %v elapsed=%s", err, time.Since(began))
			}
			flows[0].Close()
			deadline := time.Now().Add(2 * time.Second)
			for {
				conn, err := second.Open(context.Background(), target)
				if err == nil {
					err = exchange(conn)
					conn.Close()
					if err != nil {
						t.Fatal(err)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("flow slot did not recover: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			first.Close()
			deadline = time.Now().Add(2 * time.Second)
			for {
				if _, err := start(); err == nil {
					break
				} else if time.Now().After(deadline) {
					t.Fatalf("connection slot did not recover: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := exchange(flows[1]); err != nil {
				t.Fatalf("capacity rejection interrupted an established flow: %v", err)
			}
		})
	}
}
