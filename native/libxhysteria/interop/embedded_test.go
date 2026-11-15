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

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	hyproxy "github.com/xtls/xray-core/proxy/hysteria"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	hytransport "github.com/xtls/xray-core/transport/internet/hysteria"
	xtls "github.com/xtls/xray-core/transport/internet/tls"
)

func TestEmbeddedNativeServer(t *testing.T) {
	t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
	TestNativeClientAgainstXray(t)
}

func TestEmbeddedBrutal(t *testing.T) {
	t.Setenv("XHYS_TEST_BRUTAL", "1")
	TestEmbeddedNativeOutbound(t)
}

func TestEmbeddedNativeOutbound(t *testing.T) {
	for _, serverNative := range []bool{false, true} {
		for _, masked := range []bool{false, true} {
			name := "go_server"
			if serverNative {
				name = "native_server"
			}
			if masked {
				name += "_salamander"
			}
			t.Run(name, func(t *testing.T) {
				t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
				if serverNative {
					t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
				}
				server, caPath := xrayServer(t, masked)
				t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
				ca, err := os.ReadFile(caPath)
				if err != nil {
					t.Fatal(err)
				}
				client := embeddedClient(t, server, ca, masked)
				tcpTarget, udpTarget := echoTargets(t)
				for _, test := range []struct {
					network, address string
					size             int
				}{
					{"tcp", tcpTarget, 1}, {"tcp", tcpTarget, 128 * 1024},
					{"udp", udpTarget, 1}, {"udp", udpTarget, 4096},
				} {
					t.Run(fmt.Sprintf("%s_%d", test.network, test.size), func(t *testing.T) {
						ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
						defer cancel()
						target, err := xnet.ParseDestination(test.network + ":" + test.address)
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
						payload := make([]byte, test.size)
						for i := range payload {
							payload[i] = byte(i % 251)
						}
						if _, err := conn.Write(payload); err != nil {
							t.Fatal(err)
						}
						got := make([]byte, len(payload))
						if _, err := io.ReadFull(conn, got); err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(got, payload) {
							t.Fatal("payload changed through embedded native outbound")
						}
					})
				}
			})
		}
	}
}

func TestEmbeddedConcurrentTransfers(t *testing.T) {
	t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
	server, caPath := xrayServer(t, true)
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	client := embeddedClient(t, server, ca, true)
	tcp, _ := echoTargets(t)
	target, err := xnet.ParseDestination("tcp:" + tcp)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			conn, err := core.Dial(ctx, client, target)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			stop := context.AfterFunc(ctx, func() { conn.Close() })
			defer stop()
			payload := bytes.Repeat([]byte("native-transfer"), 128*1024)
			written := make(chan error, 1)
			go func() { _, err := conn.Write(payload); written <- err }()
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, got); err != nil {
				t.Error(err)
				return
			}
			if err := <-written; err != nil {
				t.Error(err)
			}
			if !bytes.Equal(payload, got) {
				t.Error("concurrent native payload mismatch")
			}
		}()
	}
	group.Wait()
}

func TestGoOutboundAgainstEmbeddedServer(t *testing.T) {
	for _, masked := range []bool{false, true} {
		t.Run(fmt.Sprintf("salamander_%t", masked), func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
			server, caPath := xrayServer(t, masked)
			t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
			ca, err := os.ReadFile(caPath)
			if err != nil {
				t.Fatal(err)
			}
			client := embeddedClient(t, server, ca, masked)
			tcp, udp := echoTargets(t)
			for _, test := range []struct{ network, address string }{{"tcp", tcp}, {"udp", udp}} {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				target, err := xnet.ParseDestination(test.network + ":" + test.address)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				conn, err := core.Dial(ctx, client, target)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				stop := context.AfterFunc(ctx, func() { conn.Close() })
				payload := bytes.Repeat([]byte{0xa5}, 4096)
				_, err = conn.Write(payload)
				if err == nil {
					got := make([]byte, len(payload))
					_, err = io.ReadFull(conn, got)
					if err == nil && !bytes.Equal(got, payload) {
						err = fmt.Errorf("%s payload mismatch", test.network)
					}
				}
				stop()
				conn.Close()
				cancel()
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func embeddedClient(t *testing.T, server string, ca []byte, masked bool) *core.Instance {
	t.Helper()
	address, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		t.Fatal(err)
	}
	stream := &internet.StreamConfig{
		ProtocolName: "hysteria",
		TransportSettings: []*internet.TransportConfig{{
			ProtocolName: "hysteria", Settings: serial.ToTypedMessage(&hytransport.Config{Auth: auth}),
		}},
		SecurityType: serial.GetMessageType(&xtls.Config{}),
		SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&xtls.Config{
			ServerName: "localhost", NextProtocol: []string{"h3"}, DisableSystemRoot: true,
			Certificate: []*xtls.Certificate{{Certificate: ca, Usage: xtls.Certificate_AUTHORITY_VERIFY}},
		})},
		QuicParams: prototypeQuicParams(),
	}
	stream.QuicParams.MaxIdleTimeout = 45
	stream.QuicParams.KeepAlivePeriod = 9
	if masked {
		stream.Udpmasks = []*serial.TypedMessage{serial.ToTypedMessage(&salamander.Config{Password: psk})}
	}
	client, err := core.New(&core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		},
		Outbound: []*core.OutboundHandlerConfig{{
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{StreamSettings: stream}),
			ProxySettings: serial.ToTypedMessage(&hyproxy.ClientConfig{Server: &protocol.ServerEndpoint{
				Address: xnet.NewIPOrDomain(xnet.IPAddress(address.IP)), Port: uint32(address.Port),
			}}),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	return client
}
