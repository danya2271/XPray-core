//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet/finalmask/header/custom"
	"github.com/xtls/xray-core/transport/internet/finalmask/mkcp/aes128gcm"
	"github.com/xtls/xray-core/transport/internet/finalmask/mkcp/header"
	"github.com/xtls/xray-core/transport/internet/finalmask/mkcp/original"
	"github.com/xtls/xray-core/transport/internet/finalmask/noise"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"github.com/xtls/xray-core/transport/internet/finalmask/sudoku"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func packetMasks(t testing.TB, kind string) []*serial.TypedMessage {
	t.Helper()
	mask := serial.ToTypedMessage(&salamander.Config{Password: psk})
	switch kind {
	case "header", "header_no_batch":
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&header.Config{ID: 5})}
	case "dns", "dtls", "srtp", "utp", "wechat":
		id := map[string]int32{"dns": 0, "dtls": 1, "srtp": 2, "utp": 3, "wechat": 4}[kind]
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&header.Config{ID: id, Domain: "example.com"})}
	case "mkcp_original":
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&original.Config{})}
	case "mkcp_aes":
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&aes128gcm.Config{Password: psk})}
	case "sudoku":
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&sudoku.Config{Password: psk})}
	case "custom":
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&custom.UDPConfig{Client: []*custom.UDPItem{{Packet: []byte("cover-client")}}, Server: []*custom.UDPItem{{Packet: []byte("cover-server")}}})}
	case "stateful":
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&custom.UDPConfig{Client: []*custom.UDPItem{{Rand: 2, RandMin: 42, RandMax: 42, Save: "txid"}}, Server: []*custom.UDPItem{{Var: "txid"}}})}
	case "standalone":
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&custom.UDPStandaloneConfig{Client: []*custom.UDPItem{{Packet: []byte("cover-client")}}, Server: []*custom.UDPItem{{Packet: []byte("cover-server")}}})}
	case "noise":
		return []*serial.TypedMessage{serial.ToTypedMessage(&noise.Config{Items: []*noise.Item{{Packet: []byte("cover packet")}}}), mask}
	case "lua":
		path := filepath.Join(t.TempDir(), "noise.lua")
		if err := os.WriteFile(path, []byte(`function HandleNoise(plan, address) plan:Add("scripted cover packet") end`), 0600); err != nil {
			t.Fatal(err)
		}
		return []*serial.TypedMessage{serial.ToTypedMessage(&noise.Config{Script: path}), mask}
	case "multiple":
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&salamander.Config{Password: psk + "-outer"})}
	case "header_inside":
		return []*serial.TypedMessage{serial.ToTypedMessage(&header.Config{ID: 5}), mask}
	case "header_only":
		return []*serial.TypedMessage{serial.ToTypedMessage(&header.Config{ID: 5})}
	case "headers_stacked":
		return []*serial.TypedMessage{mask, serial.ToTypedMessage(&header.Config{ID: 1}), serial.ToTypedMessage(&header.Config{ID: 4}), serial.ToTypedMessage(&header.Config{ID: 2})}
	case "mixed_stacked":
		return []*serial.TypedMessage{serial.ToTypedMessage(&header.Config{ID: 0, Domain: `example\.name.com`}), mask, serial.ToTypedMessage(&header.Config{ID: 4}), serial.ToTypedMessage(&salamander.Config{Password: psk + "-outer"})}
	default:
		t.Fatalf("unknown packet mask %s", kind)
		return nil
	}
}

func TestNativePacketMasks(t *testing.T) {
	for _, kind := range []string{"header", "header_no_batch", "dns", "dtls", "srtp", "utp", "wechat", "mkcp_original", "mkcp_aes", "sudoku", "custom", "stateful", "standalone", "noise", "lua", "multiple", "header_inside", "header_only", "headers_stacked", "mixed_stacked"} {
		for _, serverNative := range []bool{false, true} {
			for _, clientNative := range []bool{false, true} {
				for _, bridge := range []string{"shared", "socket"} {
					if !serverNative && !clientNative && bridge == "socket" {
						continue
					}
					t.Run(fmt.Sprintf("%s/server_%t/client_%t/%s", kind, serverNative, clientNative, bridge), func(t *testing.T) {
						t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "0")
						t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
						if serverNative {
							t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
						}
						t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
						masks := packetMasks(t, kind)
						before := native.Stats().StartedEngines
						server, caPath, _ := xrayServerWithConfig(t, false, func(config *core.Config) {
							message, err := config.Inbound[0].ReceiverSettings.GetInstance()
							if err != nil {
								t.Fatal(err)
							}
							receiver := message.(*proxyman.ReceiverConfig)
							receiver.StreamSettings.Udpmasks = masks
							receiver.StreamSettings.QuicParams.DisableGSO = kind == "header_no_batch"
							config.Inbound[0].ReceiverSettings = serial.ToTypedMessage(receiver)
						})
						if serverNative && native.Stats().StartedEngines <= before {
							t.Fatal("masked listener fell back to Go QUIC")
						}
						t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
						if clientNative {
							t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
						}
						ca, err := os.ReadFile(caPath)
						if err != nil {
							t.Fatal(err)
						}
						config := embeddedClientConfig(t, server, ca, false, auth)
						message, err := config.Outbound[0].SenderSettings.GetInstance()
						if err != nil {
							t.Fatal(err)
						}
						sender := message.(*proxyman.SenderConfig)
						sender.StreamSettings.Udpmasks = masks
						sender.StreamSettings.QuicParams.DisableGSO = kind == "header_no_batch"
						config.Outbound[0].SenderSettings = serial.ToTypedMessage(sender)
						client, err := core.New(config)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { client.Close() })
						if err := client.Start(); err != nil {
							t.Fatal(err)
						}
						tcp, udp := echoTargets(t)
						before = native.Stats().StartedEngines
						for _, transfer := range []struct {
							network, address string
							size             int
						}{{"tcp", tcp, 128 << 10}, {"udp", udp, 4096}} {
							ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
							started := native.Stats().StartedEngines
							target, err := xnet.ParseDestination(transfer.network + ":" + transfer.address)
							if err != nil {
								cancel()
								t.Fatal(err)
							}
							connection, err := core.Dial(ctx, client, target)
							if err != nil {
								cancel()
								t.Fatal(err)
							}
							stop := context.AfterFunc(ctx, func() { connection.Close() })
							payload := bytes.Repeat([]byte{0x37}, transfer.size)
							if _, err := connection.Write(payload); err != nil {
								connection.Close()
								cancel()
								t.Fatal(err)
							}
							received := make([]byte, len(payload))
							_, err = io.ReadFull(connection, received)
							stop()
							connection.Close()
							cancel()
							if err != nil || !bytes.Equal(received, payload) {
								t.Fatalf("%s masked transfer failed: %v", transfer.network, err)
							}
							if clientNative && native.Stats().StartedEngines != started+1 {
								t.Fatalf("%s masked outbound fell back to Go QUIC", transfer.network)
							}
						}
						if clientNative && native.Stats().StartedEngines <= before {
							t.Fatal("masked outbound fell back to Go QUIC")
						}
					})
				}
			}
		}
	}
}
