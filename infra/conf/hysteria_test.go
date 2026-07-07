package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	. "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/hysteria"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"google.golang.org/protobuf/proto"
)

func TestHysteria2Aliases(t *testing.T) {
	input := `{
		"inbounds": [{
			"protocol": "hy2",
			"port": 443,
			"settings": {
				"users": [{
					"auth": "secret"
				}]
			},
			"streamSettings": {
				"network": "hysteria2",
				"hysteria2Settings": {
					"auth": "secret"
				}
			}
		}],
		"outbounds": [{
			"protocol": "hysteria2",
			"settings": {
				"address": "example.com",
				"port": 443
			},
			"streamSettings": {
				"network": "hy2",
				"hy2Settings": {
					"auth": "secret"
				}
			}
		}]
	}`

	var config Config
	if err := json.Unmarshal([]byte(input), &config); err != nil {
		t.Fatal(err)
	}
	built, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}

	if len(built.Inbound) != 1 {
		t.Fatalf("expected 1 inbound, got %d", len(built.Inbound))
	}
	receiver := new(proxyman.ReceiverConfig)
	if err := proto.Unmarshal(built.Inbound[0].ReceiverSettings.Value, receiver); err != nil {
		t.Fatal(err)
	}
	if receiver.StreamSettings.GetProtocolName() != "hysteria" {
		t.Fatalf("unexpected inbound protocol: %s", receiver.StreamSettings.GetProtocolName())
	}
	if len(receiver.StreamSettings.TransportSettings) != 1 || receiver.StreamSettings.TransportSettings[0].ProtocolName != "hysteria" {
		t.Fatalf("unexpected inbound transport settings: %+v", receiver.StreamSettings.TransportSettings)
	}
	inboundProxy, err := built.Inbound[0].ProxySettings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := inboundProxy.(*hysteria.ServerConfig); !ok {
		t.Fatalf("unexpected inbound proxy config: %T", inboundProxy)
	}

	if len(built.Outbound) != 1 {
		t.Fatalf("expected 1 outbound, got %d", len(built.Outbound))
	}
	sender := new(proxyman.SenderConfig)
	if err := proto.Unmarshal(built.Outbound[0].SenderSettings.Value, sender); err != nil {
		t.Fatal(err)
	}
	if sender.StreamSettings.GetProtocolName() != "hysteria" {
		t.Fatalf("unexpected outbound protocol: %s", sender.StreamSettings.GetProtocolName())
	}
	if len(sender.StreamSettings.TransportSettings) != 1 || sender.StreamSettings.TransportSettings[0].ProtocolName != "hysteria" {
		t.Fatalf("unexpected outbound transport settings: %+v", sender.StreamSettings.TransportSettings)
	}
	outboundProxy, err := built.Outbound[0].ProxySettings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := outboundProxy.(*hysteria.ClientConfig); !ok {
		t.Fatalf("unexpected outbound proxy config: %T", outboundProxy)
	}
}

func TestHysteria2FinalMaskConfig(t *testing.T) {
	input := `{
		"outbounds": [{
			"protocol": "hysteria2",
			"settings": {
				"address": "91.218.245.151",
				"port": 8594
			},
			"streamSettings": {
				"network": "hy2",
				"security": "tls",
				"hysteria2Settings": {
					"auth": "4b01278a-1240-309e-bb6f-8a9330361ae1"
				},
				"tlsSettings": {
					"serverName": "ab.proxvless.icu",
					"alpn": ["h3"]
				},
				"finalmask": {
					"udp": [{
						"type": "salamander",
						"settings": {
							"password": "05f8a43433935932"
						}
					}],
					"quicParams": {
						"congestion": "bbr"
					}
				}
			}
		}]
	}`

	var config Config
	if err := json.Unmarshal([]byte(input), &config); err != nil {
		t.Fatal(err)
	}
	built, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}

	if len(built.Outbound) != 1 {
		t.Fatalf("expected 1 outbound, got %d", len(built.Outbound))
	}
	sender := new(proxyman.SenderConfig)
	if err := proto.Unmarshal(built.Outbound[0].SenderSettings.Value, sender); err != nil {
		t.Fatal(err)
	}
	if sender.StreamSettings == nil {
		t.Fatal("expected stream settings")
	}
	if len(sender.StreamSettings.Udpmasks) != 1 {
		t.Fatalf("expected 1 UDP mask, got %d", len(sender.StreamSettings.Udpmasks))
	}
	mask, err := sender.StreamSettings.Udpmasks[0].GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	salamanderMask, ok := mask.(*salamander.Config)
	if !ok {
		t.Fatalf("unexpected UDP mask config: %T", mask)
	}
	if salamanderMask.Password != "05f8a43433935932" {
		t.Fatalf("unexpected salamander password: %q", salamanderMask.Password)
	}
	if sender.StreamSettings.QuicParams == nil || sender.StreamSettings.QuicParams.Congestion != "bbr" {
		t.Fatalf("unexpected QUIC params: %+v", sender.StreamSettings.QuicParams)
	}
}
