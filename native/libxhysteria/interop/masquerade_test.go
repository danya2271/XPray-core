//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"bytes"
	"context"
	cryptotls "crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/app/proxyman"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	hytransport "github.com/xtls/xray-core/transport/internet/hysteria"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func TestEmbeddedNativeHTTPMasquerade(t *testing.T) {
	for _, backend := range []string{"go", "native"} {
		for _, masked := range []bool{false, true} {
			for _, kind := range []string{"404", "string", "custom_status", "no_content", "not_modified", "large_body"} {
				t.Run(fmt.Sprintf("%s/salamander_%t/%s", backend, masked, kind), func(t *testing.T) {
					t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
					if backend == "go" {
						t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
					}
					config := &hytransport.Config{Auth: auth, UdpIdleTimeout: 30, MasqType: "string", MasqString: "<html><body>cover site</body></html>", MasqStringHeaders: map[string]string{"X-Cover-Site": "available"}}
					status := 200
					body := config.MasqString
					switch kind {
					case "404":
						config.MasqType = "404"
						status, body = 404, "404 page not found\n"
					case "custom_status":
						config.MasqStringStatusCode, status = 201, 201
					case "no_content":
						config.MasqStringStatusCode, status, body = 204, 204, ""
					case "not_modified":
						config.MasqStringStatusCode, status, body = 304, 304, ""
					case "large_body":
						config.MasqString = string(bytes.Repeat([]byte("cover"), 128*1024))
						config.MasqStringHeaders["Content-Type"] = "text/plain"
						body = config.MasqString
					}
					before := native.Stats().StartedEngines
					server, caPath, _ := xrayServerWithConfig(t, masked, func(serverConfig *core.Config) {
						message, err := serverConfig.Inbound[0].ReceiverSettings.GetInstance()
						if err != nil {
							t.Fatal(err)
						}
						receiver := message.(*proxyman.ReceiverConfig)
						receiver.StreamSettings.TransportSettings[0].Settings = serial.ToTypedMessage(config)
						serverConfig.Inbound[0].ReceiverSettings = serial.ToTypedMessage(receiver)
					})
					if backend == "native" && native.Stats().StartedEngines <= before {
						t.Fatal("masquerade did not start a native server")
					}
					ca, err := os.ReadFile(caPath)
					if err != nil {
						t.Fatal(err)
					}
					roots := x509.NewCertPool()
					if !roots.AppendCertsFromPEM(ca) {
						t.Fatal("invalid masquerade certificate")
					}
					socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					if masked {
						mask, err := salamander.NewSalamanderObfuscator([]byte(psk))
						if err != nil {
							socket.Close()
							t.Fatal(err)
						}
						socket = &capacityMaskConn{PacketConn: socket, mask: mask}
					}
					transport := &quic.Transport{Conn: socket}
					defer func() { transport.Close(); socket.Close() }()
					address, err := net.ResolveUDPAddr("udp", server)
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					connection, err := transport.Dial(ctx, address, &cryptotls.Config{RootCAs: roots, ServerName: "localhost", NextProtos: []string{"h3"}}, &quic.Config{EnableDatagrams: true, DisablePathManager: true})
					if err != nil {
						t.Fatal(err)
					}
					defer connection.CloseWithError(0, "masquerade test complete")
					httpTransport := &http3.Transport{}
					defer httpTransport.Close()
					client := httpTransport.NewClientConn(connection)
					for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
						request, err := http.NewRequestWithContext(ctx, method, "https://cover.example/index.html", nil)
						if err != nil {
							t.Fatal(err)
						}
						response, err := client.RoundTrip(request)
						if err != nil {
							t.Fatal(err)
						}
						received, err := io.ReadAll(response.Body)
						response.Body.Close()
						if err != nil {
							t.Fatal(err)
						}
						expected := body
						if method == http.MethodHead {
							expected = ""
						}
						if response.StatusCode != status || string(received) != expected {
							t.Fatalf("%s masquerade response status=%d bytes=%d, want status=%d bytes=%d", method, response.StatusCode, len(received), status, len(expected))
						}
						if _, err := http.ParseTime(response.Header.Get("Date")); err != nil {
							t.Fatalf("missing/invalid masquerade Date: %v", err)
						}
						if kind == "404" {
							if response.Header.Get("X-Content-Type-Options") != "nosniff" {
								t.Fatal("missing NotFound security header")
							}
						} else if response.Header.Get("X-Cover-Site") != "available" {
							t.Fatal("custom masquerade header lost")
						}
						if method != http.MethodHead {
							contentType := "text/html; charset=utf-8"
							switch kind {
							case "404":
								contentType = "text/plain; charset=utf-8"
							case "large_body":
								contentType = "text/plain"
							case "no_content", "not_modified":
								contentType = ""
							}
							if response.Header.Get("Content-Type") != contentType {
								t.Fatalf("%s masquerade Content-Type=%q, want %q", method, response.Header.Get("Content-Type"), contentType)
							}
						}
					}
					for _, password := range []string{"incorrect", auth} {
						request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://hysteria/auth", nil)
						if err != nil {
							t.Fatal(err)
						}
						request.Header.Set("Hysteria-Auth", password)
						request.Header.Set("Hysteria-CC-RX", "0")
						response, err := client.RoundTrip(request)
						if err != nil {
							t.Fatal(err)
						}
						received, err := io.ReadAll(response.Body)
						response.Body.Close()
						if err != nil {
							t.Fatal(err)
						}
						if password == auth {
							if response.StatusCode != 233 || response.Header.Get("Hysteria-UDP") != "true" || len(received) != 0 {
								t.Fatal("HTTP masquerade intercepted valid Hysteria authentication")
							}
						} else if response.StatusCode != status || string(received) != body {
							t.Fatal("invalid authentication did not return the masquerade response")
						}
					}
					proxy := embeddedClient(t, server, ca, masked)
					tcp, udp := echoTargets(t)
					for _, targetAddress := range []string{"tcp:" + tcp, "udp:" + udp} {
						target, err := xnet.ParseDestination(targetAddress)
						if err != nil {
							t.Fatal(err)
						}
						flow, err := core.Dial(ctx, proxy, target)
						if err != nil {
							t.Fatal(err)
						}
						defer flow.Close()
						stop := context.AfterFunc(ctx, func() { flow.Close() })
						defer stop()
						payload := bytes.Repeat([]byte("authenticated"), 256)
						if _, err := flow.Write(payload); err != nil {
							t.Fatal(err)
						}
						received := make([]byte, len(payload))
						if _, err := io.ReadFull(flow, received); err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(received, payload) {
							t.Fatal("masquerade changed authenticated proxy data")
						}
					}
				})
			}
		}
	}
}
