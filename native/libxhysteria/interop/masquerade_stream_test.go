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
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	hytransport "github.com/xtls/xray-core/transport/internet/hysteria"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func masqueradeServer(t *testing.T, config *hytransport.Config, masked bool) (string, string) {
	t.Helper()
	before := native.Stats().StartedEngines
	server, ca, _ := xrayServerWithConfig(t, masked, func(serverConfig *core.Config) {
		message, err := serverConfig.Inbound[0].ReceiverSettings.GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		receiver := message.(*proxyman.ReceiverConfig)
		receiver.StreamSettings.TransportSettings[0].Settings = serial.ToTypedMessage(config)
		serverConfig.Inbound[0].ReceiverSettings = serial.ToTypedMessage(receiver)
	})
	if os.Getenv("XRAY_HYSTERIA_NATIVE") == "1" && native.Stats().StartedEngines <= before {
		t.Fatal("HTTP masquerade did not start a native engine")
	}
	return server, ca
}

func masqueradeHTTPClient(t *testing.T, server, caPath string, masked bool) *http3.ClientConn {
	t.Helper()
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid HTTP masquerade certificate")
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
	t.Cleanup(func() { transport.Close(); socket.Close() })
	address, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := transport.Dial(ctx, address, &cryptotls.Config{RootCAs: roots, ServerName: "localhost", NextProtos: []string{"h3"}}, &quic.Config{EnableDatagrams: true, DisablePathManager: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.CloseWithError(0, "HTTP masquerade test complete") })
	httpTransport := &http3.Transport{}
	t.Cleanup(func() { httpTransport.Close() })
	return httpTransport.NewClientConn(connection)
}

func masqueradeRoundTrip(t *testing.T, client *http3.ClientConn, method, path string, payload io.Reader, headers http.Header) (*http.Response, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, "https://cover.example"+path, payload)
	if err != nil {
		t.Fatal(err)
	}
	request.Header = headers
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return response, body
}

func masqueradeAuthenticate(t *testing.T, client *http3.ClientConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://hysteria/auth", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Hysteria-Auth", auth)
	request.Header.Set("Hysteria-CC-RX", "0")
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 233 || response.Header.Get("Hysteria-UDP") != "true" {
		t.Fatalf("HTTP cover handler intercepted Hysteria auth: %v", response.Status)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddedHTTPFileMasquerade(t *testing.T) {
	for _, backend := range []string{"go", "native"} {
		for _, masked := range []bool{false, true} {
			for _, bridge := range []string{"shared", "socket"} {
				t.Run(fmt.Sprintf("%s/salamander_%t/%s", backend, masked, bridge), func(t *testing.T) {
					t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
					if backend == "go" {
						t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
					}
					t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
					directory := t.TempDir()
					payload := bytes.Repeat([]byte("streamed native masquerade file\n"), 65536)
					if err := os.WriteFile(filepath.Join(directory, "large.txt"), payload, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(filepath.Join(directory, "site"), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(directory, "site", "index.html"), []byte("<html>cover site</html>"), 0600); err != nil {
						t.Fatal(err)
					}
					server, ca := masqueradeServer(t, &hytransport.Config{Auth: auth, MasqType: "file", MasqFile: directory}, masked)
					client := masqueradeHTTPClient(t, server, ca, masked)
					response, body := masqueradeRoundTrip(t, client, http.MethodGet, "/large.txt", nil, nil)
					if response.StatusCode != 200 || !bytes.Equal(body, payload) {
						t.Fatalf("file response status=%d bytes=%d", response.StatusCode, len(body))
					}
					response, body = masqueradeRoundTrip(t, client, http.MethodHead, "/large.txt", nil, nil)
					if response.StatusCode != 200 || len(body) != 0 || response.ContentLength != int64(len(payload)) {
						t.Fatal("file HEAD mismatch")
					}
					response, body = masqueradeRoundTrip(t, client, http.MethodGet, "/large.txt", nil, http.Header{"Range": {"bytes=1024-4095"}})
					if response.StatusCode != 206 || !bytes.Equal(body, payload[1024:4096]) || response.Header.Get("Content-Range") == "" {
						t.Fatal("file Range mismatch")
					}
					response, body = masqueradeRoundTrip(t, client, http.MethodGet, "/large.txt", nil, http.Header{"If-Modified-Since": {response.Header.Get("Last-Modified")}})
					if response.StatusCode != 304 || len(body) != 0 {
						t.Fatal("file conditional request mismatch")
					}
					response, _ = masqueradeRoundTrip(t, client, http.MethodGet, "/site", nil, nil)
					if response.StatusCode != 301 || response.Header.Get("Location") != "site/" {
						t.Fatal("file directory redirect mismatch")
					}
					response, body = masqueradeRoundTrip(t, client, http.MethodGet, "/site/", nil, nil)
					if response.StatusCode != 200 || string(body) != "<html>cover site</html>" {
						t.Fatal("file index mismatch")
					}
					response, _ = masqueradeRoundTrip(t, client, http.MethodGet, "/missing", nil, nil)
					if response.StatusCode != 404 {
						t.Fatal("file missing path mismatch")
					}
					response, body = masqueradeRoundTrip(t, client, http.MethodPost, "/auth", nil, http.Header{"Hysteria-Auth": {auth}, "Hysteria-Cc-Rx": {"0"}})
					if response.StatusCode == 233 {
						t.Fatal("cover host was mistaken for Hysteria authentication")
					}
					masqueradeAuthenticate(t, client)
				})
			}
		}
	}
}

func TestEmbeddedHTTPProxyMasquerade(t *testing.T) {
	for _, backend := range []string{"go", "native"} {
		for _, upstreamKind := range []string{"http", "https", "unix"} {
			for _, forwarded := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/forwarded_%t", backend, upstreamKind, forwarded), func(t *testing.T) {
					t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
					if backend == "go" {
						t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
					}
					handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						writer.Header().Set("X-Upstream-Host", request.Host)
						writer.Header().Set("X-Upstream-Path", request.URL.RequestURI())
						writer.Header().Set("X-Forwarded-For-Seen", request.Header.Get("X-Forwarded-For"))
						writer.Header().Set("X-Forwarded-Proto-Seen", request.Header.Get("X-Forwarded-Proto"))
						writer.Header().Add("Set-Cookie", "first=1")
						writer.Header().Add("Set-Cookie", "second=2")
						writer.Header().Set("Trailer", "X-Transfer-Complete")
						writer.WriteHeader(201)
						if request.Body != nil {
							io.Copy(writer, request.Body)
						}
						writer.Header().Set("X-Transfer-Complete", "yes")
					})
					var target string
					switch upstreamKind {
					case "http":
						upstream := httptest.NewServer(handler)
						defer upstream.Close()
						target = upstream.URL
					case "https":
						upstream := httptest.NewTLSServer(handler)
						defer upstream.Close()
						target = upstream.URL
					case "unix":
						path := filepath.Join(t.TempDir(), "cover.sock")
						listener, err := net.Listen("unix", path)
						if err != nil {
							t.Fatal(err)
						}
						upstream := &http.Server{Handler: handler}
						go upstream.Serve(listener)
						defer upstream.Close()
						target = "unix:" + path
					}
					server, ca := masqueradeServer(t, &hytransport.Config{Auth: auth, MasqType: "proxy", MasqUrl: target, MasqUrlInsecure: upstreamKind == "https", MasqUrlRewriteHost: forwarded, MasqUrlXForwarded: forwarded}, true)
					client := masqueradeHTTPClient(t, server, ca, true)
					payload := bytes.Repeat([]byte("streaming request and response\n"), 65536)
					response, body := masqueradeRoundTrip(t, client, http.MethodPost, "/upload?sample=1", bytes.NewReader(payload), nil)
					if response.StatusCode != 201 || !bytes.Equal(body, payload) {
						t.Fatalf("proxy response status=%d bytes=%d, want %d", response.StatusCode, len(body), len(payload))
					}
					if len(response.Header.Values("Set-Cookie")) != 2 || response.Trailer.Get("X-Transfer-Complete") != "yes" {
						t.Fatalf("proxy multivalue headers/trailer lost: %v %v", response.Header, response.Trailer)
					}
					if response.Header.Get("X-Upstream-Path") != "/upload?sample=1" {
						t.Fatal("proxy path/query changed")
					}
					if forwarded {
						if response.Header.Get("X-Upstream-Host") == "cover.example" || response.Header.Get("X-Forwarded-For-Seen") == "" || response.Header.Get("X-Forwarded-Proto-Seen") != "https" {
							t.Fatal("proxy forwarding settings lost")
						}
					} else if response.Header.Get("X-Upstream-Host") != "cover.example" || response.Header.Get("X-Forwarded-For-Seen") != "" {
						t.Fatal("proxy preserved-host settings lost")
					}
					response, body = masqueradeRoundTrip(t, client, http.MethodHead, "/head", nil, nil)
					if response.StatusCode != 201 || len(body) != 0 {
						t.Fatal("proxy HEAD mismatch")
					}
					if strings.Contains(response.Header.Get("X-Upstream-Host"), "unix:") {
						t.Fatal("Unix path leaked into HTTP Host")
					}
					masqueradeAuthenticate(t, client)
				})
			}
		}
	}
}

func TestEmbeddedHTTPProxyLifecycle(t *testing.T) {
	for _, backend := range []string{"go", "native"} {
		for _, scenario := range []string{"trailers", "cancel", "early_response", "tls_verify", "header_limit"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
				if backend == "go" {
					t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
				}
				entered := make(chan struct{}, 1)
				cancelled := make(chan struct{}, 1)
				handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					if scenario == "header_limit" {
						writer.Header().Set("X-Large", strings.Repeat("a", 40<<10))
						writer.WriteHeader(http.StatusOK)
						return
					}
					if scenario == "cancel" {
						entered <- struct{}{}
						<-request.Context().Done()
						cancelled <- struct{}{}
						return
					}
					if scenario == "early_response" {
						if err := http.NewResponseController(writer).EnableFullDuplex(); err != nil {
							t.Error(err)
						}
						writer.Header().Set("Content-Length", "6")
						writer.WriteHeader(http.StatusForbidden)
						writer.Write([]byte("denied"))
						return
					}
					payload, err := io.ReadAll(request.Body)
					if err != nil {
						t.Error(err)
						return
					}
					writer.Header().Set("Link", "</cover.css>; rel=preload")
					writer.WriteHeader(http.StatusEarlyHints)
					writer.Header().Set("X-Request-Trailer", request.Trailer.Get("X-Upload-Complete"))
					writer.Write(payload)
				})
				var upstream *httptest.Server
				if scenario == "tls_verify" {
					upstream = httptest.NewTLSServer(handler)
				} else {
					upstream = httptest.NewServer(handler)
				}
				defer upstream.Close()
				server, ca := masqueradeServer(t, &hytransport.Config{Auth: auth, MasqType: "proxy", MasqUrl: upstream.URL}, true)
				client := masqueradeHTTPClient(t, server, ca, true)
				if scenario == "tls_verify" || scenario == "header_limit" {
					response, _ := masqueradeRoundTrip(t, client, http.MethodGet, "/", nil, nil)
					status := http.StatusBadGateway
					if scenario == "header_limit" && backend == "go" {
						status = http.StatusOK
						if len(response.Header.Get("X-Large")) != 40<<10 {
							t.Fatal("Go baseline response header changed")
						}
					}
					if response.StatusCode != status {
						t.Fatalf("%s status=%d, want %d", scenario, response.StatusCode, status)
					}
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if scenario == "cancel" {
					request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://cover.example/wait", nil)
					if err != nil {
						t.Fatal(err)
					}
					result := make(chan error, 1)
					go func() {
						response, err := client.RoundTrip(request)
						if response != nil {
							response.Body.Close()
						}
						result <- err
					}()
					select {
					case <-entered:
					case <-ctx.Done():
						t.Fatal("upstream request did not start")
					}
					cancel()
					select {
					case err := <-result:
						if err == nil {
							t.Fatal("cancelled request succeeded")
						}
					case <-time.After(2 * time.Second):
						t.Fatal("client cancellation stalled")
					}
					select {
					case <-cancelled:
					case <-time.After(2 * time.Second):
						t.Fatal("client cancellation did not stop upstream promptly")
					}
					return
				}
				var reader io.Reader = strings.NewReader("upload with trailers")
				if scenario == "early_response" {
					readPipe, writePipe := io.Pipe()
					defer writePipe.Close()
					defer readPipe.Close()
					reader = readPipe
				}
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://cover.example/upload", reader)
				if err != nil {
					t.Fatal(err)
				}
				request.ContentLength = -1
				informational := make(chan int, 2)
				if scenario == "trailers" {
					request.Trailer = http.Header{"X-Upload-Complete": {"yes"}}
					request = request.WithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{Got1xxResponse: func(code int, _ textproto.MIMEHeader) error { informational <- code; return nil }}))
				}
				response, err := client.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				payload, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "early_response" {
					if response.StatusCode != http.StatusForbidden || string(payload) != "denied" {
						t.Fatal("early response waited for request body completion")
					}
				} else {
					if response.StatusCode != 200 || string(payload) != "upload with trailers" {
						t.Fatalf("proxy body changed: %v body=%q", response.Status, payload)
					}
					if backend == "native" && response.Header.Get("X-Request-Trailer") != "yes" {
						t.Fatalf("request trailer lost: %v body=%q", response.Header, payload)
					}
					if backend == "go" && response.Header.Get("X-Request-Trailer") != "yes" {
						t.Log("existing quic-go server replaces the announced trailer map, losing trailers in ReverseProxy")
					}
					select {
					case code := <-informational:
						if code != http.StatusEarlyHints {
							t.Fatal("informational HTTP status changed")
						}
					default:
						t.Fatal("informational HTTP response lost")
					}
				}
			})
		}
	}
}
