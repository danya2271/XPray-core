//go:build xhysteria_interop

package interop_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/pem"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/proxy/freedom"
	hyproxy "github.com/xtls/xray-core/proxy/hysteria"
	"github.com/xtls/xray-core/proxy/hysteria/account"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	hytransport "github.com/xtls/xray-core/transport/internet/hysteria"
	xtls "github.com/xtls/xray-core/transport/internet/tls"
)

const (
	auth = "native-interop-auth"
	psk  = "native-interop-salamander"
)

func TestNativeClientAgainstXray(t *testing.T) {
	binary := os.Getenv("XHYS_PROBE")
	if binary == "" {
		var err error
		binary, err = filepath.Abs("../target/debug/xhys-probe")
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("build probe first: cargo build --manifest-path native/libxhysteria/Cargo.toml --features probe --bin xhys-probe: %v", err)
	}
	tcpTarget, udpTarget := echoTargets(t)
	for _, masked := range []bool{false, true} {
		name := "plain"
		if masked {
			name = "salamander"
		}
		t.Run(name, func(t *testing.T) {
			address, ca := xrayServer(t, masked)
			run := func(t *testing.T, password, serverName, trustedCA, size string) (string, error) {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
				defer cancel()
				args := []string{"--server", address, "--server-name", serverName, "--ca", trustedCA,
					"--auth-env", "XHYS_TEST_AUTH", "--tcp", tcpTarget, "--udp", udpTarget,
					"--payload-size", size}
				if masked {
					args = append(args, "--salamander-env", "XHYS_TEST_PSK")
				}
				cmd := exec.CommandContext(ctx, binary, args...)
				cmd.Env = append(os.Environ(), "XHYS_TEST_AUTH="+password, "XHYS_TEST_PSK="+psk)
				out, err := cmd.CombinedOutput()
				if ctx.Err() != nil {
					t.Fatalf("probe did not exit: %v\n%s", ctx.Err(), out)
				}
				return string(out), err
			}
			for _, size := range []string{"1", "4096"} {
				t.Run("payload_"+size, func(t *testing.T) {
					out, err := run(t, auth, "localhost", ca, size)
					if err != nil {
						t.Fatalf("native probe failed: %v\n%s", err, out)
					}
					for _, marker := range []string{"auth_ok udp=true", "tcp_ok bytes=" + size, "udp_ok bytes=" + size} {
						if !strings.Contains(out, marker) {
							t.Fatalf("missing %q in output:\n%s", marker, out)
						}
					}
					if size == "4096" && strings.Contains(out, "sent_fragments=1") {
						t.Fatalf("large datagram did not exercise fragmentation:\n%s", out)
					}
					t.Log(strings.TrimSpace(out))
				})
			}
			t.Run("reject_wrong_password", func(t *testing.T) {
				out, err := run(t, "wrong-password", "localhost", ca, "1")
				if err == nil || !strings.Contains(out, "authentication failed: HTTP 404") || strings.Contains(out, "tcp_ok") {
					t.Fatalf("unexpected auth rejection: err=%v\n%s", err, out)
				}
			})
			t.Run("reject_wrong_server_name", func(t *testing.T) {
				out, err := run(t, auth, "wrong.example", ca, "1")
				if err == nil || strings.Contains(out, "auth_ok") {
					t.Fatalf("unverified TLS accepted: err=%v\n%s", err, out)
				}
			})
		})
	}
}

func TestXrayClientAgainstNative(t *testing.T) {
	binary := os.Getenv("XHYS_ECHO")
	if binary == "" {
		var err error
		binary, err = filepath.Abs("../target/debug/xhys-echo")
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("build echo fixture first: cargo build --manifest-path native/libxhysteria/Cargo.toml --features probe --bins: %v", err)
	}
	for _, masked := range []bool{false, true} {
		name := "plain"
		if masked {
			name = "salamander"
		}
		t.Run(name, func(t *testing.T) {
			certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
			dir := t.TempDir()
			certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
			if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate}), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: certificate.PrivateKey}), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			args := []string{"--listen", "127.0.0.1:0", "--cert", certPath, "--key", keyPath, "--auth-env", "XHYS_TEST_AUTH"}
			if masked {
				args = append(args, "--salamander-env", "XHYS_TEST_PSK")
			}
			cmd := exec.CommandContext(ctx, binary, args...)
			cmd.Env = append(os.Environ(), "XHYS_TEST_AUTH="+auth, "XHYS_TEST_PSK="+psk)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				cancel()
				_ = cmd.Wait()
				if stderr.Len() > 0 {
					t.Log(stderr.String())
				}
			})
			scanner := bufio.NewScanner(stdout)
			if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "listen=") {
				t.Fatal("native fixture did not publish its listen address")
			}
			address, err := net.ResolveUDPAddr("udp", strings.TrimPrefix(scanner.Text(), "listen="))
			if err != nil {
				t.Fatal(err)
			}
			stream := &internet.StreamConfig{
				ProtocolName: "hysteria",
				TransportSettings: []*internet.TransportConfig{{
					ProtocolName: "hysteria",
					Settings:     serial.ToTypedMessage(&hytransport.Config{Auth: auth}),
				}},
				SecurityType: serial.GetMessageType(&xtls.Config{}),
				SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&xtls.Config{
					ServerName: "localhost", NextProtocol: []string{"h3"}, DisableSystemRoot: true,
					Certificate: []*xtls.Certificate{{Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate}), Usage: xtls.Certificate_AUTHORITY_VERIFY}},
				})},
				QuicParams: &internet.QuicParams{DisableChromeParrot: true},
			}
			if masked {
				stream.Udpmasks = []*serial.TypedMessage{serial.ToTypedMessage(&salamander.Config{Password: psk})}
			}
			settings, err := internet.ToMemoryStreamConfig(stream)
			if err != nil {
				t.Fatal(err)
			}
			dest := xnet.UDPDestination(xnet.IPAddress(address.IP), xnet.Port(address.Port))
			payload := make([]byte, 4096)
			for i := range payload {
				payload[i] = byte(i % 251)
			}
			tcp, err := hytransport.Dial(ctx, dest, settings)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tcp.Close() })
			if err := tcp.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := hyproxy.WriteTCPRequest(tcp, "echo.example:443"); err != nil {
				t.Fatal(err)
			}
			if _, err := tcp.Write(payload); err != nil {
				t.Fatal(err)
			}
			ok, message, err := hyproxy.ReadTCPResponse(tcp)
			if err != nil || !ok {
				t.Fatalf("TCP response: accepted=%v message=%q err=%v", ok, message, err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(tcp, got); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("TCP echo mismatch: %v", err)
			}
			udp, err := hytransport.Dial(hytransport.ContextWithDatagram(ctx, true), dest, settings)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = udp.Close() })
			messageWire := &hyproxy.UDPMessage{
				SessionID: 0, PacketID: 17, FragCount: 1,
				Addr: "echo.example:53", Data: payload,
			}
			for _, fragment := range hyproxy.FragUDPMessage(messageWire, 1000) {
				wire := make([]byte, fragment.Size())
				if n := fragment.Serialize(wire); n != len(wire) {
					t.Fatal("UDP serialization failed")
				}
				if _, err := udp.Write(wire); err != nil {
					t.Fatal(err)
				}
			}
			reply := make(chan error, 1)
			go func() {
				var defrag hyproxy.Defragger
				buffer := make([]byte, 65536)
				for {
					n, err := udp.Read(buffer)
					if err != nil {
						reply <- err
						return
					}
					message, err := hyproxy.ParseUDPMessage(append([]byte(nil), buffer[:n]...))
					if err != nil {
						reply <- err
						return
					}
					if complete := defrag.Feed(message); complete != nil {
						if !bytes.Equal(complete.Data, payload) || complete.Addr != "echo.example:53" {
							reply <- io.ErrUnexpectedEOF
						} else {
							reply <- nil
						}
						return
					}
				}
			}()
			select {
			case err := <-reply:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(15 * time.Second):
				_ = udp.Close()
				t.Fatal("UDP echo timed out")
			}
			t.Log("Go-to-Rust TCP and fragmented UDP passed")
		})
	}
}

func xrayServer(t testing.TB, masked bool) (string, string) {
	t.Helper()
	address, ca, _ := xrayServerWithConfig(t, masked, nil)
	return address, ca
}

func xrayServerWithConfig(t testing.TB, masked bool, modify func(*core.Config)) (string, string, *core.Instance) {
	t.Helper()
	certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate}), 0600); err != nil {
		t.Fatal(err)
	}
	reservation, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reservation.LocalAddr().(*net.UDPAddr).Port
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	serverCertificate := xtls.ParseCertificate(certificate)
	serverCertificate.OneTimeLoading = true
	stream := &internet.StreamConfig{
		ProtocolName: "hysteria",
		TransportSettings: []*internet.TransportConfig{{
			ProtocolName: "hysteria",
			Settings:     serial.ToTypedMessage(&hytransport.Config{Auth: auth, UdpIdleTimeout: 60}),
		}},
		SecurityType: serial.GetMessageType(&xtls.Config{}),
		SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&xtls.Config{
			Certificate: []*xtls.Certificate{serverCertificate}, NextProtocol: []string{"h3"},
		})},
		QuicParams: prototypeQuicParams(),
	}
	if masked {
		stream.Udpmasks = []*serial.TypedMessage{serial.ToTypedMessage(&salamander.Config{Password: psk})}
	}
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			Tag: "hysteria-test",
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xnet.PortList{Range: []*xnet.PortRange{xnet.SinglePortRange(xnet.Port(port))}},
				Listen:   xnet.NewIPOrDomain(xnet.LocalHostIP), StreamSettings: stream,
			}),
			ProxySettings: serial.ToTypedMessage(&hyproxy.ServerConfig{Users: []*protocol.User{{
				Account: serial.ToTypedMessage(&account.Account{Auth: auth}),
			}}}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{ProxySettings: serial.ToTypedMessage(&freedom.Config{
			FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
		})}},
	}
	if modify != nil {
		modify(config)
	}
	instance, err := core.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := instance.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), caPath, instance
}

func prototypeQuicParams() *internet.QuicParams {
	q := &internet.QuicParams{DisableChromeParrot: true}
	if os.Getenv("XHYS_TEST_BRUTAL") == "1" {
		q.Congestion = "brutal"
		q.BrutalUp = 1_000_000
		q.BrutalDown = 2_000_000
	}
	return q
}

func echoTargets(t testing.TB) (string, string) {
	t.Helper()
	tcp, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tcp.Close() })
	go func() {
		for {
			conn, err := tcp.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = udp.Close() })
	go func() {
		buffer := make([]byte, 65536)
		for {
			n, address, err := udp.ReadFrom(buffer)
			if err != nil {
				return
			}
			_, _ = udp.WriteTo(buffer[:n], address)
		}
	}()
	return tcp.Addr().String(), udp.LocalAddr().String()
}
