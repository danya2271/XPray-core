//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"context"
	"encoding/pem"
	"io"
	"net"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
)

func TestNativeTLSUpdateKeepsSessionsAndChangesNewHandshakes(t *testing.T) {
	for _, bridge := range []string{"shared", "socket"} {
		t.Run(bridge, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
			certificate := func() (string, string) {
				c, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
				return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Certificate})),
					string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: c.PrivateKey}))
			}
			oldCertificate, oldKey := certificate()
			start := func(config Config, accept func(*Conn)) *Engine {
				t.Helper()
				udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer udp.Close()
				file, err := udp.File()
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				engine, err := Start(ctx, config, file, udp.LocalAddr(), nil, accept)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { engine.Close() })
				return engine
			}
			server := start(Config{Server: true, Users: []string{"test-password"}, CertificatePEM: oldCertificate, KeyPEM: oldKey}, func(c *Conn) {
				defer c.Close()
				io.Copy(c, c)
			})
			clientConfig := Config{Peer: server.local.String(), ServerName: "localhost", Auth: "test-password", CAPEM: oldCertificate}
			client := start(clientConfig, nil)
			conn, err := client.Open(context.Background(), xnet.TCPDestination(xnet.LocalHostIP, 80))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			exchange := func(c *Conn) {
				t.Helper()
				c.SetDeadline(time.Now().Add(3 * time.Second))
				if _, err := c.Write([]byte("ping")); err != nil {
					t.Fatal(err)
				}
				var got [4]byte
				if _, err := io.ReadFull(c, got[:]); err != nil || string(got[:]) != "ping" {
					t.Fatalf("echo: %v %q", err, got)
				}
			}
			exchange(conn)
			if err := server.UpdateTLS(context.Background(), "invalid certificate", oldKey); err == nil {
				t.Fatal("invalid TLS update was accepted")
			}
			exchange(conn)
			newCertificate, newKey := certificate()
			if err := server.UpdateTLS(context.Background(), newCertificate, newKey); err != nil {
				t.Fatal(err)
			}
			exchange(conn)
			// This new client trusts only the replacement certificate.
			clientConfig.CAPEM = newCertificate
			fresh := start(clientConfig, nil)
			freshConn, err := fresh.Open(context.Background(), xnet.TCPDestination(xnet.LocalHostIP, 80))
			if err != nil {
				t.Fatal(err)
			}
			defer freshConn.Close()
			exchange(freshConn)
		})
	}
}
