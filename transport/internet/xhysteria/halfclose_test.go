//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"bytes"
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
)

func TestNativeHalfClosePreservesRequestAndCompleteResponse(t *testing.T) {
	for _, mode := range []struct {
		name, bridge string
		sendBudget   uint32
	}{
		{"shared", "shared", 0}, {"socket", "socket", 0},
		{"shared_small_budget", "shared", maxFrame}, {"socket_small_budget", "socket", maxFrame},
	} {
		t.Run(mode.name, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_BRIDGE", mode.bridge)
			certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
			ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate}))
			key := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: certificate.PrivateKey}))
			start := func(config Config, accept func(*Conn)) *Engine {
				t.Helper()
				config.SendBufferBytes = mode.sendBudget
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
				engine, err := Start(context.Background(), config, file, udp.LocalAddr(), nil, accept)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { engine.Close() })
				return engine
			}
			payload := bytes.Repeat([]byte("request-with-FIN"), 16384)
			response := bytes.Repeat([]byte("complete-response"), 16384)
			served := make(chan error, 1)
			server := start(Config{Server: true, Users: []string{"test-password"}, CertificatePEM: ca, KeyPEM: key}, func(c *Conn) {
				defer c.Close()
				c.SetDeadline(time.Now().Add(5 * time.Second))
				got, err := io.ReadAll(c)
				if err == nil && !bytes.Equal(got, payload) {
					err = fmt.Errorf("request changed or FIN discarded data")
				}
				if err == nil {
					_, err = c.Write(response)
				}
				if err == nil {
					err = c.CloseWrite()
				}
				served <- err
			})
			client := start(Config{Peer: server.local.String(), ServerName: "localhost", Auth: "test-password", CAPEM: ca}, nil)
			conn, err := client.Open(context.Background(), xnet.TCPDestination(xnet.LocalHostIP, 80))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMultiBuffer(buf.MergeBytes(nil, payload)); err != nil {
				t.Fatal(err)
			}
			if err := conn.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(conn)
			if err != nil || !bytes.Equal(got, response) {
				t.Fatalf("response after half-close: bytes=%d want=%d err=%v", len(got), len(response), err)
			}
			if err := <-served; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeLeasedBuffersRemainReadableAfterEngineJoin(t *testing.T) {
	t.Setenv("XRAY_HYSTERIA_BRIDGE", "shared")
	certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate}))
	key := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: certificate.PrivateKey}))
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
		engine, err := Start(context.Background(), config, file, udp.LocalAddr(), nil, accept)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { engine.Close() })
		return engine
	}
	payload := bytes.Repeat([]byte("leased-buffer"), 17)
	server := start(Config{Server: true, Users: []string{"test-password"}, CertificatePEM: ca, KeyPEM: key}, func(connection *Conn) {
		defer connection.Close()
		connection.Write(payload)
		connection.CloseWrite()
	})
	client := start(Config{Peer: server.local.String(), ServerName: "localhost", Auth: "test-password", CAPEM: ca}, nil)
	connection, err := client.Open(context.Background(), xnet.TCPDestination(xnet.LocalHostIP, 80))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	var held buf.MultiBuffer
	defer func() { buf.ReleaseMulti(held) }()
	var received []byte
	for len(received) < len(payload) {
		parts, err := connection.ReadMultiBuffer()
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, parts...)
		for _, part := range parts {
			received = append(received, part.Bytes()...)
		}
	}
	bridge := client.bridge.(*sharedBridge)
	if bridge.owner.refs.Load() <= 1 {
		t.Fatal("native receive did not retain a leased arena")
	}
	client.Close()
	server.Close()
	if bridge.owner.refs.Load() < 1 || client.handle != nil {
		t.Fatal("engine join freed outstanding leases")
	}
	var after []byte
	for _, part := range held {
		after = append(after, part.Bytes()...)
	}
	if !bytes.Equal(after, payload) {
		t.Fatal("payload changed after native engine join")
	}
	buf.ReleaseMulti(held)
	held = nil
	if bridge.owner.refs.Load() != 0 {
		t.Fatal("arena ownership leaked after buffer release")
	}
}
