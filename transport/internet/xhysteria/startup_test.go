//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"context"
	"encoding/pem"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/protocol/tls/cert"
)

func TestStartupReportsNativeErrorsAndCancellation(t *testing.T) {
	for _, bridge := range []string{"shared", "socket"} {
		t.Run(bridge, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
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
			start := func(ctx context.Context, config Config) (*Engine, error) {
				return Start(ctx, config, file, udp.LocalAddr(), nil, nil)
			}
			// Fail before the event pump exists: the shared bridge must retain
			// the native error even when the worker immediately closes it.
			if e, err := start(context.Background(), Config{Peer: "127.0.0.1:1", ServerName: "localhost"}); err == nil || !strings.Contains(err.Error(), "no trusted certificate roots") {
				if e != nil {
					e.Close()
				}
				t.Fatalf("lost setup error: %v", err)
			}
			certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
			config := Config{Peer: udp.LocalAddr().String(), ServerName: "localhost", CAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate}))}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if e, err := start(ctx, config); !errors.Is(err, context.DeadlineExceeded) {
				if e != nil {
					e.Close()
				}
				t.Fatalf("lost caller deadline: %v", err)
			}
			t.Run("native_timeout", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				if e, err := start(ctx, config); err == nil || !strings.Contains(err.Error(), "native engine:") || !strings.Contains(err.Error(), "timed out") {
					if e != nil {
						e.Close()
					}
					t.Fatalf("lost native handshake timeout: %v", err)
				}
			})
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
			if _, err := start(ctx, config); !errors.Is(err, context.Canceled) {
				t.Fatalf("lost caller cancellation: %v", err)
			}
		})
	}
}

func TestInvalidWorkerOverrideFailsBeforeNativeInitialization(t *testing.T) {
	for _, value := range []string{"-1", "17", "invalid"} {
		t.Setenv("XRAY_HYSTERIA_SERVER_WORKERS", value)
		if _, err := Start(context.Background(), Config{Server: true}, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "SERVER_WORKERS") {
			t.Fatalf("invalid worker override %q: %v", value, err)
		}
	}
}
