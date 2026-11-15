//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func TestNativePoolReusesSessionsAndStopsWithInstance(t *testing.T) {
	for _, bridge := range []string{"shared", "socket"} {
		t.Run(bridge, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
			server, caPath := xrayServer(t, true)
			ca, err := os.ReadFile(caPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
			t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
			t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "2")
			before := native.Stats()
			client := embeddedClient(t, server, ca, true)
			tcp, udp := echoTargets(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var connections []net.Conn
			for i := range 12 {
				address := "tcp:" + tcp
				if i%3 == 0 {
					address = "udp:" + udp
				}
				target, _ := xnet.ParseDestination(address)
				conn, err := core.Dial(ctx, client, target)
				if err != nil {
					t.Fatal(err)
				}
				connections = append(connections, conn)
				defer conn.Close()
				payload := bytes.Repeat([]byte{byte(i)}, 4096)
				if _, err := conn.Write(payload); err != nil {
					t.Fatal(err)
				}
				got := make([]byte, len(payload))
				if _, err := io.ReadFull(conn, got); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatal("pooled payload mismatch")
				}
			}
			stats := native.Stats()
			if stats.StartedEngines-before.StartedEngines != 2 {
				t.Fatalf("expected two native engines for 12 live flows: before=%+v after=%+v", before, stats)
			}
			// Releasing one stream cannot close either multiplexed session.
			connections[0].Close()
			payload := []byte("other streams stay alive")
			connections[1].Write(payload)
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(connections[1], got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(payload, got) {
				t.Fatal("stream close affected another pooled flow")
			}
			for _, conn := range connections {
				conn.Close()
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			until := time.Now().Add(3 * time.Second)
			for native.Stats().LiveEngines > before.LiveEngines && time.Now().Before(until) {
				time.Sleep(20 * time.Millisecond)
			}
			if native.Stats().LiveEngines > before.LiveEngines {
				t.Fatalf("native pool survived closed instance: %+v", native.Stats())
			}
		})
	}
}

func TestNativePoolBrutalDoesNotMultiplyFixedRates(t *testing.T) {
	t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
	server, caPath := xrayServer(t, false)
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
	t.Setenv("XHYS_TEST_BRUTAL", "1")
	t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "4")
	before := native.Stats().StartedEngines
	client := embeddedClient(t, server, ca, false)
	tcp, _ := echoTargets(t)
	target, _ := xnet.ParseDestination("tcp:" + tcp)
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := core.Dial(ctx, client, target)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			if _, err := conn.Write([]byte("test")); err != nil {
				t.Error(err)
				return
			}
			if _, err := io.ReadFull(conn, make([]byte, 4)); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if native.Stats().StartedEngines-before != 1 {
		t.Fatal("fixed-rate Brutal created multiple pooled sessions")
	}
}
