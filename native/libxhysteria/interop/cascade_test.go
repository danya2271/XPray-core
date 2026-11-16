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

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet/finalmask/udphop"
)

func TestNativeCascadeTransfersSurvivePortHopping(t *testing.T) {
	for _, bridge := range []string{"shared", "socket"} {
		t.Run(bridge, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
			t.Setenv("XRAY_HYSTERIA_BRIDGE", bridge)
			t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "2")
			exit, exitCAPath := xrayServer(t, true)
			exitRelay := newHoppingRelay(t, exit)
			exitCA, err := os.ReadFile(exitCAPath)
			if err != nil {
				t.Fatal(err)
			}
			hopConfig := func(relay *hoppingRelay) *udphop.Config {
				return &udphop.Config{Remote: true, IntervalMin: 5, IntervalMax: 5, RemotePorts: relay.ports()}
			}
			entry, caPath, _ := xrayServerWithConfig(t, true, func(config *core.Config) {
				config.Outbound = embeddedClientConfig(t, "127.0.0.1:1", exitCA, true, auth, hopConfig(exitRelay)).Outbound
			})
			entryRelay := newHoppingRelay(t, entry)
			ca, err := os.ReadFile(caPath)
			if err != nil {
				t.Fatal(err)
			}
			client := embeddedClient(t, "127.0.0.1:1", ca, true, hopConfig(entryRelay))
			tcp, udp := echoTargets(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var connections []net.Conn
			for i := range 6 {
				address := "tcp:" + tcp
				if i >= 4 {
					address = "udp:" + udp
				}
				target, err := xnet.ParseDestination(address)
				if err != nil {
					t.Fatal(err)
				}
				conn, err := core.Dial(ctx, client, target)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				connections = append(connections, conn)
			}
			until := time.Now().Add(11 * time.Second)
			var workers sync.WaitGroup
			for i, conn := range connections {
				workers.Go(func() {
					stop := context.AfterFunc(ctx, func() { conn.Close() })
					defer stop()
					if i >= 4 {
						exerciseCascadeUDP(t, conn, until, i)
						return
					}
					data := bytes.Repeat([]byte{byte(i), 0xa5}, 128*1024)
					got := make([]byte, len(data))
					for time.Now().Before(until) {
						written := make(chan error, 1)
						go func() { _, err := conn.Write(data); written <- err }()
						_, err := io.ReadFull(conn, got)
						if err != nil {
							conn.Close()
						}
						writeErr := <-written
						if err == nil {
							err = writeErr
						}
						if err == nil && !bytes.Equal(got, data) {
							err = fmt.Errorf("payload changed")
						}
						if err != nil {
							t.Errorf("cascade flow %d: %v", i, err)
							return
						}
					}
				})
			}
			workers.Wait()
			for _, relay := range []*hoppingRelay{entryRelay, exitRelay} {
				relay.mu.Lock()
				if len(relay.peers) == 0 {
					t.Error("cascade never reached relay")
				}
				for address, peer := range relay.peers {
					if peer.changes < 2 {
						t.Errorf("cascade peer %s survived only %d port changes", address, peer.changes)
					}
				}
				relay.mu.Unlock()
				select {
				case err := <-relay.errors:
					t.Error(err)
				default:
				}
			}
		})
	}
}

func exerciseCascadeUDP(t *testing.T, conn net.Conn, until time.Time, id int) {
	t.Helper()
	data := bytes.Repeat([]byte{byte(id), 0xa5}, 2048)
	stop := time.AfterFunc(time.Until(until), func() { conn.Close() })
	defer stop.Stop()
	written := make(chan struct{})
	go func() {
		defer close(written)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if !time.Now().Before(until) {
				return
			}
			if _, err := conn.Write(data); err != nil {
				if time.Now().Before(until) {
					t.Errorf("cascade UDP %d write: %v", id, err)
				}
				return
			}
		}
	}()
	// A fragmented UDP datagram may be lost under TCP load. Keep sending so
	// one lost datagram cannot stall the test; a closed flow still fails it.
	got := make([]byte, len(data))
	received := 0
	for {
		if _, err := io.ReadFull(conn, got); err != nil {
			if time.Now().Before(until) {
				t.Errorf("cascade UDP %d read: %v", id, err)
			}
			break
		}
		if !bytes.Equal(got, data) {
			t.Errorf("cascade UDP %d payload changed", id)
			break
		}
		received++
	}
	conn.Close()
	<-written
	if received < 20 {
		t.Errorf("cascade UDP %d received only %d datagrams", id, received)
	}
}
