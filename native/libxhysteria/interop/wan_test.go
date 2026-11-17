//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
)

func delayedQUICRelay(t *testing.T, server string) string {
	t.Helper()
	front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	address, err := net.ResolveUDPAddr("udp4", server)
	if err != nil {
		front.Close()
		t.Fatal(err)
	}
	upstream, err := net.DialUDP("udp4", nil, address)
	if err != nil {
		front.Close()
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	t.Cleanup(func() { close(stopped); front.Close(); upstream.Close() })
	client := make(chan *net.UDPAddr, 1)
	type packet struct {
		data []byte
		due  time.Time
	}
	for _, direction := range []bool{true, false} {
		packets := make(chan packet, 16384)
		go func() {
			defer close(packets)
			buffer := make([]byte, 65536)
			first := true
			for {
				var length int
				var err error
				if direction {
					var address *net.UDPAddr
					length, address, err = front.ReadFromUDP(buffer)
					if err == nil && first {
						client <- address
						first = false
					}
				} else {
					length, err = upstream.Read(buffer)
				}
				if err != nil {
					return
				}
				select {
				case packets <- packet{data: bytes.Clone(buffer[:length]), due: time.Now().Add(50 * time.Millisecond)}:
				case <-stopped:
					return
				}
			}
		}()
		go func() {
			var address *net.UDPAddr
			if !direction {
				select {
				case address = <-client:
				case <-stopped:
					return
				}
			}
			timer := time.NewTimer(time.Hour)
			timer.Stop()
			defer timer.Stop()
			for packet := range packets {
				if delay := time.Until(packet.due); delay > 0 {
					timer.Reset(delay)
					select {
					case <-timer.C:
					case <-stopped:
						return
					}
				}
				if direction {
					if _, err := upstream.Write(packet.data); err != nil {
						return
					}
				} else if _, err := front.WriteToUDP(packet.data, address); err != nil {
					return
				}
			}
		}()
	}
	return front.LocalAddr().String()
}

func TestNativeServerBulkTransferWith100msRTT(t *testing.T) {
	backend := os.Getenv("XHYS_WAN_BACKEND")
	if backend == "" {
		backend = "native"
	}
	if backend != "native" && backend != "go" {
		t.Fatal("XHYS_WAN_BACKEND must be native or go")
	}
	t.Setenv("XRAY_HYSTERIA_NATIVE", "")
	if backend == "go" {
		t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
	}
	server, caPath, _ := xrayServerWithConfig(t, true, func(c *core.Config) { nativeServerConfig(t, c, false) })
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
	if backend == "go" {
		t.Setenv("XRAY_HYSTERIA_NATIVE", "0")
	}
	t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "1")
	client := embeddedClient(t, delayedQUICRelay(t, server), ca, true)
	tcp, _ := echoTargets(t)
	target, _ := xnet.ParseDestination("tcp:" + tcp)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := core.Dial(ctx, client, target)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	payload := bytes.Repeat([]byte{0xa7}, 16<<20)
	written := make(chan error, 1)
	started := time.Now()
	go func() { _, err := conn.Write(payload); written <- err }()
	reply := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reply, payload) {
		t.Fatal("WAN transfer changed the payload")
	}
	elapsed := time.Since(started)
	t.Logf("backend=%s RTT=100ms upload_mib=16 download_mib=16 elapsed=%s duplex_mib_per_second=%.2f", backend, elapsed, 32/elapsed.Seconds())
	if elapsed >= 8*time.Second {
		t.Fatalf("bulk transfer stayed limited by the initial receive window: %s", elapsed)
	}
}
