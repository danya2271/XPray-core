//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"context"
	cryptotls "crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/core"
	hyproxy "github.com/xtls/xray-core/proxy/hysteria"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
)

// Use nonblocking OpenStream like third-party Hysteria2 clients, without Xray's
// session pool or OpenStreamSync hiding an exhausted per-connection limit.
func singleSessionHysteriaClient(t *testing.T) (*quic.Conn, string) {
	t.Helper()
	t.Setenv("XRAY_HYSTERIA_NATIVE", "")
	t.Setenv("XRAY_HYSTERIA_SERVER_FLOWS", "256")
	masked := os.Getenv("XHYS_STREAM_MASKED") == "1"
	server, caPath, _ := xrayServerWithConfig(t, masked, func(config *core.Config) {
		nativeServerConfig(t, config, false)
	})
	ca, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid stream-limit fixture certificate")
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
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	connection, err := transport.Dial(ctx, address, &cryptotls.Config{
		RootCAs: roots, ServerName: "localhost", NextProtos: []string{"h3"},
	}, &quic.Config{EnableDatagrams: true, DisablePathManager: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.CloseWithError(0, "stream-limit test complete") })
	httpTransport := &http3.Transport{}
	t.Cleanup(func() { httpTransport.Close() })
	masqueradeAuthenticate(t, httpTransport.NewClientConn(connection))
	tcp, _ := echoTargetsWithTimeout(t, 20*time.Minute)
	return connection, tcp
}

func singleSessionTCP(t *testing.T, connection *quic.Conn, target string) *quic.Stream {
	t.Helper()
	stream, err := connection.OpenStream()
	if err != nil {
		t.Fatalf("nonblocking OpenStream on authenticated session: %v", err)
	}
	// The parent fixture closes the whole QUIC session on any failure. Avoid
	// keeping every already-closed stream alive in test cleanup during a soak.
	if err := stream.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{0x44, 0x01}); err != nil {
		t.Fatal(err)
	}
	if err := hyproxy.WriteTCPRequest(stream, target); err != nil {
		t.Fatal(err)
	}
	accepted, message, err := hyproxy.ReadTCPResponse(stream)
	if err != nil || !accepted {
		t.Fatalf("TCP request: accepted=%t message=%q error=%v", accepted, message, err)
	}
	return stream
}

func singleSessionEcho(t *testing.T, stream *quic.Stream, sequence int) {
	t.Helper()
	payload := []byte(fmt.Sprintf("stream-%08d", sequence))
	if err := stream.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(payload); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, reply); err != nil || string(reply) != string(payload) {
		t.Fatalf("echo %d: reply=%q error=%v", sequence, reply, err)
	}
}

func TestNativeSingleQUICSessionBeyond64Streams(t *testing.T) {
	connection, target := singleSessionHysteriaClient(t)
	streams := make([]*quic.Stream, 192)
	for index := range streams {
		// Use the parent test for cleanup to keep all earlier streams open.
		if index%64 == 0 {
			t.Logf("opening_single_session_stream=%d", index+1)
		}
		streams[index] = singleSessionTCP(t, connection, target)
		singleSessionEcho(t, streams[index], index)
	}
	for index, stream := range streams {
		singleSessionEcho(t, stream, index+len(streams))
	}
	t.Logf("single_quic_session_live_streams=%d", len(streams))
}

func TestNativeSingleQUICSessionRecyclesClosedStreams(t *testing.T) {
	connection, target := singleSessionHysteriaClient(t)
	// Keep one stream alive across more opens/closes than the full advertised
	// window, so reconnecting cannot accidentally conceal missing MAX_STREAMS.
	live := singleSessionTCP(t, connection, target)
	for index := range 1200 {
		if index%100 == 0 {
			t.Logf("single_session_churn_index=%d", index)
		}
		stream := singleSessionTCP(t, connection, target)
		singleSessionEcho(t, stream, index)
		if index%2 == 0 {
			// A normal third-party close: FIN plus STOP_SENDING.
			stream.CancelRead(0)
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
		} else {
			stream.CancelRead(0)
			stream.CancelWrite(0)
		}
		if index%64 == 0 {
			singleSessionEcho(t, live, index)
		}
	}
	singleSessionEcho(t, live, 1200)
	available := 0
	for range 65 {
		stream, err := connection.OpenStream()
		if err != nil {
			var limit *quic.StreamLimitReachedError
			if !errors.As(err, &limit) {
				t.Fatal(err)
			}
			break
		}
		available++
		// Leave these unwritten handles open until the session cleanup.
		_ = stream
	}
	if available > 64 {
		t.Fatal("serial stream churn expanded the low-concurrency window")
	}
	t.Logf("single_quic_session_serial_churn_available_handles=%d", available)
	t.Log("single_quic_session_recycled_streams=1200 retained_streams=1")
}

func TestNativeSingleQUICSessionFifteenMinuteSoak(t *testing.T) {
	if os.Getenv("XHYS_STREAM_SOAK") != "1" {
		t.Skip("set XHYS_STREAM_SOAK=1 for the fifteen-minute single-session soak")
	}
	connection, target := singleSessionHysteriaClient(t)
	live := singleSessionTCP(t, connection, target)
	start := time.Now()
	nextReport := time.Minute
	index := 0
	for time.Since(start) < 15*time.Minute {
		stream := singleSessionTCP(t, connection, target)
		singleSessionEcho(t, stream, index)
		stream.CancelRead(0)
		if index%2 == 0 {
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
		} else {
			stream.CancelWrite(0)
		}
		singleSessionEcho(t, live, index)
		index++
		if time.Since(start) >= nextReport {
			t.Logf("single_session_soak_elapsed=%s completed=%d", time.Since(start).Round(time.Second), index)
			nextReport += time.Minute
		}
		time.Sleep(50 * time.Millisecond)
	}
	singleSessionEcho(t, live, index)
	t.Logf("single_session_soak_complete=%s recycled=%d retained_streams=1", time.Since(start), index)
}

func TestNativeSingleQUICSessionRejectsBeyondAdvertisedLimit(t *testing.T) {
	connection, _ := singleSessionHysteriaClient(t)
	var streams []*quic.Stream
	for range 1100 {
		stream, err := connection.OpenStream()
		if err != nil {
			var limit *quic.StreamLimitReachedError
			if !errors.As(err, &limit) {
				t.Fatal(err)
			}
			// Idle stream handles do not create native flows, but QUIC still
			// needs a finite advertised bound.
			if len(streams) < 63 || len(streams) > 64 {
				t.Fatalf("idle advertised stream window exhausted at %d, want about 64", len(streams))
			}
			t.Logf("single_quic_session_finite_window=%d", len(streams))
			return
		}
		streams = append(streams, stream)
		t.Cleanup(func() { stream.CancelRead(0); stream.CancelWrite(0) })
	}
	t.Fatal("server did not enforce the advertised stream limit")
}
