package grpc

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/grpc/encoding"
	"github.com/xtls/xray-core/transport/internet/handoff"
	"github.com/xtls/xray-core/transport/internet/stat"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCTransportDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	oldGate := handoff.NewTCPListener(raw, true)
	defer oldGate.Close()
	file, err := oldGate.File()
	if err != nil {
		t.Fatal(err)
	}
	nextGate, err := handoff.ImportTCPListener(file)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer nextGate.Close()

	serve := func(gate *handoff.TCPListener, generation byte) *Listener {
		config := &internet.MemoryStreamConfig{ProtocolSettings: &Config{ServiceName: "handoff"}}
		return ServeListener(ctx, gate, config, func(conn stat.Connection) {
			go func() {
				defer conn.Close()
				var input [1]byte
				for {
					if _, err := io.ReadFull(conn, input[:]); err != nil {
						return
					}
					if _, err := conn.Write([]byte{generation, input[0]}); err != nil {
						return
					}
				}
			}()
		})
	}
	old := serve(oldGate, 'A')
	defer old.Close()
	next := serve(nextGate, 'B')
	defer next.Close()

	open := func() encoding.GRPCService_TunClient {
		t.Helper()
		conn, err := ggrpc.NewClient("passthrough:///"+raw.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		client := encoding.NewGRPCServiceClient(conn).(encoding.GRPCServiceClientX)
		stream, err := client.TunCustomName(ctx, "handoff", "Tun", ggrpc.WaitForReady(true))
		if err != nil {
			t.Fatal(err)
		}
		return stream
	}
	check := func(stream encoding.GRPCService_TunClient, generation, pulse byte) {
		t.Helper()
		if err := stream.Send(&encoding.Hunk{Data: []byte{pulse}}); err != nil {
			t.Fatal(err)
		}
		reply, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if string(reply.Data) != string([]byte{generation, pulse}) {
			t.Fatalf("unexpected generation/pulse: %q", reply.Data)
		}
	}

	streamA := open()
	check(streamA, 'A', '0')
	if err := oldGate.Pause(); err != nil {
		t.Fatal(err)
	}
	if err := nextGate.Resume(); err != nil {
		t.Fatal(err)
	}
	drained := make(chan struct{})
	go func() {
		old.Drain()
		close(drained)
	}()
	streamB := open()
	check(streamB, 'B', '1')
	check(streamA, 'A', '2')
	select {
	case <-drained:
		t.Fatal("old server drained before its stream ended")
	default:
	}
	if err := streamA.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := streamA.Recv(); err != io.EOF {
		t.Fatalf("expected normal stream close, got %v", err)
	}
	select {
	case <-drained:
	case <-ctx.Done():
		t.Fatal("old server did not drain after the final stream ended")
	}
	check(streamB, 'B', '3')
}
