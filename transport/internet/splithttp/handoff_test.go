package splithttp

import (
	"context"
	"io"
	stdnet "net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func TestXHTTPHandoff(t *testing.T) {
	for _, placement := range []string{PlacementPath, PlacementHeader} {
		for _, mode := range []string{"packet-up", "stream-up"} {
			t.Run(placement+"/"+mode, func(t *testing.T) {
				testXHTTPHandoff(t, placement, mode)
			})
		}
	}
}

func testXHTTPHandoff(t *testing.T, placement, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	directory := t.TempDir()
	config := &Config{
		Path: "/x/", Mode: mode, SessionIDPlacement: placement, SeqPlacement: placement,
		SessionIDKey: "X-Session-Id", SeqKey: "X-Seq", NoSSEHeader: true,
	}
	packetMethod := http.MethodPost
	if placement == PlacementHeader && mode == "packet-up" {
		config.UplinkDataPlacement = PlacementHeader
		config.UplinkDataKey = "X-Payload"
		packetMethod = http.MethodGet
	}
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)

	start := func(generation byte) (*Listener, *http.Client, string) {
		t.Helper()
		socket := filepath.Join(directory, string(generation)+".sock")
		listener, err := ListenXH(ctx, net.DomainAddress(socket), 0, &internet.MemoryStreamConfig{
			ProtocolName: "splithttp", ProtocolSettings: config,
		}, func(conn stat.Connection) {
			go func() {
				defer conn.Close()
				var data [1]byte
				for {
					if _, err := io.ReadFull(conn, data[:]); err != nil {
						return
					}
					if _, err := conn.Write([]byte{generation, data[0]}); err != nil {
						return
					}
				}
			}()
		})
		if err != nil {
			t.Fatal(err)
		}
		l := listener.(*Listener)
		t.Cleanup(func() { l.server.Close() })
		t.Cleanup(l.ClearHandoffPeer)
		privatePath := filepath.Join(directory, string(generation)+".peer")
		private, err := stdnet.Listen("unix", privatePath)
		if err != nil {
			t.Fatal(err)
		}
		privateServer := &http.Server{Handler: l.HandoffHandler(), Protocols: protocols}
		t.Cleanup(func() { privateServer.Close() })
		go privateServer.Serve(private)
		client := &http.Client{Transport: &http.Transport{
			Protocols: protocols,
			DialContext: func(ctx context.Context, _, _ string) (stdnet.Conn, error) {
				return (&stdnet.Dialer{}).DialContext(ctx, "unix", socket)
			},
		}}
		t.Cleanup(client.CloseIdleConnections)
		return l, client, privatePath
	}
	old, clientA, oldPrivate := start('A')
	next, clientB, nextPrivate := start('B')

	request := func(method, id, seq string, payload io.Reader) *http.Request {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, method, "http://xray.test/x/", payload)
		if err != nil {
			t.Fatal(err)
		}
		config.FillStreamRequest(r, id, seq)
		return r
	}
	do := func(client *http.Client, r *http.Request) *http.Response {
		t.Helper()
		res, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			res.Body.Close()
			t.Fatalf("%s %s: %s", r.Method, r.URL, res.Status)
		}
		t.Cleanup(func() { res.Body.Close() })
		return res
	}
	read := func(res *http.Response, generation, value byte) {
		t.Helper()
		var got [2]byte
		if _, err := io.ReadFull(res.Body, got[:]); err != nil || got != [2]byte{generation, value} {
			t.Fatalf("got %q, %v; expected %q", got, err, []byte{generation, value})
		}
	}
	packet := func(client *http.Client, id, seq, data string) {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, packetMethod, "http://xray.test/x/", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := config.FillPacketRequest(r, id, seq, buf.MultiBuffer{buf.FromBytes([]byte(data))}); err != nil {
			t.Fatal(err)
		}
		do(client, r).Body.Close()
	}

	downA := do(clientA, request(http.MethodGet, "established", "", nil))
	if mode == "packet-up" {
		packet(clientA, "uplink-first", "0", "p")
	}
	if err := next.SetHandoffPeer(oldPrivate, true); err != nil {
		t.Fatal(err)
	}
	if err := old.SetHandoffPeer(nextPrivate, false); err != nil {
		t.Fatal(err)
	}
	// Simulate a new request arriving on a previously pooled connection to A.
	downB := do(clientA, request(http.MethodGet, "new", "", nil))
	if mode == "packet-up" {
		downEarly := do(clientB, request(http.MethodGet, "uplink-first", "", nil))
		read(downEarly, 'A', 'p')
		downEarly.Body.Close()
		for _, pulse := range []struct {
			id, seq, data string
			down          *http.Response
			generation    byte
		}{
			{"established", "0", "a", downA, 'A'},
			{"new", "0", "b", downB, 'B'},
			{"established", "1", "c", downA, 'A'},
		} {
			packet(clientB, pulse.id, pulse.seq, pulse.data)
			read(pulse.down, pulse.generation, pulse.data[0])
		}
	} else {
		stream := func(id string) *io.PipeWriter {
			reader, writer := io.Pipe()
			t.Cleanup(func() { writer.Close(); reader.Close() })
			r := request(http.MethodPost, id, "", reader)
			go func() {
				res, err := clientB.Do(r)
				if err != nil {
					reader.CloseWithError(err)
					return
				}
				defer res.Body.Close()
				io.Copy(io.Discard, res.Body)
			}()
			return writer
		}
		a, b := stream("established"), stream("new")
		for _, pulse := range []struct {
			up                *io.PipeWriter
			down              *http.Response
			generation, value byte
		}{{a, downA, 'A', 'a'}, {b, downB, 'B', 'b'}, {a, downA, 'A', 'c'}} {
			if _, err := pulse.up.Write([]byte{pulse.value}); err != nil {
				t.Fatal(err)
			}
			read(pulse.down, pulse.generation, pulse.value)
		}
	}
	if err := next.SetHandoffPeer(filepath.Join(directory, "missing.peer"), true); err != nil {
		t.Fatal(err)
	}
	res, err := clientB.Do(request(http.MethodGet, "not-yet-routed", "", nil))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("an unreachable owner must not be treated as unknown: %s", res.Status)
	}
	if _, ok := next.handler.sessions.Load("not-yet-routed"); ok {
		t.Fatal("a session was opened with its ownership unknown")
	}
}
