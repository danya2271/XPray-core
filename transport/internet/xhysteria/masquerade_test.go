//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
)

func httpTestEngine(t *testing.T) *Engine {
	t.Helper()
	engine := &Engine{role: "server", done: make(chan struct{}), out: make(chan frame, 32), flows: make(map[uint64]*Conn), maxFlows: 4}
	t.Cleanup(func() { engine.Close() })
	return engine
}

func TestHTTPBridgeBodyPreservesTrailersAndReturnsCredits(t *testing.T) {
	engine := httpTestEngine(t)
	connection := newConn(engine, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	request := &http.Request{Trailer: make(http.Header)}
	body := &httpBridgeBody{connection: connection, request: request}
	connection.in <- frame{kind: dataFrame, data: []byte("body")}
	connection.in <- frame{kind: httpTrailers, data: []byte(`{"x-complete":["first","second"]}`)}
	connection.in <- frame{kind: finFrame}
	payload, err := io.ReadAll(body)
	if err != nil || string(payload) != "body" || len(request.Trailer.Values("X-Complete")) != 2 {
		t.Fatalf("body=%q trailers=%v error=%v", payload, request.Trailer, err)
	}
	credit := <-engine.out
	if credit.kind != creditFrame || !bytes.Equal(credit.data, []byte{2}) {
		t.Fatalf("incorrect HTTP receive credits: %+v", credit)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPBridgeBodyCloseInterruptsRead(t *testing.T) {
	engine := httpTestEngine(t)
	connection := newConn(engine, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	body := &httpBridgeBody{connection: connection}
	result := make(chan error, 1)
	go func() { _, err := body.Read(make([]byte, 1)); result <- err }()
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("body read was not interrupted")
		}
	case <-time.After(time.Second):
		t.Fatal("closing HTTP body blocked its read")
	}
}

func TestHTTPBridgeRejectsInvalidRequestMetadata(t *testing.T) {
	for _, metadata := range []string{
		`{`,
		`{"method":"GET","uri":"%bad"}`,
		`{"method":"bad method","uri":"https://cover.example/"}`,
		`{"method":"GET","uri":"https://cover.example/","content_length":-2}`,
		`{"method":"GET","uri":"https://cover.example/","headers":{"bad name":["value"]}}`,
		`{"method":"GET","uri":"https://cover.example/","headers":{"test":["a\r\nb"]}}`,
	} {
		t.Run(metadata, func(t *testing.T) {
			engine := httpTestEngine(t)
			if err := engine.acceptHTTP(frame{kind: incomingHTTP, id: 7, data: []byte(metadata)}); err == nil {
				t.Fatal("invalid request accepted")
			}
			select {
			case message := <-engine.out:
				if message.kind != closeFrame || message.id != 7 || len(engine.flows) != 0 {
					t.Fatal("rejected HTTP request retained a flow")
				}
			default:
				t.Fatal("rejected request did not close immediately")
			}
		})
	}
}

func TestHTTPBridgeCancellationAndShutdown(t *testing.T) {
	for _, mode := range []string{"connection", "engine", "early_response"} {
		t.Run(mode, func(t *testing.T) {
			engine := httpTestEngine(t)
			entered := make(chan *http.Request, 1)
			engine.masquerade = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				entered <- request
				if mode == "early_response" {
					writer.WriteHeader(http.StatusForbidden)
					return
				}
				<-request.Context().Done()
			})
			if err := engine.acceptHTTP(frame{kind: incomingHTTP, id: 1, data: []byte(`{"method":"POST","uri":"https://cover.example/upload","content_length":-1}`)}); err != nil {
				t.Fatal(err)
			}
			request := <-entered
			engine.mu.Lock()
			connection := engine.flows[1]
			engine.mu.Unlock()
			if mode == "early_response" {
				for _, kind := range []byte{httpHead, finFrame} {
					select {
					case message := <-engine.out:
						if message.kind != kind {
							t.Fatalf("unexpected response frame %d", message.kind)
						}
					case <-time.After(time.Second):
						t.Fatal("early response waited for an unread request body")
					}
				}
			}
			if mode == "engine" {
				engine.stop()
			} else {
				connection.fail(errors.New("client cancelled request"))
			}
			complete := make(chan struct{})
			go func() { engine.wg.Wait(); close(complete) }()
			select {
			case <-complete:
			case <-time.After(time.Second):
				t.Fatal("cancelled HTTP handler leaked")
			}
			if request.Context().Err() == nil {
				t.Fatal("handler context was not cancelled")
			}
			engine.mu.Lock()
			remaining := len(engine.flows)
			engine.mu.Unlock()
			if remaining != 0 {
				t.Fatal("cancelled HTTP flow remained registered")
			}
		})
	}
}

func TestHTTPBridgeWriterPreservesHeadersAndTrailers(t *testing.T) {
	engine := httpTestEngine(t)
	connection := newConn(engine, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	writer := &httpBridgeWriter{connection: connection, headers: make(http.Header), contentLength: -1}
	writer.Header().Add("Set-Cookie", "one=1")
	writer.Header().Add("Set-Cookie", "two=2")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("Trailer", "X-Complete")
	writer.Header().Set("X-Complete", "yes")
	if _, err := writer.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := writer.finish(); err != nil {
		t.Fatal(err)
	}
	message := <-engine.out
	var head httpResponseHead
	if message.kind != httpHead || json.Unmarshal(message.data, &head) != nil {
		t.Fatal("invalid HTTP response metadata")
	}
	if len(head.Headers.Values("Set-Cookie")) != 2 || head.Headers.Get("Connection") != "" || head.Headers.Get("X-Complete") != "" || head.Headers.Get("Date") == "" {
		t.Fatalf("response headers changed: %v", head.Headers)
	}
	if message = <-engine.out; message.kind != dataFrame || string(message.data) != "payload" {
		t.Fatal("response body changed")
	}
	message.release()
	var trailers http.Header
	if message = <-engine.out; message.kind != httpTrailers || json.Unmarshal(message.data, &trailers) != nil || trailers.Get("X-Complete") != "yes" {
		t.Fatal("response trailers changed")
	}
	if message = <-engine.out; message.kind != finFrame || len(connection.writeCredit) != 0 {
		t.Fatal("HTTP response exceeded its four-frame credit window")
	}
}

func TestHTTPBridgeWriterBoundsAndBodylessResponses(t *testing.T) {
	for _, mode := range []string{"head", "204", "304", "length", "headers", "informational"} {
		t.Run(mode, func(t *testing.T) {
			engine := httpTestEngine(t)
			connection := newConn(engine, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
			writer := &httpBridgeWriter{connection: connection, headers: make(http.Header), contentLength: -1}
			switch mode {
			case "headers":
				writer.Header().Set("X-Large", strings.Repeat("a", maxFrame))
				writer.WriteHeader(200)
				if writer.err == nil || len(engine.out) != 0 || len(connection.writeCredit) != 4 {
					t.Fatal("oversized metadata used bridge credits")
				}
				return
			case "length":
				writer.Header().Set("Content-Length", "2")
				if _, err := writer.Write([]byte("oversized")); !errors.Is(err, http.ErrContentLength) || len(engine.out) != 1 {
					t.Fatal("content-length overflow was not rejected")
				}
				return
			case "informational":
				writer.WriteHeader(103)
				if writer.status != 0 {
					t.Fatal("informational header ended the response")
				}
				writer.WriteHeader(200)
				if err := writer.finish(); err != nil || len(engine.out) != 3 {
					t.Fatal("informational and final headers did not finish")
				}
				return
			case "head":
				writer.method = http.MethodHead
				writer.WriteHeader(200)
			case "204":
				writer.Header().Set("Content-Length", "10")
				writer.WriteHeader(204)
			case "304":
				writer.WriteHeader(304)
			}
			_, err := writer.Write([]byte("unused"))
			if mode == "head" && err != nil || mode != "head" && !errors.Is(err, http.ErrBodyNotAllowed) {
				t.Fatalf("incorrect bodyless response error: %v", err)
			}
			if err := writer.finish(); err != nil || len(engine.out) != 2 {
				t.Fatal("bodyless response published body frames")
			}
			var head httpResponseHead
			json.Unmarshal((<-engine.out).data, &head)
			if mode == "204" && head.Headers.Get("Content-Length") != "" {
				t.Fatal("204 retained an invalid content-length")
			}
		})
	}
}
