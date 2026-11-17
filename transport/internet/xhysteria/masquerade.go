//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"context"
	cryptotls "crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"golang.org/x/net/http/httpguts"
)

type httpRequestHead struct {
	Method        string      `json:"method"`
	URI           string      `json:"uri"`
	Headers       http.Header `json:"headers"`
	Remote        string      `json:"remote"`
	ContentLength int64       `json:"content_length"`
}

type httpResponseHead struct {
	Status  int         `json:"status"`
	Headers http.Header `json:"headers"`
}

func (e *Engine) acceptHTTP(message frame) (err error) {
	defer message.release()
	defer func() {
		if err != nil {
			_ = e.send(frame{kind: closeFrame, id: message.id}, time.Now().Add(time.Second))
		}
	}()
	var head httpRequestHead
	if err := json.Unmarshal(message.data, &head); err != nil {
		return err
	}
	if !httpguts.ValidHeaderFieldName(head.Method) || head.ContentLength < -1 {
		return errors.New("invalid native HTTP request metadata")
	}
	address, err := url.ParseRequestURI(head.URI)
	if err != nil {
		return err
	}
	for name, values := range head.Headers {
		if !httpguts.ValidHeaderFieldName(name) {
			return errors.New("invalid native HTTP request header name")
		}
		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return errors.New("invalid native HTTP request header value")
			}
		}
	}
	e.mu.Lock()
	select {
	case <-e.done:
		e.mu.Unlock()
		return errors.New("native HTTP engine stopped")
	default:
	}
	if e.masquerade == nil || e.flows[message.id] != nil || len(e.flows) >= e.maxFlows {
		e.mu.Unlock()
		return e.send(frame{kind: closeFrame, id: message.id}, time.Now().Add(time.Second))
	}
	connection := newConn(e, message.id, xnet.Destination{Network: xnet.Network_TCP}, nil)
	e.flows[message.id] = connection
	e.mu.Unlock()
	request := &http.Request{Method: head.Method, URL: address, Header: make(http.Header, len(head.Headers)), Host: address.Host, Proto: "HTTP/3.0", ProtoMajor: 3, ProtoMinor: 0, ContentLength: head.ContentLength, RemoteAddr: head.Remote, RequestURI: address.RequestURI(), TLS: &cryptotls.ConnectionState{Version: cryptotls.VersionTLS13, HandshakeComplete: true, NegotiatedProtocol: "h3"}}
	for name, values := range head.Headers {
		request.Header[textproto.CanonicalMIMEHeaderKey(name)] = values
	}
	request.Trailer = make(http.Header)
	for _, line := range request.Header.Values("Trailer") {
		for _, name := range strings.Split(line, ",") {
			name = textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(name))
			if httpguts.ValidTrailerHeader(name) && httpguts.ValidHeaderFieldName(name) {
				request.Trailer[name] = nil
			}
		}
	}
	body := &httpBridgeBody{connection: connection, request: request}
	request.Body = body
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	request = request.WithContext(ctx)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-connection.closed:
			cancel()
		case <-e.done:
			cancel()
		case <-ctx.Done():
			connection.Close()
		}
	}()
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() { cancel(); body.Close(); connection.Close(); <-stopped }()
		writer := &httpBridgeWriter{connection: connection, headers: make(http.Header), method: request.Method, contentLength: -1}
		defer func() {
			if recovered := recover(); recovered != nil {
				e.diagnose("HTTP masquerade handler failed", fmt.Errorf("%v", recovered))
			}
		}()
		e.masquerade.ServeHTTP(writer, request)
		if err := writer.finish(); err != nil {
			e.diagnose("HTTP masquerade response failed", err)
			return
		}
		select {
		case <-connection.closed:
		case <-e.done:
		}
	}()
	return nil
}

type httpBridgeBody struct {
	connection *Conn
	request    *http.Request
	mu         sync.Mutex
	pending    frame
	offset     int
	closed     bool
}

func (body *httpBridgeBody) Read(target []byte) (int, error) {
	body.mu.Lock()
	defer body.mu.Unlock()
	if len(target) == 0 {
		return 0, nil
	}
	if body.closed {
		return 0, io.EOF
	}
	for body.offset == len(body.pending.data) {
		body.pending.release()
		body.offset = 0
		message, err := body.connection.nextFrame()
		if err != nil {
			return 0, err
		}
		switch message.kind {
		case dataFrame:
			body.pending = message
		case httpTrailers:
			var trailers http.Header
			err := json.Unmarshal(message.data, &trailers)
			message.release()
			if err != nil {
				return 0, err
			}
			for name, values := range trailers {
				if !httpguts.ValidHeaderFieldName(name) || !httpguts.ValidTrailerHeader(name) {
					return 0, errors.New("invalid native HTTP request trailer")
				}
				for _, value := range values {
					if !httpguts.ValidHeaderFieldValue(value) {
						return 0, errors.New("invalid native HTTP request trailer value")
					}
				}
				body.request.Trailer[textproto.CanonicalMIMEHeaderKey(name)] = values
			}
		case finFrame:
			message.release()
			body.closed = true
			return 0, io.EOF
		default:
			message.release()
			return 0, errors.New("unexpected native HTTP request frame")
		}
	}
	count := copy(target, body.pending.data[body.offset:])
	body.offset += count
	if body.offset == len(body.pending.data) {
		body.pending.release()
		body.offset = 0
	}
	return count, nil
}

func (body *httpBridgeBody) Close() error {
	body.connection.SetReadDeadline(time.Now())
	body.mu.Lock()
	defer body.mu.Unlock()
	body.closed = true
	body.pending.release()
	return nil
}

type httpBridgeWriter struct {
	connection    *Conn
	headers       http.Header
	method        string
	status        int
	err           error
	contentLength int64
	written       int64
}

func (writer *httpBridgeWriter) Header() http.Header { return writer.headers }

func (writer *httpBridgeWriter) WriteHeader(status int) {
	if writer.status != 0 || writer.err != nil {
		return
	}
	if status < 100 || status > 999 {
		panic(fmt.Sprintf("invalid WriteHeader code %d", status))
	}
	headers := writer.headers.Clone()
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Transfer-Encoding", "Upgrade", "Te"} {
		headers.Del(name)
	}
	for name := range headers {
		if strings.HasPrefix(name, http.TrailerPrefix) {
			delete(headers, name)
		}
	}
	for _, line := range headers.Values("Trailer") {
		for _, name := range strings.Split(line, ",") {
			headers.Del(strings.TrimSpace(name))
		}
	}
	if status >= 200 {
		writer.status = status
		if _, ok := headers["Date"]; !ok {
			headers.Set("Date", time.Now().UTC().Format(http.TimeFormat))
		}
		if value := headers.Get("Content-Length"); value != "" {
			length, err := strconv.ParseInt(value, 10, 64)
			if err != nil || length < 0 {
				headers.Del("Content-Length")
			} else {
				writer.contentLength = length
			}
		}
		if status == http.StatusNoContent {
			headers.Del("Content-Length")
			writer.contentLength = -1
		}
	}
	payload, err := json.Marshal(httpResponseHead{Status: status, Headers: headers})
	if err == nil && len(payload) > maxFrame {
		err = errors.New("native HTTP response headers exceed bridge limit")
	}
	if err == nil {
		err = writer.connection.awaitWriteCredit()
	}
	if err == nil {
		err = writer.connection.sendFrame(frame{kind: httpHead, id: writer.connection.id, data: payload})
	}
	writer.err = err
}

func (writer *httpBridgeWriter) Write(payload []byte) (int, error) {
	if writer.status == 0 {
		if _, ok := writer.headers["Content-Type"]; !ok && writer.headers.Get("Content-Encoding") == "" && len(payload) != 0 {
			writer.headers.Set("Content-Type", http.DetectContentType(payload))
		}
		writer.WriteHeader(http.StatusOK)
	}
	if writer.err != nil {
		return 0, writer.err
	}
	if writer.status == 204 || writer.status == 304 {
		return 0, http.ErrBodyNotAllowed
	}
	if writer.contentLength >= 0 && int64(len(payload)) > writer.contentLength-writer.written {
		return 0, http.ErrContentLength
	}
	if writer.method == http.MethodHead {
		writer.written += int64(len(payload))
		return len(payload), nil
	}
	count, err := writer.connection.Write(payload)
	writer.written += int64(count)
	writer.err = err
	return count, err
}

func (writer *httpBridgeWriter) FlushError() error {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.err
}

func (writer *httpBridgeWriter) Flush() { writer.FlushError() }

func (writer *httpBridgeWriter) finish() error {
	if err := writer.FlushError(); err != nil {
		return err
	}
	trailers := make(http.Header)
	for _, line := range writer.headers.Values("Trailer") {
		for _, name := range strings.Split(line, ",") {
			name = textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(name))
			if httpguts.ValidTrailerHeader(name) {
				trailers[name] = writer.headers.Values(name)
			}
		}
	}
	for name, values := range writer.headers {
		if strings.HasPrefix(name, http.TrailerPrefix) && httpguts.ValidTrailerHeader(strings.TrimPrefix(name, http.TrailerPrefix)) {
			trailers[strings.TrimPrefix(name, http.TrailerPrefix)] = values
		}
	}
	if len(trailers) != 0 {
		payload, err := json.Marshal(trailers)
		if err != nil {
			return err
		}
		if len(payload) > maxFrame {
			return errors.New("native HTTP response trailers exceed bridge limit")
		}
		if err := writer.connection.awaitWriteCredit(); err != nil {
			return err
		}
		if err := writer.connection.sendFrame(frame{kind: httpTrailers, id: writer.connection.id, data: payload}); err != nil {
			return err
		}
	}
	return writer.connection.CloseWrite()
}
