package splithttp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"path/filepath"
	"strconv"
	"time"
)

const (
	handoffLookup = "XRAY-SESSION-OWNER"
	handoffRemote = "X-Xray-Handoff-Remote-Addr"
	handoffProto  = "X-Xray-Handoff-Proto"
)

type handoffPeer struct {
	acceptsNew bool
	transport  *http.Transport
	proxy      *httputil.ReverseProxy
}

type forwardedRequest struct{}

// SetHandoffPeer prepares a TCP/Unix XHTTP listener to overlap another
// generation. Configure the replacement with acceptsNew=true before allowing
// it to receive traffic; then set acceptsNew=false on the incumbent to seal
// its session table against new IDs. The caller must publish the new ingress
// only after the incumbent has been sealed.
//
// The peer must serve HandoffHandler on a private, same-user Unix socket. Its
// HTTP server must allow cleartext HTTP/2 so a forwarded stream is not buffered
// and both request and response bodies can flow concurrently.
func (l *Listener) SetHandoffPeer(socket string, acceptsNew bool) error {
	if l.isH3 || l.handler == nil || !filepath.IsAbs(socket) {
		return fmt.Errorf("XHTTP handoff requires a TCP/Unix inbound and an absolute peer socket path")
	}
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	transport := &http.Transport{
		Protocols:          protocols,
		IdleConnTimeout:    90 * time.Second,
		DisableCompression: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socket)
		},
	}
	peer := &handoffPeer{acceptsNew: acceptsNew, transport: transport}
	peer.proxy = &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL.Scheme, request.Out.URL.Host = "http", "xray-handoff.local"
			request.Out.Host = request.In.Host
			// ReverseProxy removes these headers before Rewrite. Keep the
			// original frontend metadata, not a new hop from the Unix socket.
			for _, key := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				if values := request.In.Header.Values(key); values != nil {
					request.Out.Header[key] = values
				}
			}
			request.Out.Header.Set(handoffRemote, request.In.RemoteAddr)
			request.Out.Header.Set(handoffProto, strconv.Itoa(request.In.ProtoMajor))
		},
	}
	l.handler.setPeer(peer)
	return nil
}

// ClearHandoffPeer returns to local-only routing after the other generation
// has drained. It does not terminate any forwarded streams still in flight.
func (l *Listener) ClearHandoffPeer() { l.handler.setPeer(nil) }

func (h *requestHandler) setPeer(peer *handoffPeer) {
	h.sessionMu.Lock()
	old := h.peer.Swap(peer)
	h.sessionMu.Unlock()
	if old != nil {
		old.transport.CloseIdleConnections()
	}
}

// HandoffHandler is a private per-inbound endpoint, not a public HTTP handler.
// In particular it accepts trusted forwarding metadata from its Unix peer.
func (l *Listener) HandoffHandler() http.Handler {
	h := l.handler
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == handoffLookup {
			if _, ok := h.sessions.Load(r.URL.Query().Get("id")); ok {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		r.RemoteAddr = r.Header.Get(handoffRemote)
		r.ProtoMajor, _ = strconv.Atoi(r.Header.Get(handoffProto))
		r.Header.Del(handoffRemote)
		r.Header.Del(handoffProto)
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), forwardedRequest{}, true)))
	})
}

func (p *handoffPeer) owns(ctx context.Context, id string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, handoffLookup, "http://xray-handoff.local/", nil)
	if err != nil {
		return false, err
	}
	query := request.URL.Query()
	query.Set("id", id)
	request.URL.RawQuery = query.Encode()
	response, err := p.transport.RoundTrip(request)
	if err != nil {
		return false, err
	}
	response.Body.Close()
	switch response.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("invalid handoff lookup response: %d", response.StatusCode)
	}
}

// resolveSession returns handled=true after forwarding or rejecting a request.
// Most requests, including all packets for a known local ID, never enter the
// peer path. The configuration transition and session creation share a mutex:
// once the incumbent is sealed, it cannot race the peer and claim a new ID.
func (h *requestHandler) resolveSession(w http.ResponseWriter, r *http.Request, id string) (session *httpSession, handled bool) {
	for {
		if id != "" {
			if session, ok := h.sessions.Load(id); ok {
				return session.(*httpSession), false
			}
		}
		peer := h.peer.Load()
		if peer != nil {
			forwarded := r.Context().Value(forwardedRequest{}) != nil
			if !peer.acceptsNew {
				if forwarded {
					// The ID expired after a successful lookup. Never create a
					// second copy here or bounce the request between peers.
					w.WriteHeader(http.StatusGone)
				} else {
					forwardToPeer(peer, w, r)
				}
				return nil, true
			}
			if id != "" && !forwarded {
				owns, err := peer.owns(r.Context(), id)
				if err != nil {
					// An unavailable peer is not equivalent to an unknown ID.
					w.WriteHeader(http.StatusServiceUnavailable)
					return nil, true
				}
				if h.peer.Load() != peer {
					continue
				}
				if owns {
					forwardToPeer(peer, w, r)
					return nil, true
				}
			}
		}
		if id == "" {
			return nil, false
		}
		h.sessionMu.Lock()
		if h.peer.Load() != peer {
			h.sessionMu.Unlock()
			continue
		}
		session = h.upsertSessionLocked(id)
		h.sessionMu.Unlock()
		return session, false
	}
}

func forwardToPeer(peer *handoffPeer, w http.ResponseWriter, r *http.Request) {
	// The destination handler writes its own XHTTP response headers; do not
	// append them to the preliminary padding and CORS headers in this handler.
	clear(w.Header())
	peer.proxy.ServeHTTP(w, r)
}
