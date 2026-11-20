package hysteria

import (
	"context"
	"crypto/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/apernet/quic-go/quicvarint"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy/hysteria/account"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/handoff"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/bbr"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type httpHandler struct {
	sync.Mutex

	validator   *account.Validator
	config      *Config
	masqHandler http.Handler
	quicParams  *internet.QuicParams
	addConn     internet.ConnHandler
	conn        *quic.Conn

	auth bool
	user *protocol.MemoryUser
}

func (h *httpHandler) AuthHTTP(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodPost && r.Host == URLHost && r.URL.Path == URLPath {
		h.Lock()
		defer h.Unlock()

		udpEnabled := h.addConn != nil
		if h.auth {
			w.Header().Set(ResponseHeaderUDPEnabled, strconv.FormatBool(udpEnabled))
			w.Header().Set(CommonHeaderCCRX, strconv.FormatUint(h.quicParams.BrutalDown, 10))
			w.Header().Set(CommonHeaderPadding, AuthResponsePadding.String())
			w.WriteHeader(StatusAuthOK)
			return true
		}

		auth := r.Header.Get(RequestHeaderAuth)
		down, _ := strconv.ParseUint(r.Header.Get(CommonHeaderCCRX), 10, 64)

		var user *protocol.MemoryUser
		var ok bool
		if h.validator != nil && h.validator.NotEmpty() {
			user = h.validator.Get(auth)
		} else if h.config.Auth != "" {
			ok = auth == h.config.Auth
		}

		if user != nil || ok {
			h.auth = true
			h.user = user

			conn := h.conn
			quicParams := h.quicParams
			switch quicParams.Congestion {
			case "reno":
			case "bbr":
				congestion.UseBBR(conn, bbr.Profile(quicParams.BbrProfile))
			case "", "brutal":
				if quicParams.BrutalUp == 0 || down == 0 {
					congestion.UseBBR(conn, bbr.Profile(quicParams.BbrProfile))
				} else {
					congestion.UseBrutal(conn, min(quicParams.BrutalUp, down), quicParams.BrutalDisableLossCompensation)
				}
			case "force-brutal":
				congestion.UseBrutal(conn, quicParams.BrutalUp, quicParams.BrutalDisableLossCompensation)
			default:
				panic(quicParams.Congestion)
			}

			if udpEnabled {
				udpSM := &udpSessionManager{
					conn: h.conn,
					m:    make(map[uint32]*InterConn),

					addConn:        h.addConn,
					udpIdleTimeout: time.Duration(h.config.UdpIdleTimeout) * time.Second,
					user:           h.user,
				}
				go udpSM.clean()
				go udpSM.run()
			}

			w.Header().Set(ResponseHeaderUDPEnabled, strconv.FormatBool(udpEnabled))
			w.Header().Set(CommonHeaderCCRX, strconv.FormatUint(h.quicParams.BrutalDown, 10))
			w.Header().Set(CommonHeaderPadding, AuthResponsePadding.String())
			w.WriteHeader(StatusAuthOK)
			return true
		}
	}
	return false
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.AuthHTTP(w, r) {
		return
	}
	h.masqHandler.ServeHTTP(w, r)
}

func (h *httpHandler) StreamDispatcher(ft http3.FrameType, stream *quic.Stream, err error) (bool, error) {
	h.Lock()
	authenticated, user := h.auth, h.user
	h.Unlock()

	if err != nil || !authenticated {
		return false, nil
	}

	switch ft {
	case FrameTypeTCPRequest:
		if _, err := quicvarint.Read(quicvarint.NewReader(stream)); err != nil {
			return false, err
		}

		h.addConn(&interConn{
			stream: stream,
			local:  h.conn.LocalAddr(),
			remote: h.conn.RemoteAddr(),

			user: user,
			lock: make(chan struct{}, 1),
		})
		return true, nil
	default:
		return false, nil
	}
}

type Listener struct {
	validator   *account.Validator
	config      *Config
	masqHandler http.Handler
	quicParams  *internet.QuicParams
	addConn     internet.ConnHandler

	pktConn  net.PacketConn
	tr       *quic.Transport
	listener *quic.Listener
	tracked  bool
	live     atomic.Uint64
	closed   atomic.Bool
	work     *generationWork
}

func (l *Listener) handleClient(conn *quic.Conn) {
	handler := &httpHandler{
		validator:   l.validator,
		config:      l.config,
		masqHandler: l.masqHandler,
		quicParams:  l.quicParams,
		addConn:     l.addConn,
		conn:        conn,
	}
	h3s := http3.Server{
		Handler:          handler,
		StreamDispatcher: handler.StreamDispatcher,
	}
	err := h3s.ServeQUICConn(conn)
	_ = conn.CloseWithError(closeErrCodeOK, "")
	errors.LogDebug(context.Background(), conn.RemoteAddr(), " ServeQUICConn exited with ", err)
}

func (l *Listener) keepAccepting() {
	defer l.closed.Store(true)
	for {
		conn, err := l.listener.Accept(context.Background())
		if err != nil {
			if err != quic.ErrServerClosed {
				errors.LogErrorInner(context.Background(), err, "failed to serve hysteria")
			}
			break
		}
		if !l.tracked {
			go l.handleClient(conn)
			continue
		}
		l.live.Add(1)
		go func() {
			defer l.live.Add(^uint64(0))
			l.handleClient(conn)
		}()
	}
}

func (l *Listener) GenerationConnections() (uint64, bool) {
	return l.live.Load(), l.tracked && !l.closed.Load()
}

// ConnContext is called before the handshake is accepted and its context is
// cancelled on handshake failure or connection close. Track this separately
// from accepted HTTP/3 handlers, which may still be winding down afterwards.
type generationWork struct {
	count   atomic.Uint64
	mu      sync.Mutex
	retired bool
}

func (w *generationWork) track(ctx context.Context, _ *quic.ClientInfo) (context.Context, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.retired {
		return nil, errors.New("managed QUIC generation retired")
	}
	w.count.Add(1)
	context.AfterFunc(ctx, func() { w.count.Add(^uint64(0)) })
	return ctx, nil
}

// TryDrainGeneration is irreversible only on success. The private queue marker
// must be observed first; the QUIC server then evaluates idle after every
// previously queued unknown-CID packet, including pre-Accept handshakes.
func (l *Listener) TryDrainGeneration(token [16]byte) (bool, error) {
	fence, ok := l.pktConn.(interface{ DrainFenceSeen([16]byte) bool })
	if !l.tracked || l.work == nil || !ok || !fence.DrainFenceSeen(token) {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return l.listener.TryRetireIdle(ctx, func() bool {
		l.work.mu.Lock()
		defer l.work.mu.Unlock()
		if l.work.count.Load() != 0 || l.live.Load() != 0 {
			return false
		}
		l.work.retired = true
		return true
	})
}

func (l *Listener) GenerationStatus() handoff.UDPConnectionStatus {
	count, known := l.GenerationConnections()
	status := handoff.UDPConnectionStatus{Count: count, Known: known}
	if l.work != nil && known {
		status.TransportWork = l.work.count.Load()
		status.WorkKnown = !l.closed.Load()
	}
	return status
}

func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}

func (l *Listener) Close() error {
	l.closed.Store(true)
	return errors.Combine(l.listener.Close(), l.tr.Close(), l.pktConn.Close())
}

func Listen(ctx context.Context, address net.Address, port net.Port, streamSettings *internet.MemoryStreamConfig, handler internet.ConnHandler) (internet.Listener, error) {
	if nativeRequested(streamSettings) {
		return nativeListen(ctx, address, port, streamSettings, handler)
	}
	if address.Family().IsDomain() {
		return nil, errors.New("address is domain")
	}

	tlsConfig := tls.ConfigFromStreamSettings(streamSettings)
	if tlsConfig == nil {
		return nil, errors.New("tls config is nil")
	}

	validator := ValidatorFromContext(ctx)
	config := streamSettings.ProtocolSettings.(*Config)

	if validator == nil && config.Auth == "" {
		return nil, errors.New("validator is nil")
	}

	masqHandler, err := newMasqueradeHandler(config)
	if err != nil {
		return nil, err
	}

	quicParams := streamSettings.QuicParams
	if quicParams == nil {
		quicParams = &internet.QuicParams{
			BbrProfile: string(bbr.ProfileStandard),
		}
	}

	quicConfig := &quic.Config{
		InitialStreamReceiveWindow:     quicParams.InitStreamReceiveWindow,
		MaxStreamReceiveWindow:         quicParams.MaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: quicParams.InitConnReceiveWindow,
		MaxConnectionReceiveWindow:     quicParams.MaxConnReceiveWindow,
		MaxIdleTimeout:                 time.Duration(quicParams.MaxIdleTimeout) * time.Second,
		KeepAlivePeriod:                time.Duration(quicParams.KeepAlivePeriod) * time.Second,
		MaxIncomingStreams:             quicParams.MaxIncomingStreams,
		DisablePathMTUDiscovery:        quicParams.DisablePathMtuDiscovery || (runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin"),
		EnableDatagrams:                true,
		MaxDatagramFrameSize:           MaxDatagramFrameSize,
		AssumePeerMaxDatagramFrameSize: MaxDatagramFrameSize,
		DisablePathManager:             true,
	}
	if quicParams.InitStreamReceiveWindow == 0 {
		quicConfig.InitialStreamReceiveWindow = 8388608
	}
	if quicParams.MaxStreamReceiveWindow == 0 {
		quicConfig.MaxStreamReceiveWindow = 8388608
	}
	if quicParams.InitConnReceiveWindow == 0 {
		quicConfig.InitialConnectionReceiveWindow = 8388608 * 5 / 2
	}
	if quicParams.MaxConnReceiveWindow == 0 {
		quicConfig.MaxConnectionReceiveWindow = 8388608 * 5 / 2
	}
	if quicParams.MaxIdleTimeout == 0 {
		quicConfig.MaxIdleTimeout = 30 * time.Second
	}
	if quicParams.MaxIncomingStreams == 0 {
		quicConfig.MaxIncomingStreams = 1024
	}

	var pktConn net.PacketConn
	ingress := generationFromContext(ctx)
	if ingress != nil {
		if export, ok := ingress.packet.(interface{ RingFiles() [3]*os.File }); ok && export.RingFiles()[0] != nil {
			return nil, errors.New("managed packet ring is supported by native Hysteria only")
		}
		if export, ok := ingress.packet.(interface{ TransmitFile() (*os.File, error) }); ok {
			file, err := export.TransmitFile()
			if err != nil {
				return nil, err
			}
			if file != nil {
				file.Close()
				return nil, errors.New("direct managed UDP transmit is supported by native Hysteria only")
			}
		}
		pktConn = ingress.packet
	} else if streamSettings.FinalMask != nil {
		pktConn, err = streamSettings.FinalMask.ListenPacket(context.Background(), &net.UDPAddr{IP: address.IP(), Port: int(port)})
	} else {
		pktConn, err = internet.ListenSystemPacket(context.Background(), &net.UDPAddr{IP: address.IP(), Port: int(port)}, streamSettings.SocketSettings)
	}
	if err != nil {
		return nil, err
	}

	var k *quic.StatelessResetKey
	if !quicParams.DisableStatelessReset {
		k = &quic.StatelessResetKey{}
		common.Must2(rand.Read((*k)[:]))
	}

	tr := &quic.Transport{Conn: pktConn, DisableGSO: quicParams.DisableGSO, StatelessResetKey: k}
	var work *generationWork
	if ingress != nil {
		work = new(generationWork)
		tr.ConnContext = work.track
		tr.ConnectionIDGenerator = generationCIDGenerator{codec: handoff.NewQUICCIDCodec(ingress.key), slot: ingress.slot}
	}

	listener, err := tr.Listen(tlsConfig.GetTLSConfig(tls.WithNextProto("h3")), quicConfig)
	if err != nil {
		_ = tr.Close()
		_ = pktConn.Close()
		return nil, err
	}

	l := &Listener{
		validator:   validator,
		config:      config,
		masqHandler: masqHandler,
		quicParams:  quicParams,
		addConn:     handler,

		pktConn:  pktConn,
		tr:       tr,
		listener: listener,
		tracked:  ingress != nil,
		work:     work,
	}

	go l.keepAccepting()

	return l, nil
}

func init() {
	common.Must(internet.RegisterTransportListener(protocolName, Listen))
}

const (
	masqueradeProxyBufferSize          = 32 * 1024
	masqueradeProxyMaxIdleConnections  = 100
	masqueradeProxyMaxIdleConnsPerHost = 32
)

type masqueradeProxyBufferPool struct {
	pool sync.Pool
}

func newMasqueradeProxyBufferPool() *masqueradeProxyBufferPool {
	return &masqueradeProxyBufferPool{
		pool: sync.Pool{
			New: func() any {
				return make([]byte, masqueradeProxyBufferSize)
			},
		},
	}
}

func (p *masqueradeProxyBufferPool) Get() []byte {
	return p.pool.Get().([]byte)
}

func (p *masqueradeProxyBufferPool) Put(buf []byte) {
	if cap(buf) < masqueradeProxyBufferSize {
		return
	}
	p.pool.Put(buf[:masqueradeProxyBufferSize])
}
