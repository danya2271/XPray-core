package hysteria

import (
	"context"
	gotls "crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/bbr"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type client struct {
	lock         chan struct{}
	instance     *core.Instance
	forced       bool
	dest         net.Destination
	config       *Config
	tlsConfig    *gotls.Config
	socketConfig *internet.SocketConfig
	finalMask    *finalmask.FinalMask
	quicParams   *internet.QuicParams

	conn      *quic.Conn
	tr        *quic.Transport
	pktConn   net.PacketConn
	udpSM     *udpSessionManager
	aliveTCP  *atomic.Int64
	lastAlive time.Time
}

func (c *client) status() status {
	if c.conn == nil {
		return StatusNull
	}
	select {
	case <-c.conn.Context().Done():
		return StatusInactive
	default:
		return StatusActive
	}
}

func retryIfInactive[T any](
	ctx context.Context,
	currentStatus func() status,
	redial func(context.Context) error,
	open func() (T, error),
) (T, error) {
	result, err := open()
	if err == nil || currentStatus() != StatusInactive {
		return result, err
	}

	var zero T
	if err := redial(ctx); err != nil {
		return zero, err
	}
	return open()
}

func (c *client) close() {
	c.conn.CloseWithError(closeErrCodeOK, "")
	c.tr.Close()
	c.pktConn.Close()
	c.conn = nil
	c.tr = nil
	c.pktConn = nil
	c.udpSM = nil
}

func (c *client) dial(ctx context.Context) error {
	if c.forced {
		return errors.New("client is closed")
	}

	switch c.status() {
	case StatusActive:
		return nil
	case StatusInactive:
		c.close()
	}

	quicParams := c.quicParams
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
		DisablePathMTUDiscovery:        quicParams.DisablePathMtuDiscovery || (runtime.GOOS != "linux" && runtime.GOOS != "windows" && runtime.GOOS != "darwin"),
		ChromeParrot:                   !quicParams.DisableChromeParrot,
		EnableDatagrams:                true,
		MaxDatagramFrameSize:           MaxDatagramFrameSize,
		OmitMaxDatagramFrameSize:       true,
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
	if quicParams.KeepAlivePeriod == 0 {
		quicConfig.KeepAlivePeriod = net.QuicgoH3KeepAlivePeriod
	}

	var pktConn net.PacketConn
	var udpAddr net.Addr
	if c.finalMask != nil {
		conn, err := c.finalMask.DialUDP(ctx, c.dest)
		if err != nil {
			return errors.New("failed to dial to dest").Base(err)
		}
		pktConn = conn.(*net.PacketConnWrapper).PacketConn
		udpAddr = conn.RemoteAddr()
	} else {
		conn, err := internet.DialSystem(ctx, c.dest, c.socketConfig)
		if err != nil {
			return errors.New("failed to dial to dest").Base(err)
		}
		switch c := conn.(type) {
		case *net.PacketConnWrapper:
			pktConn = c.PacketConn
			udpAddr = c.RemoteAddr()
		case *cnc.Connection:
			pktConn = &internet.FakePacketConn{Conn: c}
			udpAddr = &net.UDPAddr{IP: []byte{0, 0, 0, 0}}
		default:
			panic(reflect.TypeOf(c))
		}
	}

	tr := &quic.Transport{Conn: pktConn, DisableGSO: quicParams.DisableGSO}

	if !quicParams.DisableChromeParrot {
		tr.ConnectionIDGenerator = quic.ZeroLengthConnectionIDGenerator{}
		c.tlsConfig.GetCertificate = nil
	}

	var conn *quic.Conn
	rt := &http3.Transport{
		TLSClientConfig: c.tlsConfig,
		QUICConfig:      quicConfig,
		Dial: func(ctx context.Context, _ string, tlsCfg *gotls.Config, cfg *quic.Config) (*quic.Conn, error) {
			qc, err := tr.DialEarly(ctx, udpAddr, tlsCfg, cfg)
			if err != nil {
				return nil, err
			}
			conn = qc
			return qc, nil
		},
	}
	req := &http.Request{
		Method: http.MethodPost,
		URL: &url.URL{
			Scheme: "https",
			Host:   URLHost,
			Path:   URLPath,
		},
		Header: http.Header{
			RequestHeaderAuth:   []string{c.config.Auth},
			CommonHeaderCCRX:    []string{strconv.FormatUint(quicParams.BrutalDown, 10)},
			CommonHeaderPadding: []string{AuthRequestPadding.String()},
		},
	}
	resp, err := rt.RoundTrip(req.WithContext(ctx))
	if err != nil {
		_ = rt.Close()
		if conn != nil {
			_ = conn.CloseWithError(closeErrCodeProtocolError, "")
		}
		_ = tr.Close()
		_ = pktConn.Close()
		return err
	}
	_ = resp.Body.Close()

	if resp.StatusCode != StatusAuthOK {
		_ = rt.Close()
		_ = conn.CloseWithError(closeErrCodeProtocolError, "")
		_ = tr.Close()
		_ = pktConn.Close()
		return errors.New("auth failed code ", resp.StatusCode)
	}

	udpEnabled := parseUDPEnabled(resp.Header.Get(ResponseHeaderUDPEnabled))
	down, _ := strconv.ParseUint(resp.Header.Get(CommonHeaderCCRX), 10, 64)
	errors.LogDebug(context.Background(), "ECHAccepted ", conn.ConnectionState().TLS.ECHAccepted)

	selectedCongestion := "bbr"
	switch quicParams.Congestion {
	case "reno":
		selectedCongestion = "reno"
	case "bbr":
		selectedCongestion = "bbr"
		congestion.UseBBR(conn, bbr.Profile(quicParams.BbrProfile))
	case "", "brutal":
		if quicParams.BrutalUp == 0 || down == 0 {
			congestion.UseBBR(conn, bbr.Profile(quicParams.BbrProfile))
		} else {
			selectedCongestion = "brutal"
			congestion.UseBrutal(conn, min(quicParams.BrutalUp, down), quicParams.BrutalDisableLossCompensation)
		}
	case "force-brutal":
		selectedCongestion = "force-brutal"
		congestion.UseBrutal(conn, quicParams.BrutalUp, quicParams.BrutalDisableLossCompensation)
	default:
		panic(quicParams.Congestion)
	}

	c.pktConn = pktConn
	c.tr = tr
	c.conn = conn
	c.aliveTCP = &atomic.Int64{}
	c.lastAlive = time.Now()
	if udpEnabled {
		c.udpSM = &udpSessionManager{
			conn: conn,
			m:    make(map[uint32]*InterConn),
			next: 1,
		}
		go c.udpSM.run()
	} else {
		c.udpSM = nil
	}
	if os.Getenv("XRAY_NET_DIAG") == "1" {
		fmt.Printf("[XRAY_NET_DIAG][HYSTERIA] connected peer=%s cc=%s\n", c.dest, selectedCongestion)
		go reportConnectionStats(conn, c.dest, c.aliveTCP, c.udpSM)
	}

	return nil
}

func reportConnectionStats(conn *quic.Conn, dest net.Destination, aliveTCP *atomic.Int64, udpSM *udpSessionManager) {
	var previousSent, previousLost uint64
	sampled := time.Now()
	logStats := func() {
		stats := conn.ConnectionStats()
		sent := stats.PacketsSent - previousSent
		lost := stats.PacketsLost - previousLost
		windowPercent := "n/a"
		if sent > 0 {
			windowPercent = fmt.Sprintf("%.3f", 100*float64(lost)/float64(sent))
		}
		udpSessions := 0
		if udpSM != nil {
			udpSM.RLock()
			udpSessions = len(udpSM.m)
			udpSM.RUnlock()
		}
		fmt.Printf(
			"[XRAY_NET_DIAG][HYSTERIA] peer=%s rtt_ms=%d/%d/%d tx_bytes=%d tx_packets=%d rx_bytes=%d rx_packets=%d lost_packets=%d tcp=%d udp=%d loss_total_pct=%.3f sample_ms=%d sent_delta=%d lost_delta=%d loss_window_pct=%s\n",
			dest,
			stats.LatestRTT.Milliseconds(),
			stats.SmoothedRTT.Milliseconds(),
			stats.MinRTT.Milliseconds(),
			stats.BytesSent,
			stats.PacketsSent,
			stats.BytesReceived,
			stats.PacketsReceived,
			stats.PacketsLost,
			aliveTCP.Load(),
			udpSessions,
			100*float64(stats.PacketsLost)/float64(max(1, stats.PacketsSent)),
			time.Since(sampled).Milliseconds(),
			sent,
			lost,
			windowPercent,
		)
		previousSent, previousLost = stats.PacketsSent, stats.PacketsLost
		sampled = time.Now()
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	logStats()
	for {
		select {
		case <-conn.Context().Done():
			logStats()
			return
		case <-ticker.C:
			logStats()
		}
	}
}

func (c *client) unlock() {
	<-c.lock
}

func (c *client) tcp(ctx context.Context) (stat.Connection, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.lock <- struct{}{}:
		defer c.unlock()
	}

	err := c.dial(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := retryIfInactive(ctx, c.status, c.dial, func() (*quic.Stream, error) {
		return c.conn.OpenStream()
	})
	if err != nil {
		return nil, err
	}

	c.lastAlive = time.Now()
	c.aliveTCP.Add(1)
	return &interConn{
		stream: stream,
		local:  c.conn.LocalAddr(),
		remote: c.conn.RemoteAddr(),

		client:   true,
		lock:     make(chan struct{}, 1),
		aliveTCP: c.aliveTCP,
	}, nil
}

func (c *client) udp(ctx context.Context) (stat.Connection, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.lock <- struct{}{}:
		defer c.unlock()
	}

	err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	if c.udpSM == nil {
		return nil, errors.New("server does not support UDP relay")
	}

	return retryIfInactive(ctx, c.status, c.dial, func() (*InterConn, error) {
		return c.udpSM.udp()
	})
}

func parseUDPEnabled(value string) bool {
	enabled, err := strconv.ParseBool(value)
	return err == nil && enabled
}

func (c *client) clean(force bool) {
	c.lock <- struct{}{}
	defer c.unlock()
	if force {
		c.forced = true
	}

	status := c.status()
	if status == StatusInactive || force && status != StatusNull {
		c.close()
		return
	}
	if status != StatusActive {
		return
	}

	udpSessions := 0
	if c.udpSM != nil {
		c.udpSM.RLock()
		udpSessions = len(c.udpSM.m)
		c.udpSM.RUnlock()
	}
	if udpSessions == 0 && (c.aliveTCP == nil || c.aliveTCP.Load() == 0) {
		if time.Since(c.lastAlive) >= net.ConnIdleTimeout {
			c.close()
			return
		}
	} else {
		c.lastAlive = time.Now()
	}
}

type dialerConf struct {
	net.Destination
	*internet.MemoryStreamConfig
}

type clientManager struct {
	sync.RWMutex
	m map[dialerConf]*client
}

const (
	nativeDialAttemptTimeout  = 11 * time.Second
	nativeDialFailureCooldown = 30 * time.Second
	nativeFallbackIdle        = 10 * time.Minute
)

type nativeFallbackKey struct {
	instance *core.Instance
	settings *internet.MemoryStreamConfig
	peer     string
}

type nativeFallbackEntry struct {
	until    time.Time
	lastUsed time.Time
	probing  bool
	healthy  bool
	changed  chan struct{}
}

type nativeDialFallback struct {
	mu             sync.Mutex
	entries        map[nativeFallbackKey]nativeFallbackEntry
	cooldown       time.Duration
	attemptTimeout time.Duration
}

var nativeFallback = nativeDialFallback{
	entries: make(map[nativeFallbackKey]nativeFallbackEntry),
}

func (b *nativeDialFallback) open(
	ctx context.Context,
	key nativeFallbackKey,
	tryNative func(context.Context) (stat.Connection, error),
	tryGo func(context.Context) (stat.Connection, error),
) (stat.Connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		now := time.Now()
		b.mu.Lock()
		if b.entries == nil {
			b.entries = make(map[nativeFallbackKey]nativeFallbackEntry)
		}
		if len(b.entries) > 512 {
			for candidate, entry := range b.entries {
				expired := !entry.until.IsZero() && !now.Before(entry.until)
				idle := !entry.lastUsed.IsZero() && now.Sub(entry.lastUsed) >= nativeFallbackIdle
				if !entry.probing && (expired || idle) {
					delete(b.entries, candidate)
				}
			}
		}
		entry := b.entries[key]
		entry.lastUsed = now
		if entry.probing {
			b.entries[key] = entry
			changed := entry.changed
			b.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-changed:
				continue
			}
		}
		if now.Before(entry.until) {
			b.entries[key] = entry
			b.mu.Unlock()
			return tryGo(ctx)
		}
		var probe chan struct{}
		if !entry.healthy {
			entry.probing = true
			entry.changed = make(chan struct{})
			probe = entry.changed
		}
		b.entries[key] = entry
		b.mu.Unlock()

		timeout := b.attemptTimeout
		if timeout <= 0 {
			timeout = nativeDialAttemptTimeout
		}
		nativeCtx, cancel := context.WithTimeout(ctx, timeout)
		conn, nativeErr := tryNative(nativeCtx)
		cancel()
		if nativeErr == nil {
			if probe != nil {
				b.finishProbe(key, probe, true, time.Time{})
			}
			return conn, nil
		}
		if ctx.Err() != nil {
			if probe != nil {
				b.finishProbe(key, probe, false, time.Time{})
			}
			return nil, nativeErr
		}

		cooldown := b.cooldown
		if cooldown <= 0 {
			cooldown = nativeDialFailureCooldown
		}
		if probe != nil {
			b.finishProbe(key, probe, false, time.Now().Add(cooldown))
		} else {
			b.mu.Lock()
			b.entries[key] = nativeFallbackEntry{until: time.Now().Add(cooldown), lastUsed: time.Now()}
			b.mu.Unlock()
		}

		if os.Getenv("XRAY_NET_DIAG") == "1" {
			fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] native dial failed; using Go backend peer=%s cooldown=%s reason=%v\n", key.peer, cooldown, nativeErr)
		}
		conn, goErr := tryGo(ctx)
		if goErr != nil {
			return nil, fmt.Errorf("native dial failed (%v); Go fallback failed: %w", nativeErr, goErr)
		}
		return conn, nil
	}
}

func (b *nativeDialFallback) finishProbe(key nativeFallbackKey, probe chan struct{}, healthy bool, until time.Time) {
	b.mu.Lock()
	entry, ok := b.entries[key]
	if ok && entry.probing && entry.changed == probe {
		entry.probing = false
		entry.healthy = healthy
		entry.until = until
		entry.lastUsed = time.Now()
		entry.changed = nil
		if !healthy && until.IsZero() {
			delete(b.entries, key)
		} else {
			b.entries[key] = entry
		}
		close(probe)
	}
	b.mu.Unlock()
}

func (m *clientManager) clean() {
	ticker := time.NewTicker(idleCleanupInterval)
	for range ticker.C {
		m.RLock()
		entries := make(map[dialerConf]*client, len(m.m))
		for k, c := range m.m {
			entries[k] = c
		}
		m.RUnlock()
		for k, c := range entries {
			force := c.instance != nil && !c.instance.IsRunning()
			c.clean(force)
			if force {
				m.Lock()
				delete(m.m, k)
				m.Unlock()
			}
		}
	}
}

var (
	manager     *clientManager
	initmanager sync.Once
)

func Dial(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (stat.Connection, error) {
	if nativeRequested(streamSettings) {
		peer := dest
		peer.Network = net.Network_UDP
		key := nativeFallbackKey{
			instance: core.FromContext(ctx),
			settings: streamSettings,
			peer:     peer.String(),
		}
		return nativeFallback.open(
			ctx,
			key,
			func(nativeCtx context.Context) (stat.Connection, error) {
				return nativeDial(nativeCtx, dest, streamSettings)
			},
			func(goCtx context.Context) (stat.Connection, error) {
				return dialGo(goCtx, dest, streamSettings)
			},
		)
	}
	return dialGo(ctx, dest, streamSettings)
}

func dialGo(ctx context.Context, dest net.Destination, streamSettings *internet.MemoryStreamConfig) (stat.Connection, error) {
	tlsConfig := tls.ConfigFromStreamSettings(streamSettings)
	if tlsConfig == nil {
		return nil, errors.New("tls config is nil")
	}

	datagram := DatagramFromContext(ctx)
	dest.Network = net.Network_UDP

	initmanager.Do(func() {
		manager = &clientManager{
			m: make(map[dialerConf]*client),
		}
		go manager.clean()
	})

	dialerConfKey := dialerConf{dest, streamSettings}

	manager.RLock()
	c := manager.m[dialerConfKey]
	manager.RUnlock()

	if c == nil {
		manager.Lock()
		c = manager.m[dialerConfKey]
		if c == nil {
			c = &client{
				lock:         make(chan struct{}, 1),
				instance:     core.FromContext(ctx),
				dest:         dest,
				config:       streamSettings.ProtocolSettings.(*Config),
				tlsConfig:    tlsConfig.GetTLSConfig(tls.WithDestination(dest)),
				socketConfig: streamSettings.SocketSettings,
				finalMask:    streamSettings.FinalMask,
				quicParams:   streamSettings.QuicParams,
			}
			manager.m[dialerConfKey] = c
		}
		manager.Unlock()
	}

	if datagram {
		return c.udp(ctx)
	}
	return c.tcp(ctx)
}

func init() {
	common.Must(internet.RegisterTransportDialer(protocolName, Dial))
}
