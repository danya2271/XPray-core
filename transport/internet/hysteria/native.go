//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"context"
	"crypto/subtle"
	cryptotls "crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	stdnet "net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask/mkcp/header"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"github.com/xtls/xray-core/transport/internet/finalmask/udphop"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
	"golang.org/x/net/http/httpguts"
)

func nativeConfig(settings *internet.MemoryStreamConfig) (native.Config, error) {
	var c native.Config
	t := tls.ConfigFromStreamSettings(settings)
	if t == nil {
		return c, fmt.Errorf("native Hysteria requires TLS")
	}
	q := settings.QuicParams
	if q == nil || !q.DisableChromeParrot {
		return c, fmt.Errorf("native prototype requires quicParams.disableChromeParrot=true")
	}
	if q.BbrProfile != "" && q.BbrProfile != "standard" {
		return c, fmt.Errorf("native prototype supports only the standard BBR profile")
	}
	switch q.Congestion {
	case "", "brutal", "force-brutal", "bbr", "reno":
	default:
		return c, fmt.Errorf("unsupported native congestion mode %q", q.Congestion)
	}
	if q.Congestion == "force-brutal" && q.BrutalUp == 0 {
		return c, fmt.Errorf("force-brutal requires a positive send rate")
	}
	if q.InitStreamReceiveWindow != 0 || q.MaxStreamReceiveWindow != 0 || q.InitConnReceiveWindow != 0 || q.MaxConnReceiveWindow != 0 || q.MaxIncomingStreams != 0 || q.DisableStatelessReset {
		return c, fmt.Errorf("native prototype does not support custom QUIC windows/stream limits/reset policy")
	}
	const maxTimerSeconds = int64((1<<63 - 1) / time.Second)
	if q.MaxIdleTimeout < 0 || q.MaxIdleTimeout > maxTimerSeconds || q.KeepAlivePeriod < 0 || q.KeepAlivePeriod > maxTimerSeconds {
		return c, fmt.Errorf("native QUIC timers must be non-negative duration-safe seconds")
	}
	if len(t.EchServerKeys) != 0 || t.EchConfigList != "" || t.Fingerprint != "" || len(t.PinnedPeerCertSha256) != 0 || len(t.VerifyPeerCertByName) != 0 || t.UseSystemCa || t.RejectUnknownSni || t.MasterKeyLog != "" || len(t.CurvePreferences) != 0 || t.CipherSuites != "" || t.EnableSessionResumption || (t.MinVersion != "" && t.MinVersion != "1.2" && t.MinVersion != "1.3") || (t.MaxVersion != "" && t.MaxVersion != "1.3") {
		return c, fmt.Errorf("native prototype does not support the selected advanced TLS settings")
	}
	for _, alpn := range t.NextProtocol {
		if alpn != "h3" {
			return c, fmt.Errorf("native Hysteria ALPN must be h3")
		}
	}
	c.ServerName = t.ServerName
	c.SystemRoots = !t.DisableSystemRoot
	c.TxBPS, c.RxBPS = q.BrutalUp, q.BrutalDown
	if c.TxBPS > uint64(^uint64(0)>>1)/2 {
		return c, fmt.Errorf("native Brutal rate exceeds supported range")
	}
	c.Congestion = q.Congestion
	c.DisablePMTUD = q.DisablePathMtuDiscovery || runtime.GOOS == "android"
	c.DisableGSO = q.DisableGSO
	c.MaxIdleTimeout = uint64(q.MaxIdleTimeout)
	c.KeepAlivePeriod = uint64(q.KeepAlivePeriod)
	c.LossCompensation = !q.BrutalDisableLossCompensation
	c.Diagnostic = os.Getenv("XRAY_NET_DIAG") == "1"
	c.ProcessWorkers = uint8(min(4, max(1, runtime.GOMAXPROCS(0))))
	memoryBytes, _ := nativeServerMemory()
	c.SendBufferBytes = 8 << 20
	c.ReceiveBufferBytes = uint32(min(uint64(8<<20), max(uint64(2<<20), memoryBytes/512)))
	c.ProcessSendBufferBytes = uint32(min(uint64(128<<20), max(uint64(8<<20), memoryBytes/16)))
	c.ProcessReceiveBufferBytes = c.ProcessSendBufferBytes
	for _, entry := range t.Certificate {
		certificate, key := entry.Certificate, entry.Key
		if (entry.CertificatePath != "" || entry.KeyPath != "") && !entry.OneTimeLoading && entry.Usage != tls.Certificate_ENCIPHERMENT {
			return c, fmt.Errorf("native verification certificate reload is not supported")
		}
		if entry.OcspStapling != 0 || entry.BuildChain {
			return c, fmt.Errorf("native prototype does not support OCSP or generated certificate chains")
		}
		switch entry.Usage {
		case tls.Certificate_AUTHORITY_VERIFY:
			c.CAPEM += string(certificate) + "\n"
		case tls.Certificate_ENCIPHERMENT:
			if c.CertificatePEM != "" {
				return c, fmt.Errorf("native prototype supports one server certificate")
			}
			pair, err := cryptotls.X509KeyPair(certificate, key)
			if err != nil {
				return c, fmt.Errorf("invalid native TLS key pair: %w", err)
			}
			normalized, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
			if err != nil {
				return c, err
			}
			c.CertificatePEM = string(certificate)
			c.KeyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: normalized}))
		default:
			return c, fmt.Errorf("native prototype does not support certificate issuing")
		}
	}
	if settings.FinalMask != nil {
		if settings.FinalMask.HasTCPMasks() {
			return c, fmt.Errorf("native Hysteria does not support TCP finalmasks")
		}
		masks := settings.FinalMask.UDPMasks()
		direct := true
		overhead := 0
		for index, mask := range masks {
			switch s := mask.(type) {
			case *salamander.Config:
				if len(s.Password) < 4 {
					return c, fmt.Errorf("native Salamander requires a password of at least four bytes")
				}
				c.UDPMasks = append(c.UDPMasks, native.UDPMaskConfig{Kind: "salamander", Password: s.Password})
				overhead += 8
			case *header.Config:
				layer := native.UDPMaskConfig{}
				switch header.HeaderID(s.ID) {
				case header.DNS:
					template, err := header.NewHeaderDNS(s.Domain)
					if err != nil {
						return c, fmt.Errorf("invalid native DNS mask: %w", err)
					}
					prefix := make([]byte, template.Size())
					template.Serialize(prefix)
					prefix[0], prefix[1] = 0, 0
					layer.Kind, layer.Prefix = "dns", hex.EncodeToString(prefix)
					overhead += len(prefix)
				case header.DTLS:
					layer.Kind = "dtls"
					overhead += 13
				case header.SRTP:
					layer.Kind = "srtp"
					overhead += 4
				case header.UTP:
					layer.Kind = "utp"
					overhead += 4
				case header.WECHAT:
					layer.Kind = "wechat"
					overhead += 13
				case header.WIREGUARD:
					layer.Kind = "wireguard"
					overhead += 4
				default:
					return c, fmt.Errorf("invalid native header mask ID %d", s.ID)
				}
				c.UDPMasks = append(c.UDPMasks, layer)
			case *udphop.Config:
				if index != 0 || c.PortHopping != nil {
					return c, fmt.Errorf("native UDP hopping must be the outermost network mask")
				}
				if s.Local {
					direct = false
					if _, err := nativeHoppingConfig(s, true); err != nil {
						return c, err
					}
					continue
				}
				var err error
				c.PortHopping, err = nativePortHopping(s)
				if err != nil {
					return c, err
				}
			default:
				direct = false
			}
		}
		if !direct || len(c.UDPMasks) > 16 || overhead > 252 {
			c.PacketBridge = true
			c.UDPMasks = nil
			c.PortHopping = nil
		} else if len(c.UDPMasks) == 1 && c.UDPMasks[0].Kind == "salamander" {
			c.Salamander = c.UDPMasks[0].Password
			c.UDPMasks = nil
		}
	}
	return c, nil
}

func nativeServerLimit(name string, fallback, maximum uint32) (uint32, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 || parsed > uint64(maximum) {
		return 0, fmt.Errorf("%s must be 1..%d", name, maximum)
	}
	return uint32(parsed), nil
}

func tuneNativeServerLimits(memoryBytes uint64) (uint32, uint32) {
	const (
		minConnections = uint64(32)
		minFlows       = uint64(64)
		maxConnections = uint64(4096)
		maxFlows       = uint64(16384)
		connectionKiB  = uint64(128)
		flowKiB        = uint64(128)
	)
	reserve := max(uint64(256<<20), memoryBytes*60/100)
	if memoryBytes <= reserve {
		return uint32(minConnections), uint32(minFlows)
	}
	budgetKiB := (memoryBytes - reserve) >> 10
	connectionBudgetKiB := budgetKiB * 40 / 100
	flowBudgetKiB := budgetKiB - connectionBudgetKiB
	connections := min(maxConnections, connectionBudgetKiB/connectionKiB)
	flows := min(maxFlows, flowBudgetKiB/flowKiB)
	connections = max(minConnections, connections)
	flows = max(minFlows, flows)
	return uint32(connections), uint32(flows)
}

func nativeMemoryLimit(path string, unlimitedAbove uint64) uint64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	value := strings.TrimSpace(string(data))
	if value == "" || value == "max" {
		return 0
	}
	limit, err := strconv.ParseUint(value, 10, 64)
	if err != nil || limit == 0 || limit >= unlimitedAbove {
		return 0
	}
	return limit
}

func nativeHostMemory() uint64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kib, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kib * 1024
	}
	return 0
}

func nativeHierarchyMemory(root, group, name string) uint64 {
	var minimum uint64
	for directory := filepath.Join(root, group); ; directory = filepath.Dir(directory) {
		if limit := nativeMemoryLimit(filepath.Join(directory, name), 1<<60); limit != 0 && (minimum == 0 || limit < minimum) {
			minimum = limit
		}
		if directory == root {
			return minimum
		}
	}
}

func nativeCgroupMemory() uint64 {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0
	}
	var minimum uint64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 {
			continue
		}
		group := strings.TrimLeft(filepath.Clean("/"+fields[2]), string(filepath.Separator))
		var limit uint64
		switch {
		case fields[0] == "0" && fields[1] == "":
			limit = nativeHierarchyMemory("/sys/fs/cgroup", group, "memory.max")
		case strings.Contains(","+fields[1]+",", ",memory,"):
			limit = nativeHierarchyMemory("/sys/fs/cgroup/memory", group, "memory.limit_in_bytes")
			if limit == 0 {
				limit = nativeHierarchyMemory("/sys/fs/cgroup", group, "memory.limit_in_bytes")
			}
		}
		if limit != 0 && (minimum == 0 || limit < minimum) {
			minimum = limit
		}
	}
	return minimum
}

func nativeServerMemory() (uint64, string) {
	host := nativeHostMemory()
	cgroup := nativeCgroupMemory()
	if cgroup != 0 && (host == 0 || cgroup < host) {
		return cgroup, "cgroup"
	}
	if host != 0 {
		return host, "host"
	}
	return 1 << 30, "fallback"
}

func nativePortHopping(hop *udphop.Config) (*native.PortHoppingConfig, error) {
	return nativeHoppingConfig(hop, false)
}

func nativeHoppingConfig(hop *udphop.Config, allowLocal bool) (*native.PortHoppingConfig, error) {
	const maxTimerSeconds = int64((1<<63 - 1) / time.Second)
	if (!allowLocal && hop.Local) || (!hop.Local && !hop.Remote && !hop.RemoteOnce) {
		return nil, fmt.Errorf("native UDP hopping requires remote hopping without local socket replacement")
	}
	if hop.IntervalMin < 5 || hop.IntervalMax < hop.IntervalMin || hop.IntervalMax > maxTimerSeconds {
		return nil, fmt.Errorf("native UDP hopping requires ordered intervals of at least five seconds")
	}
	if (!hop.Local && len(hop.RemotePorts) == 0 && len(hop.RemoteIPs) == 0) || len(hop.RemotePorts) > 65535 || len(hop.RemoteIPs) > 65535 {
		return nil, fmt.Errorf("native UDP hopping requires remote ports or IP prefixes, with at most 65535 of each")
	}
	c := &native.PortHoppingConfig{
		IntervalMin: uint64(hop.IntervalMin),
		IntervalMax: uint64(hop.IntervalMax),
		Ports:       make([]uint16, len(hop.RemotePorts)),
		Once:        !hop.Remote,
	}
	for _, address := range hop.RemoteIPs {
		prefix, err := netip.ParsePrefix(address)
		if err != nil {
			return nil, fmt.Errorf("invalid native UDP hopping IP prefix %q: %w", address, err)
		}
		if prefix.Addr().Is4In6() {
			if prefix.Bits() < 96 {
				return nil, fmt.Errorf("native UDP hopping mapped IPv4 prefixes require at least 96 bits")
			}
			prefix = netip.PrefixFrom(prefix.Addr().Unmap(), prefix.Bits()-96)
		}
		c.IPs = append(c.IPs, prefix.Masked().String())
	}
	for i, port := range hop.RemotePorts {
		if port == 0 || port > 65535 {
			return nil, fmt.Errorf("invalid native UDP hopping port %d", port)
		}
		c.Ports[i] = uint16(port)
	}
	return c, nil
}

func nativePacketFile(packet stdnet.PacketConn) (*os.File, error) {
	socket, ok := packet.(interface{ File() (*os.File, error) })
	if !ok {
		return nil, fmt.Errorf("native Hysteria requires an OS UDP socket; proxy/custom packet dialers are unsupported")
	}
	return socket.File()
}

func nativeMasquerade(hy *Config) (*native.MasqueradeConfig, error) {
	switch strings.ToLower(hy.MasqType) {
	case "", "404":
		return nil, nil
	case "string":
	default:
		return nil, fmt.Errorf("native Hysteria supports 404 and string HTTP masquerading only")
	}
	status := hy.MasqStringStatusCode
	if status == 0 {
		status = http.StatusOK
	}
	if status < 200 || status > 999 {
		return nil, fmt.Errorf("native string masquerading requires a final HTTP status from 200 to 999")
	}
	headers := make(map[string]string, len(hy.MasqStringHeaders)+1)
	for name, value := range hy.MasqStringHeaders {
		if !httpguts.ValidHeaderFieldName(name) || !httpguts.ValidHeaderFieldValue(value) {
			return nil, fmt.Errorf("invalid native string masquerade header %q", name)
		}
		name = strings.ToLower(name)
		switch name {
		case "connection", "proxy-connection", "keep-alive", "transfer-encoding", "upgrade", "te", "trailer":
			return nil, fmt.Errorf("native string masquerade header %q is unsupported in HTTP/3", name)
		}
		if _, exists := headers[name]; exists {
			return nil, fmt.Errorf("duplicate native string masquerade header %q", name)
		}
		if name == "content-length" {
			length, err := strconv.ParseUint(value, 10, 63)
			if err != nil || length != uint64(len(hy.MasqString)) {
				return nil, fmt.Errorf("native string masquerade Content-Length must match the body length")
			}
		}
		headers[name] = value
	}
	if _, exists := headers["content-type"]; !exists && headers["content-encoding"] == "" && hy.MasqString != "" && status != http.StatusNoContent && status != http.StatusNotModified {
		headers["content-type"] = http.DetectContentType([]byte(hy.MasqString[:min(512, len(hy.MasqString))]))
	}
	return &native.MasqueradeConfig{Status: uint16(status), Headers: headers, Body: hy.MasqString}, nil
}

func nativeDial(ctx context.Context, dest net.Destination, settings *internet.MemoryStreamConfig) (connection stat.Connection, resultErr error) {
	if os.Getenv("XRAY_NET_DIAG") == "1" {
		defer func() {
			if resultErr != nil {
				fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] dial failed peer=%s: %v\n", dest.NetAddr(), resultErr)
			}
		}()
	}
	config, err := nativeConfig(settings)
	if err != nil {
		return nil, err
	}
	target, ok := ctx.Value(targetKey{}).(net.Destination)
	if !ok || !target.IsValid() {
		return nil, fmt.Errorf("native Hysteria requires the outbound target context")
	}
	config.Auth = settings.ProtocolSettings.(*Config).Auth
	config.Users = []string{}
	if config.ServerName == "" {
		config.ServerName = strings.Trim(dest.Address.String(), "[]")
	}
	dest.Network = net.Network_UDP
	create := func(ctx context.Context) (*native.Engine, error) {
		var raw net.Conn
		var err error
		if config.PacketBridge && settings.FinalMask != nil {
			masks := settings.FinalMask.WithPacketConnSetup(func(packet stdnet.PacketConn) { native.TunePacketSocket(packet, false, config.Diagnostic) })
			raw, err = masks.DialUDP(ctx, dest)
		} else {
			raw, err = internet.DialSystem(ctx, dest, settings.SocketSettings)
		}
		if err != nil {
			return nil, err
		}
		wrapped, ok := raw.(*net.PacketConnWrapper)
		if !ok {
			raw.Close()
			return nil, fmt.Errorf("native Hysteria cannot use a chained/custom outbound")
		}
		current := config
		current.Peer = raw.RemoteAddr().String()
		if current.PacketBridge {
			current.Peer = dest.NetAddr()
			if peer, ok := raw.RemoteAddr().(*stdnet.UDPAddr); ok && peer != nil && peer.Port > 0 && peer.IP != nil && !peer.IP.IsUnspecified() {
				current.Peer = peer.String()
			}
			if peer, err := stdnet.ResolveUDPAddr("udp", current.Peer); err == nil && peer.IP != nil {
				current.Peer = peer.String()
				return native.StartPacketConn(ctx, current, wrapped.PacketConn, raw.LocalAddr(), peer, nil)
			}
			raw.Close()
			return nil, fmt.Errorf("native masked Hysteria requires a resolved peer address")
		}
		defer raw.Close()
		file, err := nativePacketFile(wrapped.PacketConn)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		return native.Start(ctx, current, file, raw.LocalAddr(), raw.RemoteAddr(), nil)
	}
	limit, err := nativePoolLimit(config)
	if err != nil {
		return nil, err
	}
	if limit != 0 {
		key := nativePoolKey{instance: core.FromContext(ctx), settings: settings, dest: dest, limit: limit, bridge: os.Getenv("XRAY_HYSTERIA_BRIDGE")}
		pool := pooledNative.get(key)
		conn, err := pool.open(ctx, target, create)
		if err == stdnet.ErrClosed {
			return pooledNative.get(key).open(ctx, target, create)
		}
		return conn, err
	}
	engine, err := create(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := engine.Open(ctx, target)
	if err != nil {
		engine.Close()
		return nil, err
	}
	conn.OwnEngine = true
	return conn, nil
}

type nativeListener struct {
	engine              *native.Engine
	addr                stdnet.Addr
	wg                  sync.WaitGroup
	masqueradeTransport *http.Transport
}

func (l *nativeListener) Close() error {
	err := l.engine.Close()
	l.wg.Wait()
	if l.masqueradeTransport != nil {
		l.masqueradeTransport.CloseIdleConnections()
	}
	return err
}
func (l *nativeListener) Addr() stdnet.Addr { return l.addr }

type nativeInbound struct {
	*native.Conn
	user *protocol.MemoryUser
}

func (c *nativeInbound) User() *protocol.MemoryUser { return c.user }

func nativeListen(ctx context.Context, address net.Address, port net.Port, settings *internet.MemoryStreamConfig, handler internet.ConnHandler) (internet.Listener, error) {
	config, err := nativeConfig(settings)
	if err != nil {
		return nil, err
	}
	if config.PortHopping != nil {
		return nil, fmt.Errorf("native UDP hopping is client-only")
	}
	hy := settings.ProtocolSettings.(*Config)
	if kind := strings.ToLower(hy.MasqType); kind == "file" || kind == "proxy" {
		config.ExternalMasquerade = true
		config.MasqueradeHandler, err = newMasqueradeHandler(hy)
	} else {
		config.Masquerade, err = nativeMasquerade(hy)
	}
	if err != nil {
		return nil, err
	}
	var masqueradeTransport *http.Transport
	if proxy, ok := config.MasqueradeHandler.(*httputil.ReverseProxy); ok {
		masqueradeTransport = proxy.Transport.(*http.Transport).Clone()
		masqueradeTransport.MaxResponseHeaderBytes = 32 << 10
		masqueradeTransport.MaxIdleConns = masqueradeProxyMaxIdleConnections
		masqueradeTransport.MaxIdleConnsPerHost = masqueradeProxyMaxIdleConnsPerHost
		proxy.Transport = masqueradeTransport
	}
	if hy.UdpIdleTimeout < 0 || hy.UdpIdleTimeout > 600 {
		return nil, fmt.Errorf("native UDP idle timeout must be 0..600 seconds")
	}
	if address.Family().IsDomain() {
		return nil, fmt.Errorf("native listener requires an IP address")
	}
	config.Server = true
	memoryBytes, memorySource := nativeServerMemory()
	cpus := runtime.GOMAXPROCS(0)
	autoConnections, autoFlows := tuneNativeServerLimits(memoryBytes)
	config.ServerWorkers = uint8(min(4, max(1, cpus)))
	config.SendBufferBytes = uint32(min(uint64(64<<20), max(uint64(8<<20), memoryBytes/32)))
	config.ReceiveBufferBytes = config.SendBufferBytes
	config.ProcessReceiveBufferBytes = config.ProcessSendBufferBytes
	config.MaxConnections, err = nativeServerLimit("XRAY_HYSTERIA_SERVER_CONNECTIONS", autoConnections, 4096)
	if err != nil {
		return nil, err
	}
	config.MaxFlows, err = nativeServerLimit("XRAY_HYSTERIA_SERVER_FLOWS", autoFlows, 16384)
	if err != nil {
		return nil, err
	}
	if config.Diagnostic {
		fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] server resource limits memory_bytes=%d memory_source=%s gomaxprocs=%d workers=%d max_connections=%d max_flows=%d send_buffer_bytes=%d\n",
			memoryBytes, memorySource, cpus, config.ServerWorkers, config.MaxConnections, config.MaxFlows, config.SendBufferBytes)
	}
	config.UDPIdleTimeout = uint64(hy.UdpIdleTimeout)
	validator := ValidatorFromContext(ctx)
	if validator == nil && hy.Auth == "" {
		return nil, fmt.Errorf("native listener requires authentication")
	}
	config.ExternalAuth = true
	config.Users = nil
	config.Authenticate = func(auth string) (*protocol.MemoryUser, bool) {
		if validator != nil && validator.NotEmpty() {
			user := validator.Get(auth)
			return user, user != nil
		}
		return nil, hy.Auth != "" && subtle.ConstantTimeCompare([]byte(auth), []byte(hy.Auth)) == 1
	}
	var raw net.PacketConn
	if config.PacketBridge && settings.FinalMask != nil {
		masks := settings.FinalMask.WithPacketConnSetup(func(packet stdnet.PacketConn) { native.TunePacketSocket(packet, true, config.Diagnostic) })
		raw, err = masks.ListenPacket(ctx, &net.UDPAddr{IP: address.IP(), Port: int(port)})
	} else {
		raw, err = internet.ListenSystemPacket(ctx, &net.UDPAddr{IP: address.IP(), Port: int(port)}, settings.SocketSettings)
	}
	if err != nil {
		return nil, err
	}
	if config.PacketBridge {
		local := raw.LocalAddr()
		engine, err := native.StartPacketConn(ctx, config, raw, local, nil, func(conn *native.Conn) { handler(&nativeInbound{Conn: conn, user: conn.User}) })
		if err != nil {
			return nil, err
		}
		return finishNativeListener(engine, local, settings, masqueradeTransport), nil
	}
	defer raw.Close()
	file, err := nativePacketFile(raw)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	local := raw.LocalAddr()
	engine, err := native.Start(ctx, config, file, local, nil, func(conn *native.Conn) {
		handler(&nativeInbound{Conn: conn, user: conn.User})
	})
	if err != nil {
		return nil, err
	}
	return finishNativeListener(engine, local, settings, masqueradeTransport), nil
}

func finishNativeListener(engine *native.Engine, local stdnet.Addr, settings *internet.MemoryStreamConfig, masqueradeTransport *http.Transport) *nativeListener {
	listener := &nativeListener{engine: engine, addr: local, masqueradeTransport: masqueradeTransport}
	for _, certificate := range tls.ConfigFromStreamSettings(settings).Certificate {
		if certificate.Usage == tls.Certificate_ENCIPHERMENT && !certificate.OneTimeLoading &&
			(certificate.CertificatePath != "" || certificate.KeyPath != "") {
			listener.wg.Add(1)
			go func() {
				defer listener.wg.Done()
				watchNativeCertificate(engine, certificate, 30*time.Second)
			}()
		}
	}
	return listener
}
