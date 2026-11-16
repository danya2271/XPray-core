//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"context"
	"crypto/subtle"
	cryptotls "crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	stdnet "net"
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
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"github.com/xtls/xray-core/transport/internet/finalmask/udphop"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
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
		for index, mask := range masks {
			switch s := mask.(type) {
			case *salamander.Config:
				if c.Salamander != "" || len(s.Password) < 4 {
					return c, fmt.Errorf("native Salamander requires one mask with a password of at least four bytes")
				}
				c.Salamander = s.Password
			case *udphop.Config:
				if index != 0 || c.PortHopping != nil {
					return c, fmt.Errorf("native UDP hopping must be the outermost network mask")
				}
				var err error
				c.PortHopping, err = nativePortHopping(s)
				if err != nil {
					return c, err
				}
			default:
				return c, fmt.Errorf("unsupported native UDP mask %T", mask)
			}
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

func tuneNativeServerLimits(memoryBytes uint64, cpus int) (uint32, uint32) {
	const (
		minConnections = uint64(8)
		minFlows       = uint64(16)
		maxConnections = uint64(256)
		maxFlows       = uint64(1024)
		connectionMiB  = uint64(12)
		flowMiB        = uint64(4)
	)
	if cpus < 1 {
		cpus = 1
	}
	reserve := max(uint64(256<<20), memoryBytes*60/100)
	if memoryBytes <= reserve {
		return uint32(minConnections), uint32(minFlows)
	}
	budgetMiB := (memoryBytes - reserve) >> 20
	connectionBudgetMiB := budgetMiB * 60 / 100
	flowBudgetMiB := budgetMiB - connectionBudgetMiB
	connections := min(maxConnections, min(uint64(cpus)*16, connectionBudgetMiB/connectionMiB))
	flows := min(maxFlows, min(uint64(cpus)*32, flowBudgetMiB/flowMiB))
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

func nativeCgroupMemory() uint64 {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0
	}
	const unlimited = uint64(1) << 60
	var paths []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 {
			continue
		}
		group := strings.TrimLeft(filepath.Clean("/"+fields[2]), string(filepath.Separator))
		switch {
		case fields[0] == "0" && fields[1] == "":
			paths = append(paths,
				filepath.Join("/sys/fs/cgroup", group, "memory.max"),
				"/sys/fs/cgroup/memory.max")
		case strings.Contains(":"+fields[1]+":", ":memory:"):
			paths = append(paths,
				filepath.Join("/sys/fs/cgroup/memory", group, "memory.limit_in_bytes"),
				"/sys/fs/cgroup/memory/memory.limit_in_bytes",
				filepath.Join("/sys/fs/cgroup", group, "memory.limit_in_bytes"))
		}
	}
	for _, path := range paths {
		if limit := nativeMemoryLimit(path, unlimited); limit != 0 {
			return limit
		}
	}
	return 0
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
	const maxTimerSeconds = int64((1<<63 - 1) / time.Second)
	if hop.Local || !hop.Remote || hop.RemoteOnce || len(hop.RemoteIPs) != 0 {
		return nil, fmt.Errorf("native UDP hopping supports intervalRemote ports only, without local/IP hopping")
	}
	if hop.IntervalMin < 5 || hop.IntervalMax < hop.IntervalMin || hop.IntervalMax > maxTimerSeconds {
		return nil, fmt.Errorf("native UDP hopping requires ordered intervals of at least five seconds")
	}
	if len(hop.RemotePorts) == 0 || len(hop.RemotePorts) > 65535 {
		return nil, fmt.Errorf("native UDP hopping requires 1..65535 remote ports")
	}
	c := &native.PortHoppingConfig{
		IntervalMin: uint64(hop.IntervalMin),
		IntervalMax: uint64(hop.IntervalMax),
		Ports:       make([]uint16, len(hop.RemotePorts)),
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
		raw, err := internet.DialSystem(ctx, dest, settings.SocketSettings)
		if err != nil {
			return nil, err
		}
		defer raw.Close()
		wrapped, ok := raw.(*net.PacketConnWrapper)
		if !ok {
			return nil, fmt.Errorf("native Hysteria cannot use a chained/custom outbound")
		}
		file, err := nativePacketFile(wrapped.PacketConn)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		current := config
		current.Peer = raw.RemoteAddr().String()
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
	engine *native.Engine
	addr   stdnet.Addr
	wg     sync.WaitGroup
}

func (l *nativeListener) Close() error {
	err := l.engine.Close()
	l.wg.Wait()
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
	if hy.MasqType != "" && hy.MasqType != "404" {
		return nil, fmt.Errorf("native prototype supports 404 masquerading only")
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
	autoConnections, autoFlows := tuneNativeServerLimits(memoryBytes, cpus)
	config.MaxConnections, err = nativeServerLimit("XRAY_HYSTERIA_SERVER_CONNECTIONS", autoConnections, 256)
	if err != nil {
		return nil, err
	}
	config.MaxFlows, err = nativeServerLimit("XRAY_HYSTERIA_SERVER_FLOWS", autoFlows, 1024)
	if err != nil {
		return nil, err
	}
	if config.Diagnostic {
		fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] server resource limits memory_bytes=%d memory_source=%s gomaxprocs=%d max_connections=%d max_flows=%d\n",
			memoryBytes, memorySource, cpus, config.MaxConnections, config.MaxFlows)
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
	raw, err := internet.ListenSystemPacket(ctx, &net.UDPAddr{IP: address.IP(), Port: int(port)}, settings.SocketSettings)
	if err != nil {
		return nil, err
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
	listener := &nativeListener{engine: engine, addr: local}
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
	return listener, nil
}
