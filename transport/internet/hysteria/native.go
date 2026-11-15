//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"context"
	cryptotls "crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	stdnet "net"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy/hysteria/account"
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
	if len(t.EchServerKeys) != 0 || t.EchConfigList != "" || t.Fingerprint != "" || len(t.PinnedPeerCertSha256) != 0 || len(t.VerifyPeerCertByName) != 0 || t.UseSystemCa || t.RejectUnknownSni || t.MasterKeyLog != "" || len(t.CurvePreferences) != 0 || t.CipherSuites != "" || t.EnableSessionResumption || (t.MinVersion != "" && t.MinVersion != "1.3") || (t.MaxVersion != "" && t.MaxVersion != "1.3") {
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
	c.MaxIdleTimeout = uint64(q.MaxIdleTimeout)
	c.KeepAlivePeriod = uint64(q.KeepAlivePeriod)
	c.LossCompensation = !q.BrutalDisableLossCompensation
	c.Diagnostic = os.Getenv("XRAY_NET_DIAG") == "1"
	for _, entry := range t.Certificate {
		certificate, key := entry.Certificate, entry.Key
		if (entry.CertificatePath != "" || entry.KeyPath != "") && !entry.OneTimeLoading {
			return c, fmt.Errorf("native prototype requires oneTimeLoading for certificate files; reload is not implemented")
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
				fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] dial failed: %v\n", resultErr)
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
	config.Peer = raw.RemoteAddr().String()
	engine, err := native.Start(ctx, config, file, raw.LocalAddr(), raw.RemoteAddr(), nil)
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
}

func (l *nativeListener) Close() error      { return l.engine.Close() }
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
	if hy.UdpIdleTimeout != 0 && hy.UdpIdleTimeout != 60 {
		return nil, fmt.Errorf("native prototype UDP idle timeout is fixed at 60 seconds")
	}
	if address.Family().IsDomain() {
		return nil, fmt.Errorf("native listener requires an IP address")
	}
	config.Server = true
	var users []*protocol.MemoryUser
	if validator := ValidatorFromContext(ctx); validator != nil {
		users = validator.GetAll()
	}
	for _, user := range users {
		config.Users = append(config.Users, user.Account.(*account.MemoryAccount).Auth)
	}
	if len(config.Users) == 0 && hy.Auth != "" {
		config.Users = []string{hy.Auth}
	}
	if len(config.Users) == 0 {
		return nil, fmt.Errorf("native listener requires authentication")
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
		var user *protocol.MemoryUser
		if int(conn.UserIndex) < len(users) {
			user = users[conn.UserIndex]
		}
		handler(&nativeInbound{Conn: conn, user: user})
	})
	if err != nil {
		return nil, err
	}
	return &nativeListener{engine: engine, addr: local}, nil
}
