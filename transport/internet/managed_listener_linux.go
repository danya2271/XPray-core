//go:build linux && !android

package internet

import (
	"net"
	"syscall"
	"time"
)

// ManagedListenConfig uses the same pre-bind policy as Xray, but propagates
// failures and deliberately omits SO_REUSEPORT: only this socket may receive.
func ManagedListenConfig(settings *SocketConfig) net.ListenConfig {
	config := net.ListenConfig{KeepAlive: -1}
	config.SetMultipathTCP(settings != nil && settings.TcpMptcp)
	if settings != nil {
		if settings.TcpKeepAliveIdle*settings.TcpKeepAliveInterval < 0 {
			config.Control = func(string, string, syscall.RawConn) error { return syscall.EINVAL }
			return config
		}
		config.KeepAliveConfig = net.KeepAliveConfig{Enable: settings.TcpKeepAliveIdle > 0 || settings.TcpKeepAliveInterval > 0, Idle: time.Duration(settings.TcpKeepAliveIdle) * time.Second, Interval: time.Duration(settings.TcpKeepAliveInterval) * time.Second}
		config.Control = func(network, address string, raw syscall.RawConn) error {
			var applyErr error
			err := raw.Control(func(fd uintptr) { applyErr = applyInboundSocketOptions(network, fd, settings) })
			if err != nil {
				return err
			}
			return applyErr
		}
	}
	return config
}

// CheckManagedSocketConfig rejects wrappers or outbound-only policies that an
// inherited raw listener cannot represent. Kernel options remain on the FD.
func CheckManagedSocketConfig(config *SocketConfig) error {
	if config == nil {
		return nil
	}
	if config.AcceptProxyProtocol || config.ReceiveOriginalDestAddress || config.Tproxy.IsEnabled() || config.Interface != "" || config.DialerProxy != "" {
		return &net.OpError{Op: "managed listen", Net: "tcp/udp", Err: syscall.ENOTSUP}
	}
	return nil
}
