package internet

import (
	"encoding/binary"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"github.com/xtls/xray-core/common/errors"
	"golang.org/x/sys/unix"
)

func setTcpBrutal(fd uintptr, rate uint64, cwndGain uint32) error {
	// Create a packed 12-byte buffer
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf[0:8], rate)
	binary.LittleEndian.PutUint32(buf[8:12], cwndGain)

	// Call setsockopt with the packed binary buffer
	_, _, errno := syscall.Syscall6(
		syscall.SYS_SETSOCKOPT,
		fd,
		syscall.SOL_TCP,                  // Level 6 (IPPROTO_TCP)
		23301,                            // Opt: TCP_BRUTAL_PARAMS
		uintptr(unsafe.Pointer(&buf[0])), // Pointer to the 12-byte buffer
		12,                               // Buffer length
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// applyOutboundSocketOptions applies socket options for outbound connection.
// note that unlike other part of Xray, this function needs network with speified network stack(tcp4/tcp6/udp4/udp6)
func applyOutboundSocketOptions(network string, address string, fd uintptr, config *SocketConfig) error {
	if config.Mark != 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_MARK, int(config.Mark)); err != nil {
			return errors.New("failed to set SO_MARK").Base(err)
		}
	}

	if config.Interface != "" {
		if err := syscall.BindToDevice(int(fd), config.Interface); err != nil {
			return errors.New("failed to set Interface").Base(err)
		}
	}

	if isTCPSocket(network) {
		tfo := config.ParseTFOValue()
		if tfo > 0 {
			tfo = 1
		}
		if tfo >= 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_TCP, unix.TCP_FASTOPEN_CONNECT, tfo); err != nil {
				return errors.New("failed to set TCP_FASTOPEN_CONNECT", tfo).Base(err)
			}
		}

		if config.TcpCongestion != "" {
			if err := syscall.SetsockoptString(int(fd), syscall.SOL_TCP, syscall.TCP_CONGESTION, config.TcpCongestion); err != nil {
				return errors.New("failed to set TCP_CONGESTION", err)
			}
		}

		if config.BrutalParams != "" {
			parts := strings.Split(config.BrutalParams, ",")
			if len(parts) != 2 {
				return errors.New("invalid brutal_params format, expected 'rate,cwnd_gain'")
			}
			rate, err1 := strconv.ParseUint(parts[0], 10, 64)
			cwndGain, err2 := strconv.ParseUint(parts[1], 10, 32)
			if err1 != nil || err2 != nil {
				return errors.New("failed to parse brutal rate or cwnd_gain").Base(err1).Base(err2)
			}
			if err := setTcpBrutal(fd, rate, uint32(cwndGain)); err != nil {
				return errors.New("failed to set TCP_BRUTAL_PARAMS").Base(err)
			}
		}

		if config.TcpWindowClamp > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_WINDOW_CLAMP, int(config.TcpWindowClamp)); err != nil {
				return errors.New("failed to set TCP_WINDOW_CLAMP", err)
			}
		}

		if config.TcpUserTimeout > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(config.TcpUserTimeout)); err != nil {
				return errors.New("failed to set TCP_USER_TIMEOUT", err)
			}
		}

		if config.TcpMaxSeg > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_MAXSEG, int(config.TcpMaxSeg)); err != nil {
				return errors.New("failed to set TCP_MAXSEG", err)
			}
		}
	}

	if config.Tproxy.IsEnabled() {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_TRANSPARENT, 1); err != nil {
			return errors.New("failed to set IP_TRANSPARENT").Base(err)
		}
	}

	return nil
}

// applyInboundSocketOptions applies socket options for inbound listener.
// note that unlike other part of Xray, this function needs network with speified network stack(tcp4/tcp6/udp4/udp6)
func applyInboundSocketOptions(network string, fd uintptr, config *SocketConfig) error {
	if config.Mark != 0 {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_MARK, int(config.Mark)); err != nil {
			return errors.New("failed to set SO_MARK").Base(err)
		}
	}
	if isTCPSocket(network) {
		tfo := config.ParseTFOValue()
		if tfo >= 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_TCP, unix.TCP_FASTOPEN, tfo); err != nil {
				return errors.New("failed to set TCP_FASTOPEN", tfo).Base(err)
			}
		}

		if config.TcpKeepAliveInterval > 0 || config.TcpKeepAliveIdle > 0 {
			if config.TcpKeepAliveInterval > 0 {
				if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, int(config.TcpKeepAliveInterval)); err != nil {
					return errors.New("failed to set TCP_KEEPINTVL", err)
				}
			}
			if config.TcpKeepAliveIdle > 0 {
				if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, int(config.TcpKeepAliveIdle)); err != nil {
					return errors.New("failed to set TCP_KEEPIDLE", err)
				}
			}
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1); err != nil {
				return errors.New("failed to set SO_KEEPALIVE", err)
			}
		} else if config.TcpKeepAliveInterval < 0 || config.TcpKeepAliveIdle < 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 0); err != nil {
				return errors.New("failed to unset SO_KEEPALIVE", err)
			}
		}

		if config.TcpCongestion != "" {
			if err := syscall.SetsockoptString(int(fd), syscall.SOL_TCP, syscall.TCP_CONGESTION, config.TcpCongestion); err != nil {
				return errors.New("failed to set TCP_CONGESTION", err)
			}
		}

		if config.BrutalParams != "" {
			parts := strings.Split(config.BrutalParams, ",")
			if len(parts) != 2 {
				return errors.New("invalid brutal_params format, expected 'rate,cwnd_gain'")
			}
			rate, err1 := strconv.ParseUint(parts[0], 10, 64)
			cwndGain, err2 := strconv.ParseUint(parts[1], 10, 32)
			if err1 != nil || err2 != nil {
				return errors.New("failed to parse brutal rate or cwnd_gain").Base(err1).Base(err2)
			}
			if err := setTcpBrutal(fd, rate, uint32(cwndGain)); err != nil {
				return errors.New("failed to set TCP_BRUTAL_PARAMS").Base(err)
			}
		}

		if config.TcpWindowClamp > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_WINDOW_CLAMP, int(config.TcpWindowClamp)); err != nil {
				return errors.New("failed to set TCP_WINDOW_CLAMP", err)
			}
		}

		if config.TcpUserTimeout > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(config.TcpUserTimeout)); err != nil {
				return errors.New("failed to set TCP_USER_TIMEOUT", err)
			}
		}

		if config.TcpMaxSeg > 0 {
			if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_MAXSEG, int(config.TcpMaxSeg)); err != nil {
				return errors.New("failed to set TCP_MAXSEG", err)
			}
		}
	}

	if config.Tproxy.IsEnabled() {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_TRANSPARENT, 1); err != nil {
			return errors.New("failed to set IP_TRANSPARENT").Base(err)
		}
	}

	if config.ReceiveOriginalDestAddress && isUDPSocket(network) {
		err1 := syscall.SetsockoptInt(int(fd), syscall.SOL_IPV6, unix.IPV6_RECVORIGDSTADDR, 1)
		err2 := syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_RECVORIGDSTADDR, 1)
		if err1 != nil && err2 != nil {
			return err1
		}
	}

	if config.V6Only {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_IPV6, syscall.IPV6_V6ONLY, 1); err != nil {
			return errors.New("failed to set IPV6_V6ONLY", err)
		}
	}

	return nil
}

func setReuseAddr(fd uintptr) error {
	if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return errors.New("failed to set SO_REUSEADDR").Base(err).AtWarning()
	}
	return nil
}

func setReusePort(fd uintptr) error {
	if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		return errors.New("failed to set SO_REUSEPORT").Base(err).AtWarning()
	}
	return nil
}
