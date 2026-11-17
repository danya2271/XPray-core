//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const packetHeader = 24
const maxPacket = 65535
const packetSocketBuffer = 64 << 10
const packetPeerCacheSize = 32
const packetMaxBatch = 8

type packetPeerEntry struct {
	key     [20]byte
	address *net.UDPAddr
}

type packetPeerCache [packetPeerCacheSize]packetPeerEntry

func TunePacketSocket(packet net.PacketConn, server, diagnostic bool) {
	socket, ok := packet.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		return
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		if diagnostic {
			fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] packet UDP socket access failed: %v\n", err)
		}
		return
	}
	err = raw.Control(func(descriptor uintptr) {
		for _, option := range []int{unix.SO_RCVBUF, unix.SO_SNDBUF} {
			requested := 256 << 10
			if server {
				requested = 1 << 20
			}
			if option == unix.SO_RCVBUF {
				requested *= 4
			}
			before, err := unix.GetsockoptInt(int(descriptor), unix.SOL_SOCKET, option)
			if err != nil {
				continue
			}
			if before != requested*2 {
				if err := unix.SetsockoptInt(int(descriptor), unix.SOL_SOCKET, option, requested); err != nil && diagnostic {
					fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] packet UDP buffer setup option=%d failed: %v\n", option, err)
				}
			}
			if diagnostic {
				actual, err := unix.GetsockoptInt(int(descriptor), unix.SOL_SOCKET, option)
				if err == nil {
					fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] packet UDP buffer role_server=%t option=%d actual_bytes=%d requested_bytes=%d\n", server, option, actual, requested)
				}
			}
		}
	})
	if diagnostic && err != nil {
		fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] packet UDP buffer setup failed: %v\n", err)
	}
}

type packetRelay struct {
	packet          net.PacketConn
	pipe            *net.UnixConn
	peer            *net.UDPAddr
	done            chan struct{}
	once            sync.Once
	wg              sync.WaitGroup
	err             error
	txDropped       atomic.Uint64
	txMTUDropped    atomic.Uint64
	txBufferDropped atomic.Uint64
	txMaskDropped   atomic.Uint64
}

func packetAddress(address net.Addr) (*net.UDPAddr, error) {
	if address, ok := address.(*net.UDPAddr); ok && address != nil && address.Port >= 0 && address.Port <= 65535 && address.IP.To16() != nil && address.Zone == "" {
		return address, nil
	}
	return nil, errors.New("native packet bridge requires an IP UDP address without a zone")
}

func encodePacket(wire []byte, address *net.UDPAddr, payload []byte) (int, error) {
	if len(payload) > maxPacket || len(wire) < packetHeader+len(payload) {
		return 0, errors.New("native packet bridge datagram too large")
	}
	clear(wire[:packetHeader])
	wire[0] = 1
	wire[1] = 6
	if ipv4 := address.IP.To4(); ipv4 != nil {
		wire[1] = 4
		copy(wire[16:20], ipv4)
	} else {
		copy(wire[4:20], address.IP.To16())
	}
	binary.BigEndian.PutUint16(wire[2:4], uint16(address.Port))
	binary.BigEndian.PutUint32(wire[20:24], uint32(len(payload)))
	copy(wire[packetHeader:], payload)
	return packetHeader + len(payload), nil
}

func packetHeaderAddress(wire []byte) ([]byte, error) {
	if len(wire) < packetHeader || len(wire) > packetHeader+maxPacket {
		return nil, errors.New("invalid native packet bridge frame")
	}
	length := len(wire) - packetHeader
	switch wire[0] {
	case 1:
		if int(binary.BigEndian.Uint32(wire[20:24])) != length {
			return nil, errors.New("invalid native packet bridge length")
		}
	case 2:
		stride := int(binary.BigEndian.Uint16(wire[20:22]))
		if int(binary.BigEndian.Uint16(wire[22:24])) != length || stride == 0 || stride > length || (length+stride-1)/stride > packetMaxBatch {
			return nil, errors.New("invalid native packet bridge batch")
		}
	default:
		return nil, errors.New("invalid native packet bridge version")
	}
	ip := wire[4:20]
	switch wire[1] {
	case 4:
		for _, value := range wire[4:16] {
			if value != 0 {
				return nil, errors.New("invalid native packet bridge IPv4 address")
			}
		}
		ip = wire[16:20]
	case 6:
	default:
		return nil, errors.New("invalid native packet bridge address family")
	}
	return ip, nil
}

func decodePacket(wire []byte) (*net.UDPAddr, []byte, error) {
	ip, err := packetHeaderAddress(wire)
	if err != nil {
		return nil, nil, err
	}
	return &net.UDPAddr{IP: append(net.IP(nil), ip...), Port: int(binary.BigEndian.Uint16(wire[2:4]))}, wire[packetHeader:], nil
}

func (cache *packetPeerCache) decode(wire []byte) (*net.UDPAddr, []byte, error) {
	ip, err := packetHeaderAddress(wire)
	if err != nil {
		return nil, nil, err
	}
	var key [20]byte
	copy(key[:], wire[:20])
	key[0] = 1
	hash := uint32(2166136261)
	for _, value := range key {
		hash = (hash ^ uint32(value)) * 16777619
	}
	entry := &cache[hash%packetPeerCacheSize]
	if entry.address == nil || entry.key != key {
		entry.key = key
		entry.address = &net.UDPAddr{IP: append(net.IP(nil), ip...), Port: int(binary.BigEndian.Uint16(wire[2:4]))}
	}
	return entry.address, wire[packetHeader:], nil
}

func newPacketRelay(packet net.PacketConn, peer *net.UDPAddr) (*packetRelay, *os.File, error) {
	descriptors, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	goFile := os.NewFile(uintptr(descriptors[0]), "xhysteria-packet-go")
	nativeFile := os.NewFile(uintptr(descriptors[1]), "xhysteria-packet-native")
	for _, descriptor := range descriptors {
		for _, option := range []int{unix.SO_SNDBUF, unix.SO_RCVBUF} {
			if err := unix.SetsockoptInt(descriptor, unix.SOL_SOCKET, option, packetSocketBuffer); err != nil {
				goFile.Close()
				nativeFile.Close()
				return nil, nil, err
			}
		}
	}
	connection, err := net.FileConn(goFile)
	goFile.Close()
	if err != nil {
		nativeFile.Close()
		return nil, nil, err
	}
	relay := &packetRelay{packet: packet, pipe: connection.(*net.UnixConn), peer: peer, done: make(chan struct{})}
	relay.wg.Add(2)
	go relay.receive()
	go relay.send()
	return relay, nativeFile, nil
}

func (relay *packetRelay) stop(err error) {
	relay.once.Do(func() {
		relay.err = err
		close(relay.done)
		relay.pipe.Close()
		relay.packet.Close()
	})
}

func (relay *packetRelay) receive() {
	defer relay.wg.Done()
	wire := make([]byte, packetHeader+maxPacket)
	for {
		count, address, err := relay.packet.ReadFrom(wire[packetHeader:])
		if err != nil {
			relay.stop(err)
			return
		}
		peer := relay.peer
		if peer == nil {
			peer, err = packetAddress(address)
			if err != nil {
				relay.stop(err)
				return
			}
		}
		length, err := encodePacket(wire, peer, wire[packetHeader:packetHeader+count])
		if err == nil {
			var written int
			written, err = relay.pipe.Write(wire[:length])
			if err == nil && written != length {
				err = io.ErrShortWrite
			}
		}
		if err != nil {
			relay.stop(err)
			return
		}
	}
}

func (relay *packetRelay) send() {
	defer relay.wg.Done()
	wire := make([]byte, packetHeader+maxPacket+1)
	var peers packetPeerCache
	for {
		count, err := relay.pipe.Read(wire)
		if err != nil {
			relay.stop(err)
			return
		}
		address, payload, err := peers.decode(wire[:count])
		if err != nil {
			relay.stop(err)
			return
		}
		stride := len(payload)
		if wire[0] == 2 {
			stride = int(binary.BigEndian.Uint16(wire[20:22]))
		}
		for {
			segment := payload[:min(len(payload), stride)]
			written, err := relay.packet.WriteTo(segment, address)
			if err != nil {
				if !packetSendRetryable(err) {
					relay.stop(err)
					return
				}
				relay.txDropped.Add(1)
				if errors.Is(err, unix.EMSGSIZE) {
					relay.txMTUDropped.Add(1)
				} else if errors.Is(err, unix.ENOBUFS) || errors.Is(err, unix.ENOMEM) {
					relay.txBufferDropped.Add(1)
				}
			} else if written != len(segment) {
				relay.txDropped.Add(1)
				relay.txMaskDropped.Add(1)
			}
			payload = payload[len(segment):]
			if len(payload) == 0 {
				break
			}
		}
	}
}

func packetSendRetryable(err error) bool {
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return true
	}
	for _, code := range []error{unix.EINTR, unix.EAGAIN, unix.ENOBUFS, unix.ENOMEM, unix.EMSGSIZE, unix.ECONNREFUSED, unix.EHOSTUNREACH, unix.ENETUNREACH, unix.ENETDOWN, unix.EADDRNOTAVAIL} {
		if errors.Is(err, code) {
			return true
		}
	}
	return false
}

func StartPacketConn(ctx context.Context, config Config, packet net.PacketConn, local, remote net.Addr, accept func(*Conn)) (*Engine, error) {
	if packet == nil {
		return nil, errors.New("native packet bridge requires a packet connection")
	}
	TunePacketSocket(packet, config.Server, config.Diagnostic)
	var peer *net.UDPAddr
	var err error
	if !config.Server {
		peer, err = net.ResolveUDPAddr("udp", config.Peer)
		if err == nil {
			peer, err = packetAddress(peer)
		}
		if err != nil {
			packet.Close()
			return nil, err
		}
		config.Peer = peer.String()
	}
	if local == nil {
		local = packet.LocalAddr()
	}
	localAddress, err := packetAddress(local)
	if err != nil {
		packet.Close()
		return nil, err
	}
	config.PacketBridge = true
	config.PacketBatch = !config.DisableGSO
	config.PacketLocal = localAddress.String()
	config.DisablePMTUD = true
	config.Salamander = ""
	config.PortHopping = nil
	relay, file, err := newPacketRelay(packet, peer)
	if err != nil {
		packet.Close()
		return nil, err
	}
	defer file.Close()
	attached := false
	defer func() {
		if !attached {
			relay.stop(net.ErrClosed)
			relay.wg.Wait()
		}
	}()
	engine, err := Start(ctx, config, file, localAddress, remote, accept)
	if err != nil {
		return nil, err
	}
	engine.packet = relay
	engine.wg.Add(1)
	go func() {
		defer engine.wg.Done()
		defer relay.stop(net.ErrClosed)
		var diagnostics <-chan time.Time
		if engine.diagnostic {
			timer := time.NewTicker(10 * time.Second)
			defer timer.Stop()
			diagnostics = timer.C
		}
		var previous uint64
		for {
			select {
			case <-relay.done:
				engine.diagnose("packet bridge failed", relay.err)
				engine.stop()
				return
			case <-engine.done:
				return
			case <-diagnostics:
				dropped := relay.txDropped.Load()
				if dropped != previous {
					engine.diagnose("packet bridge drops", fmt.Errorf("tx_dropped=%d tx_dropped_delta=%d tx_mtu_dropped=%d tx_buffer_dropped=%d tx_mask_dropped=%d", dropped, dropped-previous, relay.txMTUDropped.Load(), relay.txBufferDropped.Load(), relay.txMaskDropped.Load()))
					previous = dropped
				}
			}
		}
	}()
	attached = true
	if engine.diagnostic {
		fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] packet bridge started role=%s peer=%s mtu=1200 network_gso=false ipc_batch=%t socket_buffer_bytes=%d\n", engine.role, engine.peer, config.PacketBatch, packetSocketBuffer)
	}
	return engine, nil
}
