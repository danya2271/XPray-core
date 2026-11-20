package handoff

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	packetHeaderSize = 24
	// The shared wire is the single-datagram frame used by xhysteria's
	// PacketSocket. Batched native transmit is not enabled on this wire.
	packetMaximum = 65535
)

func udpAddress(peer *net.UDPAddr) netip.AddrPort {
	if peer == nil || peer.Zone != "" || peer.Port < 1 || peer.Port > 65535 {
		return netip.AddrPort{}
	}
	return peer.AddrPort()
}

func writePacketHeader(header []byte, peer netip.AddrPort, length int) bool {
	if !peer.IsValid() || peer.Addr().Zone() != "" || peer.Port() == 0 || length < 1 || length > packetMaximum {
		return false
	}
	ip := peer.Addr().Unmap()
	clear(header[:packetHeaderSize])
	header[0] = 1
	binary.BigEndian.PutUint16(header[2:4], peer.Port())
	if ip.Is4() {
		address := ip.As4()
		header[1] = 4
		copy(header[16:20], address[:])
	} else {
		address := ip.As16()
		header[1] = 6
		copy(header[4:20], address[:])
	}
	binary.BigEndian.PutUint32(header[20:24], uint32(length))
	return true
}

func readPacketHeader(header []byte, length int) (netip.AddrPort, bool) {
	if len(header) < packetHeaderSize || header[0] != 1 || length < 1 || length > packetMaximum || uint32(length) != binary.BigEndian.Uint32(header[20:24]) {
		return netip.AddrPort{}, false
	}
	var address netip.Addr
	switch header[1] {
	case 4:
		if header[4] != 0 || header[5] != 0 || header[6] != 0 || header[7] != 0 ||
			binary.BigEndian.Uint64(header[8:16]) != 0 {
			return netip.AddrPort{}, false
		}
		address = netip.AddrFrom4([4]byte(header[16:20]))
	case 6:
		address = netip.AddrFrom16([16]byte(header[4:20])).Unmap()
	default:
		return netip.AddrPort{}, false
	}
	port := binary.BigEndian.Uint16(header[2:4])
	return netip.AddrPortFrom(address, port), port != 0
}

type packetAddressCache struct {
	slots [64]struct {
		address netip.AddrPort
		value   *net.UDPAddr
	}
}

func (cache *packetAddressCache) get(address netip.AddrPort) *net.UDPAddr {
	bytes := address.Addr().As16()
	key := uint64(address.Port()) ^ binary.BigEndian.Uint64(bytes[:8]) ^ binary.BigEndian.Uint64(bytes[8:])
	key ^= key >> 16
	key *= 0x9e3779b97f4a7c15
	slot := &cache.slots[key>>58]
	if slot.address != address {
		slot.address = address
		slot.value = net.UDPAddrFromAddrPort(address)
	}
	return slot.value
}

// PacketEndpoint exposes an inherited datagram channel as a UDP PacketConn
// to Go QUIC. Rust can consume the same descriptor directly via File().
type PacketEndpoint struct {
	conn    *net.UnixConn
	local   net.UDPAddr
	readMu  sync.Mutex
	writeMu sync.Mutex
	rx, tx  []byte
	peers   packetAddressCache
}

func importUDPPacket(fd *os.File, local net.UDPAddr) (net.PacketConn, error) {
	return ImportPacketEndpoint(fd, local)
}

func ImportPacketEndpoint(fd *os.File, local net.UDPAddr) (*PacketEndpoint, error) {
	conn, err := net.FileConn(fd)
	if err != nil {
		return nil, err
	}
	socket, ok := conn.(*net.UnixConn)
	if !ok || socket.LocalAddr().Network() != "unixgram" || socket.RemoteAddr() == nil {
		conn.Close()
		return nil, errors.New("expected an inherited connected Unix datagram socket")
	}
	return &PacketEndpoint{conn: socket, local: local}, nil
}

func (e *PacketEndpoint) File() (*os.File, error)            { return e.conn.File() }
func (e *PacketEndpoint) Close() error                       { return e.conn.Close() }
func (e *PacketEndpoint) LocalAddr() net.Addr                { return &e.local }
func (e *PacketEndpoint) SetReadDeadline(t time.Time) error  { return e.conn.SetReadDeadline(t) }
func (e *PacketEndpoint) SetWriteDeadline(t time.Time) error { return e.conn.SetWriteDeadline(t) }
func (e *PacketEndpoint) SetDeadline(t time.Time) error      { return e.conn.SetDeadline(t) }

func (e *PacketEndpoint) ReadFrom(payload []byte) (int, net.Addr, error) {
	e.readMu.Lock()
	defer e.readMu.Unlock()
	if e.rx == nil {
		e.rx = make([]byte, packetHeaderSize+packetMaximum)
	}
	for {
		n, _, flags, _, err := e.conn.ReadMsgUnix(e.rx[:packetHeaderSize+min(len(payload), packetMaximum)], nil)
		if err != nil {
			return 0, nil, err
		}
		if flags&unix.MSG_TRUNC == 0 {
			if addr, ok := readPacketHeader(e.rx[:n], n-packetHeaderSize); ok {
				return copy(payload, e.rx[packetHeaderSize:n]), e.peers.get(addr), nil
			}
		}
	}
}

func (e *PacketEndpoint) WriteTo(payload []byte, address net.Addr) (int, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if e.tx == nil {
		e.tx = make([]byte, packetHeaderSize+packetMaximum)
	}
	peer, _ := address.(*net.UDPAddr)
	if !writePacketHeader(e.tx, udpAddress(peer), len(payload)) {
		return 0, errors.New("invalid handoff datagram")
	}
	copy(e.tx[packetHeaderSize:], payload)
	n, err := e.conn.Write(e.tx[:packetHeaderSize+len(payload)])
	if err != nil {
		return 0, err
	}
	return n - packetHeaderSize, nil
}

type udpPeer struct {
	conn   *net.UnixConn
	raw    syscall.RawConn
	failed atomic.Bool
}

type udpPeers [256]*udpPeer

type nonblockingDatagram struct {
	wire []byte
	err  error
}

func (s *nonblockingDatagram) send(fd uintptr) {
	for {
		_, s.err = unix.SendmsgN(int(fd), s.wire, nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL)
		if s.err != unix.EINTR {
			return
		}
	}
}

// UDPIngress owns an already configured public datagram socket. Any configured
// mask MUST be applied by this socket, once, before CID classification. A
// logical inbound has one fixed local address; port redirects must all target
// this socket, as they do for the single-process NodeX service.
type UDPIngress struct {
	public  net.PacketConn
	routes  *QUICRoutes
	peers   atomic.Pointer[udpPeers]
	mu      sync.Mutex
	closed  bool
	started bool
	used    [256]bool
	group   sync.WaitGroup
	dropped atomic.Uint64
	failed  atomic.Bool
}

func NewUDPIngress(public net.PacketConn, key [16]byte, initialSlot byte, pendingLimit int, pinLifetime time.Duration) (*UDPIngress, error) {
	if public == nil {
		return nil, errors.New("managed UDP ingress needs a public socket")
	}
	local, ok := public.LocalAddr().(*net.UDPAddr)
	if !ok || local.Zone != "" || local.IP.IsUnspecified() || local.IP.To16() == nil || key == [16]byte{} || pendingLimit < 1 || pinLifetime <= 0 {
		return nil, errors.New("managed UDP ingress requires a numeric, explicitly bound address and valid routing limits")
	}
	i := &UDPIngress{public: public, routes: NewQUICRoutes(NewQUICCIDCodec(key), initialSlot, pendingLimit, pinLifetime)}
	i.peers.Store(new(udpPeers))
	return i, nil
}

// Add returns the worker-side descriptor. The caller closes it after starting
// the child. A slot is never reused during this ingress's key lifetime.
func (i *UDPIngress) Add(slot byte) (*os.File, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || i.used[slot] {
		return nil, errors.New("UDP generation slot is unavailable")
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	parent, child := os.NewFile(uintptr(fds[0]), "xray-udp-ingress"), os.NewFile(uintptr(fds[1]), "xray-udp-generation")
	defer parent.Close()
	conn, err := net.FileConn(parent)
	if err != nil {
		child.Close()
		return nil, err
	}
	socket := conn.(*net.UnixConn)
	raw, err := socket.SyscallConn()
	if err != nil {
		socket.Close()
		child.Close()
		return nil, err
	}
	peer := &udpPeer{conn: socket, raw: raw}
	peers := *i.peers.Load()
	peers[slot] = peer
	i.peers.Store(&peers)
	i.used[slot] = true
	i.group.Go(func() { i.send(peer) })
	if !i.started {
		i.started = true
		i.group.Go(i.receive)
	}
	return child, nil
}

func (i *UDPIngress) Activate(slot byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	peer := i.peers.Load()[slot]
	if i.closed || i.failed.Load() || peer == nil || peer.failed.Load() {
		return fmt.Errorf("UDP generation %d has no healthy ingress", slot)
	}
	i.routes.Activate(slot)
	return nil
}

func (i *UDPIngress) Dropped() uint64 { return i.dropped.Load() }

func (i *UDPIngress) Close() error {
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return nil
	}
	i.closed = true
	i.public.Close()
	for _, peer := range i.peers.Load() {
		if peer != nil {
			peer.conn.Close()
		}
	}
	i.mu.Unlock()
	i.group.Wait()
	return nil
}

func (i *UDPIngress) receive() {
	defer i.failed.Store(true)
	reader := i.routes.NewReader()
	wire := make([]byte, packetHeaderSize+packetMaximum)
	var send nonblockingDatagram
	write := send.send
	for {
		var n int
		var peer netip.AddrPort
		var err error
		if udp, ok := i.public.(*net.UDPConn); ok {
			n, peer, err = udp.ReadFromUDPAddrPort(wire[packetHeaderSize:])
		} else {
			var from net.Addr
			n, from, err = i.public.ReadFrom(wire[packetHeaderSize:])
			addr, _ := from.(*net.UDPAddr)
			peer = udpAddress(addr)
		}
		if err != nil {
			return
		}
		if n < 1 || n > packetMaximum {
			i.dropped.Add(1)
			continue
		}
		slot, ok := reader.Route(wire[packetHeaderSize:packetHeaderSize+n], time.Time{})
		if !ok {
			continue
		}
		worker := i.peers.Load()[slot]
		if worker == nil || !writePacketHeader(wire, peer, n) {
			i.dropped.Add(1)
			continue
		}
		send.wire = wire[:packetHeaderSize+n]
		if err := worker.raw.Control(write); err != nil || send.err != nil {
			i.dropped.Add(1) // Never block another generation on this queue.
		}
	}
}

func (i *UDPIngress) send(peer *udpPeer) {
	defer peer.failed.Store(true)
	wire := make([]byte, packetHeaderSize+packetMaximum)
	var cache packetAddressCache
	for {
		n, _, flags, _, err := peer.conn.ReadMsgUnix(wire, nil)
		if err != nil {
			return
		}
		addr, ok := readPacketHeader(wire[:n], n-packetHeaderSize)
		if !ok || flags&unix.MSG_TRUNC != 0 {
			i.dropped.Add(1)
			continue
		}
		if udp, plain := i.public.(*net.UDPConn); plain {
			_, err = udp.WriteToUDPAddrPort(wire[packetHeaderSize:n], addr)
		} else {
			_, err = i.public.WriteTo(wire[packetHeaderSize:n], cache.get(addr))
		}
		if err != nil {
			i.dropped.Add(1)
		}
	}
}
