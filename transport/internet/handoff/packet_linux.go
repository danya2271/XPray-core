package handoff

import (
	"context"
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
	// Version 1 is a single datagram. Version 2 optionally carries up to
	// eight equal-stride datagrams (the final one can be short).
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
	return packetPeer(header)
}

func packetPeer(header []byte) (netip.AddrPort, bool) {
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

func readPacketFrame(wire []byte) (netip.AddrPort, int, bool) {
	length := len(wire) - packetHeaderSize
	if length < 1 || length > packetMaximum {
		return netip.AddrPort{}, 0, false
	}
	stride := length
	switch wire[0] {
	case 1:
		if uint32(length) != binary.BigEndian.Uint32(wire[20:24]) {
			return netip.AddrPort{}, 0, false
		}
	case 2:
		stride = int(binary.BigEndian.Uint16(wire[20:22]))
		if stride == 0 || stride > length || (length+stride-1)/stride > 8 ||
			length != int(binary.BigEndian.Uint16(wire[22:24])) {
			return netip.AddrPort{}, 0, false
		}
	default:
		return netip.AddrPort{}, 0, false
	}
	peer, ok := packetPeer(wire[:packetHeaderSize])
	return peer, stride, ok
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
	conn                         *net.UnixConn
	local                        net.UDPAddr
	readMu                       sync.Mutex
	writeMu                      sync.Mutex
	rx, tx                       []byte
	peers                        packetAddressCache
	rxOffset, rxLength, rxStride int
	rxPeer                       netip.AddrPort
	transmit                     *net.UDPConn
	ring                         [3]*os.File
	directReceive                bool
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

// DuplicatePacketSocket preserves O_NONBLOCK on the shared open file
// description. net.Conn.File().Fd() may make another live Go reader block.
func DuplicatePacketSocket(socket interface {
	SyscallConn() (syscall.RawConn, error)
}) (*os.File, error) {
	raw, err := socket.SyscallConn()
	if err != nil {
		return nil, err
	}
	fd := -1
	var duplicateErr error
	if err := raw.Control(func(descriptor uintptr) {
		fd, duplicateErr = unix.FcntlInt(descriptor, unix.F_DUPFD_CLOEXEC, 3)
	}); err != nil {
		return nil, err
	}
	if duplicateErr != nil {
		return nil, duplicateErr
	}
	return os.NewFile(uintptr(fd), "xray-nonblocking-packet-duplicate"), nil
}

func (e *PacketEndpoint) File() (*os.File, error) { return DuplicatePacketSocket(e.conn) }
func (e *PacketEndpoint) Close() error {
	for _, file := range e.ring {
		if file != nil {
			file.Close()
		}
	}
	if e.transmit != nil {
		e.transmit.Close()
	}
	return e.conn.Close()
}

func attachPacketRing(packet net.PacketConn, descriptors [3]int) error {
	e := packet.(*PacketEndpoint)
	for index, fd := range descriptors {
		e.ring[index] = os.NewFile(uintptr(fd), "xray-inherited-packet-ring")
	}
	var stat unix.Stat_t
	if err := unix.Fstat(descriptors[0], &stat); err != nil {
		return err
	}
	seals, err := unix.FcntlInt(uintptr(descriptors[0]), unix.F_GET_SEALS, 0)
	required := unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if err != nil || seals&required != required || stat.Size < 192 || stat.Size > 4<<20 {
		return errors.New("packet ring requires a bounded sealed memfd")
	}
	for _, fd := range descriptors[1:] {
		target, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
		flags, flagErr := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
		if err != nil || flagErr != nil || target != "anon_inode:[eventfd]" || flags&unix.O_NONBLOCK == 0 {
			return errors.New("packet ring requires nonblocking eventfds")
		}
	}
	return nil
}

// RingFiles belongs to this endpoint. The native setup duplicates the files
// before Close. Go QUIC must reject a ring rather than waiting on its empty RX.
func (e *PacketEndpoint) RingFiles() [3]*os.File { return e.ring }
func (e *PacketEndpoint) DirectReceive() bool    { return e.directReceive }

func setDirectReceive(packet net.PacketConn) { packet.(*PacketEndpoint).directReceive = true }

func attachUDPTransmit(packet net.PacketConn, descriptor int) error {
	f := os.NewFile(uintptr(descriptor), "xray-inherited-udp-transmit")
	defer f.Close()
	conn, err := net.FilePacketConn(f)
	if err != nil {
		return err
	}
	udp, ok := conn.(*net.UDPConn)
	if !ok || udp.LocalAddr().String() != packet.LocalAddr().String() {
		conn.Close()
		return errors.New("inherited transmit socket does not match UDP ingress address")
	}
	packet.(*PacketEndpoint).transmit = udp
	return nil
}

// TransmitFile is never read by the worker. Only the common ingress receives
// public packets; each native generation may send masked replies directly.
func (e *PacketEndpoint) TransmitFile() (*os.File, error) {
	if e.transmit == nil {
		return nil, nil
	}
	return DuplicatePacketSocket(e.transmit)
}
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
		if e.rxOffset < e.rxLength {
			end := min(e.rxOffset+e.rxStride, e.rxLength)
			n := copy(payload, e.rx[e.rxOffset:end])
			e.rxOffset = end
			return n, e.peers.get(e.rxPeer), nil
		}
		n, _, flags, _, err := e.conn.ReadMsgUnix(e.rx, nil)
		if err != nil {
			return 0, nil, err
		}
		if flags&unix.MSG_TRUNC == 0 {
			if addr, stride, ok := readPacketFrame(e.rx[:n]); ok && stride <= len(payload) {
				e.rxOffset, e.rxLength, e.rxStride, e.rxPeer = packetHeaderSize, n, stride, addr
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
	done    chan struct{}
	// Only an explicitly requested fence sets a deadline. The ordinary
	// receive/send path never checks this pointer.
	readFence atomic.Pointer[udpReadFence]
}

type udpReadFence struct{ done chan error }

func NewUDPIngress(public net.PacketConn, key [16]byte, initialSlot byte, pendingLimit int, pinLifetime time.Duration) (*UDPIngress, error) {
	if public == nil {
		return nil, errors.New("managed UDP ingress needs a public socket")
	}
	local, ok := public.LocalAddr().(*net.UDPAddr)
	if !ok || local.Zone != "" || local.IP.IsUnspecified() || local.IP.To16() == nil || key == [16]byte{} || pendingLimit < 1 || pinLifetime <= 0 {
		return nil, errors.New("managed UDP ingress requires a numeric, explicitly bound address and valid routing limits")
	}
	i := &UDPIngress{public: public, routes: NewQUICRoutes(NewQUICCIDCodec(key), initialSlot, pendingLimit, pinLifetime), done: make(chan struct{})}
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

// FenceInitials reports whether new client-chosen Initial/0-RTT packets can
// no longer reach an inactive slot through this ingress. A nonzero returned
// deadline means the caller should retry at or after the given time. On a
// successful zero deadline, all pre-fence forwarding by the receive loop has
// finished and admission did not change during the fence. The supervisor must
// serialize any future Activate with decisions made using this result.
//
// This is NOT a retirement certificate: packets may still match a server CID,
// and earlier datagrams can still be queued in the worker socket or engine.
func (i *UDPIngress) FenceInitials(ctx context.Context, slot byte) (time.Time, error) {
	i.mu.Lock()
	if i.closed || !i.started || i.failed.Load() || i.peers.Load()[slot] == nil {
		i.mu.Unlock()
		return time.Time{}, errors.New("UDP ingress is unavailable")
	}
	state, until, inactive := i.routes.initialFence(slot)
	if !inactive {
		i.mu.Unlock()
		return time.Time{}, errors.New("cannot fence an active or unknown UDP generation")
	}
	if time.Now().Before(until) {
		i.mu.Unlock()
		return until, nil
	}
	fence := &udpReadFence{done: make(chan error, 1)}
	if !i.readFence.CompareAndSwap(nil, fence) {
		i.mu.Unlock()
		return time.Time{}, errors.New("another UDP ingress fence is in progress")
	}
	err := i.public.SetReadDeadline(time.Now())
	if err != nil {
		i.readFence.CompareAndSwap(fence, nil)
	}
	i.mu.Unlock()
	if err != nil {
		return time.Time{}, err
	}
	select {
	case err := <-fence.done:
		if err != nil {
			return time.Time{}, err
		}
	case <-i.done:
		return time.Time{}, errors.New("UDP ingress stopped during fence")
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || i.failed.Load() || state != i.routes.slots.Load() {
		return time.Time{}, errors.New("UDP ingress admission changed during fence")
	}
	return time.Time{}, nil
}

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
	defer close(i.done)
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
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				fence := i.readFence.Load()
				if fence != nil {
					// Use the same lock as installation to avoid clearing a
					// successor's deadline or racing socket shutdown.
					i.mu.Lock()
					resetErr := i.public.SetReadDeadline(time.Time{})
					i.readFence.Store(nil)
					i.mu.Unlock()
					fence.done <- resetErr
					if resetErr == nil {
						continue
					}
				}
			}
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
		addr, stride, ok := readPacketFrame(wire[:n])
		if !ok || flags&unix.MSG_TRUNC != 0 {
			i.dropped.Add(1)
			continue
		}
		for offset := packetHeaderSize; offset < n; offset += stride {
			payload := wire[offset:min(offset+stride, n)]
			if udp, plain := i.public.(*net.UDPConn); plain {
				_, err = udp.WriteToUDPAddrPort(payload, addr)
			} else {
				_, err = i.public.WriteTo(payload, cache.get(addr))
			}
			if err != nil {
				i.dropped.Add(1)
			}
		}
	}
}
