package handoff

import (
	"encoding/binary"
	"errors"
	"net"
	"net/netip"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
	"golang.org/x/sys/unix"
)

const destinationSize = 17

func readDestinationPacketFrame(wire []byte) (netip.AddrPort, int, int, netip.Addr, bool) {
	if len(wire) < packetHeaderSize {
		return netip.AddrPort{}, 0, 0, netip.Addr{}, false
	}
	if wire[0] != 3 && wire[0] != 4 {
		peer, stride, ok := readPacketFrame(wire)
		return peer, stride, packetHeaderSize, netip.Addr{}, ok
	}
	if len(wire) <= packetHeaderSize+destinationSize {
		return netip.AddrPort{}, 0, 0, netip.Addr{}, false
	}
	local, ok := parseDestination(wire[packetHeaderSize:])
	if !ok {
		return netip.AddrPort{}, 0, 0, netip.Addr{}, false
	}
	var header [packetHeaderSize]byte
	copy(header[:], wire)
	header[0] -= 2
	length := len(wire) - packetHeaderSize - destinationSize
	// Parse only the legacy header and payload shape; metadata is outside it.
	stride := length
	if header[0] == 1 {
		if int(binary.BigEndian.Uint32(header[20:])) != length {
			return netip.AddrPort{}, 0, 0, local, false
		}
	} else {
		stride = int(binary.BigEndian.Uint16(header[20:22]))
		if stride < 1 || stride > length || (length+stride-1)/stride > 8 || int(binary.BigEndian.Uint16(header[22:])) != length {
			return netip.AddrPort{}, 0, 0, local, false
		}
	}
	peer, ok := packetPeer(header[:])
	return peer, stride, packetHeaderSize + destinationSize, local, ok && length <= packetMaximum
}

// packetDestination survives masks which retain PacketConn's address object.
// QUIC consumes its metadata, while the remote peer remains a normal UDPAddr.
type packetDestination struct {
	peer  *net.UDPAddr
	local netip.Addr
}

func (a *packetDestination) Network() string                            { return a.peer.Network() }
func (a *packetDestination) String() string                             { return a.peer.String() }
func (a *packetDestination) QUICPacketMetadata() (net.Addr, netip.Addr) { return a.peer, a.local }

func destinationHeader(header []byte, ip netip.Addr) {
	clear(header[:destinationSize])
	ip = ip.Unmap()
	if ip.Is4() {
		header[0] = 4
		raw := ip.As4()
		copy(header[13:17], raw[:])
	} else if ip.Is6() {
		header[0] = 6
		raw := ip.As16()
		copy(header[1:17], raw[:])
	}
}
func parseDestination(header []byte) (netip.Addr, bool) {
	if len(header) < destinationSize {
		return netip.Addr{}, false
	}
	if header[0] == 4 {
		for _, b := range header[1:13] {
			if b != 0 {
				return netip.Addr{}, false
			}
		}
		return netip.AddrFrom4([4]byte(header[13:17])), true
	}
	if header[0] == 6 {
		return netip.AddrFrom16([16]byte(header[1:17])).Unmap(), true
	}
	return netip.Addr{}, false
}
func parsePacketSource(oob []byte) (netip.Addr, error) {
	messages, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return netip.Addr{}, err
	}
	for _, message := range messages {
		if message.Header.Level == unix.IPPROTO_IP && message.Header.Type == unix.IP_PKTINFO {
			if len(message.Data) != unix.SizeofInet4Pktinfo {
				return netip.Addr{}, errors.New("invalid IPv4 packet source")
			}
			return netip.AddrFrom4([4]byte(message.Data[4:8])), nil
		}
		if message.Header.Level == unix.IPPROTO_IPV6 && message.Header.Type == unix.IPV6_PKTINFO {
			if len(message.Data) != unix.SizeofInet6Pktinfo {
				return netip.Addr{}, errors.New("invalid IPv6 packet source")
			}
			return netip.AddrFrom16([16]byte(message.Data[:16])).Unmap(), nil
		}
	}
	return netip.Addr{}, nil
}
func packetSource(ip netip.Addr) []byte {
	if ip.Is4() {
		return (&ipv4.ControlMessage{Src: net.IP(ip.AsSlice())}).Marshal()
	}
	if ip.Is6() {
		return (&ipv6.ControlMessage{Src: net.IP(ip.AsSlice())}).Marshal()
	}
	return nil
}

type destinationSocket struct {
	net.PacketConn
	udp   *net.UDPConn
	ipv4  bool
	oob   []byte
	cache [64]struct {
		peer    netip.AddrPort
		local   netip.Addr
		address *packetDestination
	}
}

// PreservePacketDestination is required before wrapping a wildcard public
// socket in masks. Only its receive goroutine accesses the bounded address cache.
func PreservePacketDestination(udp *net.UDPConn) (net.PacketConn, error) {
	is4 := udp.LocalAddr().(*net.UDPAddr).IP.To4() != nil
	var err error
	if is4 {
		err = ipv4.NewPacketConn(udp).SetControlMessage(ipv4.FlagDst, true)
	} else {
		err = ipv6.NewPacketConn(udp).SetControlMessage(ipv6.FlagDst, true)
	}
	if err != nil {
		return nil, err
	}
	return &destinationSocket{PacketConn: udp, udp: udp, ipv4: is4, oob: make([]byte, 256)}, nil
}
func (s *destinationSocket) ReadFrom(payload []byte) (int, net.Addr, error) {
	n, oobn, flags, peer, err := s.udp.ReadMsgUDP(payload, s.oob)
	if err != nil {
		return 0, nil, err
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return 0, nil, errors.New("truncated wildcard UDP metadata")
	}
	var destination net.IP
	if s.ipv4 {
		var cm ipv4.ControlMessage
		err = cm.Parse(s.oob[:oobn])
		destination = cm.Dst
	} else {
		var cm ipv6.ControlMessage
		err = cm.Parse(s.oob[:oobn])
		destination = cm.Dst
	}
	local, ok := netip.AddrFromSlice(destination)
	if err != nil || !ok {
		return 0, nil, errors.New("missing wildcard UDP destination")
	}
	local = local.Unmap()
	key := peer.AddrPort()
	slot := &s.cache[(int(key.Port())^int(local.As16()[15]))&63]
	if slot.address == nil || slot.peer != key || slot.local != local {
		slot.peer, slot.local = key, local
		slot.address = &packetDestination{peer: peer, local: local}
	}
	return n, slot.address, nil
}
func (s *destinationSocket) WriteTo(payload []byte, address net.Addr) (int, error) {
	peer, ok := address.(*packetDestination)
	if !ok {
		return s.udp.WriteTo(payload, address)
	}
	n, _, err := s.udp.WriteMsgUDP(payload, packetSource(peer.local), peer.peer)
	return n, err
}
