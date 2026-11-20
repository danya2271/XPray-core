//go:build !linux

package handoff

import (
	"errors"
	"net"
	"os"
)

func importUDPPacket(*os.File, net.UDPAddr) (net.PacketConn, error) {
	return nil, errors.New("managed UDP ingress requires Linux")
}

func attachUDPTransmit(net.PacketConn, int) error {
	return errors.New("managed UDP transmit requires Linux")
}

func attachPacketRing(net.PacketConn, [3]int) error {
	return errors.New("managed packet ring requires Linux")
}

func setDirectReceive(net.PacketConn) {}
