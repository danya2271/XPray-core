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
