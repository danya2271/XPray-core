//go:build linux && !android && (!xhysteria_native || !cgo)

package main

import (
	"errors"
	"net"
)

func makeNativePacketIngress(*net.UDPConn, brokerBinding) (packetIngress, error) {
	return nil, errors.New("native UDP broker requires a native-enabled supervisor build")
}
