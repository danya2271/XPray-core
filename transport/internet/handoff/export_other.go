//go:build !linux

package handoff

import "net"

func exportControl(net.Conn, *Generation) {}
