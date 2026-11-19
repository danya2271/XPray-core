package handoff

import (
	"errors"
	"net"

	"golang.org/x/sys/unix"
)

func validateListeningSocket(listener *net.TCPListener) error {
	control, err := listener.SyscallConn()
	if err != nil {
		return err
	}
	var listening int
	var socketErr error
	if err := control.Control(func(fd uintptr) {
		listening, socketErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
	}); err != nil {
		return err
	}
	if socketErr != nil {
		return socketErr
	}
	if listening != 1 {
		return errors.New("inherited TCP socket is not accepting connections")
	}
	return nil
}
