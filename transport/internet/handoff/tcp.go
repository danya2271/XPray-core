// Package handoff implements transport primitives for overlapping Xray generations.
package handoff

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"
)

// TCPListener gates Accept without modifying the underlying TCP listen queue.
// Each generation owns its own Go listener (and poller) for the same socket.
// Accepted connections are returned unchanged; the gate is not a TCP proxy.
type TCPListener struct {
	socket *net.TCPListener
	change sync.Mutex
	mu     sync.Mutex
	state  *sync.Cond
	active bool
	closed bool
	accept int
}

func NewTCPListener(socket *net.TCPListener, active bool) *TCPListener {
	l := &TCPListener{socket: socket, active: active}
	l.state = sync.NewCond(&l.mu)
	return l
}

// ImportTCPListener duplicates the passed listener descriptor. It does not
// consume the passed descriptor and the new gate starts paused.
func ImportTCPListener(file *os.File) (*TCPListener, error) {
	l, err := net.FileListener(file)
	if err != nil {
		return nil, err
	}
	tcp, ok := l.(*net.TCPListener)
	if !ok {
		l.Close()
		return nil, errors.New("handoff descriptor is not a TCP listener")
	}
	return NewTCPListener(tcp, false), nil
}

// File returns a duplicate of the listening socket for the next generation.
// It never transfers connected sockets or interrupts an existing accept.
func (l *TCPListener) File() (*os.File, error) { return l.socket.File() }

func (l *TCPListener) Addr() net.Addr { return l.socket.Addr() }

func (l *TCPListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		for !l.active && !l.closed {
			l.state.Wait()
		}
		if l.closed {
			return nil, net.ErrClosed
		}
		l.accept++
		l.mu.Unlock()
		conn, err := l.socket.AcceptTCP()
		l.mu.Lock()
		l.accept--
		l.state.Broadcast()
		// A successful accept won the race before Pause drained the accept
		// call. It belongs to this generation; dropping it would reset the
		// connection. A deadline used to wake a paused Accept is internal.
		if err == nil || l.closed || l.active || !errors.Is(err, os.ErrDeadlineExceeded) {
			return conn, err
		}
	}
}

// Pause waits for this generation's in-flight Accept calls to leave the
// socket. New connections can queue in the kernel until another gate resumes.
// It does not close accepted connections or the underlying listen socket.
func (l *TCPListener) Pause() error {
	l.change.Lock()
	defer l.change.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return net.ErrClosed
	}
	if l.active {
		if err := l.socket.SetDeadline(time.Now()); err != nil {
			return err
		}
		l.active = false
		for l.accept != 0 {
			l.state.Wait()
		}
	}
	return nil
}

func (l *TCPListener) Resume() error {
	l.change.Lock()
	defer l.change.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return net.ErrClosed
	}
	if !l.active {
		if err := l.socket.SetDeadline(time.Time{}); err != nil {
			return err
		}
		l.active = true
		l.state.Broadcast()
	}
	return nil
}

// Close releases only this generation's copy of the listening socket.
func (l *TCPListener) Close() error {
	l.change.Lock()
	defer l.change.Unlock()
	l.mu.Lock()
	l.closed = true
	l.state.Broadcast()
	l.mu.Unlock()
	return l.socket.Close()
}
