//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

/*
#cgo CFLAGS: -I${SRCDIR}/../../../native/libxhysteria/include
#cgo linux,!android LDFLAGS: -L${SRCDIR}/../../../native/libxhysteria/target/release -lxhysteria -ldl -lpthread -lm
#cgo android,arm64 LDFLAGS: -L${SRCDIR}/../../../native/libxhysteria/target/aarch64-linux-android/release
#cgo android,arm LDFLAGS: -L${SRCDIR}/../../../native/libxhysteria/target/armv7-linux-androideabi/release
#cgo android,amd64 LDFLAGS: -L${SRCDIR}/../../../native/libxhysteria/target/x86_64-linux-android/release
#cgo android,386 LDFLAGS: -L${SRCDIR}/../../../native/libxhysteria/target/i686-linux-android/release
#cgo android LDFLAGS: -lxhysteria -ldl -lm -llog
#include "xhysteria.h"
*/
import "C"

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
)

const (
	openTCP       = 1
	openUDP       = 2
	dataFrame     = 3
	datagramFrame = 4
	finFrame      = 5
	closeFrame    = 6
	openedFrame   = 7
	incomingTCP   = 8
	incomingUDP   = 9
	errorFrame    = 10
	readyFrame    = 11
	creditFrame   = 13
	writableFrame = 14
	maxFrame      = 65536 + 4096
)

// Config is copied by Rust during setup. No Go pointers survive the CGO call.
type Config struct {
	Server           bool               `json:"server"`
	Peer             string             `json:"peer"`
	ServerName       string             `json:"server_name"`
	Auth             string             `json:"auth"`
	Users            []string           `json:"users"`
	CAPEM            string             `json:"ca_pem"`
	CertificatePEM   string             `json:"certificate_pem"`
	KeyPEM           string             `json:"key_pem"`
	Salamander       string             `json:"salamander"`
	SystemRoots      bool               `json:"system_roots"`
	TxBPS            uint64             `json:"tx_bps"`
	RxBPS            uint64             `json:"rx_bps"`
	LossCompensation bool               `json:"loss_compensation"`
	Congestion       string             `json:"congestion"`
	DisablePMTUD     bool               `json:"disable_pmtud"`
	MaxIdleTimeout   uint64             `json:"max_idle_timeout"`
	KeepAlivePeriod  uint64             `json:"keep_alive_period"`
	PortHopping      *PortHoppingConfig `json:"port_hopping,omitempty"`
	Diagnostic       bool               `json:"diagnostic"`
}

type PortHoppingConfig struct {
	Ports       []uint16 `json:"ports"`
	IntervalMin uint64   `json:"interval_min"`
	IntervalMax uint64   `json:"interval_max"`
}

type frame struct {
	kind byte
	id   uint64
	data []byte
}

func readFrame(r io.Reader) (frame, error) {
	var header [16]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return frame{}, err
	}
	n := binary.BigEndian.Uint32(header[12:])
	if n > maxFrame || header[1] != 0 || header[2] != 0 || header[3] != 0 {
		return frame{}, errors.New("invalid native frame")
	}
	f := frame{kind: header[0], id: binary.BigEndian.Uint64(header[4:]), data: make([]byte, int(n))}
	_, err := io.ReadFull(r, f.data)
	return f, err
}

func writeFrame(w io.Writer, f frame) error {
	if len(f.data) > maxFrame {
		return errors.New("oversized native frame")
	}
	var header [16]byte
	header[0] = f.kind
	binary.BigEndian.PutUint64(header[4:], f.id)
	binary.BigEndian.PutUint32(header[12:], uint32(len(f.data)))
	buffers := net.Buffers{header[:], f.data}
	_, err := buffers.WriteTo(w)
	return err
}

type Engine struct {
	handle        unsafe.Pointer
	pipe          net.Conn
	local, remote net.Addr
	mu            sync.Mutex
	flows         map[uint64]*Conn
	next          atomic.Uint64
	out           chan frame
	done          chan struct{}
	closeOnce     sync.Once
	wg            sync.WaitGroup
	accept        func(*Conn)
}

// Start duplicates the UDP descriptor in Rust; the caller still owns udp.
func Start(ctx context.Context, config Config, udp *os.File, local, remote net.Addr, accept func(*Conn)) (*Engine, error) {
	if uint32(C.xhys_abi_version()) != 1 {
		return nil, errors.New("native ABI version mismatch")
	}
	payload, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	var fd C.int32_t
	handle := C.xhys_engine_start((*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(payload))), C.uintptr_t(len(payload)), C.int32_t(udp.Fd()), &fd)
	if handle == nil {
		return nil, errors.New("native engine initialization failed")
	}
	file := os.NewFile(uintptr(fd), "xhysteria-ipc")
	pipe, err := net.FileConn(file)
	file.Close()
	if err != nil {
		C.xhys_engine_join(handle)
		return nil, err
	}
	deadline := time.Now().Add(10 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	pipe.SetReadDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { pipe.Close() })
	ready, err := readFrame(pipe)
	stop()
	pipe.SetReadDeadline(time.Time{})
	if err != nil || ready.kind != readyFrame {
		pipe.Close()
		C.xhys_engine_join(handle)
		if err != nil {
			return nil, fmt.Errorf("native startup: %w", err)
		}
		return nil, fmt.Errorf("native startup: %s", ready.data)
	}
	e := &Engine{handle: handle, pipe: pipe, local: local, remote: remote, flows: make(map[uint64]*Conn), out: make(chan frame, 128), done: make(chan struct{}), accept: accept}
	e.wg.Add(2)
	go e.readLoop()
	go e.writeLoop()
	return e, nil
}

func (e *Engine) send(f frame, until time.Time) error {
	var timeout <-chan time.Time
	var timer *time.Timer
	if !until.IsZero() {
		if !time.Now().Before(until) {
			return os.ErrDeadlineExceeded
		}
		timer = time.NewTimer(time.Until(until))
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-e.done:
		return net.ErrClosed
	case <-timeout:
		return os.ErrDeadlineExceeded
	case e.out <- f:
		return nil
	}
}

func (e *Engine) stop() {
	e.closeOnce.Do(func() {
		close(e.done)
		e.pipe.Close()
		e.mu.Lock()
		for _, c := range e.flows {
			c.fail(net.ErrClosed)
		}
		e.flows = make(map[uint64]*Conn)
		e.mu.Unlock()
	})
}

func (e *Engine) Close() error {
	e.stop()
	e.wg.Wait()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handle != nil {
		C.xhys_engine_join(e.handle)
		e.handle = nil
	}
	return nil
}

func (e *Engine) writeLoop() {
	defer e.wg.Done()
	defer e.stop()
	for {
		select {
		case <-e.done:
			return
		case f := <-e.out:
			if writeFrame(e.pipe, f) != nil {
				return
			}
		}
	}
}

func (e *Engine) readLoop() {
	defer e.wg.Done()
	defer e.stop()
	for {
		f, err := readFrame(e.pipe)
		if err != nil {
			return
		}
		if f.kind == errorFrame && f.id == 0 {
			return
		}
		e.mu.Lock()
		c := e.flows[f.id]
		if f.kind == incomingTCP || f.kind == incomingUDP {
			if e.accept == nil || c != nil || len(e.flows) >= 128 {
				e.mu.Unlock()
				e.send(frame{kind: closeFrame, id: f.id}, time.Now().Add(time.Second))
				continue
			}
			if len(f.data) < 6 {
				e.mu.Unlock()
				return
			}
			user := binary.BigEndian.Uint32(f.data)
			peer, target, err := decodeAddress(f.data[4:])
			if err != nil {
				e.mu.Unlock()
				return
			}
			network := "tcp:"
			if f.kind == incomingUDP {
				network = "udp:"
			}
			dest, err := xnet.ParseDestination(network + string(target))
			if err != nil {
				e.mu.Unlock()
				return
			}
			remote, err := net.ResolveUDPAddr("udp", string(peer))
			if err != nil {
				e.mu.Unlock()
				return
			}
			c = newConn(e, f.id, dest, remote)
			c.UserIndex = user
			e.flows[f.id] = c
			e.mu.Unlock()
			go e.accept(c)
			continue
		}
		e.mu.Unlock()
		if c == nil {
			continue
		}
		if f.kind == writableFrame {
			select {
			case c.writeCredit <- struct{}{}:
			default:
			}
			continue
		}
		select {
		case c.in <- f:
		default:
			c.fail(errors.New("native receive queue capacity reached"))
			e.mu.Lock()
			delete(e.flows, c.id)
			e.mu.Unlock()
			e.send(frame{kind: closeFrame, id: c.id}, time.Now().Add(time.Second))
		}
		if f.kind == errorFrame || f.kind == closeFrame {
			err := error(io.EOF)
			if f.kind == errorFrame {
				err = fmt.Errorf("native flow: %s", f.data)
			}
			c.fail(err)
			e.mu.Lock()
			delete(e.flows, c.id)
			e.mu.Unlock()
		}
	}
}

func (e *Engine) Open(ctx context.Context, target xnet.Destination) (*Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	id := e.next.Add(1)
	c := newConn(e, id, target, e.remote)
	e.mu.Lock()
	if len(e.flows) >= 128 {
		e.mu.Unlock()
		return nil, errors.New("native flow capacity reached")
	}
	e.flows[id] = c
	e.mu.Unlock()
	kind := byte(openTCP)
	if target.Network == xnet.Network_UDP {
		kind = openUDP
	}
	if err := e.send(frame{kind: kind, id: id, data: []byte(target.NetAddr())}, time.Now().Add(10*time.Second)); err != nil {
		c.Close()
		return nil, err
	}
	select {
	case <-ctx.Done():
		c.Close()
		return nil, ctx.Err()
	case <-e.done:
		c.Close()
		return nil, net.ErrClosed
	case f := <-c.in:
		if f.kind != openedFrame {
			c.Close()
			return nil, fmt.Errorf("native open: %s", f.data)
		}
		return c, nil
	}
}

func decodeAddress(data []byte) ([]byte, []byte, error) {
	if len(data) < 2 {
		return nil, nil, errors.New("short native address")
	}
	n := int(binary.BigEndian.Uint16(data))
	if n == 0 || n > 2048 || n+2 > len(data) {
		return nil, nil, errors.New("invalid native address")
	}
	return data[2 : n+2], data[n+2:], nil
}

func encodeAddress(address string, payload []byte) ([]byte, error) {
	if len(address) == 0 || len(address) > 2048 {
		return nil, errors.New("invalid native address")
	}
	result := make([]byte, 2+len(address)+len(payload))
	binary.BigEndian.PutUint16(result, uint16(len(address)))
	copy(result[2:], address)
	copy(result[2+len(address):], payload)
	return result, nil
}

type Conn struct {
	engine                      *Engine
	id                          uint64
	target                      xnet.Destination
	remote                      net.Addr
	UserIndex                   uint32
	in                          chan frame
	closed                      chan struct{}
	once                        sync.Once
	mu                          sync.Mutex
	readMu, writeMu             sync.Mutex
	readDeadline, writeDeadline time.Time
	changed                     chan struct{}
	err                         error
	pending                     []byte
	writeCredit                 chan struct{}
	eof                         bool
	writeClosed                 bool
	OwnEngine                   bool
}

func newConn(e *Engine, id uint64, target xnet.Destination, remote net.Addr) *Conn {
	c := &Conn{engine: e, id: id, target: target, remote: remote, in: make(chan frame, 18), closed: make(chan struct{}), changed: make(chan struct{}), writeCredit: make(chan struct{}, 16)}
	for range 16 {
		c.writeCredit <- struct{}{}
	}
	return c
}

func (c *Conn) NativeHysteriaTarget() xnet.Destination { return c.target }
func (c *Conn) LocalAddr() net.Addr                    { return c.engine.local }
func (c *Conn) RemoteAddr() net.Addr                   { return c.remote }

func (c *Conn) fail(err error) {
	c.once.Do(func() { c.mu.Lock(); c.err = err; c.mu.Unlock(); close(c.closed) })
}

func (c *Conn) Close() error {
	c.fail(net.ErrClosed)
	c.engine.mu.Lock()
	delete(c.engine.flows, c.id)
	c.engine.mu.Unlock()
	err := c.engine.send(frame{kind: closeFrame, id: c.id}, time.Now().Add(time.Second))
	if c.OwnEngine {
		return c.engine.Close()
	}
	return err
}

func (c *Conn) CloseWrite() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	if c.writeClosed {
		c.mu.Unlock()
		return nil
	}
	c.writeClosed = true
	c.mu.Unlock()
	if err := c.awaitWriteCredit(); err != nil {
		return err
	}
	return c.sendFrame(frame{kind: finFrame, id: c.id})
}

func (c *Conn) nextFrame() (frame, error) {
	for {
		c.mu.Lock()
		until, changed, err := c.readDeadline, c.changed, c.err
		c.mu.Unlock()
		// Consume already published payload before a connection failure.
		select {
		case f := <-c.in:
			return c.received(f)
		default:
		}
		if err != nil {
			return frame{}, err
		}
		var timer *time.Timer
		var timeout <-chan time.Time
		if !until.IsZero() {
			if !time.Now().Before(until) {
				return frame{}, os.ErrDeadlineExceeded
			}
			timer = time.NewTimer(time.Until(until))
			timeout = timer.C
		}
		select {
		case f := <-c.in:
			if timer != nil {
				timer.Stop()
			}
			return c.received(f)
		case <-c.closed:
			if timer != nil {
				timer.Stop()
			}
		case <-changed:
			if timer != nil {
				timer.Stop()
			}
		case <-timeout:
			return frame{}, os.ErrDeadlineExceeded
		}
	}
}

func (c *Conn) received(f frame) (frame, error) {
	if f.kind == dataFrame || f.kind == datagramFrame {
		if err := c.engine.send(frame{kind: creditFrame, id: c.id}, time.Now().Add(10*time.Second)); err != nil {
			return frame{}, err
		}
	}
	return f, nil
}

func (c *Conn) waitWrite(f *frame) error {
	for {
		c.mu.Lock()
		until, changed, err := c.writeDeadline, c.changed, c.err
		c.mu.Unlock()
		if err != nil {
			return err
		}
		var timeout <-chan time.Time
		var timer *time.Timer
		if !until.IsZero() {
			if !time.Now().Before(until) {
				return os.ErrDeadlineExceeded
			}
			timer = time.NewTimer(time.Until(until))
			timeout = timer.C
		}
		var send chan frame
		var value frame
		var credit <-chan struct{}
		if f != nil {
			send = c.engine.out
			value = *f
		} else {
			credit = c.writeCredit
		}
		select {
		case <-c.closed:
			if timer != nil {
				timer.Stop()
			}
			return net.ErrClosed
		case <-c.engine.done:
			if timer != nil {
				timer.Stop()
			}
			return net.ErrClosed
		case <-timeout:
			return os.ErrDeadlineExceeded
		case <-changed:
			if timer != nil {
				timer.Stop()
			}
			continue
		case <-credit:
			if timer != nil {
				timer.Stop()
			}
			return nil
		case send <- value:
			if timer != nil {
				timer.Stop()
			}
			return nil
		}
	}
}

func (c *Conn) awaitWriteCredit() error { return c.waitWrite(nil) }
func (c *Conn) sendFrame(f frame) error { return c.waitWrite(&f) }

func (c *Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.eof {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(c.pending) == 0 {
		f, err := c.nextFrame()
		if err != nil {
			return 0, err
		}
		switch f.kind {
		case finFrame, closeFrame:
			c.eof = true
			return 0, io.EOF
		case errorFrame:
			return 0, fmt.Errorf("native flow: %s", f.data)
		case dataFrame:
			c.pending = f.data
		default:
			return 0, errors.New("unexpected native TCP frame")
		}
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	err, writeClosed := c.err, c.writeClosed
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if writeClosed {
		return 0, io.ErrClosedPipe
	}
	n := 0
	for len(p) > 0 {
		chunk := min(len(p), 32768)
		if err := c.awaitWriteCredit(); err != nil {
			return n, err
		}
		if err := c.sendFrame(frame{kind: dataFrame, id: c.id, data: append([]byte(nil), p[:chunk]...)}); err != nil {
			return n, err
		}
		n += chunk
		p = p[chunk:]
	}
	return n, nil
}

func (c *Conn) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if c.target.Network == xnet.Network_TCP {
		b := buf.NewWithSize(32768)
		n, err := c.Read(b.Extend(32768))
		if n == 0 {
			b.Release()
			return nil, err
		}
		b.Resize(0, int32(n))
		return buf.MultiBuffer{b}, err
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	f, err := c.nextFrame()
	if err != nil {
		return nil, err
	}
	if f.kind == closeFrame || f.kind == finFrame {
		return nil, io.EOF
	}
	if f.kind != datagramFrame {
		return nil, errors.New("native UDP flow failed")
	}
	address, payload, err := decodeAddress(f.data)
	if err != nil || len(payload) > 4096 {
		return nil, errors.New("invalid native UDP datagram")
	}
	dest, err := xnet.ParseDestination("udp:" + string(address))
	if err != nil {
		return nil, err
	}
	b := buf.NewWithSize(int32(len(payload)))
	b.Write(payload)
	b.UDP = &dest
	return buf.MultiBuffer{b}, nil
}

func (c *Conn) WriteMultiBuffer(mb buf.MultiBuffer) error {
	defer buf.ReleaseMulti(mb)
	if c.target.Network == xnet.Network_UDP {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
	}
	for _, b := range mb {
		if c.target.Network == xnet.Network_TCP {
			if _, err := c.Write(b.Bytes()); err != nil {
				return err
			}
			continue
		}
		if b.Len() == 0 || b.Len() > 4096 {
			return errors.New("native UDP size outside 1..4096")
		}
		address := c.target.NetAddr()
		if b.UDP != nil {
			address = b.UDP.NetAddr()
		}
		data, err := encodeAddress(address, b.Bytes())
		if err != nil {
			return err
		}
		c.mu.Lock()
		closedErr := c.err
		c.mu.Unlock()
		if closedErr != nil {
			return closedErr
		}
		if err := c.awaitWriteCredit(); err != nil {
			return err
		}
		if err := c.sendFrame(frame{kind: datagramFrame, id: c.id, data: data}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) setDeadline(read, write bool, deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if read {
		c.readDeadline = deadline
	}
	if write {
		c.writeDeadline = deadline
	}
	close(c.changed)
	c.changed = make(chan struct{})
	return nil
}
func (c *Conn) SetDeadline(t time.Time) error      { return c.setDeadline(true, true, t) }
func (c *Conn) SetReadDeadline(t time.Time) error  { return c.setDeadline(true, false, t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.setDeadline(false, true, t) }
