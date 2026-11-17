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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
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
	authRequest   = 15
	authResponse  = 16
	authClosed    = 17
	updateTLS     = 18
	controlResult = 19
	incomingHTTP  = 20
	httpHead      = 21
	httpTrailers  = 22
	maxFrame      = 65536 + 4096
	// Leave time to deliver Rust's ten-second handshake/auth timeout over IPC.
	StartupTimeout = 12 * time.Second
)

// Config is copied by Rust during setup. No Go pointers survive the CGO call.
type Config struct {
	Server                    bool                                      `json:"server"`
	Peer                      string                                    `json:"peer"`
	ServerName                string                                    `json:"server_name"`
	Auth                      string                                    `json:"auth"`
	Users                     []string                                  `json:"users"`
	CAPEM                     string                                    `json:"ca_pem"`
	CertificatePEM            string                                    `json:"certificate_pem"`
	KeyPEM                    string                                    `json:"key_pem"`
	Salamander                string                                    `json:"salamander"`
	UDPMasks                  []UDPMaskConfig                           `json:"udp_masks,omitempty"`
	SystemRoots               bool                                      `json:"system_roots"`
	TxBPS                     uint64                                    `json:"tx_bps"`
	RxBPS                     uint64                                    `json:"rx_bps"`
	LossCompensation          bool                                      `json:"loss_compensation"`
	Congestion                string                                    `json:"congestion"`
	DisablePMTUD              bool                                      `json:"disable_pmtud"`
	DisableGSO                bool                                      `json:"disable_gso"`
	MaxIdleTimeout            uint64                                    `json:"max_idle_timeout"`
	KeepAlivePeriod           uint64                                    `json:"keep_alive_period"`
	PortHopping               *PortHoppingConfig                        `json:"port_hopping,omitempty"`
	Masquerade                *MasqueradeConfig                         `json:"masquerade,omitempty"`
	MasqueradeHandler         http.Handler                              `json:"-"`
	ExternalMasquerade        bool                                      `json:"external_masquerade,omitempty"`
	PacketBridge              bool                                      `json:"packet_bridge,omitempty"`
	PacketBatch               bool                                      `json:"packet_batch,omitempty"`
	PacketLocal               string                                    `json:"packet_local,omitempty"`
	Diagnostic                bool                                      `json:"diagnostic"`
	ExternalAuth              bool                                      `json:"external_auth"`
	UDPIdleTimeout            uint64                                    `json:"udp_idle_timeout"`
	MaxFlows                  uint32                                    `json:"max_flows"`
	MaxConnections            uint32                                    `json:"max_connections"`
	ServerWorkers             uint8                                     `json:"server_workers"`
	ProcessWorkers            uint8                                     `json:"process_workers"`
	SendBufferBytes           uint32                                    `json:"send_buffer_bytes"`
	ReceiveBufferBytes        uint32                                    `json:"receive_buffer_bytes"`
	ProcessSendBufferBytes    uint32                                    `json:"process_send_buffer_bytes"`
	ProcessReceiveBufferBytes uint32                                    `json:"process_receive_buffer_bytes"`
	Authenticate              func(string) (*protocol.MemoryUser, bool) `json:"-"`
}

type PortHoppingConfig struct {
	Ports       []uint16 `json:"ports"`
	IPs         []string `json:"ips,omitempty"`
	Once        bool     `json:"once,omitempty"`
	IntervalMin uint64   `json:"interval_min"`
	IntervalMax uint64   `json:"interval_max"`
}

type UDPMaskConfig struct {
	Kind     string `json:"kind"`
	Password string `json:"password,omitempty"`
	Prefix   string `json:"prefix,omitempty"`
}

type MasqueradeConfig struct {
	Status  uint16            `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

type frame struct {
	kind     byte
	id       uint64
	data     []byte
	storage  *buf.Buffer
	parts    buf.MultiBuffer
	borrowed bool
}

func (f *frame) size() int {
	if f.parts != nil {
		return int(f.parts.Len())
	}
	return len(f.data)
}

func (f *frame) release() {
	if f.storage != nil {
		f.storage.Release()
		f.storage = nil
	}
	f.data = nil
	buf.ReleaseMulti(f.parts)
	f.parts = nil
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
	f := frame{kind: header[0], id: binary.BigEndian.Uint64(header[4:])}
	if n != 0 {
		f.storage = buf.NewWithSize(int32(n))
		f.data = f.storage.Extend(int32(n))
	}
	_, err := io.ReadFull(r, f.data)
	if err != nil {
		f.release()
	}
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
	bridge        frameBridge
	packet        *packetRelay
	arenaBytes    int64
	local, remote net.Addr
	mu            sync.Mutex
	flows         map[uint64]*Conn
	next          atomic.Uint64
	out           chan frame
	done          chan struct{}
	closeOnce     sync.Once
	wg            sync.WaitGroup
	accept        func(*Conn)
	submitMu      sync.Mutex
	submitWG      sync.WaitGroup
	publishMu     sync.Mutex
	queued        atomic.Int64
	stopping      bool
	authenticate  func(string) (*protocol.MemoryUser, bool)
	masquerade    http.Handler
	authUsers     map[uint64]nativeIdentity
	controls      map[uint64]chan frame
	maxFlows      int
	diagnostic    bool
	role, peer    string
}

type nativeIdentity struct {
	auth string
	user *protocol.MemoryUser
}

// Start duplicates the UDP descriptor in Rust; the caller still owns udp.
func Start(ctx context.Context, config Config, udp *os.File, local, remote net.Addr, accept func(*Conn)) (*Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("native startup: %w", err)
	}
	if config.Server {
		if value := os.Getenv("XRAY_HYSTERIA_SERVER_WORKERS"); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 || n > 16 {
				return nil, errors.New("XRAY_HYSTERIA_SERVER_WORKERS must be 0..16")
			}
			config.ServerWorkers = uint8(n)
		}
	}
	if config.MaxFlows > 65536 || config.MaxConnections > 4096 || config.ServerWorkers > 16 || config.ProcessWorkers > 16 || (config.SendBufferBytes != 0 && (config.SendBufferBytes < maxFrame || config.SendBufferBytes > 1<<30)) || (config.ExternalAuth && config.Authenticate == nil) {
		return nil, errors.New("invalid native engine limits or authenticator")
	}
	if config.ExternalMasquerade && (!config.Server || config.MasqueradeHandler == nil) {
		return nil, errors.New("native HTTP masquerade requires a server handler")
	}
	for _, limit := range []uint32{config.ReceiveBufferBytes, config.ProcessSendBufferBytes, config.ProcessReceiveBufferBytes} {
		if limit != 0 && (limit < maxFrame || limit > 1<<30) {
			return nil, errors.New("invalid native buffer budget")
		}
	}
	if uint32(C.xhys_abi_version()) != C.XHYS_ABI_VERSION {
		return nil, errors.New("native ABI version mismatch")
	}
	if config.Users == nil {
		config.Users = []string{}
	}
	payload, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	var handle unsafe.Pointer
	var bridge frameBridge
	var arenaBytes int64
	if os.Getenv("XRAY_HYSTERIA_BRIDGE") != "socket" && (runtime.GOARCH == "arm64" || runtime.GOARCH == "amd64") {
		var view C.XhysSharedView
		handle = C.xhys_engine_start_shared((*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(payload))), C.uintptr_t(len(payload)), C.int32_t(udp.Fd()), &view)
		if handle != nil {
			bridge, err = sharedFromView(view)
			arenaBytes = int64(view.len)
		}
	} else {
		var fd C.int32_t
		handle = C.xhys_engine_start((*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(payload))), C.uintptr_t(len(payload)), C.int32_t(udp.Fd()), &fd)
		if handle != nil {
			file := os.NewFile(uintptr(fd), "xhysteria-ipc")
			var pipe net.Conn
			pipe, err = net.FileConn(file)
			file.Close()
			if err == nil {
				bridge = &socketBridge{pipe}
			}
		}
	}
	if handle == nil {
		return nil, errors.New("native engine initialization failed")
	}
	if err != nil {
		C.xhys_engine_join(handle)
		return nil, err
	}
	deadline := time.Now().Add(StartupTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	bridge.setReadDeadline(deadline)
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { bridge.close(); close(stopped) })
	ready, err := bridge.read()
	if !stop() {
		<-stopped
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	} else if d, ok := ctx.Deadline(); err != nil && ok && !time.Now().Before(d) {
		// Netpoll may report the same deadline before the context timer runs.
		err = context.DeadlineExceeded
	}
	bridge.setReadDeadline(time.Time{})
	defer ready.release()
	if err != nil || ready.kind != readyFrame {
		bridge.close()
		C.xhys_engine_join(handle)
		if shared, ok := bridge.(*sharedBridge); ok {
			shared.releaseOwner()
		}
		if err != nil {
			return nil, fmt.Errorf("native startup: %w", err)
		}
		return nil, fmt.Errorf("native startup: %s", ready.data)
	}
	maxFlows := int(config.MaxFlows)
	if maxFlows == 0 {
		maxFlows = 128
		if config.Server {
			maxFlows = 64
		}
	}
	e := &Engine{handle: handle, bridge: bridge, arenaBytes: arenaBytes, local: local, remote: remote, flows: make(map[uint64]*Conn), out: make(chan frame, 128), done: make(chan struct{}), accept: accept,
		authenticate: config.Authenticate, authUsers: make(map[uint64]nativeIdentity), controls: make(map[uint64]chan frame), maxFlows: maxFlows, diagnostic: config.Diagnostic, role: "client", peer: config.Peer}
	e.masquerade = config.MasqueradeHandler
	if config.Server {
		e.role = "server"
		e.peer = fmt.Sprint(local)
	}
	liveEngines.Add(1)
	startedEngines.Add(1)
	liveArenaBytes.Add(arenaBytes)
	if config.Diagnostic {
		mode := "socket"
		if arenaBytes != 0 {
			mode = "shared"
		}
		maxConnections := int(config.MaxConnections)
		if config.Server && maxConnections == 0 {
			maxConnections = 32
		}
		packetIO := "direct"
		if config.PacketBridge {
			packetIO = "go_finalmask"
		}
		maskLayers := len(config.UDPMasks)
		if config.Salamander != "" {
			maskLayers++
		}
		fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] engine started bridge=%s arena_bytes=%d role=%s peer=%s max_flows=%d max_connections=%d packet_io=%s mask_layers=%d\n",
			mode, arenaBytes, e.role, e.peer, maxFlows, maxConnections, packetIO, maskLayers)
	}
	e.wg.Add(2)
	go e.readLoop()
	go e.writeLoop()
	return e, nil
}

func (e *Engine) diagnose(event string, err error) {
	if !e.diagnostic {
		return
	}
	select {
	case <-e.done:
		return
	default:
	}
	fmt.Fprintf(os.Stderr, "[XRAY_NET_DIAG][XHYSTERIA] %s role=%s peer=%s reason=%v\n", event, e.role, e.peer, err)
}

func (e *Engine) send(f frame, until time.Time) error {
	if !e.beginSubmit() {
		return net.ErrClosed
	}
	defer e.submitWG.Done()
	if !until.IsZero() && !time.Now().Before(until) {
		return os.ErrDeadlineExceeded
	}
	if direct, err := e.prepareFrame(&f); direct || err != nil {
		return err
	}
	queued := true
	defer func() {
		if queued {
			e.queued.Add(-1)
		}
	}()
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
		queued = false
		return nil
	}
}

func (e *Engine) prepareFrame(f *frame) (bool, error) {
	e.publishMu.Lock()
	select {
	case <-e.done:
		e.publishMu.Unlock()
		return false, net.ErrClosed
	default:
	}
	if bridge, ok := e.bridge.(*sharedBridge); ok && e.queued.Load() == 0 {
		n, err := bridge.tryWriteBatch([]frame{*f})
		if err != nil || n != 0 {
			e.publishMu.Unlock()
			if err == nil {
				err = bridge.notifyRust()
			}
			if err == nil {
				f.release()
			} else {
				e.stop()
			}
			return true, err
		}
	}
	// Count both queued and dequeued-but-unpublished frames. A direct write
	// must never overtake a batch already owned by the background writer.
	e.queued.Add(1)
	e.publishMu.Unlock()
	if f.borrowed {
		if f.parts != nil {
			f.parts = append(buf.MultiBuffer(nil), f.parts...)
		} else {
			f.storage = buf.NewWithSize(int32(len(f.data)))
			f.storage.Write(f.data)
			f.data = f.storage.Bytes()
		}
		f.borrowed = false
	}
	if bridge, ok := e.bridge.(*sharedBridge); ok {
		if err := bridge.notifyRust(); err != nil {
			e.queued.Add(-1)
			return false, err
		}
	}
	return false, nil
}

func (e *Engine) beginSubmit() bool {
	e.submitMu.Lock()
	defer e.submitMu.Unlock()
	if e.stopping {
		return false
	}
	e.submitWG.Add(1)
	return true
}

func (e *Engine) stop() {
	e.closeOnce.Do(func() {
		e.submitMu.Lock()
		e.stopping = true
		close(e.done)
		e.submitMu.Unlock()
		if e.bridge != nil {
			e.bridge.close()
		}
		e.mu.Lock()
		for _, c := range e.flows {
			c.fail(net.ErrClosed)
		}
		e.flows = make(map[uint64]*Conn)
		clear(e.authUsers)
		e.mu.Unlock()
	})
}

func (e *Engine) Close() error {
	e.stop()
	e.wg.Wait()
	if e.packet != nil {
		e.packet.stop(net.ErrClosed)
		e.packet.wg.Wait()
	}
	e.submitWG.Wait()
	if bridge, ok := e.bridge.(*sharedBridge); ok {
		bridge.releaseOwner()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handle != nil {
		C.xhys_engine_join(e.handle)
		e.handle = nil
		liveEngines.Add(-1)
		liveArenaBytes.Add(-e.arenaBytes)
	}
	for {
		select {
		case f := <-e.out:
			e.queued.Add(-1)
			f.release()
		default:
			return nil
		}
	}
}

var liveEngines, startedEngines, liveArenaBytes atomic.Int64

type RuntimeStats struct{ LiveEngines, StartedEngines, ArenaBytes int64 }

func Stats() RuntimeStats {
	return RuntimeStats{liveEngines.Load(), startedEngines.Load(), liveArenaBytes.Load()}
}
func (e *Engine) Done() <-chan struct{} { return e.done }

// UpdateTLS replaces certificates for new handshakes; existing flows stay open.
func (e *Engine) UpdateTLS(ctx context.Context, certificate, key string) error {
	payload, err := json.Marshal(struct {
		Certificate string `json:"certificate_pem"`
		Key         string `json:"key_pem"`
	}{certificate, key})
	if err != nil {
		return err
	}
	if len(payload) > maxFrame {
		return errors.New("native certificate update exceeds bridge frame size")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	id := e.next.Add(1)
	result := make(chan frame, 1)
	e.mu.Lock()
	e.controls[id] = result
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.controls, id)
		e.mu.Unlock()
		select {
		case f := <-result:
			f.release()
		default:
		}
	}()
	deadline, _ := ctx.Deadline()
	if err := e.send(frame{kind: updateTLS, id: id, data: payload}, deadline); err != nil {
		return err
	}
	select {
	case f := <-result:
		defer f.release()
		if len(f.data) != 0 {
			return fmt.Errorf("native certificate update: %s", f.data)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.done:
		return net.ErrClosed
	}
}

func (e *Engine) writeLoop() {
	defer e.wg.Done()
	defer e.stop()
	var batch [32]frame
	for {
		select {
		case <-e.done:
			return
		case f := <-e.out:
			batch[0] = f
			n := 1
		drain:
			for n < len(batch) {
				select {
				case batch[n] = <-e.out:
					n++
				default:
					break drain
				}
			}
			err := e.bridge.writeBatch(batch[:n])
			e.queued.Add(-int64(n))
			for i := range n {
				batch[i].release()
			}
			if err != nil {
				e.diagnose("bridge write failed", err)
				return
			}
		}
	}
}

func (e *Engine) readLoop() {
	defer e.wg.Done()
	defer e.stop()
	for {
		f, err := e.bridge.read()
		if err != nil {
			e.diagnose("bridge read failed", err)
			return
		}
		if f.kind == errorFrame && f.id == 0 {
			e.diagnose("engine failed", errors.New(string(f.data)))
			f.release()
			return
		}
		if f.kind == authRequest {
			if len(f.data) < 8 {
				f.release()
				return
			}
			owner, auth := binary.BigEndian.Uint64(f.data), string(f.data[8:])
			var user *protocol.MemoryUser
			accepted := false
			if e.authenticate != nil {
				user, accepted = e.authenticate(auth)
			}
			e.mu.Lock()
			if existing, ok := e.authUsers[owner]; ok && existing.auth != auth {
				accepted = false
			}
			if accepted {
				e.authUsers[owner] = nativeIdentity{auth: auth, user: user}
			}
			e.mu.Unlock()
			response := []byte{0}
			if accepted {
				response[0] = 1
			}
			id := f.id
			f.release()
			if e.send(frame{kind: authResponse, id: id, data: response}, time.Now().Add(10*time.Second)) != nil {
				return
			}
			continue
		}
		if f.kind == incomingHTTP {
			if err := e.acceptHTTP(f); err != nil {
				e.diagnose("HTTP masquerade request rejected", err)
			}
			continue
		}
		e.mu.Lock()
		if f.kind == authClosed {
			delete(e.authUsers, f.id)
			e.mu.Unlock()
			f.release()
			continue
		}
		if f.kind == controlResult {
			waiter := e.controls[f.id]
			if waiter != nil {
				waiter <- f
			} else {
				f.release()
			}
			e.mu.Unlock()
			continue
		}
		c := e.flows[f.id]
		if f.kind == incomingTCP || f.kind == incomingUDP {
			if e.accept == nil || c != nil || len(e.flows) >= e.maxFlows {
				f.release()
				e.mu.Unlock()
				e.send(frame{kind: closeFrame, id: f.id}, time.Now().Add(time.Second))
				continue
			}
			if len(f.data) < 14 {
				f.release()
				e.mu.Unlock()
				return
			}
			user := binary.BigEndian.Uint32(f.data)
			owner := binary.BigEndian.Uint64(f.data[4:])
			identity, known := e.authUsers[owner]
			if e.authenticate != nil && !known {
				f.release()
				e.mu.Unlock()
				return
			}
			peer, target, err := decodeAddress(f.data[12:])
			if err != nil {
				f.release()
				e.mu.Unlock()
				return
			}
			network := "tcp:"
			if f.kind == incomingUDP {
				network = "udp:"
			}
			dest, err := xnet.ParseDestination(network + string(target))
			if err != nil {
				f.release()
				e.mu.Unlock()
				return
			}
			remote, err := net.ResolveUDPAddr("udp", string(peer))
			if err != nil {
				f.release()
				e.mu.Unlock()
				return
			}
			c = newConn(e, f.id, dest, remote)
			c.UserIndex = user
			c.User = identity.user
			e.flows[f.id] = c
			e.mu.Unlock()
			go e.accept(c)
			f.release()
			continue
		}
		e.mu.Unlock()
		if c == nil {
			f.release()
			continue
		}
		if f.kind == writableFrame {
			count := 1
			if len(f.data) == 1 {
				count = int(f.data[0])
			}
			f.release()
			if count < 1 || count > cap(c.writeCredit) {
				return
			}
			for range count {
				select {
				case c.writeCredit <- struct{}{}:
				default:
				}
			}
			continue
		}
		c.mu.Lock()
		if c.userClosed {
			c.mu.Unlock()
			f.release()
			continue
		}
		queued := false
		select {
		case c.in <- f:
			queued = true
		default:
		}
		c.mu.Unlock()
		if !queued {
			f.release()
			c.fail(errors.New("native receive queue capacity reached"))
			e.mu.Lock()
			delete(e.flows, c.id)
			e.mu.Unlock()
			e.send(frame{kind: closeFrame, id: c.id}, time.Now().Add(time.Second))
		}
		if f.kind == errorFrame || f.kind == closeFrame {
			err := error(io.EOF)
			if f.kind == errorFrame {
				err = errors.New("native flow failed")
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
	if len(e.flows) >= e.maxFlows {
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
		defer f.release()
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
	targetAddress               string
	remote                      net.Addr
	UserIndex                   uint32
	User                        *protocol.MemoryUser
	in                          chan frame
	closed                      chan struct{}
	once                        sync.Once
	closeOnce                   sync.Once
	closeErr                    error
	mu                          sync.Mutex
	readMu, writeMu             sync.Mutex
	readDeadline, writeDeadline time.Time
	changed                     chan struct{}
	err                         error
	pending                     []byte
	pendingStorage              *buf.Buffer
	writeCredit                 chan struct{}
	readCredits                 uint8
	eof                         bool
	writeClosed                 bool
	userClosed                  bool
	OwnEngine                   bool
}

func newConn(e *Engine, id uint64, target xnet.Destination, remote net.Addr) *Conn {
	window := 8
	if e.role == "server" {
		window = 4
	}
	c := &Conn{engine: e, id: id, target: target, remote: remote, in: make(chan frame, window+2), closed: make(chan struct{}), changed: make(chan struct{}), writeCredit: make(chan struct{}, window)}
	if target.IsValid() && target.Address != nil {
		c.targetAddress = target.NetAddr()
	}
	for range window {
		c.writeCredit <- struct{}{}
	}
	return c
}

func (c *Conn) chunkSize() int32 {
	return 16384
}

func (c *Conn) NativeHysteriaTarget() xnet.Destination { return c.target }
func (c *Conn) LocalAddr() net.Addr                    { return c.engine.local }
func (c *Conn) RemoteAddr() net.Addr                   { return c.remote }

func (c *Conn) fail(err error) {
	c.once.Do(func() { c.mu.Lock(); c.err = err; c.mu.Unlock(); close(c.closed) })
}

func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.fail(net.ErrClosed)
		c.mu.Lock()
		c.userClosed = true
		c.mu.Unlock()
		c.engine.mu.Lock()
		delete(c.engine.flows, c.id)
		c.engine.mu.Unlock()
		c.closeErr = c.engine.send(frame{kind: closeFrame, id: c.id}, time.Now().Add(time.Second))
		c.readMu.Lock()
		if c.pendingStorage != nil {
			c.pendingStorage.Release()
			c.pendingStorage = nil
		}
		c.pending = nil
	drain:
		for {
			select {
			case f := <-c.in:
				f.release()
			default:
				break drain
			}
		}
		c.readMu.Unlock()
		if c.OwnEngine {
			c.closeErr = c.engine.Close()
		}
	})
	return c.closeErr
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
	if f.kind == dataFrame || f.kind == datagramFrame || f.kind == httpTrailers {
		c.readCredits++
		if c.readCredits >= uint8(cap(c.writeCredit)/2) || f.kind == datagramFrame && len(c.in) == 0 {
			count := c.readCredits
			c.readCredits = 0
			if err := c.engine.send(frame{kind: creditFrame, id: c.id, data: creditCounts[count : count+1]}, time.Now().Add(10*time.Second)); err != nil {
				select {
				case <-c.engine.done:
					// Published payload remains readable after engine shutdown.
				default:
					f.release()
					return frame{}, err
				}
			}
		}
	}
	return f, nil
}

func (c *Conn) waitWrite(f *frame) error {
	reserved := false
	defer func() {
		if reserved {
			c.engine.queued.Add(-1)
		}
	}()
	if f != nil {
		if !c.engine.beginSubmit() {
			return net.ErrClosed
		}
		defer c.engine.submitWG.Done()
	}
	for {
		c.mu.Lock()
		until, changed, err := c.writeDeadline, c.changed, c.err
		c.mu.Unlock()
		if err != nil {
			return err
		}
		if !until.IsZero() && !time.Now().Before(until) {
			return os.ErrDeadlineExceeded
		}
		select {
		case <-c.engine.done:
			return net.ErrClosed
		default:
		}
		if f == nil {
			select {
			case <-c.writeCredit:
				return nil
			default:
			}
		} else if !reserved {
			direct, err := c.engine.prepareFrame(f)
			if direct || err != nil {
				return err
			}
			reserved = true
		}
		var timeout <-chan time.Time
		var timer *time.Timer
		if !until.IsZero() {
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
			reserved = false
			if timer != nil {
				timer.Stop()
			}
			return nil
		}
	}
}

func (c *Conn) awaitWriteCredit() error { return c.waitWrite(nil) }
func (c *Conn) sendFrame(f frame) error {
	borrowed := f.borrowed
	err := c.waitWrite(&f)
	if err != nil && borrowed && f.parts == nil {
		f.release()
	}
	return err
}

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
			f.release()
			c.eof = true
			return 0, io.EOF
		case errorFrame:
			err := fmt.Errorf("native flow: %s", f.data)
			f.release()
			return 0, err
		case dataFrame:
			c.pending = f.data
			c.pendingStorage = f.storage
		default:
			f.release()
			return 0, errors.New("unexpected native TCP frame")
		}
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	if len(c.pending) == 0 && c.pendingStorage != nil {
		c.pendingStorage.Release()
		c.pendingStorage = nil
	}
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
		chunk := min(len(p), int(c.chunkSize()))
		if err := c.awaitWriteCredit(); err != nil {
			return n, err
		}
		if _, shared := c.engine.bridge.(*sharedBridge); shared {
			if err := c.sendFrame(frame{kind: dataFrame, id: c.id, data: p[:chunk], borrowed: true}); err != nil {
				return n, err
			}
		} else {
			storage := buf.NewWithSize(int32(chunk))
			storage.Write(p[:chunk])
			if err := c.sendFrame(frame{kind: dataFrame, id: c.id, data: storage.Bytes(), storage: storage}); err != nil {
				storage.Release()
				return n, err
			}
		}
		n += chunk
		p = p[chunk:]
	}
	return n, nil
}

func (c *Conn) ReadMultiBuffer() (buf.MultiBuffer, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.target.Network == xnet.Network_TCP {
		if c.eof {
			return nil, io.EOF
		}
		if len(c.pending) != 0 {
			b := c.pendingStorage
			if b == nil {
				b = buf.NewWithSize(int32(len(c.pending)))
				b.Write(c.pending)
			} else {
				b.Advance(b.Len() - int32(len(c.pending)))
			}
			c.pending, c.pendingStorage = nil, nil
			return buf.MultiBuffer{b}, nil
		}
		f, err := c.nextFrame()
		if err != nil {
			return nil, err
		}
		if f.kind != dataFrame {
			defer f.release()
			if f.kind == finFrame || f.kind == closeFrame {
				c.eof = true
				return nil, io.EOF
			}
			return nil, fmt.Errorf("native TCP flow: %s", f.data)
		}
		b := f.storage
		if b == nil {
			b = buf.NewWithSize(int32(len(f.data)))
			b.Write(f.data)
		}
		return buf.MultiBuffer{b}, nil
	}
	f, err := c.nextFrame()
	if err != nil {
		return nil, err
	}
	if f.kind == closeFrame || f.kind == finFrame {
		f.release()
		return nil, io.EOF
	}
	if f.kind != datagramFrame {
		f.release()
		return nil, errors.New("native UDP flow failed")
	}
	address, payload, err := decodeAddress(f.data)
	if err != nil || len(payload) > 4096 {
		f.release()
		return nil, errors.New("invalid native UDP datagram")
	}
	dest := c.target
	if !bytes.Equal(address, []byte(c.targetAddress)) {
		dest, err = xnet.ParseDestination("udp:" + string(address))
		if err != nil {
			f.release()
			return nil, err
		}
	}
	b := f.storage
	if b == nil {
		b = buf.NewWithSize(int32(len(payload)))
		b.Write(payload)
	} else {
		b.Advance(int32(len(f.data) - len(payload)))
	}
	b.UDP = &dest
	return buf.MultiBuffer{b}, nil
}

func (c *Conn) WriteMultiBuffer(mb buf.MultiBuffer) error {
	defer func() { buf.ReleaseMulti(mb) }()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.target.Network == xnet.Network_TCP {
		c.mu.Lock()
		err, closed := c.err, c.writeClosed
		c.mu.Unlock()
		if err != nil {
			return err
		}
		if closed {
			return io.ErrClosedPipe
		}
		for len(mb) != 0 {
			for len(mb) != 0 && mb[0].IsEmpty() {
				mb[0].Release()
				mb[0] = nil
				mb = mb[1:]
			}
			if len(mb) == 0 {
				break
			}
			if err := c.awaitWriteCredit(); err != nil {
				return err
			}
			if len(mb) == 1 && mb[0].Len() <= c.chunkSize() {
				storage := mb[0]
				if err := c.sendFrame(frame{kind: dataFrame, id: c.id, data: storage.Bytes(), storage: storage}); err != nil {
					return err
				}
				// The bridge or writer queue owns this buffer now.
				mb[0] = nil
				return nil
			}
			if _, shared := c.engine.bridge.(*sharedBridge); shared {
				count, size := 0, int32(0)
				for count < len(mb) && size+mb[count].Len() <= c.chunkSize() {
					size += mb[count].Len()
					count++
				}
				if count != 0 {
					if err := c.sendFrame(frame{kind: dataFrame, id: c.id, parts: mb[:count], borrowed: true}); err != nil {
						return err
					}
					clear(mb[:count])
					mb = mb[count:]
					continue
				}
				chunk := mb[0].BytesTo(c.chunkSize())
				if err := c.sendFrame(frame{kind: dataFrame, id: c.id, data: chunk, borrowed: true}); err != nil {
					return err
				}
				mb[0].Advance(c.chunkSize())
				continue
			}
			storage := buf.NewWithSize(c.chunkSize())
			for len(mb) != 0 && storage.Len() < c.chunkSize() {
				b := mb[0]
				n := min(b.Len(), c.chunkSize()-storage.Len())
				storage.Write(b.BytesTo(n))
				b.Advance(n)
				if b.IsEmpty() {
					b.Release()
					mb[0] = nil
					mb = mb[1:]
				}
			}
			if storage.Len() == 0 {
				storage.Release()
				continue
			}
			if err := c.sendFrame(frame{kind: dataFrame, id: c.id, data: storage.Bytes(), storage: storage}); err != nil {
				storage.Release()
				return err
			}
		}
		return nil
	}
	for index, b := range mb {
		if b.Len() == 0 || b.Len() > 4096 {
			return errors.New("native UDP size outside 1..4096")
		}
		address := c.targetAddress
		if b.UDP != nil {
			address = b.UDP.NetAddr()
		}
		if len(address) == 0 || len(address) > 2048 {
			return errors.New("invalid native UDP address")
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
		if _, shared := c.engine.bridge.(*sharedBridge); shared {
			header := buf.NewWithSize(int32(2 + len(address)))
			binary.BigEndian.PutUint16(header.Extend(2), uint16(len(address)))
			header.WriteString(address)
			if err := c.sendFrame(frame{kind: datagramFrame, id: c.id, parts: buf.MultiBuffer{header, b}}); err != nil {
				header.Release()
				return err
			}
			mb[index] = nil
			continue
		}
		storage := buf.NewWithSize(int32(2+len(address)) + b.Len())
		var size [2]byte
		binary.BigEndian.PutUint16(size[:], uint16(len(address)))
		storage.Write(size[:])
		storage.WriteString(address)
		storage.Write(b.Bytes())
		if err := c.sendFrame(frame{kind: datagramFrame, id: c.id, data: storage.Bytes(), storage: storage}); err != nil {
			storage.Release()
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
