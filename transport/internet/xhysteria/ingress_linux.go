//go:build xhysteria_native && cgo && linux && !android

package xhysteria

/*
#include "../../../native/libxhysteria/include/xhysteria.h"
*/
import "C"

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync"
	"unsafe"

	"github.com/xtls/xray-core/transport/internet/handoff"
	"golang.org/x/sys/unix"
)

// NativeIngress is an experimental Linux packet relay. The public descriptor
// is duplicated; this wrapper never transfers ownership of the caller's socket.
// Masks are configured in their physical wire order (outermost first). It has
// no crash recovery or retirement certificate and must not enable node updates.
type NativeIngress struct {
	mu     sync.Mutex
	handle unsafe.Pointer
	used   [256]bool
}

// NativeIngressStats is approximate telemetry, not a QUIC drain certificate.
type NativeIngressStats struct {
	Received, Forwarded, Backpressure  uint64
	QueueDrops, SendDrops, EgressDrops uint64
	QueuedBytes, PeakQueuedBytes       uint64
	Running                            bool
}

func (i *NativeIngress) Stats() NativeIngressStats {
	i.mu.Lock()
	defer i.mu.Unlock()
	var stats C.XhysIngressStats
	if i.handle == nil || !bool(C.xhys_ingress_stats(i.handle, &stats)) {
		return NativeIngressStats{}
	}
	return NativeIngressStats{
		Received: uint64(stats.received), Forwarded: uint64(stats.forwarded), Backpressure: uint64(stats.backpressure),
		QueueDrops: uint64(stats.queue_drops), SendDrops: uint64(stats.send_drops), EgressDrops: uint64(stats.egress_drops),
		QueuedBytes: uint64(stats.queued_bytes), PeakQueuedBytes: uint64(stats.peak_queued_bytes), Running: stats.running != 0,
	}
}

func NewNativeIngress(public *net.UDPConn, key [16]byte, slot byte, masks []UDPMaskConfig) (*NativeIngress, error) {
	return newNativeIngress(public, key, slot, masks, false)
}

// NewDormantNativeIngress must not read until AdoptReceive confirms that the
// sole direct native worker has released the same public socket.
func NewDormantNativeIngress(public *net.UDPConn, key [16]byte, slot byte, masks []UDPMaskConfig) (*NativeIngress, error) {
	return newNativeIngress(public, key, slot, masks, true)
}

func newNativeIngress(public *net.UDPConn, key [16]byte, slot byte, masks []UDPMaskConfig, dormant bool) (*NativeIngress, error) {
	if public == nil {
		return nil, errors.New("native ingress requires a public UDP socket")
	}
	config, err := json.Marshal(struct {
		Key     [16]byte        `json:"key"`
		Slot    byte            `json:"slot"`
		Masks   []UDPMaskConfig `json:"masks,omitempty"`
		Dormant bool            `json:"dormant,omitempty"`
	}{key, slot, masks, dormant})
	if err != nil {
		return nil, err
	}
	file, err := handoff.DuplicatePacketSocket(public)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	handle := C.xhys_ingress_start((*C.uint8_t)(unsafe.Pointer(&config[0])), C.uintptr_t(len(config)), C.int32_t(file.Fd()))
	if handle == nil {
		return nil, errors.New("native managed ingress could not start")
	}
	return &NativeIngress{handle: handle}, nil
}

// AdoptReceive requires the idempotent snapshot returned by the direct
// generation's release-receive command. Callers must never synthesize it.
func (i *NativeIngress) AdoptReceive(snapshot []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil || len(snapshot) == 0 || len(snapshot) > 512*1024 {
		return errors.New("invalid native receive ownership snapshot")
	}
	if !bool(C.xhys_ingress_adopt_receive(i.handle, (*C.uint8_t)(unsafe.Pointer(&snapshot[0])), C.uintptr_t(len(snapshot)))) {
		return errors.New("native receive ownership adoption was not acknowledged")
	}
	return nil
}

// FenceGeneration is irreversible once acknowledged; Activate cannot roll a
// fenced slot back into admission. False means Initial pins/queues still need
// time. It does not mean the worker has already processed the marker.
func (i *NativeIngress) FenceGeneration(slot byte, token [16]byte) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil || token == [16]byte{} {
		return false, errors.New("invalid generation fence")
	}
	return bool(C.xhys_ingress_fence(i.handle, C.uint8_t(slot), (*C.uint8_t)(unsafe.Pointer(&token[0])))), nil
}

func (i *NativeIngress) Add(slot byte) (*os.File, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil || i.used[slot] {
		return nil, errors.New("native managed generation slot unavailable")
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	parent, child := os.NewFile(uintptr(fds[0]), "native-ingress"), os.NewFile(uintptr(fds[1]), "native-generation")
	defer parent.Close()
	// A lost acknowledgement cannot prove that the receive owner did not add
	// this slot. Never reuse it after an uncertain attachment.
	i.used[slot] = true
	if !bool(C.xhys_ingress_add(i.handle, C.uint8_t(slot), C.int32_t(parent.Fd()))) {
		child.Close()
		return nil, errors.New("native ingress could not attach generation")
	}
	return child, nil
}

// AddRing exports a native-only bounded receive ring and a Unix reply channel.
// Files are returned in worker, memfd, readable-eventfd, writable-eventfd order.
func (i *NativeIngress) AddRing(slot byte) ([]*os.File, error) {
	return i.addRing(slot, false)
}

// AddDirectRing requires the worker to inherit the public transmit descriptor.
// Unlike AddRing, its private channel carries no outgoing datagrams. The ingress
// can therefore release public receive without abandoning relay egress queues.
func (i *NativeIngress) AddDirectRing(slot byte) ([]*os.File, error) {
	return i.addRing(slot, true)
}

func (i *NativeIngress) addRing(slot byte, directTransmit bool) ([]*os.File, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil || i.used[slot] {
		return nil, errors.New("native managed generation slot unavailable")
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	parent, child := os.NewFile(uintptr(fds[0]), "native-ingress"), os.NewFile(uintptr(fds[1]), "native-generation")
	defer parent.Close()
	var ring C.XhysPacketRingDescriptors
	// Reserve even if acknowledgement times out: that is uncertain admission,
	// not permission to reuse the same slot.
	i.used[slot] = true
	if !bool(C.xhys_ingress_add_ring(i.handle, C.uint8_t(slot), C.int32_t(parent.Fd()), C.bool(directTransmit), &ring)) {
		child.Close()
		return nil, errors.New("native ring attachment was not acknowledged")
	}
	return []*os.File{child, os.NewFile(uintptr(ring.file), "native-packet-ring"),
		os.NewFile(uintptr(ring.wake), "native-ring-readable"), os.NewFile(uintptr(ring.space), "native-ring-writable")}, nil
}

func (i *NativeIngress) Activate(slot byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil || !bool(C.xhys_ingress_activate(i.handle, C.uint8_t(slot))) {
		return errors.New("native ingress admission change was not acknowledged")
	}
	return nil
}

// Abandon requires proof that an unadmitted candidate has exited.
func (i *NativeIngress) Abandon(slot byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil || !bool(C.xhys_ingress_abandon(i.handle, C.uint8_t(slot))) {
		return errors.New("native candidate abandonment not acknowledged")
	}
	return nil
}

// Retire requires udp-drained from that generation with the same fence token.
func (i *NativeIngress) Retire(slot byte, token [16]byte) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil || token == [16]byte{} {
		return false, errors.New("invalid retirement token")
	}
	return bool(C.xhys_ingress_retire(i.handle, C.uint8_t(slot), (*C.uint8_t)(unsafe.Pointer(&token[0])))), nil
}

// ReleasePublic ACK must precede the active worker's acquire with this token.
func (i *NativeIngress) ReleasePublic(slot byte, token [16]byte) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil || token == [16]byte{} {
		return false, errors.New("invalid public receive release")
	}
	return bool(C.xhys_ingress_release_public(i.handle, C.uint8_t(slot), (*C.uint8_t)(unsafe.Pointer(&token[0])))), nil
}

func (i *NativeIngress) Dropped() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil {
		return 0
	}
	return uint64(C.xhys_ingress_dropped(i.handle))
}

func (i *NativeIngress) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle != nil {
		C.xhys_ingress_join(i.handle)
		i.handle = nil
	}
	return nil
}
