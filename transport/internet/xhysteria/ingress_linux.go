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

func NewNativeIngress(public *net.UDPConn, key [16]byte, slot byte, masks []UDPMaskConfig) (*NativeIngress, error) {
	if public == nil {
		return nil, errors.New("native ingress requires a public UDP socket")
	}
	config, err := json.Marshal(struct {
		Key   [16]byte        `json:"key"`
		Slot  byte            `json:"slot"`
		Masks []UDPMaskConfig `json:"masks,omitempty"`
	}{key, slot, masks})
	if err != nil {
		return nil, err
	}
	file, err := public.File()
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
	if !bool(C.xhys_ingress_add(i.handle, C.uint8_t(slot), C.int32_t(parent.Fd()))) {
		child.Close()
		return nil, errors.New("native ingress could not attach generation")
	}
	i.used[slot] = true
	return child, nil
}

func (i *NativeIngress) Activate(slot byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle == nil || !bool(C.xhys_ingress_activate(i.handle, C.uint8_t(slot))) {
		return errors.New("native ingress admission change was not acknowledged")
	}
	return nil
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
