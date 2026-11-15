//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

/*
#include "../../../native/libxhysteria/include/xhysteria.h"
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/xtls/xray-core/common/buf"
	"golang.org/x/sys/unix"
)

const (
	sharedMagic    = 0x5848595353484d32
	sharedVersion  = 2
	sharedHeader   = 320
	sharedCapacity = 32
	sharedStride   = (16 + maxFrame + 63) &^ 63
	sharedBytes    = sharedHeader + 2*sharedCapacity*sharedStride
)

var eventfdSignal = [8]byte{1}

type frameBridge interface {
	read() (frame, error)
	writeBatch([]frame) error
	close() error
	setReadDeadline(time.Time) error
}

type socketBridge struct{ net.Conn }

func (b *socketBridge) read() (frame, error)              { return readFrame(b.Conn) }
func (b *socketBridge) close() error                      { return b.Conn.Close() }
func (b *socketBridge) setReadDeadline(t time.Time) error { return b.Conn.SetReadDeadline(t) }
func (b *socketBridge) writeBatch(batch []frame) error {
	var headers [32][16]byte
	var slices [64][]byte
	for i, f := range batch {
		headers[i][0] = f.kind
		binary.BigEndian.PutUint64(headers[i][4:], f.id)
		binary.BigEndian.PutUint32(headers[i][12:], uint32(len(f.data)))
		slices[2*i], slices[2*i+1] = headers[i][:], f.data
	}
	buffers := net.Buffers(slices[:2*len(batch)])
	_, err := buffers.WriteTo(b.Conn)
	return err
}

type sharedBridge struct {
	data         []byte
	notify, wake *os.File
	space        chan struct{}
	events       chan struct{}
	closed       chan struct{}
	once         sync.Once
	released     uint32
	wakeRaw      syscall.RawConn
	pollWG       sync.WaitGroup
	errMu        sync.Mutex
	pollErr      error
}

func sharedFromView(view C.XhysSharedView) (*sharedBridge, error) {
	notify := os.NewFile(uintptr(view.notify_fd), "xhysteria-eventfd")
	wake := os.NewFile(uintptr(view.wake_fd), "xhysteria-wakefd")
	if view.data == nil || uint64(view.len) != sharedBytes || uintptr(unsafe.Pointer(view.data))%64 != 0 {
		notify.Close()
		wake.Close()
		return nil, errors.New("invalid native shared arena view")
	}
	b := &sharedBridge{
		data:   unsafe.Slice((*byte)(unsafe.Pointer(view.data)), sharedBytes),
		notify: notify, wake: wake, space: make(chan struct{}, 1), closed: make(chan struct{}),
	}
	var err error
	b.wakeRaw, err = wake.SyscallConn()
	if err != nil {
		b.close()
		return nil, err
	}
	d := b.data
	if binary.LittleEndian.Uint64(d) != sharedMagic ||
		binary.LittleEndian.Uint32(d[8:]) != sharedVersion ||
		binary.LittleEndian.Uint32(d[12:]) != sharedCapacity ||
		binary.LittleEndian.Uint32(d[16:]) != sharedStride ||
		binary.LittleEndian.Uint32(d[20:]) != maxFrame ||
		binary.LittleEndian.Uint64(d[24:]) != sharedBytes {
		b.close()
		return nil, errors.New("native shared arena layout mismatch")
	}
	// A failed deadline setup means that Go did not register this fd in netpoll.
	if err := notify.SetReadDeadline(time.Time{}); err != nil {
		b.close()
		return nil, err
	}
	b.startNotifications()
	return b, nil
}

func (b *sharedBridge) startNotifications() {
	b.events = make(chan struct{}, 1)
	b.pollWG.Add(1)
	go func() {
		defer b.pollWG.Done()
		for {
			var value [8]byte
			_, err := io.ReadFull(b.notify, value[:])
			if err != nil {
				b.errMu.Lock()
				b.pollErr = err
				b.errMu.Unlock()
			}
			select {
			case b.space <- struct{}{}:
			default:
			}
			select {
			case b.events <- struct{}{}:
			default:
			}
			if err != nil {
				return
			}
		}
	}()
}

func (b *sharedBridge) notificationError() error {
	b.errMu.Lock()
	defer b.errMu.Unlock()
	return b.pollErr
}

func (b *sharedBridge) word(offset int) *uint32 {
	return (*uint32)(unsafe.Pointer(&b.data[offset]))
}

func (b *sharedBridge) notifyRust() error {
	if b.wakeRaw == nil {
		_, err := b.wake.Write(eventfdSignal[:])
		return err
	}
	var writeErr error
	err := b.wakeRaw.Control(func(fd uintptr) {
		for {
			_, writeErr = unix.Write(int(fd), eventfdSignal[:])
			if writeErr == unix.EINTR {
				continue
			}
			if writeErr == unix.EAGAIN {
				writeErr = nil
			}
			break
		}
	})
	if err != nil {
		return err
	}
	return writeErr
}

func (b *sharedBridge) close() error {
	b.once.Do(func() {
		atomic.StoreUint32(b.word(32), 1)
		close(b.closed)
		b.notifyRust()
		b.notify.Close()
		b.wake.Close()
		b.pollWG.Wait()
	})
	return nil
}

func (b *sharedBridge) setReadDeadline(t time.Time) error { return b.notify.SetReadDeadline(t) }

func (b *sharedBridge) read() (frame, error) {
	for {
		tail := atomic.LoadUint32(b.word(256))
		count := atomic.LoadUint32(b.word(192)) - tail
		if count > sharedCapacity {
			return frame{}, errors.New("corrupt native event ring")
		}
		if count != 0 {
			offset := sharedHeader + sharedCapacity*sharedStride + int(tail&(sharedCapacity-1))*sharedStride
			d := b.data[offset:]
			kind, n := binary.LittleEndian.Uint32(d), binary.LittleEndian.Uint32(d[4:])
			if kind > 255 || n > maxFrame {
				return frame{}, errors.New("invalid native shared event")
			}
			f := frame{kind: byte(kind), id: binary.LittleEndian.Uint64(d[8:])}
			if n != 0 {
				f.storage = buf.NewWithSize(int32(n))
				f.storage.Write(d[16 : 16+int(n)])
				f.data = f.storage.Bytes()
			}
			atomic.StoreUint32(b.word(256), tail+1)
			b.released++
			if count == sharedCapacity || b.released >= sharedCapacity {
				b.released = 0
				b.notifyRust()
			}
			return f, nil
		}
		if b.released != 0 {
			b.released = 0
			b.notifyRust()
		}
		if atomic.LoadUint32(b.word(32)) != 0 {
			return frame{}, net.ErrClosed
		}
		if err := b.notificationError(); err != nil {
			return frame{}, err
		}
		select {
		case <-b.events:
		case <-b.closed:
			return frame{}, net.ErrClosed
		}
	}
}

func (b *sharedBridge) writeBatch(batch []frame) error {
	for len(batch) != 0 {
		if err := b.notificationError(); err != nil {
			return err
		}
		if atomic.LoadUint32(b.word(32)) != 0 {
			return net.ErrClosed
		}
		head := atomic.LoadUint32(b.word(64))
		count := head - atomic.LoadUint32(b.word(128))
		if count > sharedCapacity {
			return errors.New("corrupt native command ring")
		}
		n := min(len(batch), sharedCapacity-int(count))
		for i := range n {
			f := batch[i]
			if len(f.data) > maxFrame {
				return errors.New("oversized native command")
			}
			offset := sharedHeader + int((head+uint32(i))&(sharedCapacity-1))*sharedStride
			d := b.data[offset:]
			binary.LittleEndian.PutUint32(d, uint32(f.kind))
			binary.LittleEndian.PutUint32(d[4:], uint32(len(f.data)))
			binary.LittleEndian.PutUint64(d[8:], f.id)
			copy(d[16:], f.data)
		}
		if n != 0 {
			atomic.StoreUint32(b.word(64), head+uint32(n))
			batch = batch[n:]
		}
		if err := b.notifyRust(); err != nil {
			return err
		}
		if n == 0 {
			select {
			case <-b.closed:
				return net.ErrClosed
			case <-b.space:
			}
		}
	}
	return nil
}
