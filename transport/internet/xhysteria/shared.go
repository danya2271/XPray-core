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
	sharedVersion  = 5
	sharedHeader   = 320
	sharedCapacity = 8
	sharedStride   = (16 + maxFrame + 63) &^ 63
	sharedBytes    = sharedHeader + 2*sharedCapacity*sharedStride
)

var eventfdSignal = [8]byte{1}
var creditCounts = [9]byte{0, 1, 2, 3, 4, 5, 6, 7, 8}

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
	space        *os.File
	once         sync.Once
	wakeRaw      syscall.RawConn
	notifyReader *eventReader
	spaceReader  *eventReader
}

type eventReader struct {
	raw      syscall.RawConn
	value    [8]byte
	err      error
	callback func(uintptr) bool
}

func newEventReader(file *os.File) (*eventReader, error) {
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, err
	}
	reader := &eventReader{raw: raw}
	reader.callback = reader.consume
	return reader, nil
}

func (r *eventReader) consume(fd uintptr) bool {
	for {
		length, _, errno := syscall.RawSyscall(syscall.SYS_READ, fd, uintptr(unsafe.Pointer(&r.value[0])), uintptr(len(r.value)))
		switch errno {
		case unix.EINTR:
			continue
		case unix.EAGAIN:
			return false
		case 0:
			if length != uintptr(len(r.value)) {
				r.err = io.ErrUnexpectedEOF
			}
		default:
			r.err = errno
		}
		return true
	}
}

func (r *eventReader) wait() error {
	r.err = nil
	if err := r.raw.Read(r.callback); err != nil {
		return err
	}
	return r.err
}

func sharedFromView(view C.XhysSharedView) (*sharedBridge, error) {
	notify := os.NewFile(uintptr(view.notify_fd), "xhysteria-eventfd")
	wake := os.NewFile(uintptr(view.wake_fd), "xhysteria-wakefd")
	space := os.NewFile(uintptr(view.space_fd), "xhysteria-spacefd")
	if view.data == nil || uint64(view.len) != sharedBytes || uintptr(unsafe.Pointer(view.data))%64 != 0 {
		notify.Close()
		wake.Close()
		space.Close()
		return nil, errors.New("invalid native shared arena view")
	}
	b := &sharedBridge{
		data:   unsafe.Slice((*byte)(unsafe.Pointer(view.data)), sharedBytes),
		notify: notify, wake: wake, space: space,
	}
	var err error
	b.wakeRaw, err = wake.SyscallConn()
	if err != nil {
		b.close()
		return nil, err
	}
	b.notifyReader, err = newEventReader(notify)
	if err != nil {
		b.close()
		return nil, err
	}
	b.spaceReader, err = newEventReader(space)
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
	return b, nil
}

func (b *sharedBridge) wait(file *os.File, pending int) error {
	reader := b.notifyReader
	if file == b.space {
		reader = b.spaceReader
	}
	if reader == nil {
		var value [8]byte
		if _, err := io.ReadFull(file, value[:]); err != nil {
			return err
		}
	} else if err := reader.wait(); err != nil {
		return err
	}
	// Rearm before observing published ring state; a later producer must
	// signal again. RX and TX-space wait independently in Go netpoll.
	atomic.SwapUint32(b.word(pending), 0)
	return nil
}

func (b *sharedBridge) word(offset int) *uint32 {
	return (*uint32)(unsafe.Pointer(&b.data[offset]))
}

func (b *sharedBridge) notifyRust() error {
	if atomic.SwapUint32(b.word(40), 1) != 0 {
		return nil
	}
	if b.wakeRaw == nil {
		_, err := b.wake.Write(eventfdSignal[:])
		if err != nil {
			atomic.SwapUint32(b.word(40), 0)
		}
		return err
	}
	var writeErr error
	err := b.wakeRaw.Control(func(fd uintptr) {
		for {
			_, _, errno := syscall.RawSyscall(syscall.SYS_WRITE, fd, uintptr(unsafe.Pointer(&eventfdSignal[0])), uintptr(len(eventfdSignal)))
			if errno == unix.EINTR {
				continue
			}
			if errno != 0 && errno != unix.EAGAIN {
				writeErr = errno
			}
			break
		}
	})
	if err != nil {
		atomic.SwapUint32(b.word(40), 0)
		return err
	}
	if writeErr != nil {
		atomic.SwapUint32(b.word(40), 0)
	}
	return writeErr
}

func (b *sharedBridge) close() error {
	b.once.Do(func() {
		atomic.StoreUint32(b.word(32), 1)
		b.notifyRust()
		b.notify.Close()
		b.wake.Close()
		b.space.Close()
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
			if f.kind == writableFrame && n == 1 && d[16] < byte(len(creditCounts)) {
				f.data = creditCounts[d[16] : int(d[16])+1]
			} else if n != 0 {
				f.storage = buf.NewWithSize(int32(n))
				f.storage.Write(d[16 : 16+int(n)])
				f.data = f.storage.Bytes()
			}
			atomic.StoreUint32(b.word(256), tail+1)
			if atomic.LoadUint32(b.word(48)) != 0 && atomic.SwapUint32(b.word(48), 0) != 0 {
				b.notifyRust()
			}
			return f, nil
		}
		if atomic.LoadUint32(b.word(32)) != 0 {
			return frame{}, net.ErrClosed
		}
		if err := b.wait(b.notify, 36); err != nil {
			return frame{}, err
		}
	}
}

func (b *sharedBridge) writeBatch(batch []frame) error {
	for len(batch) != 0 {
		n, err := b.tryWriteBatch(batch)
		if err != nil {
			return err
		}
		batch = batch[n:]
		if n == 0 {
			if err := b.wait(b.space, 44); err != nil {
				return err
			}
		}
	}
	return nil
}

// The engine serializes publication. A full ring falls back to the bounded
// writer queue instead of blocking the caller holding the publication lock.
func (b *sharedBridge) tryWriteBatch(batch []frame) (int, error) {
	if atomic.LoadUint32(b.word(32)) != 0 {
		return 0, net.ErrClosed
	}
	head := atomic.LoadUint32(b.word(64))
	count := head - atomic.LoadUint32(b.word(128))
	if count > sharedCapacity {
		return 0, errors.New("corrupt native command ring")
	}
	if count == sharedCapacity {
		atomic.StoreUint32(b.word(52), 1)
		count = head - atomic.LoadUint32(b.word(128))
	}
	n := min(len(batch), sharedCapacity-int(count))
	for i := range n {
		f := batch[i]
		if len(f.data) > maxFrame {
			return 0, errors.New("oversized native command")
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
	}
	return n, b.notifyRust()
}
