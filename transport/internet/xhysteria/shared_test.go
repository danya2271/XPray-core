//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"golang.org/x/sys/unix"
)

func testSharedBridge(t *testing.T) *sharedBridge {
	t.Helper()
	efd := func() *os.File {
		fd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
		if err != nil {
			t.Fatal(err)
		}
		file := os.NewFile(uintptr(fd), "test-eventfd")
		if err := file.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return file
	}
	b := &sharedBridge{data: make([]byte, sharedBytes), notify: efd(), wake: efd(), closed: make(chan struct{}), space: make(chan struct{}, 1)}
	b.startNotifications()
	t.Cleanup(func() { b.close() })
	return b
}

func TestSharedBridgeBatchWraparoundAndWakeups(t *testing.T) {
	b := testSharedBridge(t)
	// The producer/consumer are Go goroutines here so -race can audit the
	// release/acquire protocol that Rust uses on the opposite side of the ABI.
	initial := ^uint32(0) - 7
	for _, offset := range []int{64, 128, 192, 256} {
		atomic.StoreUint32(b.word(offset), initial)
	}
	const frames = 4096
	result := make(chan error, 1)
	payload := bytes.Repeat([]byte("shared-bridge"), 127)
	go func() {
		for id := uint64(0); id < frames; {
			tail := atomic.LoadUint32(b.word(128))
			if atomic.LoadUint32(b.word(64)) == tail {
				var value [8]byte
				if _, err := io.ReadFull(b.wake, value[:]); err != nil {
					result <- err
					return
				}
				continue
			}
			offset := sharedHeader + int(tail&(sharedCapacity-1))*sharedStride
			d := b.data[offset:]
			if binary.LittleEndian.Uint64(d[8:]) != id || !bytes.Equal(d[16:16+len(payload)], payload) {
				result <- io.ErrUnexpectedEOF
				return
			}
			atomic.StoreUint32(b.word(128), tail+1)
			// Mirror the frame into the RX ring, respecting consumer ownership.
			for {
				head := atomic.LoadUint32(b.word(192))
				if head-atomic.LoadUint32(b.word(256)) < sharedCapacity {
					offset := sharedHeader + sharedCapacity*sharedStride + int(head&(sharedCapacity-1))*sharedStride
					d := b.data[offset:]
					binary.LittleEndian.PutUint32(d, uint32(dataFrame))
					binary.LittleEndian.PutUint32(d[4:], uint32(len(payload)))
					binary.LittleEndian.PutUint64(d[8:], id)
					copy(d[16:], payload)
					atomic.StoreUint32(b.word(192), head+1)
					break
				}
				var value [8]byte
				if _, err := io.ReadFull(b.wake, value[:]); err != nil {
					result <- err
					return
				}
			}
			var value [8]byte
			binary.LittleEndian.PutUint64(value[:], 1)
			if _, err := b.notify.Write(value[:]); err != nil {
				result <- err
				return
			}
			id++
		}
		result <- nil
	}()
	writer := make(chan error, 1)
	go func() {
		for id := 0; id < frames; {
			batch := make([]frame, min(32, frames-id))
			for i := range batch {
				batch[i] = frame{kind: dataFrame, id: uint64(id + i), data: payload}
			}
			if err := b.writeBatch(batch); err != nil {
				writer <- err
				return
			}
			id += len(batch)
		}
		writer <- nil
	}()
	for id := 0; id < frames; id++ {
		f, err := b.read()
		if err != nil {
			t.Fatal(err)
		}
		if f.id != uint64(id) || !bytes.Equal(f.data, payload) {
			t.Fatal("shared frame changed")
		}
		f.release()
	}
	if err := <-writer; err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestSharedBridgeCloseWakesFullWriterAndEmptyReader(t *testing.T) {
	for _, write := range []bool{false, true} {
		b := testSharedBridge(t)
		if write {
			atomic.StoreUint32(b.word(64), sharedCapacity)
		}
		result := make(chan error, 1)
		go func() {
			if write {
				result <- b.writeBatch([]frame{{kind: dataFrame, data: []byte("blocked")}})
			} else {
				f, err := b.read()
				f.release()
				result <- err
			}
		}()
		b.close()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("blocked operation succeeded after close")
			}
		case <-time.After(time.Second):
			t.Fatal("shared bridge close lost a wake")
		}
	}
}

func TestReadMultiBufferOwnsCopiedStorageAfterSharedRingReuse(t *testing.T) {
	b := testSharedBridge(t)
	offset := sharedHeader + sharedCapacity*sharedStride
	d := b.data[offset:]
	binary.LittleEndian.PutUint32(d, uint32(dataFrame))
	binary.LittleEndian.PutUint32(d[4:], 4)
	copy(d[16:], "test")
	atomic.StoreUint32(b.word(192), 1)
	f, err := b.read()
	if err != nil {
		t.Fatal(err)
	}
	// Simulate immediate slot reuse, before Xray consumes/releases its buffer.
	copy(d[16:], "xxxx")
	e := &Engine{done: make(chan struct{}), out: make(chan frame, 16)}
	c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	c.in <- f
	mb, err := c.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	defer buf.ReleaseMulti(mb)
	if string(mb[0].Bytes()) != "test" {
		t.Fatal("native arena escaped into an Xray buffer")
	}
}
