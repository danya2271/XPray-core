//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
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
	b := &sharedBridge{data: make([]byte, sharedBytes), notify: efd(), wake: efd(), space: efd()}
	b.initializeOwner(nil)
	atomic.StoreUint32(b.word(56), (1<<sharedPayloadSlots)-1)
	atomic.StoreUint32(b.word(60), (1<<sharedPayloadSlots)-1)
	t.Cleanup(func() { b.close() })
	var err error
	b.notifyReader, err = newEventReader(b.notify)
	if err != nil {
		t.Fatal(err)
	}
	b.spaceReader, err = newEventReader(b.space)
	if err != nil {
		t.Fatal(err)
	}
	b.wakeRaw, err = b.wake.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
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
				atomic.SwapUint32(b.word(40), 0)
				continue
			}
			offset := sharedHeader + int(tail&(sharedCapacity-1))*sharedStride
			d := b.data[offset:]
			contents := d[16 : 16+len(payload)]
			kind := binary.LittleEndian.Uint32(d)
			leased := kind&sharedPayloadFlag != 0
			slot := int(kind >> 16)
			if leased {
				offset := sharedRingBytes + slot*sharedPayloadSize
				contents = b.data[offset : offset+len(payload)]
			}
			if binary.LittleEndian.Uint64(d[8:]) != id || !bytes.Equal(contents, payload) {
				result <- io.ErrUnexpectedEOF
				return
			}
			atomic.StoreUint32(b.word(128), tail+1)
			if leased {
				atomic.OrUint32(b.word(56), 1<<slot)
			}
			if atomic.SwapUint32(b.word(52), 0) != 0 && atomic.SwapUint32(b.word(44), 1) == 0 {
				if _, err := b.space.Write(eventfdSignal[:]); err != nil {
					result <- err
					return
				}
			}
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
				atomic.StoreUint32(b.word(48), 1)
				if atomic.LoadUint32(b.word(192))-atomic.LoadUint32(b.word(256)) < sharedCapacity {
					continue
				}
				var value [8]byte
				if _, err := io.ReadFull(b.wake, value[:]); err != nil {
					result <- err
					return
				}
				atomic.SwapUint32(b.word(40), 0)
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

func TestSharedBridgeEventSpaceWakesOnlyAWaitingWriter(t *testing.T) {
	bridge := testSharedBridge(t)
	publish := func(index uint32) {
		offset := sharedHeader + sharedCapacity*sharedStride + int(index&(sharedCapacity-1))*sharedStride
		binary.LittleEndian.PutUint32(bridge.data[offset:], uint32(dataFrame))
		binary.LittleEndian.PutUint64(bridge.data[offset+8:], uint64(index))
		atomic.StoreUint32(bridge.word(192), index+1)
	}
	publish(0)
	first, err := bridge.read()
	if err != nil {
		t.Fatal(err)
	}
	first.release()
	if atomic.LoadUint32(bridge.word(40)) != 0 {
		t.Fatal("uncontended event read woke the native worker")
	}
	atomic.StoreUint32(bridge.word(48), 1)
	publish(1)
	second, err := bridge.read()
	if err != nil {
		t.Fatal(err)
	}
	second.release()
	if atomic.LoadUint32(bridge.word(48)) != 0 || atomic.LoadUint32(bridge.word(40)) != 1 {
		t.Fatal("event read failed to wake its waiting writer")
	}
}

func TestSharedBridgeFullCommandRingArmsWriter(t *testing.T) {
	bridge := testSharedBridge(t)
	atomic.StoreUint32(bridge.word(64), sharedCapacity)
	count, err := bridge.tryWriteBatch([]frame{{kind: dataFrame, id: 7}})
	if err != nil || count != 0 || atomic.LoadUint32(bridge.word(52)) != 1 {
		t.Fatalf("full command ring did not arm its writer: count=%d err=%v", count, err)
	}
	atomic.StoreUint32(bridge.word(128), 1)
	count, err = bridge.tryWriteBatch([]frame{{kind: dataFrame, id: 7}})
	if err != nil || count != 1 {
		t.Fatalf("writer did not observe newly freed space: count=%d err=%v", count, err)
	}
}

func TestSharedBridgeCreditPayloadSurvivesSlotReuse(t *testing.T) {
	bridge := testSharedBridge(t)
	offset := sharedHeader + sharedCapacity*sharedStride
	binary.LittleEndian.PutUint32(bridge.data[offset:], uint32(writableFrame))
	binary.LittleEndian.PutUint32(bridge.data[offset+4:], 1)
	bridge.data[offset+16] = 4
	atomic.StoreUint32(bridge.word(192), 1)
	value, err := bridge.read()
	if err != nil {
		t.Fatal(err)
	}
	defer value.release()
	bridge.data[offset+16] = 7
	if value.storage != nil || !bytes.Equal(value.data, []byte{4}) {
		t.Fatal("credit payload retained ring storage or allocated a forwarding buffer")
	}
}

func TestSharedBridgeRawReadHonorsDeadlineAndRecovers(t *testing.T) {
	bridge := testSharedBridge(t)
	if err := bridge.setReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.read(); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("empty event ring ignored its read deadline: %v", err)
	}
	if err := bridge.setReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	offset := sharedHeader + sharedCapacity*sharedStride
	binary.LittleEndian.PutUint32(bridge.data[offset:], uint32(dataFrame))
	atomic.StoreUint32(bridge.word(192), 1)
	value, err := bridge.read()
	value.release()
	if err != nil {
		t.Fatalf("deadline change did not restore reads: %v", err)
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

func TestSharedBridgeFullCommandRingDoesNotBlockIncomingPayload(t *testing.T) {
	b := testSharedBridge(t)
	atomic.StoreUint32(b.word(64), sharedCapacity)
	written := make(chan error, 1)
	go func() {
		written <- b.writeBatch([]frame{{kind: dataFrame, id: 7, data: []byte("outgoing")}})
	}()
	offset := sharedHeader + sharedCapacity*sharedStride
	d := b.data[offset:]
	binary.LittleEndian.PutUint32(d, uint32(dataFrame))
	binary.LittleEndian.PutUint32(d[4:], 8)
	copy(d[16:], "incoming")
	atomic.StoreUint32(b.word(192), 1)
	if _, err := b.notify.Write(eventfdSignal[:]); err != nil {
		t.Fatal(err)
	}
	f, err := b.read()
	if err != nil {
		t.Fatal(err)
	}
	if string(f.data) != "incoming" {
		f.release()
		t.Fatal("blocked writer stalled or corrupted incoming payload")
	}
	f.release()
	select {
	case err := <-written:
		t.Fatalf("writer escaped the full ring: %v", err)
	default:
	}
	atomic.StoreUint32(b.word(128), sharedCapacity)
	if _, err := b.space.Write(eventfdSignal[:]); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer lost the space notification")
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

func TestDirectAndQueuedPublicationsPreserveDataFINOrder(t *testing.T) {
	b := testSharedBridge(t)
	e := &Engine{bridge: b, done: make(chan struct{}), out: make(chan frame, 128), flows: make(map[uint64]*Conn)}
	e.wg.Add(1)
	go e.writeLoop()
	defer e.Close()
	const producers, messages = 8, 512
	result := make(chan error, 1)
	go func() {
		sequences := [producers]uint32{}
		finished := [producers]bool{}
		for received := 0; received < producers*(messages+1); {
			tail := atomic.LoadUint32(b.word(128))
			if atomic.LoadUint32(b.word(64)) == tail {
				var value [8]byte
				if _, err := io.ReadFull(b.wake, value[:]); err != nil {
					result <- err
					return
				}
				atomic.SwapUint32(b.word(40), 0)
				continue
			}
			d := b.data[sharedHeader+int(tail&(sharedCapacity-1))*sharedStride:]
			encoded := binary.LittleEndian.Uint32(d)
			kind, id := byte(encoded), binary.LittleEndian.Uint64(d[8:])
			payload := d[16:]
			leased := encoded&sharedPayloadFlag != 0
			slot := int(encoded >> 16)
			if leased {
				payload = b.data[sharedRingBytes+slot*sharedPayloadSize:]
			}
			if id >= producers || finished[id] {
				result <- io.ErrUnexpectedEOF
				return
			}
			if kind == finFrame {
				if sequences[id] != messages {
					result <- io.ErrUnexpectedEOF
					return
				}
				finished[id] = true
			} else if kind != dataFrame || binary.BigEndian.Uint32(payload) != sequences[id] {
				result <- io.ErrUnexpectedEOF
				return
			} else {
				sequences[id]++
			}
			atomic.StoreUint32(b.word(128), tail+1)
			if leased {
				atomic.OrUint32(b.word(56), 1<<slot)
			}
			if _, err := b.space.Write(eventfdSignal[:]); err != nil {
				result <- err
				return
			}
			received++
		}
		result <- nil
	}()
	var writers sync.WaitGroup
	for id := range uint64(producers) {
		writers.Go(func() {
			for sequence := range uint32(messages) {
				payload := make([]byte, 4)
				binary.BigEndian.PutUint32(payload, sequence)
				if err := e.send(frame{kind: dataFrame, id: id, data: payload}, time.Now().Add(5*time.Second)); err != nil {
					t.Error(err)
					return
				}
			}
			if err := e.send(frame{kind: finFrame, id: id}, time.Now().Add(5*time.Second)); err != nil {
				t.Error(err)
			}
		})
	}
	writers.Wait()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	e.Close()
	if e.queued.Load() != 0 {
		t.Fatalf("publication reservations leaked: %d", e.queued.Load())
	}
}

func TestSharedPayloadBufferSurvivesEngineCloseAndDescriptorReuse(t *testing.T) {
	bridge := testSharedBridge(t)
	atomic.AndUint32(bridge.word(60), ^uint32(1))
	offset := sharedRingBytes + sharedPayloadSlots*sharedPayloadSize
	copy(bridge.data[offset:], "owned payload")
	descriptor := bridge.data[sharedHeader+sharedCapacity*sharedStride:]
	binary.LittleEndian.PutUint32(descriptor, uint32(dataFrame)|sharedPayloadFlag)
	binary.LittleEndian.PutUint32(descriptor[4:], 13)
	atomic.StoreUint32(bridge.word(192), 1)
	value, err := bridge.read()
	if err != nil {
		t.Fatal(err)
	}
	if &value.data[0] != &bridge.data[offset] {
		t.Fatal("shared receive payload was copied")
	}
	clear(descriptor[:16+13])
	engine := &Engine{bridge: bridge, done: make(chan struct{}), out: make(chan frame, 16), flows: make(map[uint64]*Conn)}
	connection := newConn(engine, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	connection.in <- value
	prefix := make([]byte, 6)
	if _, err := connection.Read(prefix); err != nil || string(prefix) != "owned " {
		t.Fatalf("partial read=%q err=%v", prefix, err)
	}
	buffers, err := connection.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	engine.Close()
	if string(buffers[0].Bytes()) != "payload" || &buffers[0].Bytes()[0] != &bridge.data[offset+6] {
		t.Fatal("leased buffer lost its payload during shutdown")
	}
	if atomic.LoadUint32(bridge.word(60))&1 != 0 || bridge.owner.refs.Load() != 1 {
		t.Fatal("payload slot released before the reader finished")
	}
	buf.ReleaseMulti(buffers)
	if atomic.LoadUint32(bridge.word(60))&1 == 0 || bridge.owner.refs.Load() != 0 {
		t.Fatal("payload owner was not released")
	}
}

func TestSharedBorrowedWriteAndVectoredPartsCopyOnlyIntoDestination(t *testing.T) {
	bridge := testSharedBridge(t)
	engine := &Engine{bridge: bridge, done: make(chan struct{}), out: make(chan frame, 16), flows: make(map[uint64]*Conn)}
	connection := newConn(engine, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	payload := []byte("direct write")
	if _, err := connection.Write(payload); err != nil {
		t.Fatal(err)
	}
	clear(payload)
	if string(bridge.data[sharedRingBytes:sharedRingBytes+12]) != "direct write" {
		t.Fatal("borrowed write retained caller memory")
	}
	parts := buf.MultiBuffer{buf.FromBytes([]byte("first")), buf.FromBytes([]byte("second"))}
	if err := connection.WriteMultiBuffer(parts); err != nil {
		t.Fatal(err)
	}
	descriptor := bridge.data[sharedHeader+sharedStride:]
	slot := int(binary.LittleEndian.Uint32(descriptor) >> 16)
	offset := sharedRingBytes + slot*sharedPayloadSize
	if string(bridge.data[offset:offset+11]) != "firstsecond" || parts[0] != nil || parts[1] != nil {
		t.Fatal("vectored ownership or payload was lost")
	}
	large := bytes.Repeat([]byte{0xa7}, 2*sharedPayloadSize+1)
	buffers := buf.MultiBuffer{buf.FromBytes(large)}
	if err := connection.WriteMultiBuffer(buffers); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadUint32(bridge.word(64)) != 5 || buffers[0] != nil {
		t.Fatal("oversized buffer did not split into bounded frames")
	}
	for index, size := range []int{sharedPayloadSize, sharedPayloadSize, 1} {
		descriptor := bridge.data[sharedHeader+(index+2)*sharedStride:]
		slot := int(binary.LittleEndian.Uint32(descriptor) >> 16)
		offset := sharedRingBytes + slot*sharedPayloadSize
		if int(binary.LittleEndian.Uint32(descriptor[4:])) != size || !bytes.Equal(bridge.data[offset:offset+size], large[index*sharedPayloadSize:index*sharedPayloadSize+size]) {
			t.Fatal("split payload changed")
		}
	}
	udp := newConn(engine, 2, xnet.UDPDestination(xnet.LocalHostIP, 53), nil)
	datagram := buf.MultiBuffer{buf.FromBytes([]byte("UDP payload"))}
	if err := udp.WriteMultiBuffer(datagram); err != nil {
		t.Fatal(err)
	}
	descriptor = bridge.data[sharedHeader+5*sharedStride:]
	slot = int(binary.LittleEndian.Uint32(descriptor) >> 16)
	offset = sharedRingBytes + slot*sharedPayloadSize
	size := int(binary.LittleEndian.Uint32(descriptor[4:]))
	address, contents, err := decodeAddress(bridge.data[offset : offset+size])
	if err != nil || string(address) != udp.targetAddress || string(contents) != "UDP payload" || datagram[0] != nil {
		t.Fatalf("vectored UDP changed: %q %q %v", address, contents, err)
	}
}

func TestSharedQueuedBorrowedWriteTakesSnapshotAndReleasesOnClose(t *testing.T) {
	bridge := testSharedBridge(t)
	engine := &Engine{bridge: bridge, done: make(chan struct{}), out: make(chan frame, 16), flows: make(map[uint64]*Conn)}
	connection := newConn(engine, 1, xnet.TCPDestination(xnet.LocalHostIP, 80), nil)
	atomic.StoreUint32(bridge.word(64), sharedCapacity)
	payload := []byte("queued snapshot")
	if _, err := connection.Write(payload); err != nil {
		t.Fatal(err)
	}
	clear(payload)
	queued := <-engine.out
	if queued.borrowed || queued.storage == nil || string(queued.data) != "queued snapshot" {
		t.Fatal("queued write retained caller memory")
	}
	engine.out <- queued
	engine.Close()
	if queued.storage.Bytes() != nil || engine.queued.Load() != 0 {
		t.Fatal("queued write was not released on close")
	}
}

func TestSharedPayloadPoolFallbackAndMalformedLease(t *testing.T) {
	bridge := testSharedBridge(t)
	atomic.StoreUint32(bridge.word(56), 0)
	if count, err := bridge.tryWriteBatch([]frame{{kind: dataFrame, id: 1, data: []byte("fallback")}, {kind: finFrame, id: 2}}); count != 2 || err != nil {
		t.Fatalf("exhausted pool blocked other frames: %d %v", count, err)
	}
	descriptor := bridge.data[sharedHeader:]
	if binary.LittleEndian.Uint32(descriptor) != uint32(dataFrame) || string(descriptor[16:24]) != "fallback" {
		t.Fatal("fallback payload changed")
	}
	for _, kind := range []uint32{uint32(dataFrame) | sharedPayloadFlag, uint32(dataFrame) | sharedPayloadFlag | sharedPayloadSlots<<16, uint32(dataFrame) | 1<<9} {
		descriptor := bridge.data[sharedHeader+sharedCapacity*sharedStride:]
		binary.LittleEndian.PutUint32(descriptor, kind)
		binary.LittleEndian.PutUint32(descriptor[4:], 1)
		atomic.StoreUint32(bridge.word(192), 1)
		if value, err := bridge.read(); err == nil {
			value.release()
			t.Fatalf("accepted invalid lease %#x", kind)
		}
	}
}
