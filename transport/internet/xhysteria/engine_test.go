//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"bytes"
	"errors"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
)

func TestTCPReadDropsConsumedPayloadReference(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(strconv.FormatBool(owned), func(t *testing.T) {
			engine := &Engine{done: make(chan struct{}), out: make(chan frame, 8)}
			connection := newConn(engine, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
			payload := []byte("payload")
			message := frame{kind: dataFrame, id: connection.id, data: payload}
			released := 0
			if owned {
				message.storage = buf.FromBytesWithRelease(payload, func() { released++ })
			}
			connection.in <- message
			if count, err := connection.Read(nil); count != 0 || err != nil || len(connection.in) != 1 {
				t.Fatalf("empty read consumed a frame: count=%d err=%v", count, err)
			}
			prefix := make([]byte, 3)
			if count, err := connection.Read(prefix); count != len(prefix) || err != nil || string(prefix) != "pay" {
				t.Fatalf("partial read: count=%d err=%v data=%q", count, err, prefix)
			}
			if string(connection.pending) != "load" || released != 0 {
				t.Fatal("partial read released unread payload")
			}
			suffix := make([]byte, 4)
			if count, err := connection.Read(suffix); count != len(suffix) || err != nil || string(suffix) != "load" {
				t.Fatalf("final read: count=%d err=%v data=%q", count, err, suffix)
			}
			if connection.pending != nil || connection.pendingStorage != nil {
				t.Fatal("completed read retained a payload reference")
			}
			if owned && released != 1 {
				t.Fatalf("payload released %d times", released)
			}
			connection.in <- frame{kind: dataFrame, id: connection.id, data: []byte("next")}
			if count, err := connection.Read(suffix); count != len(suffix) || err != nil || string(suffix) != "next" {
				t.Fatalf("next frame: count=%d err=%v data=%q", count, err, suffix)
			}
		})
	}
}

func BenchmarkConsumedTCPReadMemory(b *testing.B) {
	const flows, size = 2048, 16384
	for b.Loop() {
		debug.FreeOSMemory()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		engine := &Engine{done: make(chan struct{}), out: make(chan frame, 8)}
		connections := make([]*Conn, flows)
		reply := make([]byte, size)
		for index := range connections {
			connection := newConn(engine, uint64(index+1), xnet.Destination{Network: xnet.Network_TCP}, nil)
			payload := bytes.Repeat([]byte{0xa5}, size)
			connection.in <- frame{kind: dataFrame, id: connection.id, data: payload}
			if count, err := connection.Read(reply); count != size || err != nil || !bytes.Equal(reply, payload) {
				b.Fatalf("payload read: count=%d err=%v", count, err)
			}
			connections[index] = connection
		}
		debug.FreeOSMemory()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/(1<<20), "retained-heap-MiB")
		retained := 0
		for _, connection := range connections {
			if connection.pending != nil {
				retained++
			}
		}
		b.ReportMetric(float64(retained), "retained-payload-refs")
		if data, err := os.ReadFile("/proc/self/statm"); err == nil {
			fields := strings.Fields(string(data))
			if len(fields) >= 2 {
				if pages, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					b.ReportMetric(float64(pages)*float64(os.Getpagesize())/(1<<20), "current-rss-MiB")
				}
			}
		}
		runtime.KeepAlive(connections)
	}
}

func TestServerFlowQueuesAndCreditsStayWithinChunkBudget(t *testing.T) {
	e := &Engine{role: "server", done: make(chan struct{}), out: make(chan frame, 8)}
	c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	payload := bytes.Repeat([]byte{0xa7}, 65536)
	if _, err := c.Write(payload); err != nil {
		t.Fatal(err)
	}
	if len(e.out) != 4 || len(c.writeCredit) != 0 || cap(c.in) != 6 {
		t.Fatal("server exceeded its four-frame flow window")
	}
	for range 4 {
		f := <-e.out
		if len(f.data) != 16384 {
			t.Fatalf("server chunk is %d bytes, want 16384", len(f.data))
		}
		f.release()
	}
	for range 4 {
		if _, err := c.received(frame{kind: dataFrame}); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.out) != 2 {
		t.Fatal("server did not return credits at half its flow window")
	}
	for range 2 {
		f := <-e.out
		if f.kind != creditFrame || !bytes.Equal(f.data, []byte{2}) {
			t.Fatalf("incorrect server credit frame: %+v", f)
		}
	}
}

func TestEmptyTCPMultiBuffersPreserveWriteCredits(t *testing.T) {
	e := &Engine{done: make(chan struct{}), out: make(chan frame, 1)}
	c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	c.SetWriteDeadline(time.Now().Add(time.Second))
	for range 32 {
		if err := c.WriteMultiBuffer(buf.MultiBuffer{buf.New(), buf.New()}); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.writeCredit) != 8 || len(e.out) != 0 {
		t.Fatal("empty writes consumed credits or queued payload")
	}
}

func TestSingleTCPBufferTransfersOwnershipWithoutCopy(t *testing.T) {
	e := &Engine{done: make(chan struct{}), out: make(chan frame, 1)}
	c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	storage := buf.New()
	storage.WriteString("owned payload")
	mb := buf.MultiBuffer{storage}
	if err := c.WriteMultiBuffer(mb); err != nil {
		t.Fatal(err)
	}
	f := <-e.out
	defer f.release()
	if f.storage != storage || string(f.data) != "owned payload" || mb[0] != nil {
		t.Fatal("single TCP buffer was copied or ownership retained")
	}
}

func TestReceiveCreditsAreBatchedWithoutDelayingPayload(t *testing.T) {
	e := &Engine{done: make(chan struct{}), out: make(chan frame, 16)}
	c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	for sequence := range 37 {
		c.in <- frame{kind: dataFrame, id: c.id, data: []byte{byte(sequence)}}
		mb, err := c.ReadMultiBuffer()
		if err != nil {
			t.Fatal(err)
		}
		if len(mb) != 1 || mb[0].Byte(0) != byte(sequence) {
			t.Fatal("credit batching delayed or changed payload")
		}
		buf.ReleaseMulti(mb)
		if len(e.out) != (sequence+1)/4 {
			t.Fatal("unexpected credit count")
		}
	}
	for range 9 {
		f := <-e.out
		if f.kind != creditFrame || len(f.data) != 1 || f.data[0] != 4 {
			t.Fatalf("incorrect credit batch: %+v", f)
		}
	}
}

func TestSparseUDPCreditsReturnImmediatelyAndBurstsStayBatched(t *testing.T) {
	for _, role := range []string{"client", "server"} {
		t.Run(role, func(t *testing.T) {
			engine := &Engine{role: role, done: make(chan struct{}), out: make(chan frame, 16)}
			connection := newConn(engine, 1, xnet.Destination{Network: xnet.Network_UDP}, nil)
			read := func() {
				queued, err := connection.nextFrame()
				if err != nil {
					t.Fatal(err)
				}
				queued.release()
			}
			for range 3 {
				connection.in <- frame{kind: datagramFrame, id: connection.id, data: []byte("sparse")}
				read()
				select {
				case credit := <-engine.out:
					if credit.kind != creditFrame || len(credit.data) != 1 || credit.data[0] != 1 {
						t.Fatalf("sparse UDP credit was delayed: %+v", credit)
					}
				default:
					t.Fatal("sparse UDP credit waits for another datagram")
				}
			}
			window := cap(connection.writeCredit)
			for range window - 1 {
				connection.in <- frame{kind: datagramFrame, id: connection.id, data: []byte("burst")}
			}
			for range window - 1 {
				read()
			}
			if len(engine.out) != 2 {
				t.Fatalf("UDP burst did not batch credits: %d", len(engine.out))
			}
			for _, count := range []int{window / 2, window/2 - 1} {
				credit := <-engine.out
				if credit.kind != creditFrame || len(credit.data) != 1 || int(credit.data[0]) != count {
					t.Fatalf("incorrect UDP burst credit count: %+v", credit)
				}
			}
			if connection.readCredits != 0 {
				t.Fatal("last UDP burst retained unused credits")
			}
		})
	}
}

func TestQueuedPayloadSurvivesEngineShutdownAtCreditBoundary(t *testing.T) {
	e := &Engine{done: make(chan struct{}), out: make(chan frame, 16)}
	c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	c.readCredits = 3
	c.in <- frame{kind: dataFrame, id: c.id, data: []byte("last payload")}
	close(e.done)
	c.fail(errors.New("connection stopped"))
	mb, err := c.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	defer buf.ReleaseMulti(mb)
	if len(mb) != 1 || string(mb[0].Bytes()) != "last payload" {
		t.Fatal("shutdown discarded published payload")
	}
}

func TestChangingDeadlineInterruptsBlockedNativeReadAndWrite(t *testing.T) {
	for _, write := range []bool{false, true} {
		name := "read"
		if write {
			name = "write"
		}
		t.Run(name, func(t *testing.T) {
			e := &Engine{done: make(chan struct{}), out: make(chan frame, 1)}
			c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
			if write {
				for range cap(c.writeCredit) {
					<-c.writeCredit
				}
			}
			result := make(chan error, 1)
			go func() {
				if write {
					_, err := c.Write([]byte("blocked"))
					result <- err
				} else {
					_, err := c.Read(make([]byte, 1))
					result <- err
				}
			}()
			if write {
				c.SetWriteDeadline(time.Now().Add(-time.Second))
			} else {
				c.SetReadDeadline(time.Now().Add(-time.Second))
			}
			select {
			case err := <-result:
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatalf("got %v, want deadline exceeded", err)
				}
			case <-time.After(time.Second):
				t.Fatal("deadline update did not wake the blocked operation")
			}
		})
	}
}

func TestAvailableWriteCreditHonorsDeadlineAndShutdown(t *testing.T) {
	for _, state := range []string{"expired", "engine_closed", "flow_closed"} {
		t.Run(state, func(t *testing.T) {
			e := &Engine{done: make(chan struct{}), out: make(chan frame, 1)}
			c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
			want := error(net.ErrClosed)
			switch state {
			case "expired":
				c.SetWriteDeadline(time.Now().Add(-time.Second))
				want = os.ErrDeadlineExceeded
			case "engine_closed":
				close(e.done)
			case "flow_closed":
				c.fail(net.ErrClosed)
			}
			if err := c.awaitWriteCredit(); !errors.Is(err, want) {
				t.Fatalf("got %v, want %v", err, want)
			}
			if len(c.writeCredit) != cap(c.writeCredit) {
				t.Fatal("failed operation consumed an available write credit")
			}
		})
	}
}

func TestAvailableWriteCreditDoesNotAllocateDeadlineTimer(t *testing.T) {
	e := &Engine{done: make(chan struct{}), out: make(chan frame, 1)}
	c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	c.SetWriteDeadline(time.Now().Add(time.Minute))
	allocations := testing.AllocsPerRun(100, func() {
		if err := c.awaitWriteCredit(); err != nil {
			t.Fatal(err)
		}
		c.writeCredit <- struct{}{}
	})
	if allocations != 0 {
		t.Fatalf("available write credit allocated %.1f times", allocations)
	}
}

func TestDirectSharedWriteDoesNotAllocateDeadlineTimer(t *testing.T) {
	bridge := testSharedBridge(t)
	e := &Engine{done: make(chan struct{}), out: make(chan frame, 1), bridge: bridge}
	c := newConn(e, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
	c.SetWriteDeadline(time.Now().Add(time.Minute))
	payload := frame{kind: dataFrame, id: c.id, data: []byte("direct")}
	allocations := testing.AllocsPerRun(100, func() {
		if err := c.sendFrame(payload); err != nil {
			t.Fatal(err)
		}
		atomic.StoreUint32(bridge.word(128), atomic.LoadUint32(bridge.word(64)))
	})
	if allocations != 0 {
		t.Fatalf("direct shared write allocated %.1f times", allocations)
	}
	if e.queued.Load() != 0 || len(e.out) != 0 {
		t.Fatal("direct publication queued a frame")
	}
	c.SetWriteDeadline(time.Now().Add(-time.Second))
	if err := c.sendFrame(payload); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}
	if atomic.LoadUint32(bridge.word(128)) != atomic.LoadUint32(bridge.word(64)) {
		t.Fatal("expired write published a frame")
	}
}

func BenchmarkAvailableWriteCredit(b *testing.B) {
	for _, deadline := range []bool{false, true} {
		name := "no_deadline"
		if deadline {
			name = "future_deadline"
		}
		b.Run(name, func(b *testing.B) {
			engine := &Engine{done: make(chan struct{}), out: make(chan frame, 1)}
			conn := newConn(engine, 1, xnet.Destination{Network: xnet.Network_TCP}, nil)
			if deadline {
				conn.SetWriteDeadline(time.Now().Add(time.Minute))
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := conn.awaitWriteCredit(); err != nil {
					b.Fatal(err)
				}
				conn.writeCredit <- struct{}{}
			}
		})
	}
}
