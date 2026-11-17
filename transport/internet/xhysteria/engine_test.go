//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"bytes"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
)

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
