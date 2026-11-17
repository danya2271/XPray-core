//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"bytes"
	"errors"
	"os"
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
