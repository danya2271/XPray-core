package buf_test

import (
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
)

func TestReusableBufferTransfersMetadataOnlyAfterRelease(t *testing.T) {
	var reusable buf.ReusableBuffer
	var released int
	data := []byte("borrowed data")
	first := reusable.Reset(data, func() { released++ })
	first.Advance(9)
	if string(first.Bytes()) != "data" || &first.Bytes()[0] != &data[9] {
		t.Fatal("reusable buffer copied or changed its payload")
	}
	first.Release()
	first.Release()
	if released != 1 {
		t.Fatalf("release count=%d", released)
	}
	second := reusable.Reset(nil, func() { released++ })
	if second != first || !second.IsEmpty() {
		t.Fatal("metadata was not reused")
	}
	second.Release()
	if released != 2 {
		t.Fatalf("empty release count=%d", released)
	}
}

func TestReusableBufferCanRebindFromReleaseCallback(t *testing.T) {
	var reusable buf.ReusableBuffer
	var next *buf.Buffer
	destination := net.UDPDestination(net.LocalHostIP, 53)
	first := reusable.Reset([]byte("old"), func() {
		next = reusable.Reset([]byte("new"), func() {})
		next.UDP = &destination
	})
	first.Release()
	if next != first || string(next.Bytes()) != "new" || next.UDP != &destination {
		t.Fatal("release modified metadata after returning ownership")
	}
	next.Release()
}

func TestReusableBufferRejectsLiveReuse(t *testing.T) {
	var reusable buf.ReusableBuffer
	first := reusable.Reset(nil, func() {})
	defer first.Release()
	defer func() {
		if recover() == nil {
			t.Fatal("live empty buffer was rebound")
		}
	}()
	reusable.Reset([]byte("replacement"), func() {})
}

func BenchmarkOwnedBufferMetadata(b *testing.B) {
	for _, reuse := range []bool{false, true} {
		name := "allocate"
		if reuse {
			name = "reuse"
		}
		b.Run(name, func(b *testing.B) {
			data := make([]byte, 16384)
			release := func() {}
			frames := make(chan *buf.Buffer, 1)
			var reusable buf.ReusableBuffer
			b.ReportAllocs()
			for b.Loop() {
				var frame *buf.Buffer
				if reuse {
					frame = reusable.Reset(data, release)
				} else {
					frame = buf.FromBytesWithRelease(data, release)
				}
				frames <- frame
				(<-frames).Release()
			}
		})
	}
}
