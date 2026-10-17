package splithttp_test

import (
	"testing"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

func Test_regression_readzero(t *testing.T) {
	q := NewUploadQueue(10)
	q.Push(Packet{
		Payload: []byte("x"),
		Seq:     0,
	})
	buf := make([]byte, 20)
	n, err := q.Read(buf)
	common.Must(err)
	if n != 1 {
		t.Error("n=", n)
	}
}

func TestUploadQueueReordersPackets(t *testing.T) {
	q := NewUploadQueue(4)
	if err := q.Push(Packet{Payload: []byte("world"), Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(Packet{Payload: []byte("hello "), Seq: 0}); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, 11)
	n, err := q.Read(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(got[:n]) != "hello " {
		t.Fatalf("first payload = %q", got[:n])
	}

	n, err = q.Read(got[n:])
	if err != nil {
		t.Fatal(err)
	}
	if string(got[:6+n]) != "hello world" {
		t.Fatalf("reordered payload = %q", got[:6+n])
	}
}

func TestUploadQueueClearsPendingPayload(t *testing.T) {
	q := NewUploadQueue(1)
	if err := q.Push(Packet{Payload: []byte("payload"), Seq: 0}); err != nil {
		t.Fatal(err)
	}

	first := make([]byte, 3)
	if _, err := q.Read(first); err != nil {
		t.Fatal(err)
	}
	second := make([]byte, 4)
	if _, err := q.Read(second); err != nil {
		t.Fatal(err)
	}
	if string(second) != "load" {
		t.Fatalf("remaining payload = %q", second)
	}
}
