//go:build !wasm && !openbsd
// +build !wasm,!openbsd

package buf_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"golang.org/x/sync/errgroup"
)

func TestReadvReader(t *testing.T) {
	tcpServer := &tcp.Server{
		MsgProcessor: func(b []byte) []byte {
			return b
		},
	}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	conn, err := net.Dial("tcp", dest.NetAddr())
	common.Must(err)
	defer conn.Close()

	const size = 8192
	data := make([]byte, 8192)
	common.Must2(rand.Read(data))

	var errg errgroup.Group
	errg.Go(func() error {
		writer := NewWriter(conn)
		mb := MergeBytes(nil, data)

		return writer.WriteMultiBuffer(mb)
	})

	defer func() {
		if err := errg.Wait(); err != nil {
			t.Error(err)
		}
	}()

	rawConn, err := conn.(*net.TCPConn).SyscallConn()
	common.Must(err)

	reader := NewReadVReader(conn, rawConn, nil)
	var rmb MultiBuffer
	for {
		mb, err := reader.ReadMultiBuffer()
		if err != nil {
			t.Fatal("unexpected error: ", err)
		}
		rmb, _ = MergeMulti(rmb, mb)
		if rmb.Len() == size {
			break
		}
	}

	rdata := make([]byte, size)
	SplitBytes(rmb, rdata)

	if r := cmp.Diff(data, rdata); r != "" {
		t.Fatal(r)
	}
}

func TestReadvReaderRecoversFromDeadlineAndRepeatedReads(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	raw, err := client.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	reader := NewReadVReader(client, raw, nil)
	exchange := func(payload []byte) {
		t.Helper()
		if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		written := make(chan error, 1)
		go func() {
			_, err := server.Write(payload)
			written <- err
		}()
		var received []byte
		for len(received) < len(payload) {
			buffers, err := reader.ReadMultiBuffer()
			if err != nil {
				ReleaseMulti(buffers)
				t.Fatal(err)
			}
			for _, buffer := range buffers {
				received = append(received, buffer.Bytes()...)
			}
			ReleaseMulti(buffers)
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(received, payload) {
			t.Fatal("repeated readv changed payload")
		}
	}
	exchange(bytes.Repeat([]byte{0x71}, 8*Size))
	if err := client.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffers, err := reader.ReadMultiBuffer()
	ReleaseMulti(buffers)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("empty readv ignored deadline: %v", err)
	}
	exchange(bytes.Repeat([]byte{0xa7}, 3*Size+17))
	server.Close()
	buffers, err = reader.ReadMultiBuffer()
	ReleaseMulti(buffers)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("readv retained previous read size after EOF: %v", err)
	}
}
