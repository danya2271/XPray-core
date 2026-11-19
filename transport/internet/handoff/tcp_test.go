package handoff

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestSharedTCPListener(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "::1"} {
		t.Run(address, func(t *testing.T) {
			raw, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP(address)})
			if err != nil {
				if address == "::1" {
					t.Skipf("IPv6 loopback unavailable: %v", err)
				}
				t.Fatal(err)
			}
			old := NewTCPListener(raw, true)
			defer old.Close()
			file, err := old.File()
			if err != nil {
				t.Fatal(err)
			}
			next, err := ImportTCPListener(file)
			file.Close()
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()

			type result struct {
				conn net.Conn
				err  error
			}
			accept := func(l *TCPListener) <-chan result {
				out := make(chan result, 1)
				go func() {
					conn, err := l.Accept()
					out <- result{conn, err}
				}()
				return out
			}
			receive := func(out <-chan result) net.Conn {
				t.Helper()
				select {
				case r := <-out:
					if r.err != nil {
						t.Fatal(r.err)
					}
					return r.conn
				case <-time.After(3 * time.Second):
					t.Fatal("listener did not accept")
					return nil
				}
			}
			dial := func() net.Conn {
				t.Helper()
				conn, err := net.DialTimeout("tcp", raw.Addr().String(), 3*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { conn.Close() })
				return conn
			}
			check := func(client, server net.Conn, value byte) {
				t.Helper()
				if client.LocalAddr().String() != server.RemoteAddr().String() {
					t.Fatal("remote client address not preserved")
				}
				server.SetReadDeadline(time.Now().Add(3 * time.Second))
				if _, err := client.Write([]byte{value}); err != nil {
					t.Fatal(err)
				}
				var got [1]byte
				if _, err := io.ReadFull(server, got[:]); err != nil || got[0] != value {
					t.Fatalf("stream read %q, %v, wanted %q", got, err, value)
				}
			}

			waitingB := accept(next)
			waitingA := accept(old)
			clientA := dial()
			connA := receive(waitingA)
			defer connA.Close()
			check(clientA, connA, 'a')

			waitingA = accept(old) // May already be blocked in the kernel.
			if err := old.Pause(); err != nil {
				t.Fatal(err)
			}
			clientB := dial() // Both generations paused; the kernel queues it.
			if err := next.Resume(); err != nil {
				t.Fatal(err)
			}
			connB := receive(waitingB)
			defer connB.Close()
			check(clientA, connA, 'b')
			check(clientB, connB, 'c')

			// A rollback switches new connections back without closing B's.
			if err := next.Pause(); err != nil {
				t.Fatal(err)
			}
			if err := old.Resume(); err != nil {
				t.Fatal(err)
			}
			clientC := dial()
			connC := receive(waitingA)
			defer connC.Close()
			check(clientB, connB, 'd')
			check(clientC, connC, 'e')

			waitingB = accept(next)
			if err := next.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case r := <-waitingB:
				if !errors.Is(r.err, net.ErrClosed) {
					t.Fatalf("after close: %v", r.err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("close failed to wake the paused accept")
			}
			check(clientB, connB, 'f')
		})
	}
}
