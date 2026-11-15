//go:build xhysteria_native && cgo && (linux || android)

package xhysteria

import (
	"errors"
	"os"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
)

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
				for range 16 {
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
