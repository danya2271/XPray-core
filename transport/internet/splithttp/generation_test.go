package splithttp

import (
	"testing"
	"time"
)

func TestHandoffLifetime(t *testing.T) {
	for _, forced := range []bool{false, true} {
		life := newHandoffLifetime()
		if !life.enter() {
			t.Fatal("listener should initially accept requests")
		}
		life.addSession()
		life.exit() // Partial session can outlive its first request.
		finished := make(chan bool, 1)
		go func() { finished <- life.settle() }()
		if life.isSettled() || !life.enter() {
			t.Fatal("partial session was not counted")
		}
		life.exit() // Remove the session while a request is still active.
		if life.isSettled() {
			t.Fatal("forwarded request was not counted")
		}
		if forced {
			life.close()
		} else {
			life.exit()
		}
		select {
		case complete := <-finished:
			if complete == forced || life.isSettled() == forced || life.enter() {
				t.Fatalf("unexpected retirement for forced=%v, complete=%v", forced, complete)
			}
		case <-time.After(time.Second):
			t.Fatal("retirement did not complete")
		}
	}
}

func BenchmarkManagedXHTTPRequestAccounting(b *testing.B) {
	life := newHandoffLifetime()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !life.enter() {
			b.Fatal("active listener rejected a request")
		}
		life.exit()
	}
}
