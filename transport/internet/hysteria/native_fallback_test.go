package hysteria

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet/stat"
)

func TestNativeDialFallbackCooldown(t *testing.T) {
	fallback := &nativeDialFallback{cooldown: time.Minute, attemptTimeout: time.Second}
	key := nativeFallbackKey{peer: "udp:blocked.example:443"}
	var nativeCalls, goCalls atomic.Int32
	tryNative := func(context.Context) (stat.Connection, error) {
		nativeCalls.Add(1)
		return nil, errors.New("startup timeout")
	}
	tryGo := func(context.Context) (stat.Connection, error) {
		goCalls.Add(1)
		return nil, nil
	}

	for i := 0; i < 2; i++ {
		if _, err := fallback.open(context.Background(), key, tryNative, tryGo); err != nil {
			t.Fatalf("fallback open failed: %v", err)
		}
	}
	if nativeCalls.Load() != 1 || goCalls.Load() != 2 {
		t.Fatalf("native calls=%d Go calls=%d, want 1 and 2", nativeCalls.Load(), goCalls.Load())
	}
}

func TestNativeDialFallbackRetriesAfterCooldown(t *testing.T) {
	fallback := &nativeDialFallback{cooldown: time.Millisecond, attemptTimeout: time.Second}
	key := nativeFallbackKey{peer: "udp:peer.example:443"}
	var nativeCalls, goCalls atomic.Int32
	tryNative := func(context.Context) (stat.Connection, error) {
		if nativeCalls.Add(1) == 1 {
			return nil, errors.New("startup timeout")
		}
		return nil, nil
	}
	tryGo := func(context.Context) (stat.Connection, error) {
		goCalls.Add(1)
		return nil, nil
	}

	if _, err := fallback.open(context.Background(), key, tryNative, tryGo); err != nil {
		t.Fatalf("initial fallback failed: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := fallback.open(context.Background(), key, tryNative, tryGo); err != nil {
		t.Fatalf("native probe after cooldown failed: %v", err)
	}
	if nativeCalls.Load() != 2 || goCalls.Load() != 1 {
		t.Fatalf("native calls=%d Go calls=%d, want 2 and 1", nativeCalls.Load(), goCalls.Load())
	}
}

func TestNativeDialFallbackHonorsParentCancellation(t *testing.T) {
	fallback := &nativeDialFallback{cooldown: time.Minute, attemptTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	var nativeCalls, goCalls atomic.Int32
	_, err := fallback.open(ctx, nativeFallbackKey{}, func(context.Context) (stat.Connection, error) {
		nativeCalls.Add(1)
		cancel()
		return nil, ctx.Err()
	}, func(context.Context) (stat.Connection, error) {
		goCalls.Add(1)
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got error %v, want context cancellation", err)
	}
	if nativeCalls.Load() != 1 || goCalls.Load() != 0 {
		t.Fatalf("canceled dial called native=%d Go=%d, want 1 and 0", nativeCalls.Load(), goCalls.Load())
	}
}

func TestNativeDialFallbackCoalescesNativeProbe(t *testing.T) {
	fallback := &nativeDialFallback{cooldown: time.Minute, attemptTimeout: time.Second}
	key := nativeFallbackKey{peer: "udp:peer.example:443"}
	started := make(chan struct{})
	release := make(chan struct{})
	var nativeCalls, goCalls atomic.Int32
	tryNative := func(context.Context) (stat.Connection, error) {
		if nativeCalls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil, nil
	}
	tryGo := func(context.Context) (stat.Connection, error) {
		goCalls.Add(1)
		return nil, nil
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := fallback.open(context.Background(), key, tryNative, tryGo)
		firstDone <- err
	}()
	<-started
	secondDone := make(chan error, 1)
	go func() {
		_, err := fallback.open(context.Background(), key, tryNative, tryGo)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("concurrent dial did not wait for the native probe: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("native probe failed: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("concurrent native dial failed: %v", err)
	}
	if nativeCalls.Load() != 2 || goCalls.Load() != 0 {
		t.Fatalf("native calls=%d Go calls=%d, want 2 and 0", nativeCalls.Load(), goCalls.Load())
	}
}

func TestNativeDialFallbackUsesGoAfterNativeAttemptTimeout(t *testing.T) {
	fallback := &nativeDialFallback{cooldown: time.Minute, attemptTimeout: 5 * time.Millisecond}
	var goCalls atomic.Int32
	_, err := fallback.open(context.Background(), nativeFallbackKey{}, func(ctx context.Context) (stat.Connection, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, func(context.Context) (stat.Connection, error) {
		goCalls.Add(1)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Go fallback failed: %v", err)
	}
	if goCalls.Load() != 1 {
		t.Fatalf("Go fallback calls=%d, want 1", goCalls.Load())
	}
}
