//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func TestNativeClientWorkerBudget(t *testing.T) {
	previous := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, test := range []struct {
		value string
		want  uint8
	}{
		{"", 4}, {"0", 4}, {"1", 1}, {"4", 4}, {"8", 8}, {"16", 16},
		{"-1", 0}, {"17", 0}, {"many", 0},
	} {
		t.Run(test.value, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_CLIENT_WORKERS", test.value)
			workers, err := nativeClientWorkers()
			if workers != test.want || (err != nil) != (test.want == 0) {
				t.Fatalf("workers=%d err=%v want=%d", workers, err, test.want)
			}
		})
	}
	t.Setenv("XRAY_HYSTERIA_CLIENT_WORKERS", "")
	for _, cpus := range []int{1, 2, 4, 8, 32} {
		runtime.GOMAXPROCS(cpus)
		workers, err := nativeClientWorkers()
		if err != nil || workers != uint8(min(4, cpus)) {
			t.Fatalf("cpus=%d workers=%d err=%v", cpus, workers, err)
		}
	}
}

func TestNativePoolSizingAndRateBudget(t *testing.T) {
	previous := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, test := range []struct {
		value   string
		config  native.Config
		want    int
		invalid bool
	}{
		{"", native.Config{}, 2, false},
		{"", native.Config{ProcessWorkers: 1}, 2, false},
		{"", native.Config{ProcessWorkers: 2}, 2, false},
		{"", native.Config{ProcessWorkers: 4}, 4, false},
		{"", native.Config{ProcessWorkers: 8}, 8, false},
		{"", native.Config{ProcessWorkers: 16}, 8, false},
		{"", native.Config{ProcessWorkers: 8, Congestion: "brutal", TxBPS: 10}, 1, false},
		{"", native.Config{ProcessWorkers: 8, RxBPS: 10}, 1, false},
		{"0", native.Config{}, 0, false},
		{"2", native.Config{ProcessWorkers: 8}, 2, false},
		{"4", native.Config{Congestion: "bbr", TxBPS: 10}, 4, false},
		{"4", native.Config{Congestion: "bbr", RxBPS: 10}, 1, false},
		{"4", native.Config{Congestion: "reno", RxBPS: 10}, 1, false},
		{"4", native.Config{Congestion: "brutal", TxBPS: 10}, 1, false},
		{"4", native.Config{Congestion: "force-brutal", TxBPS: 10}, 1, false},
		{"4", native.Config{RxBPS: 10}, 1, false},
		{"9", native.Config{}, 0, true},
		{"nope", native.Config{}, 0, true},
	} {
		t.Run(test.value+test.config.Congestion, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_POOL_SIZE", test.value)
			got, err := nativePoolLimit(test.config)
			if (err != nil) != test.invalid || got != test.want {
				t.Fatalf("size=%d error=%v want=%d", got, err, test.want)
			}
		})
	}
}

func TestNativePoolPreservesSmallCPUBudget(t *testing.T) {
	t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "")
	previous := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, cpus := range []int{1, 2, 3, 4, 6, 7, 8, 16} {
		runtime.GOMAXPROCS(cpus)
		want := 2
		if cpus >= 8 {
			want = 4
		}
		limit, err := nativePoolLimit(native.Config{ProcessWorkers: uint8(min(4, cpus))})
		if err != nil || limit != want {
			t.Fatalf("cpus=%d limit=%d err=%v want=%d", cpus, limit, err, want)
		}
	}
	runtime.GOMAXPROCS(1)
	limit, err := nativePoolLimit(native.Config{ProcessWorkers: 8})
	if err != nil || limit != 8 {
		t.Fatalf("explicit worker budget: limit=%d err=%v", limit, err)
	}
	t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "4")
	limit, err = nativePoolLimit(native.Config{ProcessWorkers: 1})
	if err != nil || limit != 4 {
		t.Fatalf("explicit session budget: limit=%d err=%v", limit, err)
	}
}

func TestNativePoolFullWaitHonorsCancellation(t *testing.T) {
	pool := &nativePool{
		key:      nativePoolKey{limit: 1},
		sessions: []*nativeSession{{engine: &native.Engine{}, users: nativeSessionFlows}},
		changed:  make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := pool.open(ctx, net.Destination{}, func(context.Context) (*native.Engine, error) {
		t.Fatal("full pool created another session")
		return nil, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full pool did not honor cancellation: %v", err)
	}
}
