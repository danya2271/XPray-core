//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func TestNativePoolSizingAndRateBudget(t *testing.T) {
	for _, test := range []struct {
		value   string
		config  native.Config
		want    int
		invalid bool
	}{
		{"", native.Config{}, 2, false},
		{"0", native.Config{}, 0, false},
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
