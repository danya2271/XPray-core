//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"testing"

	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/tls"
)

func TestNativeQuicTimers(t *testing.T) {
	for _, test := range []struct {
		name        string
		idle, alive int64
		wantError   bool
	}{
		{"defaults", 0, 0, false},
		{"profile", 45, 9, false},
		{"negative_idle", -1, 9, true},
		{"negative_keepalive", 45, -1, true},
		{"overflow_idle", 1<<63 - 1, 9, true},
		{"overflow_keepalive", 45, 1<<63 - 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, err := nativeConfig(&internet.MemoryStreamConfig{
				SecuritySettings: &tls.Config{},
				QuicParams: &internet.QuicParams{
					DisableChromeParrot: true,
					MaxIdleTimeout:      test.idle,
					KeepAlivePeriod:     test.alive,
				},
			})
			if (err != nil) != test.wantError {
				t.Fatalf("nativeConfig error: %v, wantError=%t", err, test.wantError)
			}
			if err == nil && (config.MaxIdleTimeout != uint64(test.idle) || config.KeepAlivePeriod != uint64(test.alive)) {
				t.Fatalf("timers were not forwarded: %+v", config)
			}
		})
	}
}
