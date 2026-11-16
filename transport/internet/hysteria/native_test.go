//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"testing"

	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"github.com/xtls/xray-core/transport/internet/finalmask/udphop"
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

func TestNativeDisableGSOIsForwarded(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		config, err := nativeConfig(&internet.MemoryStreamConfig{
			SecuritySettings: &tls.Config{},
			QuicParams:       &internet.QuicParams{DisableChromeParrot: true, DisableGSO: disabled},
		})
		if err != nil || config.DisableGSO != disabled {
			t.Fatalf("disableGSO=%t config=%+v err=%v", disabled, config, err)
		}
	}
}

func TestNativeRemotePortHopping(t *testing.T) {
	for _, test := range []struct {
		name      string
		edit      func(*udphop.Config)
		wantError bool
	}{
		{"remote_ports", func(*udphop.Config) {}, false},
		{"local", func(c *udphop.Config) { c.Local = true }, true},
		{"per_connection", func(c *udphop.Config) { c.RemoteOnce = true }, true},
		{"no_remote", func(c *udphop.Config) { c.Remote = false }, true},
		{"remote_ip", func(c *udphop.Config) { c.RemoteIPs = []string{"127.0.0.1/32"} }, true},
		{"no_ports", func(c *udphop.Config) { c.RemotePorts = nil }, true},
		{"zero_port", func(c *udphop.Config) { c.RemotePorts = []uint32{0} }, true},
		{"overflow_port", func(c *udphop.Config) { c.RemotePorts = []uint32{65536} }, true},
		{"short_interval", func(c *udphop.Config) { c.IntervalMin = 4 }, true},
		{"reversed_interval", func(c *udphop.Config) { c.IntervalMax = 4 }, true},
		{"overflow_interval", func(c *udphop.Config) { c.IntervalMax = 1<<63 - 1 }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			hop := &udphop.Config{Remote: true, IntervalMin: 5, IntervalMax: 10, RemotePorts: []uint32{12000, 65535}}
			test.edit(hop)
			config, err := nativePortHopping(hop)
			if (err != nil) != test.wantError {
				t.Fatalf("nativePortHopping error: %v, wantError=%t", err, test.wantError)
			}
			if err == nil && (len(config.Ports) != 2 || config.Ports[0] != 12000 || config.Ports[1] != 65535 || config.IntervalMin != 5 || config.IntervalMax != 10) {
				t.Fatalf("hopping settings were not forwarded: %+v", config)
			}
		})
	}
}

func TestNativeHoppingMaskOrder(t *testing.T) {
	hop := &udphop.Config{Remote: true, IntervalMin: 5, IntervalMax: 5, RemotePorts: []uint32{12000, 12001}}
	mask := &salamander.Config{Password: "native-test"}
	for _, test := range []struct {
		name      string
		masks     []finalmask.UDPMask
		wantError bool
	}{
		{"hopping", []finalmask.UDPMask{hop}, false},
		{"salamander_then_hopping", []finalmask.UDPMask{mask, hop}, false},
		{"wrong_order", []finalmask.UDPMask{hop, mask}, true},
		{"duplicate_hopping", []finalmask.UDPMask{hop, hop}, true},
		{"duplicate_salamander", []finalmask.UDPMask{mask, mask}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, err := nativeConfig(&internet.MemoryStreamConfig{
				SecuritySettings: &tls.Config{},
				QuicParams:       &internet.QuicParams{DisableChromeParrot: true},
				FinalMask:        finalmask.NewFinalMask(nil, test.masks, nil, nil, nil, nil),
			})
			if (err != nil) != test.wantError {
				t.Fatalf("nativeConfig error: %v, wantError=%t", err, test.wantError)
			}
			if err == nil && config.PortHopping == nil {
				t.Fatal("native hopping configuration missing")
			}
			if err == nil && len(test.masks) == 2 && config.Salamander != mask.Password {
				t.Fatal("Salamander configuration missing")
			}
		})
	}
}
