//go:build xhysteria_native && cgo && (linux || android)

package hysteria

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/finalmask/salamander"
	"github.com/xtls/xray-core/transport/internet/finalmask/udphop"
	"github.com/xtls/xray-core/transport/internet/tls"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

func TestNativeServerResourceLimits(t *testing.T) {
	for _, test := range []struct {
		name      string
		value     string
		fallback  uint32
		maximum   uint32
		want      uint32
		wantError bool
	}{
		{"default", "", 32, 256, 32, false},
		{"override", "48", 32, 256, 48, false},
		{"zero", "0", 32, 256, 0, true},
		{"over_limit", "257", 32, 256, 0, true},
		{"invalid", "many", 32, 256, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_SERVER_CONNECTIONS", test.value)
			got, err := nativeServerLimit("XRAY_HYSTERIA_SERVER_CONNECTIONS", test.fallback, test.maximum)
			if (err != nil) != test.wantError || got != test.want {
				t.Fatalf("limit=%d error=%v, want=%d error=%t", got, err, test.want, test.wantError)
			}
		})
	}
}

func TestNativeFallbackAllowsQUICAndAuthStartupToFinish(t *testing.T) {
	if nativeDialAttemptTimeout <= 10*time.Second || nativeDialAttemptTimeout >= native.StartupTimeout {
		t.Fatalf("native dial timeout=%s must exceed the Rust 10s handshake and stay below startup timeout %s",
			nativeDialAttemptTimeout, native.StartupTimeout)
	}
}

func TestTuneNativeServerLimits(t *testing.T) {
	for _, test := range []struct {
		name        string
		memoryBytes uint64
		connections uint32
		flows       uint32
	}{
		{"one_gib", 1 << 30, 1310, 1966},
		{"ru_vps", 929935360, 1135, 1702},
		{"512_mib", 512 << 20, 655, 983},
		{"large_host", 8 << 30, 4096, 15728},
		{"very_small_host", 128 << 20, 32, 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			connections, flows := tuneNativeServerLimits(test.memoryBytes)
			if connections != test.connections || flows != test.flows {
				t.Fatalf("got connections=%d flows=%d, want %d and %d",
					connections, flows, test.connections, test.flows)
			}
		})
	}
}

func TestNativeMemoryLimitParsing(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
		want uint64
	}{
		{"bytes", "1073741824\n", 1 << 30},
		{"unlimited", "max\n", 0},
		{"v1_unlimited", "1152921504606846976\n", 0},
		{"invalid", "invalid\n", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/memory.limit"
			if err := os.WriteFile(path, []byte(test.data), 0600); err != nil {
				t.Fatal(err)
			}
			if got := nativeMemoryLimit(path, 1<<60); got != test.want {
				t.Fatalf("got %d, want %d", got, test.want)
			}
		})
	}
}

func TestNativeMemoryLimitIncludesParentSlices(t *testing.T) {
	root := t.TempDir()
	group := filepath.Join("danvless.slice", "dataplane.slice", "xray.service")
	if err := os.MkdirAll(filepath.Join(root, group), 0700); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct{ path, value string }{
		{"memory.max", "2147483648"},
		{"danvless.slice/memory.max", "536870912"},
		{"danvless.slice/dataplane.slice/memory.max", "805306368"},
		{group + "/memory.max", "max"},
	} {
		if err := os.WriteFile(filepath.Join(root, entry.path), []byte(entry.value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if limit := nativeHierarchyMemory(root, group, "memory.max"); limit != 512<<20 {
		t.Fatalf("hierarchical limit=%d, want 512 MiB", limit)
	}
}

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
		{"per_connection", func(c *udphop.Config) { c.Remote = false; c.RemoteOnce = true }, false},
		{"remote_and_once", func(c *udphop.Config) { c.RemoteOnce = true }, false},
		{"no_remote", func(c *udphop.Config) { c.Remote = false }, true},
		{"remote_ip", func(c *udphop.Config) { c.RemoteIPs = []string{"127.0.0.1/32"} }, false},
		{"ip_only", func(c *udphop.Config) { c.RemotePorts = nil; c.RemoteIPs = []string{"127.0.0.1/32"} }, false},
		{"ipv6", func(c *udphop.Config) { c.RemoteIPs = []string{"2001:db8::1234/64"} }, false},
		{"mapped_ipv4", func(c *udphop.Config) { c.RemoteIPs = []string{"::ffff:127.0.0.1/128"} }, false},
		{"mapped_ipv4_too_wide", func(c *udphop.Config) { c.RemoteIPs = []string{"::ffff:127.0.0.1/95"} }, true},
		{"invalid_ip", func(c *udphop.Config) { c.RemoteIPs = []string{"127.0.0.1"} }, true},
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
			if err == nil {
				if len(config.Ports) != len(hop.RemotePorts) || len(config.IPs) != len(hop.RemoteIPs) || config.Once != !hop.Remote || config.IntervalMin != 5 || config.IntervalMax != 10 {
					t.Fatalf("hopping settings were not forwarded: %+v", config)
				}
				for index, port := range hop.RemotePorts {
					if uint32(config.Ports[index]) != port {
						t.Fatalf("hopping port was not forwarded: %+v", config)
					}
				}
				if test.name == "ipv6" && config.IPs[0] != "2001:db8::/64" || test.name == "mapped_ipv4" && config.IPs[0] != "127.0.0.1/32" {
					t.Fatalf("hopping IP prefix was not normalized: %+v", config)
				}
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
		{"duplicate_salamander", []finalmask.UDPMask{mask, mask}, false},
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
			if err == nil && test.name == "duplicate_salamander" {
				if !config.PacketBridge || config.PortHopping != nil || config.Salamander != "" {
					t.Fatal("combined masks did not select the packet adapter")
				}
				return
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

func TestNativePacketMaskLocalHoppingValidation(t *testing.T) {
	for _, test := range []struct {
		name      string
		change    func(*udphop.Config)
		wantError bool
	}{
		{"local_only", func(*udphop.Config) {}, false},
		{"local_remote", func(config *udphop.Config) { config.Remote = true; config.RemoteIPs = []string{"127.0.0.1/32"} }, false},
		{"local_once", func(config *udphop.Config) { config.RemoteOnce = true; config.RemotePorts = []uint32{443} }, false},
		{"interval_order", func(config *udphop.Config) { config.IntervalMax = 4 }, true},
		{"interval_overflow", func(config *udphop.Config) { config.IntervalMax = 1<<63 - 1 }, true},
		{"invalid_prefix", func(config *udphop.Config) { config.RemoteIPs = []string{"invalid"} }, true},
		{"invalid_port", func(config *udphop.Config) { config.RemotePorts = []uint32{65536} }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			hop := &udphop.Config{Local: true, IntervalMin: 5, IntervalMax: 5}
			test.change(hop)
			config, err := nativeConfig(&internet.MemoryStreamConfig{
				SecuritySettings: &tls.Config{},
				QuicParams:       &internet.QuicParams{DisableChromeParrot: true},
				FinalMask:        finalmask.NewFinalMask(nil, []finalmask.UDPMask{hop}, nil, nil, nil, nil),
			})
			if (err != nil) != test.wantError {
				t.Fatalf("local mask validation: error=%v wantError=%t", err, test.wantError)
			}
			if err == nil && (!config.PacketBridge || config.PortHopping != nil || config.Salamander != "") {
				t.Fatal("local hopping did not select packet bridge")
			}
		})
	}
}

func TestNativeStringMasquerade(t *testing.T) {
	for _, test := range []struct {
		name      string
		edit      func(*Config)
		wantError bool
	}{
		{"default_status", func(*Config) {}, false},
		{"custom_status", func(config *Config) { config.MasqStringStatusCode = 201 }, false},
		{"no_content", func(config *Config) { config.MasqStringStatusCode = 204 }, false},
		{"not_modified", func(config *Config) { config.MasqStringStatusCode = 304 }, false},
		{"provisional_status", func(config *Config) { config.MasqStringStatusCode = 103 }, true},
		{"overflow_status", func(config *Config) { config.MasqStringStatusCode = 1000 }, true},
		{"negative_status", func(config *Config) { config.MasqStringStatusCode = -1 }, true},
		{"case_insensitive_type", func(config *Config) { config.MasqType = "STRING" }, false},
		{"valid_length", func(config *Config) { config.MasqStringHeaders["Content-Length"] = "18" }, false},
		{"invalid_length", func(config *Config) { config.MasqStringHeaders["Content-Length"] = "19" }, true},
		{"bad_header_name", func(config *Config) { config.MasqStringHeaders["bad name"] = "value" }, true},
		{"header_injection", func(config *Config) { config.MasqStringHeaders["test"] = "a\r\nb" }, true},
		{"connection_header", func(config *Config) { config.MasqStringHeaders["Connection"] = "keep-alive" }, true},
		{"transfer_encoding", func(config *Config) { config.MasqStringHeaders["Transfer-Encoding"] = "chunked" }, true},
		{"duplicate_header", func(config *Config) { config.MasqStringHeaders["x-test"] = "duplicate" }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := &Config{MasqType: "string", MasqString: "masquerade content", MasqStringHeaders: map[string]string{"X-Test": "present"}}
			test.edit(config)
			prepared, err := nativeMasquerade(config)
			if (err != nil) != test.wantError {
				t.Fatalf("nativeMasquerade: %v, want error=%t", err, test.wantError)
			}
			if err == nil {
				status := config.MasqStringStatusCode
				if status == 0 {
					status = 200
				}
				contentType := "text/plain; charset=utf-8"
				if status == 204 || status == 304 {
					contentType = ""
				}
				if prepared.Status != uint16(status) || prepared.Body != config.MasqString || prepared.Headers["x-test"] != "present" || prepared.Headers["content-type"] != contentType {
					t.Fatalf("masquerade settings were not forwarded: %+v", prepared)
				}
			}
		})
	}
	for _, kind := range []string{"", "404", "file", "proxy"} {
		prepared, err := nativeMasquerade(&Config{MasqType: kind})
		if prepared != nil || (err != nil) != (kind == "file" || kind == "proxy") {
			t.Fatalf("unexpected native masquerade support for %q: %v", kind, err)
		}
	}
}
