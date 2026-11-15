package hysteria

import (
	"testing"

	"github.com/xtls/xray-core/transport/internet"
)

func TestNativeBackendSelectionAndProcessOverride(t *testing.T) {
	for _, test := range []struct {
		env              string
		configured, want bool
	}{
		{"", false, false}, {"", true, true}, {"0", true, false}, {"1", false, true},
	} {
		t.Setenv("XRAY_HYSTERIA_NATIVE", test.env)
		if got := nativeRequested(&internet.MemoryStreamConfig{ProtocolSettings: &Config{Native: test.configured}}); got != test.want {
			t.Fatalf("env=%q configured=%t selected=%t", test.env, test.configured, got)
		}
	}
}
