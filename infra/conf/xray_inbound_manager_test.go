package conf

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	core "github.com/xtls/xray-core/core"
)

func TestBuildRegistersInboundManagerLast(t *testing.T) {
	config := new(Config)
	if err := json.Unmarshal([]byte(`{
		"inbounds": [{
			"listen": "127.0.0.1",
			"port": 1080,
			"protocol": "socks"
		}],
		"outbounds": [{
			"protocol": "freedom"
		}]
	}`), config); err != nil {
		t.Fatal(err)
	}

	built, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(built.Inbound) != 1 {
		t.Fatalf("built %d inbound handlers, want 1", len(built.Inbound))
	}
	if len(built.App) == 0 {
		t.Fatal("config has no app features")
	}

	lastFeature, err := built.App[len(built.App)-1].GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := lastFeature.(*proxyman.InboundConfig); !ok {
		t.Fatalf("last app feature is %T, want *proxyman.InboundConfig", lastFeature)
	}

	inboundManagerCount := 0
	for _, app := range built.App {
		feature, err := app.GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := feature.(*proxyman.InboundConfig); ok {
			inboundManagerCount++
		}
	}
	if inboundManagerCount != 1 {
		t.Fatalf("inbound manager count = %d, want 1", inboundManagerCount)
	}

	instance, err := core.New(built)
	if err != nil {
		t.Fatalf("core.New failed: %v", err)
	}
	if err := instance.Close(); err != nil {
		t.Fatalf("closing instance failed: %v", err)
	}
}
