//go:build xray_server

package all_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/infra/conf"
	_ "github.com/xtls/xray-core/main/distro/all"
)

func TestServerDistributionAPIAndStats(t *testing.T) {
	var config conf.Config
	if err := json.Unmarshal([]byte(`{
		"api":{"tag":"api","services":["HandlerService","LoggerService","StatsService"]},
		"stats":{},"metrics":{"listen":"127.0.0.1:0"},
		"inbounds":[{"tag":"vless","listen":"127.0.0.1","port":0,"protocol":"vless","settings":{"decryption":"none","clients":[]}}],
		"policy":{"levels":{"0":{"statsUserUplink":true,"statsUserDownlink":true}}}
	}`), &config); err != nil {
		t.Fatal(err)
	}
	built, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}
	instance, err := core.New(built)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if instance.GetFeature(stats.ManagerType()) == nil {
		t.Fatal("server stats manager missing")
	}
}

func TestServerDistributionBuildsDefaultFakeDNSPools(t *testing.T) {
	var config conf.Config
	if err := json.Unmarshal([]byte(`{"dns":{"servers":["fakedns"],"queryStrategy":"UseIPv4"}}`), &config); err != nil {
		t.Fatal(err)
	}
	built, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}
	if config.FakeDNS == nil {
		t.Fatal("FakeDNS pools were not generated")
	}
	instance, err := core.New(built)
	if err != nil {
		t.Fatal(err)
	}
	instance.Close()
}

func TestServerDistributionBuildsBurstObservatory(t *testing.T) {
	var config conf.Config
	if err := json.Unmarshal([]byte(`{"burstObservatory":{"subjectSelector":["hy"],"pingConfig":{"destination":"https://example.test","interval":"30s","timeout":"5s","sampling":1}}}`), &config); err != nil {
		t.Fatal(err)
	}
	built, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, app := range built.App {
		if app.Type == "xray.core.app.observatory.burst.Config" {
			found = true
		}
	}
	if !found {
		t.Fatal("burst observatory was not restored in the server distribution")
	}
}
