//go:build !xray_server

package all_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xtls/xray-core/infra/conf"
)

func TestClientDistributionKeepsServerFeaturesDisabled(t *testing.T) {
	var config conf.Config
	if err := json.Unmarshal([]byte(`{"api":{"tag":"api","services":["StatsService"]}}`), &config); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Build(); err == nil || !strings.Contains(err.Error(), "disabled in this distro build") {
		t.Fatalf("client distribution accepted server config: %v", err)
	}
}
