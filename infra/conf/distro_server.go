//go:build xray_server

package conf

import (
	"encoding/json"
	"fmt"

	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"google.golang.org/protobuf/proto"
)

func init() {
	inboundCreators := ConfigCreatorCache{
		"vless":       func() interface{} { return new(VLessInboundConfig) },
		"vmess":       func() interface{} { return new(VMessInboundConfig) },
		"shadowsocks": func() interface{} { return new(ShadowsocksServerConfig) },
		"wireguard":   func() interface{} { return &WireGuardConfig{IsClient: false} },
	}
	for name, creator := range inboundCreators {
		common.Must(inboundConfigLoader.cache.RegisterCreator(name, creator))
	}
	outboundCreators := ConfigCreatorCache{
		"vmess":       func() interface{} { return new(VMessOutboundConfig) },
		"shadowsocks": func() interface{} { return new(ShadowsocksClientConfig) },
		"wireguard":   func() interface{} { return &WireGuardConfig{IsClient: true} },
		"loopback":    func() interface{} { return new(LoopbackConfig) },
	}
	for name, creator := range outboundCreators {
		common.Must(outboundConfigLoader.cache.RegisterCreator(name, creator))
	}
}

func (c *Config) buildDistroApps(config *core.Config) error {
	if c.Reverse != nil {
		return errors.PrintRemovedFeatureError(`"legacy reverse"`, `"VLESS Reverse Proxy"`)
	}
	api, metrics := &APIConfig{}, &MetricsConfig{}
	stat, fakeDNS := &stats.Config{}, &FakeDNSConfig{}
	observatory, burst := &ObservatoryConfig{}, &BurstObservatoryConfig{}
	version, geodata := &VersionConfig{}, &GeodataConfig{}
	sections := []struct {
		name  string
		raw   *json.RawMessage
		value any
		build func() (proto.Message, error)
	}{
		{"api", c.API, api, func() (proto.Message, error) { return api.Build() }},
		{"metrics", c.Metrics, metrics, func() (proto.Message, error) { return metrics.Build() }},
		{"stats", c.Stats, stat, func() (proto.Message, error) { return stat, nil }},
		{"fakeDns", c.FakeDNS, fakeDNS, func() (proto.Message, error) { return fakeDNS.Build() }},
		{"observatory", c.Observatory, observatory, observatory.Build},
		{"burstObservatory", c.BurstObservatory, burst, func() (proto.Message, error) { return burst.Build() }},
		{"version", c.Version, version, func() (proto.Message, error) { return version.Build() }},
		{"geodata", c.Geodata, geodata, func() (proto.Message, error) { return geodata.Build() }},
	}
	for _, section := range sections {
		if section.raw == nil {
			continue
		}
		if err := json.Unmarshal(*section.raw, section.value); err != nil {
			return fmt.Errorf("invalid %s configuration: %w", section.name, err)
		}
		message, err := section.build()
		if err != nil {
			return fmt.Errorf("failed to build %s configuration: %w", section.name, err)
		}
		if section.name == "fakeDns" {
			config.App = append([]*serial.TypedMessage{serial.ToTypedMessage(message)}, config.App...)
		} else {
			config.App = append(config.App, serial.ToTypedMessage(message))
		}
	}
	return nil
}
