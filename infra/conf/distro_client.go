//go:build !xray_server

package conf

import "github.com/xtls/xray-core/core"

func (c *Config) buildDistroApps(_ *core.Config) error {
	return c.rejectDisabledConfigs()
}
