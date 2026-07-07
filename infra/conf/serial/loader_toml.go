//go:build xray_toml

package serial

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/pelletier/go-toml"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf"
)

func init() {
	registerReaderDecoder("toml", DecodeTOMLConfig)
}

// DecodeTOMLConfig reads from reader and decode the config into *conf.Config
// using github.com/pelletier/go-toml and map to convert toml to json.
func DecodeTOMLConfig(reader io.Reader) (*conf.Config, error) {
	tomlFile, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.New("failed to read config file").Base(err)
	}

	configMap := make(map[string]interface{})
	if err := toml.Unmarshal(tomlFile, &configMap); err != nil {
		return nil, errors.New("failed to convert toml to map").Base(err)
	}

	jsonFile, err := json.Marshal(&configMap)
	if err != nil {
		return nil, errors.New("failed to convert map to json").Base(err)
	}

	return DecodeJSONConfig(bytes.NewReader(jsonFile))
}

func LoadTOMLConfig(reader io.Reader) (*core.Config, error) {
	tomlConfig, err := DecodeTOMLConfig(reader)
	if err != nil {
		return nil, err
	}

	pbConfig, err := tomlConfig.Build()
	if err != nil {
		return nil, errors.New("failed to parse toml config").Base(err)
	}

	return pbConfig, nil
}
