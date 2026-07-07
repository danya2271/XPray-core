//go:build xray_yaml

package serial

import (
	"bytes"
	"io"

	"github.com/ghodss/yaml"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf"
)

func init() {
	registerReaderDecoder("yaml", DecodeYAMLConfig)
}

// DecodeYAMLConfig reads from reader and decode the config into *conf.Config
// using github.com/ghodss/yaml to convert yaml to json.
func DecodeYAMLConfig(reader io.Reader) (*conf.Config, error) {
	yamlFile, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.New("failed to read config file").Base(err)
	}

	jsonFile, err := yaml.YAMLToJSON(yamlFile)
	if err != nil {
		return nil, errors.New("failed to convert yaml to json").Base(err)
	}

	return DecodeJSONConfig(bytes.NewReader(jsonFile))
}

func LoadYAMLConfig(reader io.Reader) (*core.Config, error) {
	yamlConfig, err := DecodeYAMLConfig(reader)
	if err != nil {
		return nil, err
	}

	pbConfig, err := yamlConfig.Build()
	if err != nil {
		return nil, errors.New("failed to parse yaml config").Base(err)
	}

	return pbConfig, nil
}
