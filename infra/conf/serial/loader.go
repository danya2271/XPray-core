package serial

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf"
	json_reader "github.com/xtls/xray-core/infra/conf/json"
)

type offset struct {
	line int
	char int
}

func findOffset(b []byte, o int) *offset {
	if o >= len(b) || o < 0 {
		return nil
	}

	line := 1
	char := 0
	for i, x := range b {
		if i == o {
			break
		}
		if x == '\n' {
			line++
			char = 0
		} else {
			char++
		}
	}

	return &offset{line: line, char: char}
}

// DecodeJSONConfig reads from reader and decode the config into *conf.Config
// syntax error could be detected.
//
// Permissive: accepts JSON with Java/Python-style comments via json_reader.Reader.
// Used for local files and stdin where the config is human-edited.
func DecodeJSONConfig(reader io.Reader) (*conf.Config, error) {
	jsonConfig := &conf.Config{}

	jsonContent := bytes.NewBuffer(make([]byte, 0, 10240))
	jsonReader := io.TeeReader(&json_reader.Reader{
		Reader: reader,
	}, jsonContent)
	decoder := json.NewDecoder(jsonReader)

	if err := decoder.Decode(jsonConfig); err != nil {
		var pos *offset
		cause := errors.Cause(err)
		switch tErr := cause.(type) {
		case *json.SyntaxError:
			pos = findOffset(jsonContent.Bytes(), int(tErr.Offset))
		case *json.UnmarshalTypeError:
			pos = findOffset(jsonContent.Bytes(), int(tErr.Offset))
		}
		if pos != nil {
			return nil, errors.New("failed to read config file at line ", pos.line, " char ", pos.char).Base(err)
		}
		return nil, errors.New("failed to read config file").Base(err)
	}

	return jsonConfig, nil
}

// DecodeJSONConfigStrict reads standard RFC 8259 JSON without comment-stripping.
// Used for remote sources (http/https/http+unix) where the payload is produced by
// automated systems and cannot contain JSON5/JSONC extensions. Avoids the
// byte-by-byte comment stripper and TeeReader, which are significant overhead on
// large configs.
func DecodeJSONConfigStrict(reader io.Reader) (*conf.Config, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.New("failed to read config file").Base(err)
	}
	jsonConfig := &conf.Config{}
	if err := json.Unmarshal(data, jsonConfig); err != nil {
		return nil, errors.New("failed to parse remote JSON config").Base(err)
	}
	return jsonConfig, nil
}

func LoadJSONConfig(reader io.Reader) (*core.Config, error) {
	jsonConfig, err := DecodeJSONConfig(reader)
	if err != nil {
		return nil, err
	}

	pbConfig, err := jsonConfig.Build()
	if err != nil {
		return nil, errors.New("failed to parse json config").Base(err)
	}

	return pbConfig, nil
}
