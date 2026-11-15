//go:build !xhysteria_native || !cgo || (!linux && !android)

package hysteria

import (
	"context"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func nativeDial(context.Context, net.Destination, *internet.MemoryStreamConfig) (stat.Connection, error) {
	return nil, errors.New("native Hysteria requires CGO and -tags xhysteria_native on Linux/Android")
}

func nativeListen(context.Context, net.Address, net.Port, *internet.MemoryStreamConfig, internet.ConnHandler) (internet.Listener, error) {
	return nil, errors.New("native Hysteria requires CGO and -tags xhysteria_native on Linux/Android")
}
