//go:build linux && !android && xhysteria_native && cgo

package main

import (
	"context"
	"encoding/json"
	"net"

	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/hysteria"
	native "github.com/xtls/xray-core/transport/internet/xhysteria"
)

type managedNativeIngress struct{ *native.NativeIngress }

func (i managedNativeIngress) FenceGeneration(_ context.Context, slot byte, token [16]byte) (bool, error) {
	return i.NativeIngress.FenceGeneration(slot, token)
}

func makeNativePacketIngress(public *net.UDPConn, binding brokerBinding) (packetIngress, error) {
	var config conf.StreamConfig
	if err := json.Unmarshal(binding.Stream, &config); err != nil {
		return nil, err
	}
	built, err := config.Build()
	if err != nil {
		return nil, err
	}
	settings, err := internet.ToMemoryStreamConfig(built)
	if err != nil {
		return nil, err
	}
	masks, err := hysteria.ManagedNativeMasks(settings)
	if err != nil {
		return nil, err
	}
	i, err := native.NewDormantNativeIngress(public, binding.Key, 0, masks)
	if err != nil {
		return nil, err
	}
	return managedNativeIngress{i}, nil
}
