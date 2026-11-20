//go:build linux && !android

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/transport/internet"
)

type binding struct {
	tag, address, transport string
	port                    int
	mode                    os.FileMode
	native                  bool
	sockopt                 string
	packetPolicy            string
}

type listener interface {
	net.Listener
	File() (*os.File, error)
}

func configBindings(config []byte) ([]binding, error) {
	stripped, _, err := managedAPIConfig(config, "")
	if err != nil {
		return nil, err
	}
	config = stripped
	var parsed struct {
		API      json.RawMessage
		Inbounds []struct {
			Tag, Listen, Protocol string
			Port                  int
			Stream                struct {
				Network, Security string
				Sockopt           map[string]json.RawMessage
				FinalMask         json.RawMessage
				Hysteria          struct{ Native bool }   `json:"hysteriaSettings"`
				TLS               struct{ ALPN []string } `json:"tlsSettings"`
			} `json:"streamSettings"`
		}
	}
	if err := json.Unmarshal(config, &parsed); err != nil {
		return nil, errors.New("expected JSON config with single integer inbound ports")
	}
	if len(parsed.Inbounds) == 0 || len(parsed.Inbounds) > 128 {
		return nil, errors.New("expected 1–128 managed inbounds")
	}
	bindings := make([]binding, 0, len(parsed.Inbounds))
	tags := make(map[string]bool)
	for _, in := range parsed.Inbounds {
		stream := in.Stream
		if in.Tag == "" || in.Tag == "api" || tags[in.Tag] ||
			strings.EqualFold(in.Protocol, "tun") ||
			(stream.Network != "grpc" && stream.Network != "xhttp" && stream.Network != "splithttp" && stream.Network != "hysteria") ||
			(stream.Network != "hysteria" && (slices.Contains(stream.TLS.ALPN, "h3") || len(stream.FinalMask) != 0)) {
			return nil, errors.New("managed update supports tagged gRPC, TCP/Unix XHTTP and Hysteria inbounds")
		}
		tags[in.Tag] = true
		b := binding{tag: in.Tag, address: in.Listen, port: in.Port, transport: stream.Network, native: stream.Network == "hysteria" && stream.Hysteria.Native}
		if len(stream.Sockopt) != 0 {
			raw, err := json.Marshal(stream.Sockopt)
			if err != nil {
				return nil, err
			}
			b.sockopt = string(raw)
			settings, err := socketSettings(b)
			if err != nil {
				return nil, err
			}
			if err := internet.CheckManagedSocketConfig(settings); err != nil {
				return nil, err
			}
		}
		if b.transport == "hysteria" {
			var final struct {
				UDP json.RawMessage `json:"udp"`
			}
			if len(stream.FinalMask) != 0 {
				if err := json.Unmarshal(stream.FinalMask, &final); err != nil {
					return nil, err
				}
			}
			b.packetPolicy = string(final.UDP)
			switch os.Getenv("XRAY_HYSTERIA_NATIVE") {
			case "0":
				b.native = false
			case "1":
				b.native = true
			}
			if b.address == "" {
				b.address = "::"
			}
			ip, err := netip.ParseAddr(b.address)
			if err != nil || ip.Zone() != "" {
				return nil, errors.New("managed Hysteria requires a numeric IP without a scope zone")
			}
		}
		if b.transport == "splithttp" {
			b.transport = "xhttp"
		}
		if strings.HasPrefix(in.Listen, "/") {
			path, perm, explicit := strings.Cut(in.Listen, ",")
			b.address, b.port, b.mode = path, 0, 0600
			if filepath.Clean(path) != path || b.transport != "xhttp" || in.Port != 0 {
				return nil, fmt.Errorf("invalid Unix origin for inbound %q", in.Tag)
			}
			if explicit {
				mode, err := strconv.ParseUint(perm, 8, 9)
				if err != nil {
					return nil, fmt.Errorf("invalid Unix origin mode for inbound %q", in.Tag)
				}
				b.mode = os.FileMode(mode)
			}
		} else {
			if b.address == "" {
				b.address = "::"
			}
			ip, err := netip.ParseAddr(b.address)
			if err != nil || ip.Zone() != "" || b.port < 1 || b.port > 65535 {
				return nil, fmt.Errorf("invalid TCP bind for inbound %q", in.Tag)
			}
			b.address = ip.Unmap().String()
		}
		bindings = append(bindings, b)
	}
	slices.SortFunc(bindings, func(a, b binding) int { return strings.Compare(a.tag, b.tag) })
	return bindings, nil
}

func listen(b binding) (listener, error) {
	if b.port != 0 {
		settings, err := socketSettings(b)
		if err != nil {
			return nil, err
		}
		lc := internet.ManagedListenConfig(settings)
		socket, err := lc.Listen(context.Background(), "tcp", net.JoinHostPort(b.address, strconv.Itoa(b.port)))
		if err != nil {
			return nil, err
		}
		return socket.(*net.TCPListener), nil
	}
	// Do not remove a pre-existing socket, even if it looks stale. It may
	// belong to a live unmanaged Xray or another service.
	socket, err := net.ListenUnix("unix", &net.UnixAddr{Name: b.address, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(b.address, b.mode); err != nil {
		socket.Close()
		return nil, err
	}
	return socket, nil
}

func socketSettings(binding binding) (*internet.SocketConfig, error) {
	if binding.sockopt == "" {
		return nil, nil
	}
	var config conf.SocketConfig
	if err := json.Unmarshal([]byte(binding.sockopt), &config); err != nil {
		return nil, err
	}
	return config.Build()
}
