//go:build linux && !android

package main

import (
	"bytes"
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
)

type binding struct {
	tag, address, transport string
	port                    int
	mode                    os.FileMode
}

type listener interface {
	net.Listener
	File() (*os.File, error)
}

func configBindings(config []byte) ([]binding, error) {
	var parsed struct {
		API      json.RawMessage
		Inbounds []struct {
			Tag, Listen, Protocol string
			Port                  int
			Stream                struct {
				Network, Security string
				Sockopt           map[string]json.RawMessage
				FinalMask         json.RawMessage
				TLS               struct{ ALPN []string } `json:"tlsSettings"`
			} `json:"streamSettings"`
		}
	}
	if err := json.Unmarshal(config, &parsed); err != nil {
		return nil, errors.New("expected JSON config with single integer inbound ports")
	}
	// Agent integration must coordinate user mutations and accounting across
	// both generations before a NodeX API inbound can be used here.
	if len(parsed.API) != 0 && !bytes.Equal(parsed.API, []byte("null")) {
		return nil, errors.New("pilot does not manage the Xray API")
	}
	if len(parsed.Inbounds) == 0 || len(parsed.Inbounds) > 128 {
		return nil, errors.New("expected 1–128 managed inbounds")
	}
	bindings := make([]binding, 0, len(parsed.Inbounds))
	tags := make(map[string]bool)
	for _, in := range parsed.Inbounds {
		stream := in.Stream
		if in.Tag == "" || in.Tag == "api" || tags[in.Tag] ||
			strings.EqualFold(in.Protocol, "hysteria") || strings.EqualFold(in.Protocol, "tun") ||
			(stream.Network != "grpc" && stream.Network != "xhttp" && stream.Network != "splithttp") ||
			slices.Contains(stream.TLS.ALPN, "h3") || len(stream.FinalMask) != 0 {
			return nil, errors.New("pilot supports uniquely tagged TCP gRPC and TCP/Unix XHTTP inbounds only")
		}
		tags[in.Tag] = true
		// An imported descriptor does not run the normal Xray bind hook.
		// Reject options whose effect we cannot preserve, rather than ignore them.
		for option, value := range stream.Sockopt {
			name, raw := strings.ToLower(option), string(value)
			if (slices.Contains([]string{"v6only", "acceptproxyprotocol", "penetrate"}, name) && raw == "false") ||
				(name == "mark" && raw == "0") ||
				(slices.Contains([]string{"interface", "dialerproxy"}, name) && raw == `""`) ||
				(name == "domainstrategy" && (raw == `"UseIP"` || raw == `"AsIs"`)) {
				continue
			}
			return nil, fmt.Errorf("unsupported socket option for inbound %q: %s", in.Tag, option)
		}
		b := binding{tag: in.Tag, address: in.Listen, port: in.Port, transport: stream.Network}
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
		lc := net.ListenConfig{KeepAlive: -1}
		lc.SetMultipathTCP(false)
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
