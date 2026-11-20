//go:build linux && !android

package main

import (
	"encoding/json"
	"errors"
	"net"
	"strconv"
)

type apiBinding struct {
	Address string
	Tag     string
	Metrics string
}

// Each generation receives a private Unix API. Only the supervisor owns the
// stable loopback endpoint. Existing NodeX CLI calls continue to use that port.
func managedAPIConfig(data []byte, private string) ([]byte, *apiBinding, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, nil, err
	}
	raw := root["api"]
	if len(raw) == 0 || string(raw) == "null" {
		return data, nil, nil
	}
	var api struct {
		Tag      string
		Listen   string
		Services []string
	}
	if err := json.Unmarshal(raw, &api); err != nil {
		return nil, nil, err
	}
	if api.Tag == "" {
		return nil, nil, errors.New("managed API requires a tag")
	}
	for _, service := range api.Services {
		if service != "HandlerService" && service != "StatsService" && service != "LoggerService" {
			return nil, nil, errors.New("unsupported managed API service: " + service)
		}
	}
	address := api.Listen
	var inbounds []map[string]json.RawMessage
	if err := json.Unmarshal(root["inbounds"], &inbounds); err != nil {
		return nil, nil, err
	}
	var kept []map[string]json.RawMessage
	for _, inbound := range inbounds {
		var tag string
		json.Unmarshal(inbound["tag"], &tag)
		if tag != api.Tag {
			kept = append(kept, inbound)
			continue
		}
		var protocol, listen string
		var port int
		json.Unmarshal(inbound["protocol"], &protocol)
		json.Unmarshal(inbound["listen"], &listen)
		json.Unmarshal(inbound["port"], &port)
		if address != "" || protocol != "dokodemo-door" || net.ParseIP(listen) == nil || !net.ParseIP(listen).IsLoopback() || port < 1 || port > 65535 {
			return nil, nil, errors.New("managed API inbound must be a unique loopback dokodemo endpoint")
		}
		address = net.JoinHostPort(listen, strconv.Itoa(port))
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || port == "0" {
		return nil, nil, errors.New("managed API must listen on an explicit loopback TCP address")
	}
	root["inbounds"], _ = json.Marshal(kept)
	metrics := ""
	if len(root["metrics"]) != 0 {
		var settings map[string]json.RawMessage
		if err := json.Unmarshal(root["metrics"], &settings); err != nil {
			return nil, nil, err
		}
		json.Unmarshal(settings["listen"], &metrics)
		if metrics != "" {
			host, _, err := net.SplitHostPort(metrics)
			if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
				return nil, nil, errors.New("managed metrics must use loopback TCP")
			}
			if private != "" {
				settings["listen"], _ = json.Marshal(private + ".metrics")
			} else {
				delete(settings, "listen")
			}
			root["metrics"], _ = json.Marshal(settings)
		}
	}
	if private == "" {
		delete(root, "api")
	} else {
		var settings map[string]json.RawMessage
		json.Unmarshal(raw, &settings)
		settings["listen"], _ = json.Marshal(private)
		root["api"], _ = json.Marshal(settings)
	}
	// The old API routing rule must not address a removed outbound handler.
	var routing map[string]json.RawMessage
	if json.Unmarshal(root["routing"], &routing) == nil {
		var rules []map[string]json.RawMessage
		if json.Unmarshal(routing["rules"], &rules) == nil {
			var retained []map[string]json.RawMessage
			for _, rule := range rules {
				var tags []string
				json.Unmarshal(rule["inboundTag"], &tags)
				var filtered []string
				for _, tag := range tags {
					if tag != api.Tag {
						filtered = append(filtered, tag)
					}
				}
				if len(tags) != len(filtered) {
					if len(filtered) == 0 {
						continue
					}
					rule["inboundTag"], _ = json.Marshal(filtered)
				}
				retained = append(retained, rule)
			}
			routing["rules"], _ = json.Marshal(retained)
			root["routing"], _ = json.Marshal(routing)
		}
	}
	output, err := json.Marshal(root)
	return output, &apiBinding{Address: address, Tag: api.Tag, Metrics: metrics}, err
}
