//go:build linux && !android

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"time"
)

func (s *supervisor) serveMetrics(address string) error {
	if address == "" {
		return nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		s.api.mu.Lock()
		id := s.api.active
		s.api.mu.Unlock()
		if id == "" {
			return nil, errors.New("active generation metrics unavailable")
		}
		return (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(s.root, id, "api.sock.metrics"))
	}, DisableKeepAlives: true, ResponseHeaderTimeout: 3 * time.Second}
	proxy := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: "metrics.local"})
	proxy.Transport = transport
	s.metrics = &http.Server{Handler: proxy, ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 5 * time.Second}
	go s.metrics.Serve(listener)
	return nil
}
