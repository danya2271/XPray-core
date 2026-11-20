//go:build linux && !android

package main

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	handler "github.com/xtls/xray-core/app/proxyman/command"
)

func TestSupervisorPublicRealityProbe(t *testing.T) {
	binary, probe := os.Getenv("XRAY_TEST_GENERATION_BINARY"), os.Getenv("NODEX_TEST_PUBLIC_PROBE")
	if binary == "" || probe == "" {
		t.Skip("requires a managed Xray build and the built-in public probe")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	root := socketRoot(t)
	runtime := filepath.Join(root, "run")
	if err := os.Mkdir(runtime, 0700); err != nil {
		t.Fatal(err)
	}
	decoy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	decoy.EnableHTTP2 = true
	decoy.Config.ErrorLog = log.New(io.Discard, "", 0)
	decoy.StartTLS()
	defer decoy.Close()
	response := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer response.Close()
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	freePort := func() int {
		t.Helper()
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		return listener.Addr().(*net.TCPAddr).Port
	}
	var inbounds []any
	for _, network := range []string{"grpc", "xhttp"} {
		inbounds = append(inbounds, map[string]any{
			"tag": network, "listen": "127.0.0.1", "port": freePort(), "protocol": "vless",
			"settings": map[string]any{"decryption": "none", "clients": []any{map[string]any{"id": "11111111-1111-4111-8111-111111111111", "email": "existing@invalid"}}},
			"streamSettings": map[string]any{"network": network, "security": "reality",
				"realitySettings":    map[string]any{"target": decoy.Listener.Addr().String(), "serverNames": []string{"localhost"}, "shortIds": []string{"1234567890abcdef"}, "privateKey": base64.RawURLEncoding.EncodeToString(key.Bytes())},
				network + "Settings": map[string]any{"serviceName": "probe", "path": "/probe/", "mode": "auto"}},
		})
	}
	host, port, err := net.SplitHostPort(response.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	config := map[string]any{
		"log": map[string]any{"loglevel": "warning"}, "inbounds": inbounds,
		"api":       map[string]any{"tag": "api", "listen": net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort())), "services": []string{"HandlerService", "StatsService"}},
		"outbounds": []any{map[string]any{"protocol": "freedom", "settings": map[string]any{"redirect": net.JoinHostPort(host, port), "finalRules": []any{map[string]any{"action": "allow", "ip": []string{"127.0.0.1/32"}}}}}},
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := newSupervisor(ctx, runtime, probe, release{binary, path})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); s.close() }()
	checkUsers := func() {
		t.Helper()
		for _, tag := range []string{"grpc", "xhttp"} {
			users, err := s.api.GetInboundUsers(ctx, &handler.GetInboundUserRequest{Tag: tag})
			if err != nil || len(users.GetUsers()) != 1 || users.Users[0].Email != "existing@invalid" {
				t.Fatalf("%s candidate credential was not cleaned up: %v, %v", tag, users, err)
			}
		}
	}
	checkUsers()
	if err := s.upgrade(release{binary, path}); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		state := s.status()
		if state.Phase == "active" && state.Active != nil && state.Active.ID == "g2" {
			checkUsers()
			return
		}
		if state.Phase == "blocked" || state.Phase == "admission-unknown" || state.Error != "" {
			t.Fatalf("public Reality probe update failed: %+v", state)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("public Reality probe update did not finish")
}
