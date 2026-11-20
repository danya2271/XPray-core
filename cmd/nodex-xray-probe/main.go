// Command nodex-xray-probe verifies fresh tunnels using a candidate-only user.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	handler "github.com/xtls/xray-core/app/proxyman/command"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/core"
	confserial "github.com/xtls/xray-core/infra/conf/serial"
	_ "github.com/xtls/xray-core/main/distro/all"
	"github.com/xtls/xray-core/proxy/hysteria/account"
	"github.com/xtls/xray-core/proxy/vless"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func withCandidateUser(ctx context.Context, api handler.HandlerServiceClient, tag string, user *protocol.User, verify func() error) (err error) {
	if _, err := api.AlterInbound(ctx, &handler.AlterInboundRequest{Tag: tag, Operation: serial.ToTypedMessage(&handler.AddUserOperation{User: user})}); err != nil {
		return fmt.Errorf("candidate credential: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, cleanupErr := api.AlterInbound(cleanup, &handler.AlterInboundRequest{Tag: tag, Operation: serial.ToTypedMessage(&handler.RemoveUserOperation{Email: user.Email})}); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("candidate credential cleanup: %w", cleanupErr))
		}
	}()
	return verify()
}

type endpoint struct {
	Address, Host, SNI, CA string
	Port                   int
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "NodeX candidate probe:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 || os.Args[1] == "" {
		return errors.New("generation identity required")
	}
	// The packaged probe has no native archive. Inheriting the server's native
	// override would first attempt an unavailable backend before Go fallback.
	if err := os.Setenv("XRAY_HYSTERIA_NATIVE", "0"); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	configPath, apiPath := os.Getenv("NODEX_CANDIDATE_CONFIG"), os.Getenv("NODEX_CANDIDATE_API")
	if configPath == "" || apiPath == "" {
		return errors.New("candidate private config and API required")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	var config struct {
		Inbounds []struct {
			Tag, Protocol, Listen string
			Port                  int
			Stream                map[string]any `json:"streamSettings"`
		}
		VXT map[string]endpoint `json:"_vxt"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}
	endpoints := config.VXT
	if endpoints == nil {
		endpoints = make(map[string]endpoint)
	}
	if path := os.Getenv("NODEX_MANAGED_PROBE_ENDPOINTS"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &endpoints); err != nil {
			return err
		}
	}
	connection, err := grpc.NewClient("unix://"+apiPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer connection.Close()
	api := handler.NewHandlerServiceClient(connection)
	checked := 0
	for _, inbound := range config.Inbounds {
		if inbound.Tag == "api" {
			continue
		}
		if inbound.Protocol != "vless" && inbound.Protocol != "hysteria" {
			return fmt.Errorf("%s: unsupported production probe protocol", inbound.Tag)
		}
		stream := inbound.Stream
		if stream == nil {
			return errors.New("missing inbound transport")
		}
		delete(stream, "sockopt")
		identifier := uuid.New()
		id := identifier.String()
		secret := make([]byte, 24)
		rand.Read(secret)
		email := "nodex-probe-" + hex.EncodeToString(secret[:8]) + "@invalid"
		user := &protocol.User{Email: email}
		settings := map[string]any{}
		if inbound.Protocol == "vless" {
			user.Account = serial.ToTypedMessage(&vless.Account{Id: id})
			settings["id"] = id
			settings["encryption"] = "none"
		} else {
			auth := hex.EncodeToString(secret)
			user.Account = serial.ToTypedMessage(&account.Account{Auth: auth})
			hysteria, _ := stream["hysteriaSettings"].(map[string]any)
			if hysteria == nil {
				hysteria = make(map[string]any)
			}
			hysteria["auth"] = auth
			hysteria["native"] = false
			stream["hysteriaSettings"] = hysteria
		}
		public := endpoints[inbound.Tag]
		if public.Address == "" {
			if strings.HasPrefix(inbound.Listen, "/") {
				return fmt.Errorf("%s: public Nginx probe endpoint is required", inbound.Tag)
			}
			public.Address = inbound.Listen
			public.Port = inbound.Port
			if public.Address == "" || public.Address == "::" || public.Address == "0.0.0.0" {
				public.Address = "127.0.0.1"
			}
		}
		if public.Port < 1 || public.Port > 65535 {
			return fmt.Errorf("%s: invalid public probe port", inbound.Tag)
		}
		security, _ := stream["security"].(string)
		if security == "reality" {
			reality, ok := stream["realitySettings"].(map[string]any)
			if !ok {
				return errors.New("missing Reality settings")
			}
			private, _ := reality["privateKey"].(string)
			raw, err := base64.RawURLEncoding.DecodeString(private)
			if err != nil {
				return err
			}
			key, err := ecdh.X25519().NewPrivateKey(raw)
			if err != nil {
				return err
			}
			names, _ := reality["serverNames"].([]any)
			shortIDs, _ := reality["shortIds"].([]any)
			if len(names) == 0 || len(shortIDs) == 0 {
				return errors.New("Reality server name and short ID required")
			}
			stream["realitySettings"] = map[string]any{"publicKey": base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes()), "serverName": names[0], "shortId": shortIDs[0], "fingerprint": "chrome"}
		} else if security == "tls" {
			tls, ok := stream["tlsSettings"].(map[string]any)
			if !ok {
				return errors.New("missing TLS settings")
			}
			certificates, _ := tls["certificates"].([]any)
			if len(certificates) == 0 {
				return errors.New("TLS certificate required for candidate pinning")
			}
			certificate, _ := certificates[0].(map[string]any)
			path, _ := certificate["certificateFile"].(string)
			var pemData []byte
			if path != "" {
				pemData, err = os.ReadFile(path)
			} else if lines, ok := certificate["certificate"].([]any); ok {
				for _, line := range lines {
					pemData = append(pemData, []byte(fmt.Sprint(line)+"\n")...)
				}
			}
			if err != nil {
				return err
			}
			block, _ := pem.Decode(pemData)
			if block == nil {
				return errors.New("cannot pin candidate TLS certificate")
			}
			pin := sha256.Sum256(block.Bytes)
			name, _ := tls["serverName"].(string)
			if public.SNI != "" {
				name = public.SNI
			}
			stream["tlsSettings"] = map[string]any{"serverName": name, "pinnedPeerCertSha256": hex.EncodeToString(pin[:])}
		} else if public.SNI != "" {
			stream["security"] = "tls"
			tls := map[string]any{"serverName": public.SNI, "fingerprint": "unsafe", "alpn": []string{"h2", "http/1.1"}}
			if public.CA != "" {
				tls["disableSystemRoot"] = true
				tls["certificates"] = []any{map[string]any{"certificateFile": public.CA, "usage": "verify"}}
			}
			stream["tlsSettings"] = tls
		}
		if public.Host != "" {
			xhttp, _ := stream["xhttpSettings"].(map[string]any)
			if xhttp == nil {
				xhttp = make(map[string]any)
			}
			xhttp["host"] = public.Host
			stream["xhttpSettings"] = xhttp
		}
		settings["address"], settings["port"] = public.Address, public.Port
		err = withCandidateUser(ctx, api, inbound.Tag, user, func() error {
			clientConfig := map[string]any{"log": map[string]any{"loglevel": "none"}, "outbounds": []any{map[string]any{"protocol": inbound.Protocol, "settings": settings, "streamSettings": stream}}}
			wire, err := json.Marshal(clientConfig)
			if err != nil {
				return err
			}
			built, err := confserial.LoadJSONConfig(bytes.NewReader(wire))
			if err != nil {
				return err
			}
			client, err := core.New(built)
			if err != nil {
				return err
			}
			defer client.Close()
			if err := client.Start(); err != nil {
				return err
			}
			tunnel, err := core.Dial(ctx, client, xnet.TCPDestination(xnet.DomainAddress("proxvless.test"), 80))
			if err != nil {
				return err
			}
			defer tunnel.Close()
			stop := context.AfterFunc(ctx, func() { tunnel.Close() })
			defer stop()
			if _, err := fmt.Fprint(tunnel, "GET /generate_204 HTTP/1.1\r\nHost: proxvless.test\r\nConnection: close\r\n\r\n"); err != nil {
				return err
			}
			response, err := http.ReadResponse(bufio.NewReader(tunnel), &http.Request{Method: "GET"})
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.StatusCode != 204 {
				return fmt.Errorf("unexpected tunnel response %d", response.StatusCode)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("%s public tunnel: %w", inbound.Tag, err)
		}
		checked++
	}
	if checked == 0 {
		return errors.New("no public inbounds were verified")
	}
	return nil
}
