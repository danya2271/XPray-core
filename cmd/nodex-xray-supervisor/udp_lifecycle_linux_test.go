//go:build linux && !android

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	stats "github.com/xtls/xray-core/app/stats/command"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func hysteriaTunnel(t *testing.T, ctx context.Context, binary, root string, port int) (net.Conn, func()) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	local := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	directory, err := os.MkdirTemp(root, "client-")
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := os.ReadFile(filepath.Join(root, "cert.pem"))
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certificate)
	if block == nil {
		t.Fatal("missing test certificate")
	}
	pin := sha256.Sum256(block.Bytes)
	config := fmt.Sprintf(`{"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":%d,"protocol":"socks","settings":{"auth":"noauth","udp":false}}],"outbounds":[{"protocol":"hysteria","settings":{"address":"127.0.0.1","port":%d},"streamSettings":{"network":"hysteria","security":"tls","hysteriaSettings":{"auth":"test-auth"},"tlsSettings":{"serverName":"localhost","pinnedPeerCertSha256":"%x"},"finalmask":{"quicParams":{"disableChromeParrot":true,"congestion":"reno"},"udp":[{"type":"salamander","settings":{"password":"test-password"}}]}}}]}`, local, port, pin)
	path := filepath.Join(directory, "client.json")
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, binary, "run", "-config", path)
	log, err := os.Create(filepath.Join(directory, "client.log"))
	if err != nil {
		t.Fatal(err)
	}
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			command.Process.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() { command.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				command.Process.Kill()
				<-done
			}
			log.Close()
		})
	}
	var connection net.Conn
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		connection, err = net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", local), 100*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		stop()
		data, _ := os.ReadFile(log.Name())
		t.Fatalf("client startup: %v: %s", err, data)
	}
	connection.SetDeadline(time.Now().Add(5 * time.Second))
	connection.Write([]byte{5, 1, 0})
	var greeting [2]byte
	if _, err := io.ReadFull(connection, greeting[:]); err != nil || greeting != [2]byte{5, 0} {
		stop()
		t.Fatalf("SOCKS greeting %v: %v", greeting, err)
	}
	request := []byte{5, 1, 0, 1, 127, 0, 0, 1, 0, 80}
	connection.Write(request)
	var header [4]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil || header[1] != 0 {
		stop()
		t.Fatalf("SOCKS connect %v: %v", header, err)
	}
	count := 6
	if header[3] == 4 {
		count = 18
	}
	if _, err := io.CopyN(io.Discard, connection, int64(count)); err != nil {
		stop()
		t.Fatal(err)
	}
	return connection, func() { connection.Close(); stop() }
}

func hysteriaPulse(t *testing.T, connection net.Conn, marker, value byte) {
	t.Helper()
	connection.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := connection.Write([]byte{value}); err != nil {
		t.Fatal(err)
	}
	var reply [2]byte
	if _, err := io.ReadFull(connection, reply[:]); err != nil || reply != [2]byte{marker, value} {
		t.Fatalf("Hysteria pulse %v: %v; want %v", reply, err, [2]byte{marker, value})
	}
}

func TestHysteriaCandidateProbe(t *testing.T) {
	configPath := os.Getenv("NODEX_TEST_HY_PROBE")
	if configPath == "" {
		t.Skip("probe subprocess only")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Binary, Root string
		Port         int
	}
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	id := os.Getenv("NODEX_TEST_PROBE_ID")
	marker := byte('A')
	if id == "g2" {
		marker = 'B'
	}
	if id == "g3" {
		marker = 'C'
	}
	if os.Getenv("NODEX_CANDIDATE_PID") == "" {
		t.Fatal("probe identity missing")
	}
	connection, stop := hysteriaTunnel(t, t.Context(), config.Binary, config.Root, config.Port)
	defer stop()
	hysteriaPulse(t, connection, marker, 'p')
	if gate := os.Getenv("NODEX_TEST_HY_SWITCH_GATE"); id == "g2" && gate != "" {
		if _, err := os.Stat(gate + ".fail"); err == nil {
			t.Fatal("injected candidate failure after supervisor crash")
		}
	}
	if gate := os.Getenv("NODEX_TEST_HY_SWITCH_GATE"); id == "g2" && gate != "" {
		if _, err := os.Stat(gate); err == nil {
			if err := os.WriteFile(gate+".ready", nil, 0600); err != nil {
				t.Fatal(err)
			}
			deadline := time.NewTimer(15 * time.Second)
			defer deadline.Stop()
			for {
				if _, err := os.Stat(gate); os.IsNotExist(err) {
					break
				}
				select {
				case <-deadline.C:
					t.Fatal("switch probe gate was not released")
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
	}
}

func TestSupervisorHysteriaSwitchRecovery(t *testing.T) {
	t.Setenv("NODEX_TEST_CRASH_DURING_SWITCH", "1")
	TestSupervisorHysteriaGenerations(t)
}

func TestSupervisorHysteriaSwitchRecoveryRollback(t *testing.T) {
	t.Setenv("NODEX_TEST_CRASH_DURING_SWITCH", "1")
	t.Setenv("NODEX_TEST_RECOVERY_PROBE_FAILURE", "1")
	TestSupervisorHysteriaGenerations(t)
}

func TestSupervisorHysteriaGenerations(t *testing.T) {
	binary := os.Getenv("XRAY_TEST_GENERATION_BINARY")
	if binary == "" {
		t.Skip("requires a managed Xray build")
	}
	for _, native := range []bool{false, true} {
		if native && os.Getenv("XRAY_TEST_MANAGED_NATIVE") != "1" {
			continue
		}
		t.Run(fmt.Sprintf("native-%t", native), func(t *testing.T) {
			t.Setenv("XRAY_HYSTERIA_NATIVE", "")
			ctx, cancel := context.WithTimeout(t.Context(), 75*time.Second)
			defer cancel()
			root := socketRoot(t)
			runtime := filepath.Join(root, "run")
			if err := os.Mkdir(runtime, 0700); err != nil {
				t.Fatal(err)
			}
			public, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			port := public.LocalAddr().(*net.UDPAddr).Port
			public.Close()
			certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
			certificatePEM, keyPEM := certificate.ToPEM()
			certificatePath, keyPath := filepath.Join(root, "cert.pem"), filepath.Join(root, "key.pem")
			os.WriteFile(certificatePath, certificatePEM, 0600)
			os.WriteFile(keyPath, keyPEM, 0600)
			freeTCP := func() int {
				listener, err := net.Listen("tcp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				return listener.Addr().(*net.TCPAddr).Port
			}
			apiPort, metricsPort := freeTCP(), freeTCP()
			probeHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
			defer probeHTTP.Close()
			makeRelease := func(marker byte) release {
				out := backend(t, marker)
				address := "0.0.0.0"
				data := fmt.Sprintf(`{"log":{"loglevel":"warning"},"inbounds":[{"tag":"hy","listen":%q,"port":%d,"protocol":"hysteria","settings":{"users":[{"auth":"test-auth","email":"tester"}]},"streamSettings":{"network":"hysteria","security":"tls","hysteriaSettings":{"native":%t},"tlsSettings":{"certificates":[{"certificateFile":%q,"keyFile":%q,"oneTimeLoading":true}]},"finalmask":{"quicParams":{"disableChromeParrot":true,"congestion":"reno"},"udp":[{"type":"salamander","settings":{"password":"test-password"}}]}}}],"outbounds":[{"protocol":"freedom","settings":{"redirect":%q,"finalRules":[{"action":"allow","ip":["127.0.0.1/32"]}]}}]}`, address, port, native, certificatePath, keyPath, fmt.Sprintf("127.0.0.1:%d", out))
				path := filepath.Join(root, fmt.Sprintf("config-%c.json", marker))
				var parsed map[string]any
				json.Unmarshal([]byte(data), &parsed)
				parsed["api"] = map[string]any{"tag": "api", "listen": fmt.Sprintf("127.0.0.1:%d", apiPort), "services": []string{"HandlerService", "StatsService", "LoggerService"}}
				parsed["metrics"] = map[string]any{"tag": "metrics", "listen": fmt.Sprintf("127.0.0.1:%d", metricsPort)}
				parsed["stats"] = map[string]any{}
				parsed["policy"] = map[string]any{"levels": map[string]any{"0": map[string]any{"statsUserUplink": true, "statsUserDownlink": true}}}
				parsed["outbounds"] = append(parsed["outbounds"].([]any), map[string]any{"tag": "probe", "protocol": "freedom", "settings": map[string]any{"redirect": strings.TrimPrefix(probeHTTP.URL, "http://"), "finalRules": []any{map[string]any{"action": "allow", "ip": []string{"127.0.0.1/32"}}}}})
				parsed["routing"] = map[string]any{"rules": []any{map[string]any{"type": "field", "domain": []string{"full:proxvless.test"}, "outboundTag": "probe"}}}
				wire, err := json.Marshal(parsed)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, wire, 0600); err != nil {
					t.Fatal(err)
				}
				return release{binary, path}
			}
			a, b, c := makeRelease('A'), makeRelease('B'), makeRelease('C')
			metadata, _ := json.Marshal(map[string]any{"Binary": binary, "Root": root, "Port": port})
			metaPath := filepath.Join(root, "probe.json")
			os.WriteFile(metaPath, metadata, 0600)
			t.Setenv("NODEX_TEST_HY_PROBE", metaPath)
			executable, _ := os.Executable()
			t.Setenv("NODEX_TEST_EXEC", executable)
			switchGate := filepath.Join(root, "switch-gate")
			switchCrash := os.Getenv("NODEX_TEST_CRASH_DURING_SWITCH") == "1"
			if switchCrash {
				if err := os.WriteFile(switchGate, nil, 0600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("NODEX_TEST_HY_SWITCH_GATE", switchGate)
			}
			probe := filepath.Join(root, "probe")
			os.WriteFile(probe, []byte("#!/bin/sh\nNODEX_TEST_PROBE_ID=\"$1\" exec \"$NODEX_TEST_EXEC\" -test.run '^TestHysteriaCandidateProbe$'\n"), 0700)
			if publicProbe := os.Getenv("NODEX_TEST_PUBLIC_PROBE"); publicProbe != "" {
				os.WriteFile(probe, []byte("#!/bin/sh\nset -e\n\"$NODEX_TEST_PUBLIC_PROBE\" \"$1\"\nNODEX_TEST_PROBE_ID=\"$1\" exec \"$NODEX_TEST_EXEC\" -test.run '^TestHysteriaCandidateProbe$'\n"), 0700)
			}
			var processes []*exec.Cmd
			spawn := func(action, config string) *exec.Cmd {
				t.Helper()
				arguments, _ := json.Marshal([]string{"-runtime", runtime, "-binary", binary, "-config", config, "-probe", probe, action})
				command := exec.Command(executable, "-test.run", "^TestSupervisorSubprocess$")
				command.Env = append(os.Environ(), "NODEX_TEST_SUPERVISOR_ARGS="+string(arguments))
				command.Stdout, command.Stderr = os.Stderr, os.Stderr
				if err := command.Start(); err != nil {
					t.Fatal(err)
				}
				processes = append(processes, command)
				return command
			}
			defer func() {
				for _, command := range processes {
					if command.ProcessState == nil {
						command.Process.Signal(syscall.SIGTERM)
						command.Wait()
					}
				}
			}()
			client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(runtime, "supervisor.sock"))
			}}}
			state := func() status {
				var result status
				response, err := client.Get("http://nodex.local/status")
				if err != nil {
					return result
				}
				defer response.Body.Close()
				json.NewDecoder(response.Body).Decode(&result)
				return result
			}
			wait := func(phase string) status {
				t.Helper()
				for ctx.Err() == nil {
					result := state()
					if result.Phase == phase {
						return result
					}
					if result.Phase == "blocked" || result.Phase == "admission-unknown" {
						t.Fatalf("supervisor blocked: %+v", result)
					}
					time.Sleep(20 * time.Millisecond)
				}
				t.Fatal(ctx.Err())
				return status{}
			}
			first := spawn("serve", a.Config)
			wait("active")
			old, stopOld := hysteriaTunnel(t, ctx, binary, root, port)
			defer stopOld()
			hysteriaPulse(t, old, 'A', 'a')
			data, _ := json.Marshal(b)
			response, err := client.Post("http://nodex.local/upgrade", "application/json", bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusAccepted {
				t.Fatalf("upgrade: %s", response.Status)
			}
			var before status
			if switchCrash {
				for {
					if _, err := os.Stat(switchGate + ".ready"); err == nil {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("candidate probe did not reach the switch gate")
					case <-time.After(10 * time.Millisecond):
					}
				}
				before = wait("switching")
				if !before.Uncertain || before.Active.ID != "g1" || before.Other.ID != "g2" {
					t.Fatalf("test did not interrupt uncommitted admission: %+v", before)
				}
			} else {
				before = wait("draining")
			}
			next, stopNext := hysteriaTunnel(t, ctx, binary, root, port)
			defer stopNext()
			hysteriaPulse(t, old, 'A', 'b')
			hysteriaPulse(t, next, 'B', 'c')
			if err := first.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			first.Wait()
			hysteriaPulse(t, old, 'A', 'x')
			hysteriaPulse(t, next, 'B', 'y')
			if switchCrash {
				if err := os.Remove(switchGate); err != nil {
					t.Fatal(err)
				}
			}
			rollbackRecovery := switchCrash && os.Getenv("NODEX_TEST_RECOVERY_PROBE_FAILURE") == "1"
			if rollbackRecovery {
				if err := os.WriteFile(switchGate+".fail", nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			spawn("recover", b.Config)
			after := wait("draining")
			previousActive, previousOther := before.Active.PID, before.Other.PID
			if switchCrash && !rollbackRecovery {
				previousActive, previousOther = previousOther, previousActive
			}
			if previousActive != after.Active.PID || previousOther != after.Other.PID || before.Broker.PID != after.Broker.PID {
				t.Fatalf("recovery replaced UDP ownership: before=%+v after=%+v", before, after)
			}
			hysteriaPulse(t, old, 'A', 'z')
			hysteriaPulse(t, next, 'B', 'w')
			if rollbackRecovery {
				if !strings.Contains(after.Error, "restored previous admission") || after.Active.ID != "g1" || after.Other.ID != "g2" {
					t.Fatalf("recovery did not report verified rollback: %+v", after)
				}
				fresh, stopFresh := hysteriaTunnel(t, ctx, binary, root, port)
				hysteriaPulse(t, fresh, 'A', 'r')
				stopFresh()
				stopNext()
				wait("active")
				hysteriaPulse(t, old, 'A', 's')
				return
			}
			stopOld()
			for state().Phase != "active" {
				current := state()
				if current.Phase == "blocked" {
					t.Fatalf("drain blocked: %+v", current)
				}
				hysteriaPulse(t, next, 'B', 'd')
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(200 * time.Millisecond):
				}
			}
			if native {
				management := &supervisor{root: runtime}
				if err := management.recoverBroker(ctx, state().Broker); err != nil {
					t.Fatal(err)
				}
				response, _, err := management.broker.command(ctx, brokerRequest{Operation: "status", Tag: "hy"})
				if err != nil || !response.Direct {
					t.Fatalf("direct RX not restored: %+v, %v", response, err)
				}
			}
			hysteriaPulse(t, next, 'B', 'e')
			if os.Getenv("NODEX_TEST_REPEAT_UPDATE") == "1" {
				data, _ := json.Marshal(c)
				response, err := client.Post("http://nodex.local/upgrade", "application/json", bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if response.StatusCode != http.StatusAccepted {
					t.Fatalf("second upgrade: %s", response.Status)
				}
				wait("draining")
				third, stopThird := hysteriaTunnel(t, ctx, binary, root, port)
				defer stopThird()
				hysteriaPulse(t, next, 'B', 'f')
				hysteriaPulse(t, third, 'C', 'g')
				stopNext()
				wait("active")
				hysteriaPulse(t, third, 'C', 'h')
			}
			api, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", apiPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer api.Close()
			counters, err := stats.NewStatsServiceClient(api).QueryStats(ctx, &stats.QueryStatsRequest{Pattern: "user>>>tester>>>"})
			if err != nil || len(counters.Stat) == 0 {
				t.Fatalf("final generation accounting: %v, %v", counters, err)
			}
			response, err = (&http.Client{Timeout: 2 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d/debug/vars", metricsPort))
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatalf("metrics gateway: %s", response.Status)
			}
		})
	}
}
