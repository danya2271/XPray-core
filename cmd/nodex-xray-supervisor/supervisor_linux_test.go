//go:build linux && !android

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/transport/internet/grpc/encoding"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRejectUnsupportedConfigAndExistingSocket(t *testing.T) {
	for _, conf := range []string{
		`{"api":{},"inbounds":[]}`,
		`{"inbounds":[{"tag":"h","port":443,"protocol":"hysteria","streamSettings":{"network":"hysteria"}}]}`,
		`{"inbounds":[{"tag":"x","port":443,"streamSettings":{"network":"xhttp","tlsSettings":{"alpn":["h3"]}}}]}`,
		`{"inbounds":[{"tag":"g","port":443,"streamSettings":{"network":"grpc","sockopt":{"mark":42}}}]}`,
		`{"inbounds":[{"tag":"g","port":"1-65535","streamSettings":{"network":"grpc"}}]}`,
	} {
		if _, err := configBindings([]byte(conf)); err == nil {
			t.Errorf("accepted unsupported config: %s", conf)
		}
	}
	root := socketRoot(t)
	lock, err := claimRuntime(root)
	if err != nil {
		t.Fatal(err)
	}
	lock.Close()
	if _, err := claimRuntime(root); err == nil {
		t.Fatal("reused claimed runtime")
	}
	path := filepath.Join(root, "used.sock")
	b := binding{address: path, mode: 0660}
	l, err := listen(b)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err := listen(b); err == nil {
		t.Fatal("replaced existing Unix socket")
	}
	if info, err := os.Lstat(path); err != nil || info.Mode().Perm() != 0660 {
		t.Fatalf("socket changed: %v, %v", info, err)
	}
}

func TestStateWriteFailureBlocksDrain(t *testing.T) {
	root := socketRoot(t)
	if err := os.Mkdir(filepath.Join(root, "state.json"), 0700); err != nil {
		t.Fatal(err)
	}
	s := &supervisor{root: root, ctx: t.Context()}
	// No control endpoints are provided: even attempting drain would panic.
	s.retire(nil, nil, nil)
	if got := s.status(); got.Phase != "blocked" || !strings.Contains(got.Error, "could not persist") {
		t.Fatalf("state write failure did not block: %+v", got)
	}
}

func socketRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "nx-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	return root
}

func backend(t *testing.T, marker byte) int {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Minute))
				var pulse [1]byte
				for {
					if _, err := io.ReadFull(conn, pulse[:]); err != nil {
						return
					}
					if _, err := conn.Write([]byte{marker, pulse[0]}); err != nil {
						return
					}
				}
			}()
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}

func grpcStream(t *testing.T, ctx context.Context, address string) encoding.GRPCService_TunClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	stream, err := encoding.NewGRPCServiceClient(conn).(encoding.GRPCServiceClientX).TunCustomName(ctx, "canary", "Tun", grpc.WaitForReady(true))
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func grpcPulse(t *testing.T, stream encoding.GRPCService_TunClient, marker, value byte) {
	t.Helper()
	if err := stream.Send(&encoding.Hunk{Data: []byte{value}}); err != nil {
		t.Fatal(err)
	}
	reply, err := stream.Recv()
	if err != nil || !bytes.Equal(reply.GetData(), []byte{marker, value}) {
		t.Fatalf("gRPC pulse: got %q (%v), want %q", reply.GetData(), err, []byte{marker, value})
	}
}

func unixClient(t *testing.T, socket string) *http.Client {
	t.Helper()
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	c := &http.Client{Transport: &http.Transport{Protocols: protocols, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func xhttpRequest(t *testing.T, ctx context.Context, c *http.Client, id, sequence string, value byte) *http.Response {
	t.Helper()
	method := http.MethodGet
	if sequence != "" {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://origin.test/x/", nil)
	if err != nil {
		t.Fatal(err)
	}
	config := splithttp.Config{Path: "/x/", Mode: "packet-up"}
	if sequence == "" {
		config.FillStreamRequest(req, id, "")
	} else if err := config.FillPacketRequest(req, id, sequence, buf.MultiBuffer{buf.FromBytes([]byte{value})}); err != nil {
		t.Fatal(err)
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Body.Close() })
	if res.StatusCode != http.StatusOK {
		t.Fatalf("XHTTP %s: %s", method, res.Status)
	}
	return res
}

func xhttpPulse(t *testing.T, ctx context.Context, c *http.Client, down *http.Response, id, sequence string, marker, value byte) {
	t.Helper()
	xhttpRequest(t, ctx, c, id, sequence, value).Body.Close()
	var got [2]byte
	if _, err := io.ReadFull(down.Body, got[:]); err != nil || got != [2]byte{marker, value} {
		t.Fatalf("XHTTP pulse: got %q (%v), want %q", got, err, []byte{marker, value})
	}
}

// The supervisor runs a trusted executable for each new generation. Run the
// test binary in a separate process so this probe uses fresh connections.
func TestCandidateProbe(t *testing.T) {
	id := os.Getenv("NODEX_TEST_PROBE_ID")
	if id == "" {
		t.Skip("probe subprocess only")
	}
	marker := map[string]byte{"g1": 'A', "g2": 'B', "g3": 'C'}[id]
	if marker == 0 || len(os.Getenv("NODEX_CANDIDATE_SHA256")) != 64 || os.Getenv("NODEX_CANDIDATE_PID") == "" {
		t.Fatal("missing candidate identity")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream := grpcStream(t, ctx, os.Getenv("NODEX_TEST_GRPC"))
	grpcPulse(t, stream, marker, 'p')
	stream.CloseSend()
	if path := os.Getenv("NODEX_TEST_XHTTP"); path != "" {
		client := unixClient(t, path)
		down := xhttpRequest(t, ctx, client, "probe-"+id, "", 0)
		xhttpPulse(t, ctx, client, down, "probe-"+id, "0", marker, 'q')
		down.Body.Close()
	}
	if id == "g2" && os.Getenv("NODEX_TEST_GATE") != "" {
		if err := os.WriteFile(os.Getenv("NODEX_TEST_GATE")+".ready", nil, 0600); err != nil {
			t.Fatal(err)
		}
		for ctx.Err() == nil {
			if _, err := os.Stat(os.Getenv("NODEX_TEST_GATE")); err == nil {
				t.Fatal("injected failure after a successful candidate data-path check")
			}
			time.Sleep(15 * time.Millisecond)
		}
		t.Fatal("test did not release probe")
	}
}

func TestSupervisorSubprocess(t *testing.T) {
	args := os.Getenv("NODEX_TEST_SUPERVISOR_ARGS")
	if args == "" {
		t.Skip("supervisor subprocess only")
	}
	var argv []string
	if err := json.Unmarshal([]byte(args), &argv); err != nil {
		t.Fatal(err)
	}
	if err := run(argv); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverAfterSupervisorCrash(t *testing.T) {
	binary := os.Getenv("XRAY_TEST_GENERATION_BINARY")
	if binary == "" || os.Geteuid() == 0 {
		t.Skip("requires a managed Xray build and an unprivileged service account")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	root := socketRoot(t)
	runtime := filepath.Join(root, "run")
	public, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address, port := public.Addr().String(), public.Addr().(*net.TCPAddr).Port
	public.Close()
	t.Setenv("NODEX_TEST_GRPC", address)
	t.Setenv("NODEX_TEST_XHTTP", "")
	t.Setenv("NODEX_TEST_GATE", "")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("NODEX_TEST_EXEC", executable)
	probe := filepath.Join(root, "probe")
	if err := os.WriteFile(probe, []byte("#!/bin/sh\nNODEX_TEST_PROBE_ID=\"$1\" exec \"$NODEX_TEST_EXEC\" -test.run '^TestCandidateProbe$'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	makeConfig := func(marker byte) string {
		out := backend(t, marker)
		config := fmt.Sprintf(`{"inbounds":[{"tag":"grpc","listen":"127.0.0.1","port":%d,"protocol":"dokodemo-door","settings":{"address":"127.0.0.1","port":%d,"network":"tcp"},"streamSettings":{"network":"grpc","grpcSettings":{"serviceName":"canary"}}}],"outbounds":[{"protocol":"freedom"}]}`, port, out)
		path := filepath.Join(root, "config-"+string(marker)+".json")
		if err := os.WriteFile(path, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	a, b := makeConfig('A'), makeConfig('B')
	spawn := func(action, config string) *exec.Cmd {
		t.Helper()
		args, _ := json.Marshal([]string{"-runtime", runtime, "-binary", binary, "-config", config, "-probe", probe, action})
		cmd := exec.Command(executable, "-test.run", "^TestSupervisorSubprocess$")
		cmd.Env = append(os.Environ(), "NODEX_TEST_SUPERVISOR_ARGS="+string(args))
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Signal(syscall.SIGTERM)
				_ = cmd.Wait()
			}
		})
		return cmd
	}
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(runtime, "supervisor.sock"))
		}}}
	wait := func(phase, id string) status {
		t.Helper()
		deadline := time.Now().Add(12 * time.Second)
		var state status
		for time.Now().Before(deadline) {
			res, err := client.Get("http://nodex.local/status")
			if err == nil {
				state = status{}
				err = json.NewDecoder(res.Body).Decode(&state)
				res.Body.Close()
				if err == nil && state.Phase == phase && state.Active != nil && state.Active.ID == id {
					return state
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("did not reach %s/%s: %+v", phase, id, state)
		return status{}
	}
	first := spawn("serve", a)
	before := wait("active", "g1")
	oldCtx, endOld := context.WithCancel(ctx)
	defer endOld()
	stream := grpcStream(t, oldCtx, address)
	grpcPulse(t, stream, 'A', '0')
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err == nil {
		t.Fatal("supervisor was not killed")
	}
	// If a later assertion fails before recovery, do not leak the test Xray.
	orphaned := true
	t.Cleanup(func() {
		if orphaned {
			if g, err := adopt(context.Background(), runtime, *before.Active); err == nil {
				g.stop()
			}
		}
	})
	grpcPulse(t, stream, 'A', '1')
	wrong := spawn("recover", b)
	if err := wrong.Wait(); err == nil {
		t.Fatal("recovered using the wrong config")
	}
	grpcPulse(t, stream, 'A', '2')
	spawn("recover", a)
	after := wait("active", "g1")
	orphaned = false
	if *before.Active != *after.Active {
		t.Fatalf("recovery replaced the active process: before=%+v after=%+v", *before.Active, *after.Active)
	}
	grpcPulse(t, stream, 'A', '3')
	data, _ := json.Marshal(release{binary, b})
	response, err := client.Post("http://nodex.local/upgrade", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("recovered supervisor refused upgrade: %s", response.Status)
	}
	wait("draining", "g2")
	grpcPulse(t, stream, 'A', '4')
	grpcPulse(t, grpcStream(t, ctx, address), 'B', '5')
	endOld()
	wait("active", "g2")
}

func TestSupervisorRealGenerations(t *testing.T) {
	binary := os.Getenv("XRAY_TEST_GENERATION_BINARY")
	if binary == "" {
		t.Skip("set XRAY_TEST_GENERATION_BINARY to a managed Xray build")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	root := socketRoot(t)
	public, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address, port := public.Addr().String(), public.Addr().(*net.TCPAddr).Port
	public.Close()
	unix := filepath.Join(root, "origin.sock")
	gate := filepath.Join(root, "release-probe")
	t.Setenv("NODEX_TEST_GRPC", address)
	t.Setenv("NODEX_TEST_XHTTP", unix)
	t.Setenv("NODEX_TEST_GATE", gate)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("NODEX_TEST_EXEC", executable)
	probe := filepath.Join(root, "probe")
	if err := os.WriteFile(probe, []byte("#!/bin/sh\nNODEX_TEST_PROBE_ID=\"$1\" exec \"$NODEX_TEST_EXEC\" -test.run '^TestCandidateProbe$'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	makeRelease := func(marker byte) release {
		out := backend(t, marker)
		config := fmt.Sprintf(`{"inbounds":[
		{"tag":"grpc","listen":"127.0.0.1","port":%d,"protocol":"dokodemo-door","settings":{"address":"127.0.0.1","port":%d,"network":"tcp"},"streamSettings":{"network":"grpc","grpcSettings":{"serviceName":"canary"}}},
		{"tag":"origin","listen":%q,"protocol":"dokodemo-door","settings":{"address":"127.0.0.1","port":%d,"network":"tcp"},"streamSettings":{"network":"xhttp","xhttpSettings":{"path":"/x/","mode":"packet-up"}}}],"outbounds":[{"protocol":"freedom"}]}`, port, out, unix+",0660", out)
		path := filepath.Join(root, "config-"+string(marker)+".json")
		if err := os.WriteFile(path, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		return release{binary, path}
	}
	a, b, c := makeRelease('A'), makeRelease('B'), makeRelease('C')
	runtime := filepath.Join(root, "run")
	if err := os.Mkdir(runtime, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := newSupervisor(ctx, runtime, probe, a)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); s.close() }()
	api := s.handler()
	upgrade := func(r release, code int) {
		t.Helper()
		body, _ := json.Marshal(r)
		reqCtx, disconnect := context.WithCancel(ctx)
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequestWithContext(reqCtx, "POST", "/upgrade", bytes.NewReader(body)))
		disconnect() // The work must continue after the request's client goes away.
		if response.Code != code {
			t.Fatalf("upgrade returned %d: %s", response.Code, response.Body)
		}
	}
	wait := func(phase, active string) status {
		t.Helper()
		limit := time.Now().Add(12 * time.Second)
		for time.Now().Before(limit) {
			st := s.status()
			if st.Phase == phase && st.Active != nil && st.Active.ID == active {
				return st
			}
			if st.Phase == "blocked" || st.Phase == "admission-unknown" {
				t.Fatalf("supervisor blocked: %+v", st)
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("did not reach %s %s: %+v", phase, active, s.status())
		return status{}
	}
	oldCtx, endOld := context.WithCancel(ctx)
	defer endOld()
	old := grpcStream(t, oldCtx, address)
	grpcPulse(t, old, 'A', '0')
	oldDown := xhttpRequest(t, oldCtx, unixClient(t, unix), "old", "", 0)
	xhttpPulse(t, oldCtx, unixClient(t, unix), oldDown, "old", "0", 'A', '1')
	upgrade(b, http.StatusAccepted)
	wait("switching", "g1")
	for until := time.Now().Add(10 * time.Second); ; {
		if _, err := os.Stat(gate + ".ready"); err == nil {
			break
		}
		if time.Now().After(until) {
			t.Fatalf("probe never verified g2: %+v", s.status())
		}
		time.Sleep(15 * time.Millisecond)
	}
	grpcPulse(t, old, 'A', '2')
	xhttpPulse(t, oldCtx, unixClient(t, unix), oldDown, "old", "1", 'A', '3')
	bCtx, endB := context.WithCancel(ctx)
	defer endB()
	bStream := grpcStream(t, bCtx, address)
	grpcPulse(t, bStream, 'B', '4')
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	failure := wait("draining", "g1")
	if failure.Error == "" || failure.Other == nil || !failure.Other.Running {
		t.Fatalf("rollback did not retain the candidate: %+v", failure)
	}
	upgrade(c, http.StatusConflict)
	grpcPulse(t, bStream, 'B', '5')
	shortCtx, endShort := context.WithCancel(ctx)
	grpcPulse(t, grpcStream(t, shortCtx, address), 'A', '6')
	endShort()
	endB()
	wait("active", "g1")
	upgrade(c, http.StatusAccepted)
	cutover := wait("draining", "g3")
	if cutover.Other == nil || !cutover.Other.Running || cutover.Other.ID != "g1" {
		t.Fatalf("cutover did not retain the old generation: %+v", cutover)
	}
	grpcPulse(t, old, 'A', '7')
	xhttpPulse(t, oldCtx, unixClient(t, unix), oldDown, "old", "2", 'A', '8')
	grpcPulse(t, grpcStream(t, ctx, address), 'C', '9')
	oldDown.Body.Close()
	endOld()
	final := wait("active", "g3")
	if final.Other != nil || final.Error != "" {
		t.Fatalf("unexpected final status: %+v", final)
	}
	if _, err := os.Stat(filepath.Join(runtime, "g1")); !os.IsNotExist(err) {
		t.Fatalf("completed generation not cleaned: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(runtime, "state.json"))
	var disk status
	if err != nil || json.Unmarshal(data, &disk) != nil || disk.Active == nil || disk.Active.ID != final.Active.ID {
		t.Fatalf("unexpected persistent status %s: %v", strconv.Quote(string(data)), err)
	}
}
