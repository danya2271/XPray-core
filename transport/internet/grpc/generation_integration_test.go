//go:build linux

package grpc

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/grpc/encoding"
	"github.com/xtls/xray-core/transport/internet/handoff"
	"github.com/xtls/xray-core/transport/internet/reality"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestManagedXrayProcesses(t *testing.T) {
	binary := os.Getenv("XRAY_TEST_GENERATION_BINARY")
	if binary == "" {
		t.Skip("set XRAY_TEST_GENERATION_BINARY to the built Xray executable")
	}
	t.Run("grpc", func(t *testing.T) { testManagedXrayProcesses(t, binary, false) })
	t.Run("grpc-reality", func(t *testing.T) { testManagedXrayProcesses(t, binary, true) })
}

func testManagedXrayProcesses(t *testing.T, binary string, secure bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	public, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	file, err := public.File()
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	port := public.Addr().(*net.TCPAddr).Port
	root := t.TempDir()
	var realitySettings string
	var clientReality *reality.Config
	if secure {
		decoy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		decoy.EnableHTTP2 = true
		decoy.Config.ErrorLog = log.New(io.Discard, "", 0)
		decoy.StartTLS()
		t.Cleanup(decoy.Close)
		key, err := ecdh.X25519().GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		realitySettings = fmt.Sprintf(`,"security":"reality","realitySettings":{"target":%q,"serverNames":["localhost"],"privateKey":%q,"shortIds":["1234567890abcdef"]}`,
			decoy.Listener.Addr().String(), base64.RawURLEncoding.EncodeToString(key.Bytes()))
		clientReality = &reality.Config{
			ServerName: "localhost", PublicKey: key.PublicKey().Bytes(), Fingerprint: "chrome", SpiderX: "/",
			ShortId: []byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef},
		}
	}
	start := func(generation string, valid bool) (string, *exec.Cmd) {
		t.Helper()
		backend, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { backend.Close() })
		go func() {
			for {
				conn, err := backend.Accept()
				if err != nil {
					return
				}
				go func() {
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(40 * time.Second))
					var pulse [1]byte
					for {
						if _, err := io.ReadFull(conn, pulse[:]); err != nil {
							return
						}
						if _, err := conn.Write([]byte{generation[0], pulse[0]}); err != nil {
							return
						}
					}
				}()
			}
		}()
		directory := filepath.Join(root, generation)
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
		socket := filepath.Join(directory, "ctl.sock")
		transport := "grpc"
		if !valid {
			transport = "tcp"
		}
		config := fmt.Sprintf(`{"inbounds":[{"tag":"vpn","listen":"127.0.0.1","port":%d,"protocol":"dokodemo-door","settings":{"address":"127.0.0.1","port":%d,"network":"tcp"},"streamSettings":{"network":%q,"grpcSettings":{"serviceName":"handoff"}%s}}],"outbounds":[{"protocol":"freedom"}]}`,
			port, backend.Addr().(*net.TCPAddr).Port, transport, realitySettings)
		manifest, err := json.Marshal(handoff.Manifest{Control: socket, TCP: []handoff.TCPBinding{{Tag: "vpn", Port: port, FD: 3}}})
		if err != nil {
			t.Fatal(err)
		}
		configFile, manifestFile := filepath.Join(directory, "config.json"), filepath.Join(directory, "manifest.json")
		if err := os.WriteFile(configFile, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestFile, manifest, 0600); err != nil {
			t.Fatal(err)
		}
		log, err := os.Create(filepath.Join(directory, "process.log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, binary, "run", "-format=json", "-config", configFile, "-handoff-manifest", manifestFile)
		cmd.Stdout, cmd.Stderr, cmd.ExtraFiles = log, log, []*os.File{file}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				cmd.Process.Kill()
				cmd.Wait()
			}
			log.Close()
			if t.Failed() {
				data, _ := os.ReadFile(log.Name())
				t.Logf("generation %s: %s", generation, data)
			}
		})
		return socket, cmd
	}
	ctl := func(socket, command string) (handoff.Response, error) {
		var reply handoff.Response
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", socket)
		if err != nil {
			return reply, err
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := fmt.Fprintln(conn, command); err != nil {
			return reply, err
		}
		return reply, json.NewDecoder(conn).Decode(&reply)
	}
	wait := func(socket, state string) {
		t.Helper()
		for ctx.Err() == nil {
			reply, err := ctl(socket, "status")
			if err == nil && reply.Protocol == handoff.ProtocolVersion && reply.State == state {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("%s never reported %s", socket, state)
	}
	a, old := start("A", true)
	wait(a, "paused")
	command := func(socket, op, state string) {
		t.Helper()
		reply, err := ctl(socket, op)
		if err != nil || reply.Protocol != handoff.ProtocolVersion || reply.Error != "" || reply.State != state {
			t.Fatalf("%s: %#v, %v", op, reply, err)
		}
	}
	command(a, "resume", "active")
	open := func() encoding.GRPCService_TunClient {
		t.Helper()
		options := []ggrpc.DialOption{ggrpc.WithTransportCredentials(insecure.NewCredentials())}
		if clientReality != nil {
			options = append(options, ggrpc.WithContextDialer(func(ctx context.Context, remote string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", remote)
				if err != nil {
					return nil, err
				}
				secure, err := reality.UClient(conn, clientReality, ctx, xnet.TCPDestination(xnet.LocalHostIP, xnet.Port(port)))
				if err != nil {
					conn.Close()
				}
				return secure, err
			}))
		}
		conn, err := ggrpc.NewClient("passthrough:///"+public.Addr().String(), options...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		client := encoding.NewGRPCServiceClient(conn).(encoding.GRPCServiceClientX)
		stream, err := client.TunCustomName(ctx, "handoff", "Tun", ggrpc.WaitForReady(true))
		if err != nil {
			t.Fatal(err)
		}
		return stream
	}
	check := func(stream encoding.GRPCService_TunClient, generation, pulse byte) {
		t.Helper()
		if err := stream.Send(&encoding.Hunk{Data: []byte{pulse}}); err != nil {
			t.Fatal(err)
		}
		reply, err := stream.Recv()
		if err != nil || string(reply.Data) != string([]byte{generation, pulse}) {
			t.Fatalf("response %v, %v; expected %c%c", reply, err, generation, pulse)
		}
	}
	oldStream := open()
	check(oldStream, 'A', '0')
	invalidSocket, invalid := start("invalid", false)
	if err := invalid.Wait(); err == nil {
		t.Fatal("unsupported raw TCP transport started in managed mode")
	}
	if _, err := os.Lstat(invalidSocket); !os.IsNotExist(err) {
		t.Fatalf("unsupported configuration created its control socket: %v", err)
	}
	check(oldStream, 'A', '1')
	b, _ := start("B", true)
	wait(b, "paused")
	var candidateStream encoding.GRPCService_TunClient
	failure := errors.New("simulated data-path regression")
	result, err := handoff.Switch(ctx, handoff.ControlSocket(a), handoff.ControlSocket(b), func(context.Context) error {
		candidateStream = open()
		check(candidateStream, 'B', '2')
		return failure
	})
	if !errors.Is(err, failure) || !result.RolledBack || result.Active != "previous" {
		t.Fatalf("failed probe did not restore admission to A: %+v, %v", result, err)
	}
	check(oldStream, 'A', '3')
	check(candidateStream, 'B', '4')
	rollbackStream := open()
	check(rollbackStream, 'A', '5')
	if err := rollbackStream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := rollbackStream.Recv(); err != io.EOF {
		t.Fatalf("rollback stream did not finish normally: %v", err)
	}
	var newStream encoding.GRPCService_TunClient
	result, err = handoff.Switch(ctx, handoff.ControlSocket(a), handoff.ControlSocket(b), func(context.Context) error {
		newStream = open()
		check(newStream, 'B', '6')
		return nil
	})
	if err != nil || result.Active != "candidate" || result.RolledBack {
		t.Fatalf("switch was not verified: %+v, %v", result, err)
	}
	command(a, "drain", "draining")
	check(oldStream, 'A', '7')
	command(a, "status", "draining")
	if err := oldStream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := oldStream.Recv(); err != io.EOF {
		t.Fatalf("old stream did not finish normally: %v", err)
	}
	wait(a, "drained")
	if err := old.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := old.Wait(); err != nil {
		t.Fatal(err)
	}
	check(newStream, 'B', '8')
	check(candidateStream, 'B', '9')
}
