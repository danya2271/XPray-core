//go:build linux

package splithttp

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
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/handoff"
	"github.com/xtls/xray-core/transport/internet/reality"
)

func TestManagedXHTTPProcesses(t *testing.T) {
	binary := os.Getenv("XRAY_TEST_GENERATION_BINARY")
	if binary == "" {
		t.Skip("set XRAY_TEST_GENERATION_BINARY to the built Xray executable")
	}
	t.Run("unix-and-tcp", func(t *testing.T) { testManagedXHTTPProcesses(t, binary, false) })
	t.Run("unix-and-reality", func(t *testing.T) { testManagedXHTTPProcesses(t, binary, true) })
}

func testManagedXHTTPProcesses(t *testing.T, binary string, secure bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("", "xh-switch-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	public, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	port := public.Addr().(*net.TCPAddr).Port
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
		clientReality = &reality.Config{ServerName: "localhost", PublicKey: key.PublicKey().Bytes(), Fingerprint: "chrome", SpiderX: "/",
			ShortId: []byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef}}
	}
	sharedPath := filepath.Join(root, "vxt.sock")
	shared, err := net.ListenUnix("unix", &net.UnixAddr{Name: sharedPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()
	tcpFD, err := public.File()
	if err != nil {
		t.Fatal(err)
	}
	defer tcpFD.Close()
	unixFD, err := shared.File()
	if err != nil {
		t.Fatal(err)
	}
	defer unixFD.Close()

	start := func(gen byte) (handoff.ControlSocket, *exec.Cmd) {
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
					conn.SetDeadline(time.Now().Add(45 * time.Second))
					var pulse [1]byte
					for {
						if _, err := io.ReadFull(conn, pulse[:]); err != nil {
							return
						}
						if _, err := conn.Write([]byte{gen, pulse[0]}); err != nil {
							return
						}
					}
				}()
			}
		}()
		dir := filepath.Join(root, string(gen))
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		ctl := filepath.Join(dir, "ctl.sock")
		destination := backend.Addr().(*net.TCPAddr).Port
		config := fmt.Sprintf(`{"inbounds":[
{"tag":"vxr","listen":"127.0.0.1","port":%d,"protocol":"dokodemo-door","settings":{"address":"127.0.0.1","port":%d,"network":"tcp"},"streamSettings":{"network":"xhttp","xhttpSettings":{"path":"/x/","mode":"auto"}%s}},
{"tag":"vxt","listen":%q,"protocol":"dokodemo-door","settings":{"address":"127.0.0.1","port":%d,"network":"tcp"},"streamSettings":{"network":"xhttp","xhttpSettings":{"path":"/x/","mode":"auto"}}}],"outbounds":[{"protocol":"freedom"}]}`,
			port, destination, realitySettings, sharedPath+",0666", destination)
		if gen == 'C' {
			config = strings.ReplaceAll(config, `"mode":"auto"`, `"mode":"stream-one"`)
		}
		manifest, err := json.Marshal(handoff.Manifest{Control: ctl,
			TCP:  []handoff.TCPBinding{{Tag: "vxr", Port: port, FD: 3}},
			Unix: []handoff.UnixBinding{{Tag: "vxt", Path: sharedPath, FD: 4}},
		})
		if err != nil {
			t.Fatal(err)
		}
		configPath, manifestPath := filepath.Join(dir, "config.json"), filepath.Join(dir, "manifest.json")
		if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestPath, manifest, 0600); err != nil {
			t.Fatal(err)
		}
		log, err := os.Create(filepath.Join(dir, "process.log"))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, binary, "run", "-format=json", "-config", configPath, "-handoff-manifest", manifestPath)
		cmd.ExtraFiles, cmd.Stdout, cmd.Stderr = []*os.File{tcpFD, unixFD}, log, log
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				cmd.Process.Kill()
				cmd.Wait()
			}
			log.Close()
			if t.Failed() {
				content, _ := os.ReadFile(log.Name())
				t.Logf("generation %c: %s", gen, content)
			}
		})
		return handoff.ControlSocket(ctl), cmd
	}
	wait := func(socket handoff.ControlSocket, state string) {
		t.Helper()
		for ctx.Err() == nil {
			if reply, err := socket.Command(ctx, "status"); err == nil && reply.State == state {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("%s never reported %s", socket, state)
	}
	command := func(socket handoff.ControlSocket, op, expected string) {
		t.Helper()
		if reply, err := socket.Command(ctx, op); err != nil || reply.State != expected {
			t.Fatalf("%s: %s -> %#v, %v", socket, op, reply, err)
		}
	}
	client := func(unix bool) *http.Client {
		protocols := new(http.Protocols)
		protocols.SetUnencryptedHTTP2(true)
		c := &http.Client{Transport: &http.Transport{
			Protocols: protocols, MaxConnsPerHost: 1,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				d := &net.Dialer{}
				if unix {
					return d.DialContext(ctx, "unix", sharedPath)
				}
				conn, err := d.DialContext(ctx, "tcp", public.Addr().String())
				if err != nil || clientReality == nil {
					return conn, err
				}
				secure, err := reality.UClient(conn, clientReality, ctx, xnet.TCPDestination(xnet.LocalHostIP, xnet.Port(port)))
				if err != nil {
					conn.Close()
				}
				return secure, err
			},
		}}
		t.Cleanup(c.CloseIdleConnections)
		return c
	}
	config := &Config{Path: "/x/", Mode: "packet-up"}
	do := func(client *http.Client, id, seq string, value byte) *http.Response {
		t.Helper()
		method := http.MethodGet
		if seq != "" {
			method = http.MethodPost
		}
		req, err := http.NewRequestWithContext(ctx, method, "http://origin.test/x/", nil)
		if err != nil {
			t.Fatal(err)
		}
		if seq == "" {
			config.FillStreamRequest(req, id, "")
		} else if err := config.FillPacketRequest(req, id, seq, buf.MultiBuffer{buf.FromBytes([]byte{value})}); err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { response.Body.Close() })
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: %s", method, id, response.Status)
		}
		return response
	}
	pulse := func(c *http.Client, down *http.Response, id, seq string, generation, value byte) {
		t.Helper()
		do(c, id, seq, value).Body.Close()
		var got [2]byte
		if _, err := io.ReadFull(down.Body, got[:]); err != nil || got != [2]byte{generation, value} {
			t.Fatalf("session %s: got %q (%v), expected %c%c", id, got, err, generation, value)
		}
	}
	streamOne := func(client *http.Client) (*io.PipeWriter, *http.Response) {
		t.Helper()
		up, input := io.Pipe()
		t.Cleanup(func() { input.Close(); up.Close() })
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://origin.test/x/", up)
		if err != nil {
			t.Fatal(err)
		}
		config.FillStreamRequest(r, "", "")
		res, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { res.Body.Close() })
		if res.StatusCode != http.StatusOK {
			t.Fatalf("stream-one: %s", res.Status)
		}
		return input, res
	}
	streamPulse := func(up *io.PipeWriter, down *http.Response, generation, value byte) {
		t.Helper()
		if _, err := up.Write([]byte{value}); err != nil {
			t.Fatal(err)
		}
		var got [2]byte
		if _, err := io.ReadFull(down.Body, got[:]); err != nil || got != [2]byte{generation, value} {
			t.Fatalf("stream-one: got %q (%v), expected %c%c", got, err, generation, value)
		}
	}
	a, old := start('A')
	wait(a, "paused")
	command(a, "resume", "active")
	oldTCP, oldUnix := client(false), client(true)
	downTCP, downUnix := do(oldTCP, "tcp-old", "", 0), do(oldUnix, "unix-old", "", 0)
	upSingle, downSingle := streamOne(oldTCP)
	streamPulse(upSingle, downSingle, 'A', 'j')
	pulse(oldTCP, downTCP, "tcp-old", "0", 'A', 'a')
	pulse(oldUnix, downUnix, "unix-old", "0", 'A', 'b')
	b, _ := start('B')
	wait(b, "paused")
	if _, err := a.Command(ctx, "pause"); err == nil {
		t.Fatal("old generation paused before linking XHTTP peers")
	}
	incompatible, rejected := start('C')
	wait(incompatible, "paused")
	if err := handoff.LinkXHTTP(ctx, a, incompatible); err == nil || !strings.Contains(err.Error(), "incompatible session configuration") {
		t.Fatalf("incompatible candidate linked: %v", err)
	}
	if err := rejected.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := rejected.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := handoff.LinkXHTTP(ctx, a, b); err != nil {
		t.Fatal(err)
	}
	failedProbe := errors.New("candidate probe failed")
	var rolledBackDown *http.Response
	rollbackClient := client(true)
	result, err := handoff.Switch(ctx, a, b, func(context.Context) error {
		rolledBackDown = do(rollbackClient, "rollback-b", "", 0)
		pulse(rollbackClient, rolledBackDown, "rollback-b", "0", 'B', 'r')
		return failedProbe
	})
	if !errors.Is(err, failedProbe) || !result.RolledBack || result.Active != "previous" {
		t.Fatalf("XHTTP rollback not verified: %+v (%v)", result, err)
	}
	pulse(oldUnix, rolledBackDown, "rollback-b", "1", 'B', 's')
	pulse(rollbackClient, downUnix, "unix-old", "1", 'A', 't')
	rolledBackDown.Body.Close()
	var downB *http.Response
	newTCP, newUnix := client(false), client(true)
	result, err = handoff.Switch(ctx, a, b, func(context.Context) error {
		downB = do(newTCP, "tcp-new", "", 0)
		pulse(newTCP, downB, "tcp-new", "0", 'B', 'c')
		return nil
	})
	if err != nil || result.Active != "candidate" {
		t.Fatalf("unverified XHTTP cutover: %+v (%v)", result, err)
	}
	// This new logical session uses an already open HTTP/2 connection to A.
	// Its downlink must keep A alive even though its owning backend is B.
	forwarded := do(oldUnix, "unix-new-forwarded", "", 0)
	pulse(newUnix, forwarded, "unix-new-forwarded", "0", 'B', 'd')
	command(a, "drain", "draining")
	pulse(newTCP, downTCP, "tcp-old", "1", 'A', 'e')
	pulse(newUnix, downUnix, "unix-old", "2", 'A', 'f')
	streamPulse(upSingle, downSingle, 'A', 'k')
	if _, err := b.Command(ctx, "unlink"); err == nil {
		t.Fatal("active generation unlinked a peer with live requests")
	}
	downTCP.Body.Close()
	downUnix.Body.Close()
	upSingle.Close()
	downSingle.Body.Close()
	command(a, "status", "draining")
	pulse(newUnix, forwarded, "unix-new-forwarded", "1", 'B', 'g')
	forwarded.Body.Close()
	wait(a, "drained")
	command(b, "unlink", "active")
	if err := old.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := old.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(sharedPath); err != nil {
		t.Fatalf("old process removed supervisor's listener: %v", err)
	}
	pulse(newTCP, downB, "tcp-new", "1", 'B', 'h')
	after := do(newUnix, "after-old-exit", "", 0)
	pulse(newUnix, after, "after-old-exit", "0", 'B', 'i')
}
