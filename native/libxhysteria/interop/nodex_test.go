//go:build xhysteria_interop && xhysteria_native && cgo && (linux || android)

package interop_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/core"
)

func TestNodeXNativeServerBinary(t *testing.T) {
	binary, generator := os.Getenv("XHYS_SERVER_BINARY"), os.Getenv("XHYS_NODEX_CONFIG_DUMP")
	if binary == "" || generator == "" {
		t.Skip("set XHYS_SERVER_BINARY and XHYS_NODEX_CONFIG_DUMP to test the packaged server")
	}
	t.Setenv("XRAY_HYSTERIA_NATIVE", "")
	t.Setenv("XRAY_HYSTERIA_POOL_SIZE", "0")
	dir := t.TempDir()
	certificate, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate})
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(certPath, ca)
	write(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: certificate.PrivateKey}))
	udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := udp.LocalAddr().String()
	port := udp.LocalAddr().(*net.UDPAddr).Port
	udp.Close()
	apiListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	api := apiListener.Addr().String()
	apiPort := apiListener.Addr().(*net.TCPAddr).Port
	apiListener.Close()
	config, err := exec.Command(generator, "--dump-native-config", certPath, keyPath, strconv.Itoa(port), strconv.Itoa(apiPort)).Output()
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "server.json")
	write(configPath, config)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "run", "-c", configPath)
	cmd.Env = append(os.Environ(), "XRAY_NET_DIAG=1")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		t.Log(output.String())
		if !strings.Contains(output.String(), "engine started bridge=") {
			t.Error("NodeX config did not activate the native backend")
		}
	})
	cli := func(args ...string) ([]byte, error) {
		call, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		return exec.CommandContext(call, binary, args...).CombinedOutput()
	}
	ready := false
	for i := 0; i < 50; i++ {
		if _, err := cli("api", "statsquery", "--server="+api, "-pattern", "user>>>"); err == nil {
			ready = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		t.Fatal("packaged Xray API did not start")
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(config, &document); err != nil {
		t.Fatal(err)
	}
	var inbounds []map[string]any
	if err := json.Unmarshal(document["inbounds"], &inbounds); err != nil {
		t.Fatal(err)
	}
	var target map[string]any
	for _, in := range inbounds {
		if in["tag"] == "hy" {
			target = in
			break
		}
	}
	if target == nil {
		t.Fatal("DanVlessBot did not generate a Hysteria inbound")
	}
	target["settings"].(map[string]any)["clients"] = []map[string]any{{"auth": auth, "email": "native-node@example.test"}}
	userConfig, err := json.Marshal(map[string]any{"inbounds": []map[string]any{target}})
	if err != nil {
		t.Fatal(err)
	}
	userPath := filepath.Join(dir, "users.json")
	write(userPath, userConfig)
	if out, err := cli("api", "adu", "--server="+api, userPath); err != nil || !strings.Contains(string(out), "Added 1 user(s)") {
		t.Fatalf("NodeX adu failed: %v %s", err, out)
	}
	t.Setenv("XRAY_HYSTERIA_NATIVE", "1")
	client := embeddedClient(t, server, ca, true)
	tcp, udpTarget := echoTargets(t)
	dial := func(network, address string) net.Conn {
		t.Helper()
		target, err := xnet.ParseDestination(network + ":" + address)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := core.Dial(ctx, client, target)
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	exchange := func(conn net.Conn) error {
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		unblock := time.AfterFunc(4*time.Second, func() { conn.Close() })
		defer unblock.Stop()
		data := bytes.Repeat([]byte{0x75}, 4096)
		if _, err := conn.Write(data); err != nil {
			return err
		}
		got := make([]byte, len(data))
		if _, err := io.ReadFull(conn, got); err != nil {
			return err
		}
		if !bytes.Equal(data, got) {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	active := dial("tcp", tcp)
	if err := exchange(active); err != nil {
		t.Fatal(err)
	}
	if err := exchange(dial("udp", udpTarget)); err != nil {
		t.Fatal(err)
	}
	stats, err := cli("api", "statsquery", "--server="+api, "-pattern", "user>>>")
	if err != nil {
		t.Fatalf("NodeX statsquery failed: %v %s", err, stats)
	}
	var counters struct {
		Stat []struct {
			Name  string
			Value json.Number
		}
	}
	if err := json.Unmarshal(stats, &counters); err != nil {
		t.Fatal(err)
	}
	traffic := int64(0)
	for _, stat := range counters.Stat {
		if strings.Contains(stat.Name, "native-node@example.test") {
			value, err := stat.Value.Int64()
			if err != nil {
				t.Fatal(err)
			}
			traffic += value
		}
	}
	if traffic == 0 {
		t.Fatalf("native traffic was not attributed to the NodeX user: %s", stats)
	}
	if out, err := cli("api", "rmu", "--server="+api, "-tag=hy", "native-node@example.test"); err != nil {
		t.Fatalf("NodeX rmu failed: %v %s", err, out)
	}
	if err := exchange(active); err != nil {
		t.Fatalf("rmu closed an active TCP flow: %v", err)
	}
	if err := exchange(dial("tcp", tcp)); err == nil {
		t.Fatal("removed user opened a new native connection")
	}
}
