package handoff

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestReadManifest(t *testing.T) {
	data := `{"control":"/run/xray/a/control.sock","tcp":[{"tag":"vg","port":443,"fd":3}]}`
	manifest, err := ReadManifest(strings.NewReader(data))
	if err != nil || len(manifest.TCP) != 1 || manifest.TCP[0].Port != 443 {
		t.Fatalf("unexpected manifest: %#v %v", manifest, err)
	}
	for _, invalid := range []string{data + "{}", data + strings.Repeat(" ", 32768), `{"tcp":[],"typo":true}`} {
		if _, err := ReadManifest(strings.NewReader(invalid)); err == nil {
			t.Fatal("invalid handoff manifest accepted")
		}
	}
}

func TestReceiveReleaseRequiresExplicitNativeOwner(t *testing.T) {
	key := bindingKey{"hy", 443}
	g := &Generation{state: "active", udp: map[bindingKey]*udpBinding{
		key: {spec: UDPBinding{Tag: "hy", Port: 443}},
	}}
	calls := 0
	release := func() ([]byte, error) {
		calls++
		return []byte(`{"slot":0,"pins":[]}`), nil
	}
	g.udp[key].releaseReceive = release
	if _, _, err := g.ReleaseUDPReceive("hy:443"); err == nil || calls != 0 {
		t.Fatal("released a generation without a direct receive lease")
	}
	g.udp[key].spec.DirectReceive = true
	if _, _, err := g.ReleaseUDPReceive("other:443"); err == nil || calls != 0 {
		t.Fatal("released wrong UDP binding")
	}
	if state, data, err := g.ReleaseUDPReceive("hy:443"); err != nil || state != "active" || len(data) == 0 || calls != 1 {
		t.Fatalf("explicit release failed: %s %s %v", state, data, err)
	}
	g.state = "draining"
	if _, _, err := g.ReleaseUDPReceive("hy:443"); err == nil || calls != 1 {
		t.Fatal("released receive ownership while draining")
	}
}

func TestPrivateControlSocket(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("test uses Linux filesystem permissions for Unix sockets")
	}
	parent := filepath.Join(t.TempDir(), "g")
	if err := os.Mkdir(parent, 0755); err != nil {
		t.Fatal(err)
	}
	addr := filepath.Join(parent, "ctl.sock")
	generation := &Generation{state: "paused"}
	if listener, err := ListenControl(addr, generation); err == nil {
		listener.Close()
		t.Fatal("non-private control directory accepted")
	}
	if err := os.Chmod(parent, 0700); err != nil {
		t.Fatal(err)
	}
	listener, err := ListenControl(addr, generation)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if info, err := os.Stat(addr); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("unexpected control socket permissions: %v (%v)", info, err)
	}
	if other, err := ListenControl(addr, generation); err == nil {
		other.Close()
		t.Fatal("existing control socket was replaced")
	}
	if _, err := ControlSocket(addr).Command(t.Context(), "unsupported"); err == nil {
		t.Fatal("invalid control operation succeeded")
	}
	if got, err := ControlSocket(addr).Command(t.Context(), "status"); err != nil || got.State != "paused" {
		t.Fatalf("invalid command changed admission: %+v (%v)", got, err)
	}
}
