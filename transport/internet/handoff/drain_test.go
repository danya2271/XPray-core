package handoff

import (
	"fmt"
	"testing"
)

func TestUDPDrainRequiresMarkerBackendAndMatchingToken(t *testing.T) {
	key := bindingKey{"hy", 443}
	ready := false
	g := &Generation{state: "paused", udp: map[bindingKey]*udpBinding{key: {drain: func([16]byte) (bool, error) { return ready, nil }}}}
	var token [16]byte
	token[0] = 1
	command := fmt.Sprintf("drain-udp hy:443 %x", token)
	if _, err := g.Command("drain"); err == nil {
		t.Fatal("drained before UDP fence")
	}
	if state, err := g.Command(command); err != nil || state != "udp-waiting" {
		t.Fatalf("%s %v", state, err)
	}
	if _, err := g.Command("resume"); err == nil {
		t.Fatal("resumed irreversible UDP drain")
	}
	other := token
	other[0] = 2
	if _, err := g.Command(fmt.Sprintf("drain-udp hy:443 %x", other)); err == nil {
		t.Fatal("changed drain token")
	}
	ready = true
	if state, err := g.Command(command); err != nil || state != "udp-drained" {
		t.Fatalf("%s %v", state, err)
	}
	if state, err := g.Command(command); err != nil || state != "udp-drained" {
		t.Fatal("drain retry failed")
	}
	if _, err := g.Command("drain"); err != nil {
		t.Fatal(err)
	}
}

func TestDrainFrameTokens(t *testing.T) {
	var token [16]byte
	token[0] = 1
	if got, ok := readDrainFrame(DrainFrame(token)); !ok || got != token {
		t.Fatal("marker mismatch")
	}
	for _, wire := range [][]byte{nil, DrainFrame([16]byte{}), append(DrainFrame(token), 0)} {
		if _, ok := readDrainFrame(wire); ok {
			t.Fatal("malformed marker accepted")
		}
	}
}
