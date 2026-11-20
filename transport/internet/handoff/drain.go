package handoff

import (
	"bytes"
	"encoding/hex"
	"errors"
)

var drainMagic = [8]byte{'N', 'X', 'D', 'F', 'E', 'N', 'C', 1}

// DrainFrame is private to the authenticated generation channel, never public
// QUIC. A random per-operation token prevents a stale marker proving a fence.
func DrainFrame(token [16]byte) []byte {
	wire := make([]byte, 24)
	copy(wire, drainMagic[:])
	copy(wire[8:], token[:])
	return wire
}

func readDrainFrame(wire []byte) ([16]byte, bool) {
	var token [16]byte
	if len(wire) != 24 || !bytes.Equal(wire[:8], drainMagic[:]) {
		return token, false
	}
	copy(token[:], wire[8:])
	return token, token != [16]byte{}
}

func parseDrainToken(value string) ([16]byte, error) {
	var token [16]byte
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != 16 {
		return token, errors.New("expected a 128-bit drain token")
	}
	copy(token[:], raw)
	if token == [16]byte{} {
		return token, errors.New("zero drain token")
	}
	return token, nil
}
