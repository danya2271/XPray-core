package handoff

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

// QUICCIDCodec issues opaque, fixed-length connection IDs. The key is shared
// within a node; it is separate from Hysteria credentials and TLS keys. Each
// connection ID advertised by an endpoint must use the same generation slot.
// It is a routing format, not a replacement for QUIC authentication.
type QUICCIDCodec struct {
	block cipher.Block
}

const QUICCIDLength = aes.BlockSize

func NewQUICCIDCodec(key [16]byte) *QUICCIDCodec {
	block, _ := aes.NewCipher(key[:])
	return &QUICCIDCodec{block: block}
}

func (c *QUICCIDCodec) Generate(slot byte) [QUICCIDLength]byte {
	plain := [QUICCIDLength]byte{'N', 'X', 'C', 'I', 1, slot}
	// crypto/rand.Read fills the whole buffer or terminates on modern Go.
	rand.Read(plain[6:])
	var cid [QUICCIDLength]byte
	c.block.Encrypt(cid[:], plain[:])
	return cid
}

func (c *QUICCIDCodec) slot(cid []byte, plain *[QUICCIDLength]byte) (byte, bool) {
	if len(cid) != QUICCIDLength {
		return 0, false
	}
	c.block.Decrypt(plain[:], cid)
	return plain[5], plain[0] == 'N' && plain[1] == 'X' && plain[2] == 'C' && plain[3] == 'I' && plain[4] == 1
}

type quicSlots struct {
	current byte
	live    [256]bool
}

type initialKey struct {
	version uint32
	length  uint8
	cid     [20]byte
}

type initialOwner struct {
	slot  byte
	until time.Time
}

// QUICRoutes is the classification step for an overlap receiver, operating on
// datagrams *after* UDP masking. Established packets take a lock-free path.
// Client-chosen Initial DCIDs need a bounded pin across the switch; packets
// with an unknown non-Initial CID must not go to another endpoint and risk a
// stateless reset. Neither client source address nor the hop port is a key.
type QUICRoutes struct {
	codec   *QUICCIDCodec
	slots   atomic.Pointer[quicSlots]
	mu      sync.Mutex
	initial map[initialKey]initialOwner
	limit   int
	ttl     time.Duration
	nextGC  time.Time
}

// QUICRouteReader owns decryption scratch for one receive loop. It is not
// shared between goroutines and does not allocate per established packet.
type QUICRouteReader struct {
	routes *QUICRoutes
	plain  [QUICCIDLength]byte
}

func (r *QUICRoutes) NewReader() *QUICRouteReader { return &QUICRouteReader{routes: r} }

func NewQUICRoutes(codec *QUICCIDCodec, initialSlot byte, maxPending int, ttl time.Duration) *QUICRoutes {
	r := &QUICRoutes{codec: codec, initial: make(map[initialKey]initialOwner), limit: maxPending, ttl: ttl}
	state := &quicSlots{current: initialSlot}
	state.live[initialSlot] = true
	r.slots.Store(state)
	return r
}

// Activate sends new handshakes to slot, without invalidating the previous
// slot or changing ownership of handshakes that have already started.
func (r *QUICRoutes) Activate(slot byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := *r.slots.Load()
	state.current = slot
	state.live[slot] = true
	r.slots.Store(&state)
}

// Retire must only be called once the owner has no remaining live work.
// A retired slot must not be reused under the same key while its old CIDs
// could still be received.
func (r *QUICRoutes) Retire(slot byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := *r.slots.Load()
	if state.current == slot {
		return false
	}
	state.live[slot] = false
	r.slots.Store(&state)
	return true
}

func (reader *QUICRouteReader) Route(datagram []byte, now time.Time) (byte, bool) {
	r := reader.routes
	if len(datagram) < 1 {
		return 0, false
	}
	if datagram[0]&0x80 == 0 { // short header
		if len(datagram) < 1+QUICCIDLength+1 {
			return 0, false
		}
		slot, ok := r.codec.slot(datagram[1:1+QUICCIDLength], &reader.plain)
		return slot, ok && r.slots.Load().live[slot]
	}
	if len(datagram) < 7 {
		return 0, false
	}
	version := binary.BigEndian.Uint32(datagram[1:5])
	n := int(datagram[5])
	if n > 20 || len(datagram) <= 6+n {
		return 0, false
	}
	dcid := datagram[6 : 6+n]
	scidLen := int(datagram[6+n])
	if scidLen > 20 || len(datagram) < 7+n+scidLen {
		return 0, false
	}
	if slot, ok := r.codec.slot(dcid, &reader.plain); ok {
		return slot, r.slots.Load().live[slot]
	}
	packetType := (datagram[0] >> 4) & 3
	// RFC 9000 (v1), RFC 9369 (v2). A long header that isn't an
	// Initial/0-RTT packet must have a known server-issued CID.
	isInitial := (version == 1 && packetType == 0) || (version == 0x6b3343cf && packetType == 1)
	isEarlyData := (version == 1 && packetType == 1) || (version == 0x6b3343cf && packetType == 2)
	if n < 8 || (!isInitial && !isEarlyData) {
		return 0, false
	}
	if now.IsZero() {
		now = time.Now() // Only client-chosen Initial/0-RTT needs a clock read.
	}
	key := initialKey{version: version, length: uint8(n)}
	copy(key.cid[:], dcid)
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.slots.Load()
	if owner, ok := r.initial[key]; ok && now.Before(owner.until) {
		return owner.slot, state.live[owner.slot]
	}
	if !isInitial || len(datagram) < 1200 {
		return 0, false
	}
	if !now.Before(r.nextGC) {
		for key, owner := range r.initial {
			if !now.Before(owner.until) {
				delete(r.initial, key)
			}
		}
		r.nextGC = now.Add(time.Second)
	}
	if _, existed := r.initial[key]; !existed && len(r.initial) >= r.limit {
		return 0, false
	}
	r.initial[key] = initialOwner{slot: state.current, until: now.Add(r.ttl)}
	return state.current, true
}
