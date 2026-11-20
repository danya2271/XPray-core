package handoff

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// TCPBinding identifies a listener descriptor passed in by the process
// supervisor. The descriptor has the final bind address and socket options.
type TCPBinding struct {
	Tag  string `json:"tag"`
	Port int    `json:"port"`
	FD   int    `json:"fd"`
}

type UnixBinding struct {
	Tag  string `json:"tag"`
	Path string `json:"path"`
	FD   int    `json:"fd"`
}

type UDPBinding struct {
	Tag           string   `json:"tag"`
	Address       string   `json:"address"`
	Port          int      `json:"port"`
	FD            int      `json:"fd"`
	Key           [16]byte `json:"key"`
	Slot          byte     `json:"slot"`
	PacketBatch   bool     `json:"packet_batch,omitempty"`
	TransmitFD    int      `json:"transmit_fd,omitempty"`
	RingFDs       []int    `json:"ring_fds,omitempty"`
	DirectReceive bool     `json:"direct_receive,omitempty"`
}

type Manifest struct {
	Control string        `json:"control"`
	TCP     []TCPBinding  `json:"tcp"`
	Unix    []UnixBinding `json:"unix,omitempty"`
	UDP     []UDPBinding  `json:"udp,omitempty"`
}

type bindingKey struct {
	tag  string
	port int
}

type binding struct {
	listener *TCPListener
	claimed  bool
	drain    func() error
	xhttp    XHTTP
}

type udpBinding struct {
	spec           UDPBinding
	packet         net.PacketConn
	status         func() UDPConnectionStatus
	releaseReceive func() ([]byte, error)
}

// XHTTP participates in the admission transaction in addition to having
// a public listener. Linking peers alone does not change admission.
type XHTTP interface {
	Drain() error
	StartHandoff(path string) error
	LinkHandoffPeer(path string) error
	UnlinkHandoffPeer() error
	SetHandoffAdmission(active bool) error
	HandoffPeerLinked() bool
}

type Generation struct {
	mu       sync.Mutex
	state    string
	bindings map[bindingKey]*binding
	udp      map[bindingKey]*udpBinding
	control  string
}

func ReadManifest(file io.Reader) (Manifest, error) {
	const maxSize = 32768
	raw, err := io.ReadAll(io.LimitReader(file, maxSize+1))
	if err != nil {
		return Manifest{}, err
	}
	if len(raw) > maxSize {
		return Manifest{}, errors.New("handoff manifest exceeds 32 KiB")
	}
	var manifest Manifest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&manifest); err != nil {
		return Manifest{}, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return Manifest{}, errors.New("unexpected data after handoff manifest")
	}
	return manifest, nil
}

func ImportGeneration(manifest Manifest) (*Generation, error) {
	count := len(manifest.TCP) + len(manifest.Unix) + len(manifest.UDP)
	if count == 0 || count > 128 {
		return nil, errors.New("handoff requires 1–128 transport bindings")
	}
	if err := PrivateSocketDirectory(filepath.Dir(manifest.Control)); err != nil {
		return nil, err
	}
	seen := make(map[int]bool, count)
	g := &Generation{state: "starting", control: manifest.Control, bindings: make(map[bindingKey]*binding, count)}
	g.udp = make(map[bindingKey]*udpBinding, len(manifest.UDP))
	add := func(key bindingKey, fd int, path string) error {
		if key.tag == "" || fd < 3 || seen[fd] || g.bindings[key] != nil {
			return fmt.Errorf("invalid or duplicate handoff binding for %q port %d", key.tag, key.port)
		}
		seen[fd] = true
		file := os.NewFile(uintptr(fd), "xray-inherited-listener")
		defer file.Close()
		var listener *TCPListener
		var err error
		if path == "" {
			listener, err = ImportTCPListener(file)
		} else {
			listener, err = ImportUnixListener(file)
		}
		if err != nil {
			return fmt.Errorf("importing handoff socket for %q: %w", key.tag, err)
		}
		if path != "" && listener.Addr().String() != path {
			listener.Close()
			return fmt.Errorf("Unix handoff descriptor does not match path %q", path)
		}
		g.bindings[key] = &binding{listener: listener}
		return nil
	}
	for _, spec := range manifest.TCP {
		if spec.Port < 1 || spec.Port > 65535 {
			g.Close()
			return nil, fmt.Errorf("invalid handoff port %d", spec.Port)
		}
		if err := add(bindingKey{spec.Tag, spec.Port}, spec.FD, ""); err != nil {
			g.Close()
			return nil, err
		}
	}
	for _, spec := range manifest.Unix {
		if !filepath.IsAbs(spec.Path) || filepath.Clean(spec.Path) != spec.Path {
			g.Close()
			return nil, errors.New("Unix handoff binding requires a clean absolute path")
		}
		if err := add(bindingKey{spec.Tag, 0}, spec.FD, spec.Path); err != nil {
			g.Close()
			return nil, err
		}
	}
	for _, spec := range manifest.UDP {
		key := bindingKey{spec.Tag, spec.Port}
		ip, err := netip.ParseAddr(spec.Address)
		if spec.Port < 1 || spec.Port > 65535 || spec.Tag == "" || spec.FD < 3 || seen[spec.FD] ||
			spec.Key == [16]byte{} || err != nil || ip.IsUnspecified() || ip.Zone() != "" || g.udp[key] != nil {
			g.Close()
			return nil, fmt.Errorf("invalid or duplicate inherited UDP binding for %q", spec.Tag)
		}
		seen[spec.FD] = true
		file := os.NewFile(uintptr(spec.FD), "xray-inherited-packet-channel")
		packet, err := importUDPPacket(file, net.UDPAddr{IP: ip.Unmap().AsSlice(), Port: spec.Port})
		file.Close()
		if err != nil {
			g.Close()
			return nil, fmt.Errorf("importing handoff datagram socket for %q: %w", spec.Tag, err)
		}
		if spec.TransmitFD != 0 {
			if spec.TransmitFD < 3 || seen[spec.TransmitFD] {
				packet.Close()
				g.Close()
				return nil, errors.New("invalid inherited UDP transmit descriptor")
			}
			seen[spec.TransmitFD] = true
			if err := attachUDPTransmit(packet, spec.TransmitFD); err != nil {
				packet.Close()
				g.Close()
				return nil, err
			}
		}
		if len(spec.RingFDs) != 0 {
			if !spec.PacketBatch || len(spec.RingFDs) != 3 {
				packet.Close()
				g.Close()
				return nil, errors.New("packet ring requires explicit batch capability")
			}
			for _, fd := range spec.RingFDs {
				if fd < 3 || seen[fd] {
					packet.Close()
					g.Close()
					return nil, errors.New("invalid or duplicate packet ring descriptor")
				}
				seen[fd] = true
			}
			if err := attachPacketRing(packet, [3]int(spec.RingFDs)); err != nil {
				packet.Close()
				g.Close()
				return nil, err
			}
		}
		if spec.DirectReceive {
			if spec.TransmitFD == 0 || len(spec.RingFDs) == 0 {
				packet.Close()
				g.Close()
				return nil, errors.New("direct managed receive requires native transmit and ring descriptors")
			}
			setDirectReceive(packet)
		}
		g.udp[key] = &udpBinding{spec: spec, packet: packet}
	}
	return g, nil
}

func (g *Generation) AttachUDP(tag string, port int, address net.IP,
	start func(net.PacketConn, [16]byte, byte, bool) (func() UDPConnectionStatus, error),
) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	b := g.udp[bindingKey{tag, port}]
	if b == nil || b.status != nil || g.state != "starting" {
		return fmt.Errorf("no unclaimed UDP handoff binding for %q port %d", tag, port)
	}
	actual := b.packet.LocalAddr().(*net.UDPAddr).IP
	if !actual.Equal(address) && !(address.IsUnspecified() && (actual.To4() != nil) == (address.To4() != nil)) {
		return fmt.Errorf("handoff UDP address %s does not match inbound %s", actual, address)
	}
	status, err := start(b.packet, b.spec.Key, b.spec.Slot, b.spec.PacketBatch)
	if err != nil {
		return err
	}
	b.status = status
	return nil
}

func (g *Generation) AttachUDPReceiveRelease(tag string, port int, release func() ([]byte, error)) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	b := g.udp[bindingKey{tag, port}]
	if b == nil || b.status == nil || b.releaseReceive != nil || g.state != "starting" {
		return errors.New("cannot attach managed receive ownership callback")
	}
	b.releaseReceive = release
	return nil
}

func (g *Generation) ReleaseUDPReceive(target string) (string, []byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state != "active" && g.state != "paused" {
		return g.state, nil, errors.New("cannot release receive ownership in current generation state")
	}
	for key, b := range g.udp {
		if fmt.Sprintf("%s:%d", key.tag, key.port) == target && b.spec.DirectReceive && b.releaseReceive != nil {
			data, err := b.releaseReceive()
			return g.state, data, err
		}
	}
	return g.state, nil, errors.New("no direct native receive owner for requested UDP binding")
}

func (g *Generation) UDPStatus() (string, map[string]UDPConnectionStatus) {
	g.mu.Lock()
	defer g.mu.Unlock()
	result := make(map[string]UDPConnectionStatus, len(g.udp))
	for key, b := range g.udp {
		status := UDPConnectionStatus{}
		if b.status != nil {
			status = b.status()
		}
		result[fmt.Sprintf("%s:%d", key.tag, key.port)] = status
	}
	return g.state, result
}

type generationContextKey struct{}
type listenerContextKey struct{}

func WithGeneration(ctx context.Context, generation *Generation) context.Context {
	return context.WithValue(ctx, generationContextKey{}, generation)
}

func FromContext(ctx context.Context) *Generation {
	generation, _ := ctx.Value(generationContextKey{}).(*Generation)
	return generation
}

func WithTCPListener(ctx context.Context, listener *TCPListener) context.Context {
	return context.WithValue(ctx, listenerContextKey{}, listener)
}

func TCPListenerFromContext(ctx context.Context) *TCPListener {
	listener, _ := ctx.Value(listenerContextKey{}).(*TCPListener)
	return listener
}

// ClaimTCP rejects missing bindings for public transports in managed mode;
// silently binding a new socket would bypass the generation admission gate.
func (g *Generation) ClaimTCP(tag string, address net.IP, port int) (*TCPListener, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	b := g.bindings[bindingKey{tag, port}]
	if b == nil || b.claimed || g.state != "starting" {
		return nil, fmt.Errorf("no unclaimed TCP handoff binding for %q port %d", tag, port)
	}
	bound := b.listener.Addr().(*net.TCPAddr)
	if bound.Port != port || !(bound.IP.Equal(address) || (bound.IP.IsUnspecified() && address.IsUnspecified())) {
		return nil, fmt.Errorf("handoff descriptor address %s does not match %q (%s port %d)", bound, tag, address, port)
	}
	b.claimed = true
	return b.listener, nil
}

func (g *Generation) ClaimUnix(tag, address string) (*TCPListener, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	b := g.bindings[bindingKey{tag, 0}]
	path, _, _ := strings.Cut(address, ",")
	if b == nil || b.claimed || g.state != "starting" || b.listener.Addr().String() != path {
		return nil, fmt.Errorf("no unclaimed Unix handoff binding for %q at %q", tag, path)
	}
	b.claimed = true
	return b.listener, nil
}

func peerName(key bindingKey) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", key.tag, key.port)))
	return fmt.Sprintf("xh-%x", digest[:8])
}

func (g *Generation) AttachXHTTP(tag string, port int, xhttp XHTTP) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := bindingKey{tag, port}
	b := g.bindings[key]
	if b == nil || !b.claimed || b.drain != nil || g.state != "starting" {
		return fmt.Errorf("cannot attach XHTTP handoff to %q port %d", tag, port)
	}
	if err := xhttp.StartHandoff(filepath.Join(filepath.Dir(g.control), peerName(key))); err != nil {
		return err
	}
	b.xhttp, b.drain = xhttp, xhttp.Drain
	return nil
}

func (g *Generation) AttachDrain(tag string, port int, drain func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	b := g.bindings[bindingKey{tag, port}]
	if b == nil || !b.claimed || b.drain != nil || drain == nil || g.state != "starting" {
		return fmt.Errorf("cannot attach drain to %q port %d", tag, port)
	}
	b.drain = drain
	return nil
}

func (g *Generation) Ready() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state != "starting" {
		return fmt.Errorf("invalid generation state %q", g.state)
	}
	for key, b := range g.bindings {
		if !b.claimed || b.drain == nil {
			return fmt.Errorf("handoff binding for %q port %d was not started", key.tag, key.port)
		}
	}
	for key, b := range g.udp {
		if b.status == nil {
			return fmt.Errorf("handoff UDP binding for %q port %d was not started", key.tag, key.port)
		}
		if !b.status().Known {
			return fmt.Errorf("handoff UDP transport for %q port %d is not ready", key.tag, key.port)
		}
	}
	g.state = "paused"
	return nil
}

func (g *Generation) Command(command string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch command {
	case "status":
		return g.state, nil
	case "pause", "resume":
		if g.state != "active" && g.state != "paused" {
			return g.state, fmt.Errorf("cannot %s admission in %s", command, g.state)
		}
		if (command == "pause" && g.state == "paused") || (command == "resume" && g.state == "active") {
			return g.state, nil
		}
		if command == "pause" {
			for _, b := range g.bindings {
				if b.xhttp != nil && !b.xhttp.HandoffPeerLinked() {
					return g.state, errors.New("cannot pause before linking every XHTTP inbound")
				}
			}
		}
		for _, b := range g.bindings {
			if b.xhttp != nil {
				if err := b.xhttp.SetHandoffAdmission(command == "resume"); err != nil {
					g.state = "uncertain"
					return g.state, err
				}
			}
		}
		for _, b := range g.bindings {
			var err error
			if command == "pause" {
				err = b.listener.Pause()
			} else {
				err = b.listener.Resume()
			}
			if err != nil {
				g.state = "uncertain"
				return g.state, err
			}
		}
		if command == "pause" {
			g.state = "paused"
		} else {
			g.state = "active"
		}
	case "drain":
		if g.state == "draining" || g.state == "drained" {
			return g.state, nil
		}
		if g.state != "paused" {
			return g.state, fmt.Errorf("cannot drain a generation in %s", g.state)
		}
		if len(g.udp) != 0 {
			return g.state, errors.New("cannot prove UDP ingress and handshake retirement; retaining the old generation")
		}
		g.state = "draining"
		drain := make([]func() error, 0, len(g.bindings))
		for _, b := range g.bindings {
			drain = append(drain, b.drain)
		}
		go g.finishDrain(drain)
	case "unlink":
		if g.state != "active" {
			return g.state, errors.New("only an active generation can unlink its retired peer")
		}
		for _, b := range g.bindings {
			if b.xhttp != nil {
				if err := b.xhttp.UnlinkHandoffPeer(); err != nil {
					return g.state, err
				}
			}
		}
	default:
		dir, link := strings.CutPrefix(command, "link ")
		if !link || (g.state != "paused" && g.state != "active") {
			return g.state, errors.New("unknown or unavailable generation command")
		}
		if dir == filepath.Dir(g.control) {
			return g.state, errors.New("cannot link a generation to itself")
		}
		if err := PrivateSocketDirectory(dir); err != nil {
			return g.state, err
		}
		for key, b := range g.bindings {
			if b.xhttp != nil {
				if err := b.xhttp.LinkHandoffPeer(filepath.Join(dir, peerName(key))); err != nil {
					return g.state, err
				}
			}
		}
	}
	return g.state, nil
}

func (g *Generation) finishDrain(callbacks []func() error) {
	var group sync.WaitGroup
	failures := make(chan error, len(callbacks))
	for _, callback := range callbacks {
		group.Go(func() {
			if err := callback(); err != nil {
				failures <- err
			}
		})
	}
	group.Wait()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == "draining" {
		if len(failures) == 0 {
			g.state = "drained"
		} else {
			g.state = "drain_failed"
		}
	}
}

func (g *Generation) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state = "closed"
	for _, b := range g.bindings {
		b.listener.Close()
	}
	for _, b := range g.udp {
		b.packet.Close()
	}
}
