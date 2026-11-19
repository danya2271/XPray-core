package handoff

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
)

// TCPBinding identifies a listener descriptor passed in by the process
// supervisor. The descriptor has the final bind address and socket options.
type TCPBinding struct {
	Tag  string `json:"tag"`
	Port int    `json:"port"`
	FD   int    `json:"fd"`
}

type Manifest struct {
	Control string       `json:"control"`
	TCP     []TCPBinding `json:"tcp"`
}

type bindingKey struct {
	tag  string
	port int
}

type binding struct {
	listener *TCPListener
	claimed  bool
	drain    func()
}

type Generation struct {
	mu       sync.Mutex
	state    string
	bindings map[bindingKey]*binding
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
	if len(manifest.TCP) == 0 || len(manifest.TCP) > 128 {
		return nil, errors.New("handoff requires 1–128 TCP bindings")
	}
	seen := make(map[int]bool, len(manifest.TCP))
	g := &Generation{state: "starting", bindings: make(map[bindingKey]*binding, len(manifest.TCP))}
	for _, spec := range manifest.TCP {
		key := bindingKey{spec.Tag, spec.Port}
		if spec.Tag == "" || spec.Port < 1 || spec.Port > 65535 || spec.FD < 3 || seen[spec.FD] || g.bindings[key] != nil {
			g.Close()
			return nil, fmt.Errorf("invalid or duplicate handoff binding for %q port %d", spec.Tag, spec.Port)
		}
		seen[spec.FD] = true
		file := os.NewFile(uintptr(spec.FD), "xray-inherited-listener")
		listener, err := ImportTCPListener(file)
		file.Close()
		if err != nil {
			g.Close()
			return nil, fmt.Errorf("importing handoff socket for %q: %w", spec.Tag, err)
		}
		g.bindings[key] = &binding{listener: listener}
	}
	return g, nil
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

func (g *Generation) AttachDrain(tag string, port int, drain func()) error {
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
		g.state = "draining"
		drain := make([]func(), 0, len(g.bindings))
		for _, b := range g.bindings {
			drain = append(drain, b.drain)
		}
		go g.finishDrain(drain)
	default:
		return g.state, errors.New("unknown generation command")
	}
	return g.state, nil
}

func (g *Generation) finishDrain(callbacks []func()) {
	var group sync.WaitGroup
	for _, callback := range callbacks {
		group.Go(callback)
	}
	group.Wait()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == "draining" {
		g.state = "drained"
	}
}

func (g *Generation) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state = "closed"
	for _, b := range g.bindings {
		b.listener.Close()
	}
}
