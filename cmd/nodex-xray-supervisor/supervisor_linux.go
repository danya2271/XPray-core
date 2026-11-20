//go:build linux && !android

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/xtls/xray-core/transport/internet/handoff"
)

const (
	operationLimit = 45 * time.Second
	configLimit    = 16 << 20
)

type release struct {
	Binary string `json:"binary"`
	Config string `json:"config"`
}

type generation struct {
	id         string
	dir        string
	hash       string
	configHash string
	control    handoff.ControlSocket
	pid        int
	startTicks uint64
	signal     func(syscall.Signal) error
	done       chan struct{}
	waitErr    error // Written before done closes.
}

type generationStatus struct {
	ID           string `json:"id"`
	PID          int    `json:"pid"`
	SHA256       string `json:"sha256"`
	ConfigSHA256 string `json:"config_sha256"`
	StartTicks   uint64 `json:"start_ticks"`
	Running      bool   `json:"running"`
}

func (g *generation) status() *generationStatus {
	if g == nil {
		return nil
	}
	return &generationStatus{ID: g.id, PID: g.pid, SHA256: g.hash, ConfigSHA256: g.configHash, StartTicks: g.startTicks, Running: g.running()}
}

func (g *generation) running() bool {
	select {
	case <-g.done:
		return false
	default:
		return true
	}
}

func (g *generation) stop() {
	if g.running() {
		_ = g.signal(syscall.SIGTERM)
	}
	select {
	case <-g.done:
	case <-time.After(3 * time.Second):
		_ = g.signal(syscall.SIGKILL)
		<-g.done
	}
}

type supervisor struct {
	ctx            context.Context
	root, probe    string
	boot           string
	bindings       []binding
	listeners      []listener
	xhttp          bool
	seq            uint64
	wg             sync.WaitGroup
	mu             sync.Mutex
	active, other  *generation
	phase, lastErr string
}

type status struct {
	Phase     string            `json:"phase"`
	Sequence  uint64            `json:"sequence"`
	BootID    string            `json:"boot_id"`
	Active    *generationStatus `json:"last_confirmed_active,omitempty"`
	Other     *generationStatus `json:"other,omitempty"`
	Uncertain bool              `json:"admission_uncertain,omitempty"`
	Error     string            `json:"error,omitempty"`
}

func (s *supervisor) status() status {
	s.mu.Lock()
	defer s.mu.Unlock()
	phase := s.phase
	if phase == "active" && (s.active == nil || !s.active.running()) {
		phase = "active-exited"
	}
	return s.snapshot(phase)
}

func (s *supervisor) snapshot(phase string) status {
	return status{Phase: phase, Sequence: s.seq, BootID: s.boot, Active: s.active.status(), Other: s.other.status(),
		Uncertain: phase == "switching" || phase == "admission-unknown", Error: s.lastErr}
}

func (s *supervisor) record(phase string, active, other *generation, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active, s.other, s.phase, s.lastErr = active, other, phase, ""
	if err != nil {
		s.lastErr = err.Error()
	}
	if err := saveState(s.root, s.snapshot(phase)); err != nil {
		if phase != "admission-unknown" {
			s.phase = "blocked"
		}
		s.lastErr = "could not persist supervisor state: " + err.Error()
		return err
	}
	return nil
}

func saveState(root string, snapshot status) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(root, ".state.new"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), filepath.Join(root, "state.json")); err != nil {
		return err
	}
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func readConfig(r release) ([]byte, error) {
	for _, path := range []string{r.Binary, r.Config} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return nil, errors.New("binary and config require clean absolute paths")
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("release path is not a regular file: %s", path)
		}
	}
	f, err := os.Open(r.Config)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, configLimit+1))
	if len(data) > configLimit {
		return nil, errors.New("generation config exceeds size limit")
	}
	return data, err
}

func newSupervisor(ctx context.Context, root, probe string, r release) (*supervisor, error) {
	config, err := readConfig(r)
	if err != nil {
		return nil, err
	}
	bindings, err := configBindings(config)
	if err != nil {
		return nil, err
	}
	boot, err := bootID()
	if err != nil {
		return nil, err
	}
	s := &supervisor{ctx: ctx, root: root, probe: probe, bindings: bindings, boot: boot, phase: "starting"}
	for _, b := range bindings {
		l, err := listen(b)
		if err != nil {
			s.close()
			return nil, err
		}
		s.listeners = append(s.listeners, l)
		s.xhttp = s.xhttp || b.transport == "xhttp"
	}
	ctx, cancel := context.WithTimeout(ctx, operationLimit)
	defer cancel()
	g, err := s.start(ctx, r, config)
	if err != nil {
		s.close()
		return nil, err
	}
	s.active = g
	if err := expect(ctx, g, "resume", "active"); err != nil {
		s.close()
		return nil, err
	}
	if err := s.verify(ctx, g); err != nil {
		s.close()
		return nil, err
	}
	if err := s.record("active", g, nil, nil); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

func (s *supervisor) start(ctx context.Context, r release, config []byte) (*generation, error) {
	s.mu.Lock()
	s.seq++ // Only the bootstrap or a single admitted upgrade can call start.
	id := "g" + strconv.FormatUint(s.seq, 10)
	s.mu.Unlock()
	dir := filepath.Join(s.root, id)
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	started := false
	defer func() {
		if !started {
			_ = os.RemoveAll(dir)
		}
	}()
	binary := filepath.Join(dir, "xray")
	hash, err := snapshotBinary(r.Binary, binary)
	if err != nil {
		return nil, err
	}
	manifest := handoff.Manifest{Control: filepath.Join(dir, "ctl.sock")}
	files := make([]*os.File, 0, len(s.listeners))
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	for i, b := range s.bindings {
		f, err := s.listeners[i].File()
		if err != nil {
			return nil, err
		}
		files = append(files, f)
		if b.port == 0 {
			manifest.Unix = append(manifest.Unix, handoff.UnixBinding{Tag: b.tag, Path: b.address, FD: 3 + i})
		} else {
			manifest.TCP = append(manifest.TCP, handoff.TCPBinding{Tag: b.tag, Port: b.port, FD: 3 + i})
		}
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, data, 0600); err != nil {
		return nil, err
	}
	cmd := exec.Command(binary, "run", "-format=json", "-config", "stdin:", "-handoff-manifest", manifestPath)
	cmd.Stdin, cmd.ExtraFiles = bytes.NewReader(config), files
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	g := &generation{id: id, dir: dir, hash: hash, configHash: configDigest(config),
		pid: cmd.Process.Pid, signal: func(sig syscall.Signal) error { return cmd.Process.Signal(sig) },
		control: handoff.ControlSocket(manifest.Control), done: make(chan struct{})}
	go func() {
		g.waitErr = cmd.Wait()
		close(g.done)
	}()
	g.startTicks, err = processStart(g.pid)
	if err != nil {
		g.stop()
		return nil, err
	}
	ready := time.NewTicker(25 * time.Millisecond)
	defer ready.Stop()
	for {
		if err := expect(ctx, g, "status", "paused"); err == nil {
			started = true
			return g, nil
		}
		select {
		case <-g.done:
			return nil, fmt.Errorf("generation %s exited before readiness: %v", id, g.waitErr)
		case <-ctx.Done():
			g.stop() // No admission command was issued for this candidate.
			return nil, fmt.Errorf("generation %s did not become ready: %w", id, ctx.Err())
		case <-ready.C:
		}
	}
}

func configDigest(config []byte) string { return fmt.Sprintf("%x", sha256.Sum256(config)) }

func snapshotBinary(source, destination string) (string, error) {
	src, err := os.Open(source)
	if err != nil {
		return "", err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > 256<<20 {
		return "", errors.New("candidate binary must be a nonempty regular file of at most 256 MiB")
	}
	dest, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0500)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(dest, hash), io.LimitReader(src, (256<<20)+1))
	syncErr, closeErr := dest.Sync(), dest.Close()
	if n != info.Size() {
		copyErr = errors.Join(copyErr, errors.New("candidate size changed while copying"))
	}
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		os.Remove(destination)
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func expect(ctx context.Context, g *generation, op, state string) error {
	response, err := g.control.Command(ctx, op)
	if err != nil {
		return fmt.Errorf("%s %s: %w", g.id, op, err)
	}
	if response.State != state || response.UDPManaged {
		return fmt.Errorf("%s %s: expected TCP-only state %q, got %q", g.id, op, state, response.State)
	}
	return nil
}

func (s *supervisor) verify(ctx context.Context, g *generation) error {
	// The trusted probe must create fresh work through every public inbound
	// and verify candidate identity (see the operator contract in the docs).
	cmd := exec.CommandContext(ctx, s.probe, g.id)
	cmd.Env = append(os.Environ(),
		"NODEX_CANDIDATE_PID="+strconv.Itoa(g.pid),
		"NODEX_CANDIDATE_SHA256="+g.hash)
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("new-connection probe failed for %s: %w", g.id, err)
	}
	return expect(ctx, g, "status", "active")
}

func (s *supervisor) upgrade(r release) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.phase != "active" || !s.active.running() {
		return fmt.Errorf("upgrade unavailable in state %s", s.phase)
	}
	old := s.active
	s.phase, s.lastErr = "starting", ""
	// A replacement supervisor must not read a stable journal while this
	// operation is already starting a second process.
	if err := saveState(s.root, s.snapshot(s.phase)); err != nil {
		s.phase, s.lastErr = "blocked", "could not persist upgrade start: "+err.Error()
		return err
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runUpgrade(old, r)
	}()
	return nil
}

func (s *supervisor) runUpgrade(old *generation, r release) {
	ctx, cancel := context.WithTimeout(s.ctx, operationLimit)
	defer cancel()
	config, err := readConfig(r)
	if err != nil {
		s.record("active", old, nil, err)
		return
	}
	bindings, err := configBindings(config)
	if err != nil || !slices.Equal(s.bindings, bindings) {
		s.record("active", old, nil, errors.Join(errors.New("candidate must keep the same managed listeners"), err))
		return
	}
	candidate, err := s.start(ctx, r, config)
	if err != nil {
		s.record("active", old, nil, err)
		return
	}
	if err := s.record("switching", old, candidate, nil); err != nil {
		return // Both remain reachable; no admission change has been issued.
	}
	if s.xhttp {
		if err := handoff.LinkXHTTP(ctx, old.control, candidate.control); err != nil {
			// A failed reply can mean some inbounds were already linked.
			// Do not remove either side while its peer state is uncertain.
			s.record("blocked", old, candidate, err)
			return
		}
	}
	probeCalled := false
	result, err := handoff.Switch(ctx, old.control, candidate.control, func(ctx context.Context) error {
		probeCalled = true
		return s.verify(ctx, candidate)
	})
	switch result.Active {
	case "candidate":
		s.retire(candidate, old, nil)
	case "previous":
		if !result.RolledBack && s.xhttp {
			// No candidate admission, but peers can still be partly linked.
			s.record("blocked", old, candidate, err)
			return
		}
		if s.xhttp && !probeCalled {
			// Candidate resume may have failed mid-command. Do not try to
			// retire it if the XHTTP peer relationship was never confirmed.
			s.record("blocked", old, candidate, err)
			return
		}
		s.retire(old, candidate, err)
	default:
		s.record("admission-unknown", old, candidate, err)
	}
}

func (s *supervisor) retire(active, retiring *generation, switchErr error) {
	if err := s.record("draining", active, retiring, switchErr); err != nil {
		return
	}
	if err := expect(s.ctx, retiring, "drain", "draining"); err != nil {
		// The command could have applied even if its reply was lost.
		if _, statusErr := drainStatus(s.ctx, retiring); statusErr != nil {
			s.record("blocked", active, retiring, errors.Join(switchErr, err, statusErr))
			return
		}
	}
	check := time.NewTicker(250 * time.Millisecond)
	defer check.Stop()
	for s.ctx.Err() == nil {
		drained, err := drainStatus(s.ctx, retiring)
		if err != nil || !active.running() {
			s.record("blocked", active, retiring, errors.Join(switchErr, err, errors.New("cannot verify drain or active generation")))
			return
		}
		if drained {
			if s.xhttp {
				if err := expect(s.ctx, active, "unlink", "active"); err != nil {
					s.record("blocked", active, retiring, errors.Join(switchErr, err))
					return
				}
			}
			retiring.stop()
			s.record("active", active, nil, errors.Join(switchErr, os.RemoveAll(retiring.dir)))
			return
		}
		select {
		case <-s.ctx.Done():
		case <-check.C:
		}
	}
}

func drainStatus(ctx context.Context, g *generation) (bool, error) {
	r, err := g.control.Command(ctx, "status")
	if err != nil {
		return false, err
	}
	if r.UDPManaged || (r.State != "draining" && r.State != "drained") {
		return false, fmt.Errorf("%s: unexpected drain state %q (UDP managed: %t)", g.id, r.State, r.UDPManaged)
	}
	return r.State == "drained", nil
}

// close requires a cancelled service context and no more API requests;
// newSupervisor can also call it before the upgrade API is exposed.
func (s *supervisor) close() {
	// An HTTP handler may already have passed the context check in upgrade.
	// Synchronize with its Add before Wait; later calls see cancellation.
	s.mu.Lock()
	s.mu.Unlock()
	s.wg.Wait()
	for _, g := range []*generation{s.active, s.other} {
		if g != nil {
			g.stop()
		}
	}
	for _, l := range s.listeners {
		_ = l.Close()
	}
}
