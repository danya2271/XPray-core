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
	"net"
	"net/http"
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
	slot       byte
	udp        bool
}

type generationStatus struct {
	ID           string `json:"id"`
	PID          int    `json:"pid"`
	SHA256       string `json:"sha256"`
	ConfigSHA256 string `json:"config_sha256"`
	StartTicks   uint64 `json:"start_ticks"`
	Running      bool   `json:"running"`
	Slot         byte   `json:"slot,omitempty"`
	UDP          bool   `json:"udp,omitempty"`
}

func (g *generation) status() *generationStatus {
	if g == nil {
		return nil
	}
	return &generationStatus{ID: g.id, PID: g.pid, SHA256: g.hash, ConfigSHA256: g.configHash, StartTicks: g.startTicks, Running: g.running(), Slot: g.slot, UDP: g.udp}
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
	broker         *brokerClient
	brokerState    *brokerStatus
	brokerStop     func()
	api            *apiGateway
	apiBinding     *apiBinding
	metrics        *http.Server
	brokerBackup   *directBrokerBackup
}

type status struct {
	Phase               string                 `json:"phase"`
	Sequence            uint64                 `json:"sequence"`
	BootID              string                 `json:"boot_id"`
	Active              *generationStatus      `json:"last_confirmed_active,omitempty"`
	Other               *generationStatus      `json:"other,omitempty"`
	Uncertain           bool                   `json:"admission_uncertain,omitempty"`
	Error               string                 `json:"error,omitempty"`
	Broker              *brokerStatus          `json:"broker,omitempty"`
	BrokerRecoveryReady bool                   `json:"broker_recovery_ready,omitempty"`
	BrokerControl       *brokerControlIdentity `json:"broker_control,omitempty"`
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
	var control *brokerControlIdentity
	if s.broker != nil && s.broker.control != nil {
		identity := s.broker.control.identity()
		control = &identity
	}
	return status{Phase: phase, Sequence: s.seq, BootID: s.boot, Active: s.active.status(), Other: s.other.status(), Broker: s.brokerState,
		BrokerControl:       control,
		BrokerRecoveryReady: phase == "active" && s.brokerBackup != nil,
		Uncertain:           phase == "switching" || phase == "admission-unknown", Error: s.lastErr}
}

func (s *supervisor) record(phase string, active, other *generation, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active, s.other, s.phase, s.lastErr = active, other, phase, ""
	if s.api != nil && active != nil && (phase == "active" || phase == "draining" || phase == "retiring") {
		s.api.mu.Lock()
		s.api.active = active.id
		s.api.mu.Unlock()
	}
	if err != nil {
		s.lastErr = err.Error()
	}
	if phase == "active" {
		if checkpointErr := s.checkpointDirectBrokerLocked(); checkpointErr != nil {
			s.lastErr = errors.Join(err, fmt.Errorf("broker recovery checkpoint unavailable: %w", checkpointErr)).Error()
		}
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
		if b.transport == "hysteria" {
			s.listeners = append(s.listeners, nil)
			continue
		}
		l, err := listen(b)
		if err != nil {
			s.close()
			return nil, err
		}
		s.listeners = append(s.listeners, l)
		s.xhttp = s.xhttp || b.transport == "xhttp"
	}
	if err := s.startBroker(config); err != nil {
		s.close()
		return nil, err
	}
	_, api, err := managedAPIConfig(config, "")
	if err != nil {
		s.close()
		return nil, err
	}
	if api != nil {
		s.apiBinding = api
		s.api, err = newAPIGateway(root)
		if err == nil {
			err = s.api.serve(api.Address)
		}
		if err != nil {
			s.close()
			return nil, err
		}
		if err := s.serveMetrics(api.Metrics); err != nil {
			s.close()
			return nil, err
		}
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
	s.watchBroker()
	return s, nil
}

func (s *supervisor) start(ctx context.Context, r release, config []byte) (*generation, error) {
	s.mu.Lock()
	s.seq++ // Only the bootstrap or a single admitted upgrade can call start.
	id := "g" + strconv.FormatUint(s.seq, 10)
	sequence := s.seq
	s.mu.Unlock()
	if s.broker != nil && sequence > 256 {
		return nil, errors.New("UDP CID slot capacity reached; refusing to reuse a routing identity")
	}
	slot := byte(sequence - 1)
	dir := filepath.Join(s.root, id)
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	started := false
	defer func() {
		if !started {
			if s.broker != nil && sequence != 1 {
				cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				for _, binding := range s.bindings {
					if binding.transport == "hysteria" {
						_, _, _ = s.broker.command(cleanup, brokerRequest{Operation: "abandon", Tag: binding.tag, Slot: slot})
					}
				}
				cancel()
			}
			if s.api != nil {
				s.api.mu.Lock()
				if connection := s.api.clients[id]; connection != nil {
					connection.Close()
					delete(s.api.clients, id)
				}
				s.api.mu.Unlock()
			}
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
		if b.transport == "hysteria" {
			response, inherited, err := s.broker.command(ctx, brokerRequest{Operation: "add", Tag: b.tag, Slot: slot})
			if err != nil {
				return nil, err
			}
			if !response.Ready || response.Binding == nil || len(inherited) != 1 && len(inherited) != 5 {
				for _, file := range inherited {
					file.Close()
				}
				return nil, errors.New("invalid broker descriptor export")
			}
			base := 3 + len(files)
			files = append(files, inherited...)
			udp := handoff.UDPBinding{Tag: b.tag, Address: b.address, Port: b.port, FD: base, Key: response.Binding.Key, Slot: slot}
			udp.PacketDestination = !b.native && net.ParseIP(b.address).IsUnspecified()
			if b.native {
				if len(inherited) != 5 {
					return nil, errors.New("native generation needs public TX and receive ring")
				}
				udp.PacketBatch = true
				udp.RingFDs = []int{base + 1, base + 2, base + 3}
				udp.TransmitFD = base + 4
				udp.DirectReceive = sequence == 1
			}
			manifest.UDP = append(manifest.UDP, udp)
			continue
		}
		f, err := s.listeners[i].File()
		if err != nil {
			return nil, err
		}
		fd := 3 + len(files)
		files = append(files, f)
		if b.port == 0 {
			manifest.Unix = append(manifest.Unix, handoff.UnixBinding{Tag: b.tag, Path: b.address, FD: fd})
		} else {
			manifest.TCP = append(manifest.TCP, handoff.TCPBinding{Tag: b.tag, Port: b.port, FD: fd})
		}
	}
	gateRead, gateWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer gateRead.Close()
	defer gateWrite.Close()
	manifest.StartupFD = 3 + len(files)
	files = append(files, gateRead)
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, data, 0600); err != nil {
		return nil, err
	}
	if err := writePrivateSnapshot(filepath.Join(dir, "config.json"), config); err != nil {
		return nil, err
	}
	launchConfig, _, err := managedAPIConfig(config, filepath.Join(dir, "api.sock"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary, "run", "-format=json", "-config", "stdin:", "-handoff-manifest", manifestPath)
	cmd.Stdin, cmd.ExtraFiles = bytes.NewReader(launchConfig), files
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	g := &generation{id: id, dir: dir, hash: hash, configHash: configDigest(config),
		slot: slot, udp: len(manifest.UDP) != 0,
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
	// The child is blocked before initializing Xray. A lost supervisor closes
	// the pipe, so an unjournaled process can never become an orphan receiver.
	s.mu.Lock()
	previous := s.active
	s.mu.Unlock()
	if err := s.record("preparing", previous, g, nil); err != nil {
		g.stop()
		return nil, err
	}
	if _, err := gateWrite.Write([]byte{1}); err != nil {
		g.stop()
		return nil, err
	}
	gateWrite.Close()
	ready := time.NewTicker(25 * time.Millisecond)
	defer ready.Stop()
	for {
		if err := expect(ctx, g, "status", "paused"); err == nil {
			if s.api != nil {
				tags := make([]string, 0, len(s.bindings))
				for _, binding := range s.bindings {
					tags = append(tags, binding.tag)
				}
				if err := s.api.bootstrap(ctx, g, tags); err != nil {
					g.stop()
					return nil, err
				}
			}
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

func writePrivateSnapshot(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	syncErr, closeErr := f.Sync(), f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

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
	if response.State != state || response.UDPManaged != g.udp {
		return fmt.Errorf("%s %s: expected state %q (UDP %t), got %q (UDP %t)", g.id, op, state, g.udp, response.State, response.UDPManaged)
	}
	return nil
}

func (s *supervisor) verify(ctx context.Context, g *generation) error {
	// The trusted probe must create fresh work through every public inbound
	// and verify candidate identity (see the operator contract in the docs).
	cmd := exec.CommandContext(ctx, s.probe, g.id)
	cmd.Env = append(os.Environ(),
		"NODEX_CANDIDATE_PID="+strconv.Itoa(g.pid),
		"NODEX_CANDIDATE_SHA256="+g.hash,
		"NODEX_CANDIDATE_CONFIG="+filepath.Join(g.dir, "config.json"),
		"NODEX_CANDIDATE_API="+filepath.Join(g.dir, "api.sock"))
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("new-connection probe failed for %s: %w", g.id, err)
	}
	if err := expect(ctx, g, "status", "active"); err != nil {
		return err
	}
	if s.api != nil {
		s.api.mu.Lock()
		s.api.active = g.id
		s.api.mu.Unlock()
	}
	return nil
}

func (s *supervisor) upgrade(r release) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.phase != "active" || !s.active.running() {
		return fmt.Errorf("upgrade unavailable in state %s", s.phase)
	}
	if s.broker != nil {
		dead, err := brokerDead(s.broker.pid, s.broker.startTicks)
		if err != nil || dead {
			return errors.Join(err, errors.New("UDP broker unavailable; wait for verified recovery before upgrading"))
		}
	}
	old := s.active
	// A checkpoint must never be used after a candidate may have received work.
	s.clearBrokerBackup()
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
	_, oldAPI, oldAPIErr := managedAPIConfig(config, "")
	if oldAPIErr != nil || (oldAPI != nil) != (s.api != nil) || (oldAPI != nil && s.apiBinding != nil && *oldAPI != *s.apiBinding) {
		s.record("active", old, nil, errors.New("candidate must preserve the managed API binding"))
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
	if s.broker != nil {
		if err := s.adoptUDPReceive(ctx, old); err != nil {
			s.record("admission-unknown", old, candidate, err)
			return
		}
	}
	probeCalled := false
	verify := func(ctx context.Context) error {
		probeCalled = true
		return s.verify(ctx, candidate)
	}
	var result handoff.SwitchResult
	if s.broker != nil {
		result, err = handoff.SwitchWithIngress(ctx, old.control, candidate.control, func(useCandidate bool) error {
			if useCandidate {
				return s.activateUDP(ctx, candidate)
			}
			return s.activateUDP(ctx, old)
		}, verify)
	} else {
		result, err = handoff.Switch(ctx, old.control, candidate.control, verify)
	}
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
	if s.broker != nil {
		if err := s.drainUDP(active, retiring); err != nil {
			s.record("blocked", active, retiring, err)
			return
		}
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
			if s.api != nil {
				ctx, cancel := apiContext(s.ctx)
				err := s.api.finalize(ctx, retiring.id)
				cancel()
				if err != nil {
					s.record("blocked", active, retiring, err)
					return
				}
			}
			if s.broker != nil {
				if err := s.returnUDPDirect(active, retiring); err != nil {
					s.record("blocked", active, retiring, err)
					return
				}
			}
			if s.xhttp {
				if err := expect(s.ctx, active, "unlink", "active"); err != nil {
					s.record("blocked", active, retiring, errors.Join(switchErr, err))
					return
				}
			}
			if err := s.record("retiring", active, retiring, switchErr); err != nil {
				return
			}
			retiring.stop()
			err := os.RemoveAll(retiring.dir)
			s.record("active", active, nil, errors.Join(switchErr, err))
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
	if r.UDPManaged != g.udp || (r.State != "draining" && r.State != "drained") {
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
		if l != nil {
			_ = l.Close()
		}
	}
	if s.brokerStop != nil {
		s.brokerStop()
	}
	s.clearBrokerBackup()
	if s.api != nil {
		s.api.close()
	}
	if s.metrics != nil {
		s.metrics.Close()
	}
}
