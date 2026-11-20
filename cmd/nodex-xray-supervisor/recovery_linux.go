//go:build linux && !android

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/xtls/xray-core/transport/internet/handoff"
	"golang.org/x/sys/unix"
)

func bootID() (string, error) {
	value, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return strings.TrimSpace(string(value)), err
}

func processStart(pid int) (uint64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	// comm may contain spaces and ')'. Remaining fields begin at field 3;
	// starttime is field 22.
	closing := strings.LastIndexByte(string(data), ')')
	if closing < 0 {
		return 0, errors.New("invalid process identity")
	}
	fields := strings.Fields(string(data[closing+1:]))
	if len(fields) < 20 {
		return 0, errors.New("invalid process identity fields")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

func claimRecovery(root string) (*os.File, error) {
	if err := handoff.PrivateSocketDirectory(root); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil, errors.New("runtime must be owned by the supervisor user")
	}
	fd, err := unix.Open(filepath.Join(root, ".lock"), unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "supervisor-recovery-lock")
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("another supervisor owns this runtime")
	}
	return lock, nil
}

func fileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	_, err = io.Copy(hash, io.LimitReader(f, (256<<20)+1))
	return fmt.Sprintf("%x", hash.Sum(nil)), err
}

func adopt(ctx context.Context, root string, saved generationStatus, states ...string) (*generation, error) {
	n, err := strconv.ParseUint(strings.TrimPrefix(saved.ID, "g"), 10, 64)
	if err != nil || saved.ID != "g"+strconv.FormatUint(n, 10) || n == 0 || saved.PID <= 1 {
		return nil, errors.New("invalid saved generation identity")
	}
	dir := filepath.Join(root, saved.ID)
	if err := handoff.PrivateSocketDirectory(dir); err != nil {
		return nil, err
	}
	// pidfd pins process identity for signals and exit observation; no kill
	// by numeric PID is used for an adopted process.
	fd, err := unix.PidfdOpen(saved.PID, 0)
	if err != nil {
		return nil, fmt.Errorf("pidfd unavailable: %w", err)
	}
	var pidLock sync.Mutex
	keep := false
	defer func() {
		if !keep {
			unix.Close(fd)
		}
	}()
	ticks, err := processStart(saved.PID)
	if err != nil || ticks == 0 || ticks != saved.StartTicks {
		return nil, errors.New("saved PID no longer identifies the generation")
	}
	executable, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", saved.PID))
	if err != nil || executable != filepath.Join(dir, "xray") {
		return nil, errors.New("generation executable does not match the private release")
	}
	hash, err := fileDigest(fmt.Sprintf("/proc/%d/exe", saved.PID))
	if err != nil || hash != saved.SHA256 {
		return nil, errors.New("generation binary hash changed")
	}
	g := &generation{id: saved.ID, dir: dir, hash: hash, configHash: saved.ConfigSHA256,
		slot: saved.Slot, udp: saved.UDP,
		pid: saved.PID, startTicks: ticks, control: handoff.ControlSocket(filepath.Join(dir, "ctl.sock")), done: make(chan struct{}),
		signal: func(sig syscall.Signal) error {
			pidLock.Lock()
			defer pidLock.Unlock()
			if fd < 0 {
				return os.ErrProcessDone
			}
			return unix.PidfdSendSignal(fd, unix.Signal(sig), nil, 0)
		}}
	if len(states) == 0 {
		states = []string{"active"}
	}
	response, err := g.control.Command(ctx, "status")
	if err != nil || !slices.Contains(states, response.State) || response.UDPManaged != saved.UDP {
		return nil, errors.Join(err, fmt.Errorf("unexpected recovered generation state %q", response.State))
	}
	if g.configHash == "" {
		return nil, errors.New("missing generation config identity")
	}
	if data, err := os.ReadFile(filepath.Join(dir, "config.json")); err != nil {
		return nil, fmt.Errorf("generation config snapshot unavailable: %w", err)
	} else if configDigest(data) != g.configHash {
		return nil, errors.New("generation config snapshot changed")
	}
	keep = true
	go func() {
		defer close(g.done)
		defer func() {
			pidLock.Lock()
			defer pidLock.Unlock()
			unix.Close(fd)
			fd = -1
		}()
		for {
			fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			if _, err := unix.Poll(fds, -1); err == unix.EINTR {
				continue
			}
			return
		}
	}()
	return g, nil
}

func recoverSupervisor(ctx context.Context, root, probe string, r release) (*supervisor, error) {
	data, err := os.ReadFile(filepath.Join(root, "state.json"))
	if err != nil {
		return nil, err
	}
	var saved status
	if err := json.Unmarshal(data, &saved); err != nil || saved.Active == nil || saved.Sequence == 0 ||
		!slices.Contains([]string{"active", "starting", "preparing", "switching", "draining", "retiring", "admission-unknown", "broker-recovering"}, saved.Phase) {
		return nil, errors.New("unsupported or incomplete supervisor recovery journal")
	}
	boot, err := bootID()
	if err != nil || boot != saved.BootID || boot == "" {
		return nil, errors.New("saved generation belongs to another boot")
	}
	config, err := readConfig(r)
	if err != nil {
		return nil, err
	}
	if configDigest(config) != saved.Active.ConfigSHA256 &&
		(saved.Other == nil || configDigest(config) != saved.Other.ConfigSHA256) {
		return nil, errors.New("recovery config must match a journaled generation's snapshot")
	}
	bindings, err := configBindings(config)
	if err != nil {
		return nil, err
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(saved.Active.ID, "g"), 10, 64)
	if err != nil || id == 0 || saved.Active.ID != "g"+strconv.FormatUint(id, 10) || id > saved.Sequence {
		return nil, errors.New("invalid saved generation sequence")
	}
	timeout, cancel := context.WithTimeout(ctx, operationLimit)
	defer cancel()
	// Both processes may be paused in the middle of a switch. Export from an
	// active or paused survivor; a draining server may have closed its copies.
	control := handoff.ControlSocket(filepath.Join(root, saved.Active.ID, "ctl.sock"))
	export, err := handoff.ExportListeners(timeout, control)
	if err != nil {
		return nil, err
	}
	defer export.Close()
	if export.PID != saved.Active.PID {
		return nil, errors.New("listener exporter is not the saved generation process")
	}
	s := &supervisor{ctx: ctx, root: root, probe: probe, bindings: bindings,
		seq: saved.Sequence, boot: boot, phase: "active", lastErr: saved.Error}
	if err := s.recoverBroker(timeout, saved.Broker); err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			// Never signal a live child when recovery validation fails.
			for _, l := range s.listeners {
				if l != nil {
					l.Close()
				}
			}
		}
	}()
	imported := make(map[string]handoff.TCPBinding)
	unixBindings := make(map[string]handoff.UnixBinding)
	for _, b := range export.Manifest.TCP {
		if _, duplicate := imported[b.Tag]; duplicate {
			return nil, errors.New("duplicate recovered TCP tag")
		}
		imported[b.Tag] = b
	}
	for _, b := range export.Manifest.Unix {
		if _, duplicate := unixBindings[b.Tag]; duplicate {
			return nil, errors.New("duplicate recovered Unix tag")
		}
		unixBindings[b.Tag] = b
	}
	tcpCount := 0
	for _, binding := range bindings {
		if binding.transport != "hysteria" {
			tcpCount++
		}
	}
	if len(imported)+len(unixBindings) != tcpCount {
		return nil, errors.New("recovered listener set differs from the config")
	}
	used := make([]int, 0, len(bindings))
	for _, b := range bindings {
		if b.transport == "hysteria" {
			if s.broker == nil {
				return nil, errors.New("UDP recovery requires the original persistent broker")
			}
			response, _, err := s.broker.command(timeout, brokerRequest{Operation: "status", Tag: b.tag})
			if err != nil || response.Binding == nil || response.Binding.Address != b.address || response.Binding.Port != b.port || response.Binding.Native != b.native {
				return nil, errors.Join(err, errors.New("recovered UDP broker binding mismatch"))
			}
			s.listeners = append(s.listeners, nil)
			continue
		}
		var fd int
		if b.port == 0 {
			entry, ok := unixBindings[b.tag]
			if !ok || entry.Path != b.address {
				return nil, errors.New("recovered Unix binding mismatch")
			}
			fd = entry.FD
		} else {
			entry, ok := imported[b.tag]
			if !ok || entry.Port != b.port {
				return nil, errors.New("recovered TCP binding mismatch")
			}
			fd = entry.FD
		}
		if fd < 3 || fd-3 >= len(export.Files) || slices.Contains(used, fd) {
			return nil, errors.New("invalid exported file index")
		}
		used = append(used, fd)
		l, err := net.FileListener(export.Files[fd-3])
		if err != nil {
			return nil, err
		}
		switch l := l.(type) {
		case *net.UnixListener:
			l.SetUnlinkOnClose(false) // Failed recovery must not remove the live origin.
			if b.port != 0 || l.Addr().String() != b.address {
				l.Close()
				return nil, errors.New("recovered Unix address mismatch")
			}
			s.listeners = append(s.listeners, l)
		case *net.TCPListener:
			address := l.Addr().(*net.TCPAddr)
			wanted := net.ParseIP(b.address)
			if b.port != address.Port || !(wanted.Equal(address.IP) || (wanted.IsUnspecified() && address.IP.IsUnspecified())) {
				l.Close()
				return nil, errors.New("recovered TCP address mismatch")
			}
			s.listeners = append(s.listeners, l)
		default:
			l.Close()
			return nil, errors.New("unexpected recovered socket")
		}
		s.xhttp = s.xhttp || b.transport == "xhttp"
	}
	// No signal or admission change is issued until every process identity and
	// descriptor has been validated. The journal determines the next operation.
	s.active, err = adopt(timeout, root, *saved.Active, "active", "paused")
	if err != nil {
		return nil, err
	}
	if saved.Other != nil {
		if saved.Other.ID == saved.Active.ID || saved.Other.PID == saved.Active.PID {
			return nil, errors.New("duplicate recovery generation")
		}
		otherSequence, sequenceErr := strconv.ParseUint(strings.TrimPrefix(saved.Other.ID, "g"), 10, 64)
		if sequenceErr != nil || otherSequence > saved.Sequence {
			return nil, errors.New("invalid other generation sequence")
		}
		s.other, err = adopt(timeout, root, *saved.Other, "active", "paused", "draining", "drained")
		if err != nil {
			// During preparation EOF on the launch pipe can terminate the
			// candidate; during retirement stopping it was already committed.
			_, identityErr := processStart(saved.Other.PID)
			if !slices.Contains([]string{"preparing", "retiring"}, saved.Phase) || !os.IsNotExist(identityErr) {
				return nil, err
			}
		}
	}
	if err := s.recoverAPI(timeout, config); err != nil {
		return nil, err
	}
	if err := s.reconcileRecovery(timeout, saved.Phase); err != nil {
		return nil, err
	}
	s.watchBroker()
	keep = true
	return s, nil
}

func (s *supervisor) recoverAPI(ctx context.Context, config []byte) error {
	_, api, err := managedAPIConfig(config, "")
	if err != nil || api == nil {
		return err
	}
	s.api, err = newAPIGateway(s.root)
	if err != nil {
		return err
	}
	for _, generation := range []*generation{s.active, s.other} {
		if generation == nil || s.api.journal.Finalized[generation.id] {
			continue
		}
		if err := s.api.connect(generation.id, filepath.Join(generation.dir, "api.sock")); err != nil {
			return err
		}
		if err := s.api.replay(ctx, s.api.clients[generation.id]); err != nil {
			return err
		}
	}
	s.api.active = s.active.id
	s.apiBinding = api
	if err := s.api.serve(api.Address); err != nil {
		return err
	}
	return s.serveMetrics(api.Metrics)
}

func (s *supervisor) reconcileRecovery(ctx context.Context, phase string) error {
	active, other := s.active, s.other
	a, err := active.control.Command(ctx, "status")
	if err != nil {
		return err
	}
	if other == nil {
		if a.State != "active" {
			return errors.New("single recovered generation is not active")
		}
		if phase == "broker-recovering" {
			for _, binding := range s.bindings {
				if binding.transport != "hysteria" {
					continue
				}
				if !binding.native || s.broker == nil {
					return errors.New("interrupted broker recovery requires native direct receive")
				}
				response, _, err := s.broker.command(ctx, brokerRequest{Operation: "status", Tag: binding.tag})
				if err != nil || !response.Direct || response.Current != active.slot {
					return errors.Join(err, errors.New("interrupted broker recovery did not retain direct receive"))
				}
			}
		}
		return s.record("active", active, nil, nil)
	}
	b, err := other.control.Command(ctx, "status")
	if err != nil {
		return err
	}
	var recoveryErr error
	if phase == "draining" || phase == "retiring" {
		if a.State != "active" || !slices.Contains([]string{"paused", "draining", "drained"}, b.State) {
			return errors.New("drain journal does not match live admission")
		}
	} else {
		if a.State == "paused" && b.State == "active" {
			if s.broker != nil {
				if err := s.adoptUDPReceive(ctx, active); err != nil {
					return err
				}
				if err := s.activateUDP(ctx, other); err != nil {
					return err
				}
			}
			if probeErr := s.verify(ctx, other); probeErr != nil {
				// B already accepted real work before the crash. Restore new
				// admission to A, but preserve both generations' established
				// work and drain B only after verified rollback.
				rollback, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				verifyPrevious := func(ctx context.Context) error { return s.verify(ctx, active) }
				var result handoff.SwitchResult
				var rollbackErr error
				if s.broker != nil {
					result, rollbackErr = handoff.SwitchWithIngress(rollback, other.control, active.control, func(usePrevious bool) error {
						if usePrevious {
							return s.activateUDP(rollback, active)
						}
						return s.activateUDP(rollback, other)
					}, verifyPrevious)
				} else {
					result, rollbackErr = handoff.Switch(rollback, other.control, active.control, verifyPrevious)
				}
				cancel()
				if result.Active != "candidate" || rollbackErr != nil {
					failure := errors.Join(probeErr, rollbackErr, errors.New("cannot verify previous generation recovery; retaining both"))
					return errors.Join(failure, s.record("admission-unknown", active, other, failure))
				}
				recoveryErr = fmt.Errorf("recovered candidate probe failed; restored previous admission: %w", probeErr)
			} else {
				active, other = other, active
			}
		} else if b.State == "paused" && (a.State == "active" || a.State == "paused") {
			if s.broker != nil {
				if err := s.adoptUDPReceive(ctx, active); err != nil {
					return err
				}
			}
			if s.xhttp {
				if err := handoff.LinkXHTTP(ctx, active.control, other.control); err != nil {
					// LinkXHTTP expects the previous side active. Restore only
					// after both links exist, so use the individual commands.
					if _, err := active.control.Command(ctx, "link "+other.dir); err != nil {
						return err
					}
					if _, err := other.control.Command(ctx, "link "+active.dir); err != nil {
						return err
					}
				}
			}
			if err := expect(ctx, active, "resume", "active"); err != nil {
				return err
			}
			if s.broker != nil {
				if err := s.activateUDP(ctx, active); err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("cannot reconcile admission: %s/%s", a.State, b.State)
		}
	}
	if err := s.record("draining", active, other, recoveryErr); err != nil {
		return err
	}
	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.retire(active, other, recoveryErr) }()
	return nil
}
