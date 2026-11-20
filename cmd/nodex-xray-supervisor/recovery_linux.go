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

func adopt(ctx context.Context, root string, saved generationStatus) (*generation, error) {
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
		pid: saved.PID, startTicks: ticks, control: handoff.ControlSocket(filepath.Join(dir, "ctl.sock")), done: make(chan struct{}),
		signal: func(sig syscall.Signal) error {
			pidLock.Lock()
			defer pidLock.Unlock()
			if fd < 0 {
				return os.ErrProcessDone
			}
			return unix.PidfdSendSignal(fd, unix.Signal(sig), nil, 0)
		}}
	if err := expect(ctx, g, "status", "active"); err != nil {
		return nil, err
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
	if err := json.Unmarshal(data, &saved); err != nil || saved.Phase != "active" ||
		saved.Uncertain || saved.Other != nil || saved.Active == nil || saved.Sequence == 0 {
		return nil, errors.New("only a confirmed single active TCP generation can be recovered")
	}
	boot, err := bootID()
	if err != nil || boot != saved.BootID || boot == "" {
		return nil, errors.New("saved generation belongs to another boot")
	}
	config, err := readConfig(r)
	if err != nil {
		return nil, err
	}
	if configDigest(config) != saved.Active.ConfigSHA256 {
		return nil, errors.New("recovery config must match the active generation's snapshot")
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
	keep := false
	defer func() {
		if !keep {
			// Never signal a live child when recovery validation fails.
			for _, l := range s.listeners {
				l.Close()
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
	if len(imported)+len(unixBindings) != len(bindings) {
		return nil, errors.New("recovered listener set differs from the config")
	}
	used := make([]int, 0, len(bindings))
	for _, b := range bindings {
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
	// Validate all descriptors and journal before adopting process control.
	// Recovery does not alter admission or execute a probe that could force
	// a rollback on an already running generation.
	s.active, err = adopt(timeout, root, *saved.Active)
	if err != nil {
		return nil, err
	}
	keep = true
	return s, nil
}
