//go:build linux && !android

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Backups only: no additional UDP reader, reactor or packet copies.
type directBrokerBackup struct {
	owner  string
	config brokerConfig
	files  []*os.File // public sockets first, then four producer descriptors/binding
}

func (s *supervisor) clearBrokerBackup() {
	if s.brokerBackup != nil {
		for _, file := range s.brokerBackup.files {
			file.Close()
		}
		s.brokerBackup = nil
	}
}

// Called with s.mu held, before active becomes visible to status/upgrade.
func (s *supervisor) checkpointDirectBrokerLocked() error {
	s.clearBrokerBackup()
	if s.broker == nil || s.phase != "active" || s.other != nil || s.active == nil {
		return nil
	}
	for _, b := range s.bindings {
		if b.transport == "hysteria" && !b.native {
			return nil
		}
	}
	backup := &directBrokerBackup{owner: s.active.id, config: brokerConfig{Nonce: s.broker.nonce}}
	var public, channels []*os.File
	keep := false
	defer func() {
		if !keep {
			for _, file := range append(public, channels...) {
				file.Close()
			}
		}
	}()
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	for _, b := range s.bindings {
		if b.transport != "hysteria" {
			continue
		}
		response, files, err := s.broker.command(ctx, brokerRequest{Operation: "checkpoint-direct", Tag: b.tag, Slot: s.active.slot})
		if err != nil {
			return err
		}
		if len(files) != 5 || !response.Ready || !response.Direct || response.Binding == nil || response.Current != s.active.slot {
			for _, file := range files {
				file.Close()
			}
			return errors.New("invalid native direct broker checkpoint")
		}
		public = append(public, files[0])
		channels = append(channels, files[1:]...)
		backup.config.Bindings = append(backup.config.Bindings, *response.Binding)
		backup.config.Direct = append(backup.config.Direct, brokerDirectRestore{
			Tag: b.tag, Slot: s.active.slot, ReservedBefore: uint16(min(s.seq, 256)),
		})
	}
	if len(public) == 0 {
		return nil
	}
	backup.files = append(public, channels...)
	s.brokerBackup = backup
	keep = true
	return nil
}

func brokerDead(pid int, start uint64) (bool, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err == unix.ESRCH {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer unix.Close(fd)
	ticks, err := processStart(pid)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	// Never signal or trust a process that reused the saved numeric PID.
	if ticks != start {
		return true, nil
	}
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	_, err = unix.Poll(fds, 0)
	return fds[0].Revents&unix.POLLIN != 0, err
}

// Wait in the kernel, with no polling timer or repeated /proc reads while the
// broker lives. A cancellation pipe wakes Poll without closing/reusing its fd.
func waitBrokerExit(ctx context.Context, client brokerClient) error {
	fd, err := unix.PidfdOpen(client.pid, 0)
	if err == unix.ESRCH {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	ticks, err := processStart(client.pid)
	if os.IsNotExist(err) || err == nil && ticks != client.startTicks {
		return nil
	}
	if err != nil {
		return err
	}
	read, write, err := os.Pipe()
	if err != nil {
		return err
	}
	defer read.Close()
	defer write.Close()
	stop := context.AfterFunc(ctx, func() { _, _ = write.Write([]byte{1}) })
	defer stop()
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}, {Fd: int32(read.Fd()), Events: unix.POLLIN}}
	for {
		_, err := unix.Poll(fds, -1)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fds[0].Revents&unix.POLLIN != 0 {
			return nil
		}
		return errors.New("unexpected broker pidfd readiness")
	}
}

func (s *supervisor) watchBroker() {
	if s.broker == nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			s.mu.Lock()
			client := *s.broker
			s.mu.Unlock()
			if err := waitBrokerExit(s.ctx, client); err != nil {
				return
			}
			for {
				s.mu.Lock()
				phase := s.phase
				s.mu.Unlock()
				if phase == "blocked" || phase == "admission-unknown" {
					return
				}
				if phase == "active" {
					// If upgrade won the lock, continue observing. A failed
					// reconstruction records blocked and cannot retry.
					if err := s.replaceDirectBroker(); err == nil {
						break
					}
				}
				select {
				case <-s.ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}()
}

func (s *supervisor) replaceDirectBroker() error {
	s.mu.Lock()
	if s.phase != "active" {
		s.mu.Unlock()
		return errors.New("broker recovery cannot race a generation update")
	}
	backup, active := s.brokerBackup, s.active
	if backup == nil || active == nil || backup.owner != active.id || !active.running() || s.other != nil {
		err := errors.New("UDP broker died without a proven native direct checkpoint; retaining live generations")
		s.phase, s.lastErr = "blocked", err.Error()
		writeErr := saveState(s.root, s.snapshot("blocked"))
		s.mu.Unlock()
		return errors.Join(err, writeErr)
	}
	s.phase = "broker-recovering"
	if err := saveState(s.root, s.snapshot(s.phase)); err != nil {
		s.phase, s.lastErr = "blocked", err.Error()
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	dead, err := brokerDead(s.broker.pid, s.broker.startTicks)
	if err != nil || !dead {
		failure := errors.Join(err, errors.New("previous UDP producer death is not proven"))
		s.record("blocked", active, nil, failure)
		return failure
	}
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	if err := s.launchDirectBroker(ctx, backup); err != nil {
		s.record("blocked", active, nil, err)
		return err
	}
	if err := s.record("active", active, nil, nil); err != nil {
		return err
	}
	return nil
}

func (s *supervisor) launchDirectBroker(ctx context.Context, backup *directBrokerBackup) error {
	directory := filepath.Join(s.root, fmt.Sprintf("broker-recovery-%d", s.broker.pid))
	if err := os.Mkdir(directory, 0700); err != nil {
		return err
	}
	path := filepath.Join(directory, "manifest.json")
	data, err := json.Marshal(backup.config)
	if err != nil {
		return err
	}
	if err := writePrivateSnapshot(path, data); err != nil {
		return err
	}
	binary := filepath.Join(directory, "daemon")
	hash, err := snapshotBinary(filepath.Join(filepath.Dir(s.broker.path), "daemon"), binary)
	if err != nil {
		return err
	}
	if s.brokerState == nil || hash != s.brokerState.SHA256 {
		return errors.New("saved broker executable changed")
	}
	read, write, err := os.Pipe()
	if err != nil {
		return err
	}
	defer read.Close()
	defer write.Close()
	command := exec.Command(binary)
	command.Env = append(os.Environ(), "NODEX_INTERNAL_BROKER_MANIFEST="+path)
	command.ExtraFiles = append(append([]*os.File{}, backup.files...), read)
	command.Stdout, command.Stderr = os.Stderr, os.Stderr
	// The prior process is proven dead; release an adopted broker's pidfd.
	if s.brokerStop != nil {
		s.brokerStop()
		s.brokerStop = nil
	}
	if err := command.Start(); err != nil {
		return err
	}
	ticks, err := processStart(command.Process.Pid)
	if err != nil {
		command.Process.Kill()
		command.Wait()
		return err
	}
	go command.Wait()
	client := &brokerClient{path: filepath.Join(directory, "ctl.sock"), nonce: backup.config.Nonce, pid: command.Process.Pid, startTicks: ticks}
	state := &brokerStatus{PID: client.pid, StartTicks: ticks, SHA256: hash, ConfigSHA256: configDigest(data), Directory: filepath.Base(directory)}
	// Persist the launched process before releasing its startup gate.
	s.mu.Lock()
	s.broker, s.brokerState = client, state
	s.brokerStop = func() { command.Process.Signal(syscall.SIGTERM) }
	err = saveState(s.root, s.snapshot("broker-recovering"))
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if _, err := write.Write([]byte{1}); err != nil {
		return err
	}
	write.Close()
	for {
		if response, _, err := client.command(ctx, brokerRequest{Operation: "ping"}); err == nil && response.Ready {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	for _, binding := range backup.config.Direct {
		response, _, err := client.command(ctx, brokerRequest{Operation: "status", Tag: binding.Tag})
		if err != nil || !response.Ready || !response.Direct || response.Current != binding.Slot {
			return errors.Join(err, errors.New("replacement broker did not retain direct ownership"))
		}
	}
	return nil
}
