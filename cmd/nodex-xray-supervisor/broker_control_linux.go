//go:build linux && !android

package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/xtls/xray-core/transport/internet/handoff"
	"golang.org/x/sys/unix"
)

// The control process never owns a public UDP socket, packet ring, Initial pin
// or pending datagram. Its loss cannot remove data-plane state in the owner.
type brokerControlManifest struct {
	Owner brokerStatus `json:"owner"`
	Nonce [32]byte     `json:"nonce"`
}

type brokerControlIdentity struct {
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	Sequence   uint64 `json:"sequence"`
}

type brokerControl struct {
	mu      sync.Mutex
	root    string
	owner   brokerClient
	config  brokerControlManifest
	target  brokerControlIdentity
	closed  bool
	failure error
}

// Tests can replace this before the internal child entrypoint's init.
var brokerControlAfterCommand = func(brokerRequest) {}

func (c *brokerControl) identity() brokerControlIdentity {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.target
}

func (c *brokerControl) client(identity brokerControlIdentity) brokerClient {
	return brokerClient{path: filepath.Join(c.root, fmt.Sprintf("c%d", identity.Sequence), "ctl.sock"),
		nonce: c.owner.nonce, pid: identity.PID, startTicks: identity.StartTicks}
}

func runBrokerControl(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("invalid control manifest path")
	}
	directory := filepath.Dir(path)
	if err := handoff.PrivateSocketDirectory(directory); err != nil {
		return err
	}
	if err := handoff.PrivateSocketDirectory(filepath.Dir(directory)); err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var config brokerControlManifest
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}
	if config.Nonce == [32]byte{} || config.Owner.PID < 2 || config.Owner.StartTicks == 0 {
		return errors.New("invalid control owner identity")
	}
	if err := handoff.AwaitStartup(handoff.Manifest{StartupFD: 3}); err != nil {
		return err
	}
	ownerRoot := filepath.Dir(filepath.Dir(directory))
	owner := brokerClient{path: filepath.Join(ownerRoot, "ctl.sock"), nonce: config.Nonce,
		pid: config.Owner.PID, startTicks: config.Owner.StartTicks}
	if err := validateBrokerProcess(ownerRoot, &config.Owner); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if response, _, err := owner.commandOnce(ctx, brokerRequest{Operation: "ping"}); err != nil || !response.Ready {
		return errors.Join(err, errors.New("packet owner did not acknowledge control attach"))
	}
	pid, ticks := os.Getpid(), uint64(0)
	ticks, err = processStart(pid)
	if err != nil {
		return err
	}
	return serveBrokerCommands(ctx, filepath.Join(directory, "ctl.sock"), func(ctx context.Context, request brokerRequest) (brokerResponse, []*os.File, bool) {
		if subtle.ConstantTimeCompare(request.Nonce[:], config.Nonce[:]) != 1 {
			return brokerResponse{PID: pid, StartTicks: ticks, Error: "broker control identity mismatch"}, nil, false
		}
		response, files, err := owner.commandOnce(ctx, request)
		if err != nil {
			response = brokerResponse{Error: err.Error()}
		}
		response.PID, response.StartTicks = pid, ticks
		brokerControlAfterCommand(request)
		return response, files, true // Forwarded SCM_RIGHTS are temporary duplicates.
	})
}

func validateBrokerProcess(directory string, saved *brokerStatus) error {
	ticks, err := processStart(saved.PID)
	if err != nil || ticks != saved.StartTicks {
		return errors.New("packet owner process identity changed")
	}
	path, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", saved.PID))
	if err != nil || path != filepath.Join(directory, "daemon") {
		return errors.New("packet owner executable path changed")
	}
	hash, err := fileDigest(fmt.Sprintf("/proc/%d/exe", saved.PID))
	if err != nil || hash != saved.SHA256 {
		return errors.New("packet owner executable hash changed")
	}
	data, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil || configDigest(data) != saved.ConfigSHA256 {
		return errors.New("packet owner launch config changed")
	}
	return nil
}

// A separate journal is used because a control retry can happen with the
// supervisor state mutex already held. Ownership/admission is never inferred
// from this journal: all actual state stays in the original packet owner.
func (s *supervisor) attachBrokerControl(ctx context.Context) error {
	if s.broker == nil || s.brokerState == nil {
		return errors.New("missing packet owner")
	}
	response, files, err := s.broker.commandOnce(ctx, brokerRequest{Operation: "ping"})
	for _, file := range files {
		file.Close()
	}
	if err != nil || !response.Ready {
		return errors.Join(err, errors.New("packet owner unavailable before controller attach"))
	}
	// Recovery of a pre-controller owner keeps its old management route.
	// Its immutable daemon cannot understand the new internal entrypoint.
	if !response.ControlCapable {
		return nil
	}
	c := &brokerControl{root: filepath.Join(filepath.Dir(s.broker.path), "control"), owner: *s.broker,
		config: brokerControlManifest{Owner: *s.brokerState, Nonce: s.broker.nonce}}
	c.owner.control = nil
	if err := os.Mkdir(c.root, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	if err := handoff.PrivateSocketDirectory(c.root); err != nil {
		return err
	}
	lock, err := c.claim(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	data, err := os.ReadFile(filepath.Join(c.root, "state.json"))
	if err == nil {
		if err := json.Unmarshal(data, &c.target); err != nil || c.target.PID < 2 || c.target.StartTicks == 0 || c.target.Sequence == 0 {
			return errors.New("invalid broker control journal")
		}
		dead, err := brokerDead(c.target.PID, c.target.StartTicks)
		if err != nil {
			return err
		}
		if !dead {
			if err := c.validate(); err != nil {
				return err
			}
			if response, _, err := c.client(c.target).commandOnce(ctx, brokerRequest{Operation: "ping"}); err != nil || !response.Ready {
				return errors.Join(err, errors.New("live broker control did not acknowledge recovery"))
			}
		} else {
			if err := c.validateManifest(); err != nil {
				return err
			}
			if err := c.launch(ctx); err != nil {
				return err
			}
		}
	} else if os.IsNotExist(err) {
		entries, err := os.ReadDir(c.root)
		if err != nil || len(entries) != 1 {
			return errors.New("incomplete broker control startup; retaining packet owner")
		}
		if err := c.launch(ctx); err != nil {
			return err
		}
	} else {
		return err
	}
	s.mu.Lock()
	s.broker.control = c
	s.mu.Unlock()
	stopOwner := s.brokerStop
	s.brokerStop = func() {
		c.close()
		if stopOwner != nil {
			stopOwner()
		}
	}
	return nil
}

func (c *brokerControl) validate() error {
	executable, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", c.target.PID))
	if err != nil || executable != filepath.Join(filepath.Dir(c.root), "daemon") {
		return errors.New("broker control executable path changed")
	}
	hash, err := fileDigest(fmt.Sprintf("/proc/%d/exe", c.target.PID))
	if err != nil || hash != c.config.Owner.SHA256 {
		return errors.New("broker control executable hash changed")
	}
	return c.validateManifest()
}

func (c *brokerControl) validateManifest() error {
	directory := filepath.Join(c.root, fmt.Sprintf("c%d", c.target.Sequence))
	data, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil {
		return err
	}
	var manifest brokerControlManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return err
	}
	if manifest != c.config {
		return errors.New("broker control launch owner changed")
	}
	return nil
}

func (c *brokerControl) launch(ctx context.Context) error {
	if c.closed || c.target.Sequence == ^uint64(0) {
		return errors.New("broker control unavailable")
	}
	if err := validateBrokerProcess(filepath.Dir(c.root), &c.config.Owner); err != nil {
		return err
	}
	// All prior controllers are either absent (bootstrap) or proven dead.
	c.target.Sequence++
	directory := filepath.Join(c.root, fmt.Sprintf("c%d", c.target.Sequence))
	if err := os.Mkdir(directory, 0700); err != nil {
		return err
	}
	path := filepath.Join(directory, "manifest.json")
	data, err := json.Marshal(c.config)
	if err != nil {
		return err
	}
	if err := writePrivateSnapshot(path, data); err != nil {
		return err
	}
	read, write, err := os.Pipe()
	if err != nil {
		return err
	}
	defer read.Close()
	defer write.Close()
	command := exec.Command(filepath.Join(filepath.Dir(c.root), "daemon"))
	command.Env = append(os.Environ(), "NODEX_INTERNAL_BROKER_MANIFEST=", "NODEX_INTERNAL_BROKER_CONTROL="+path)
	command.ExtraFiles = []*os.File{read}
	command.Stdout, command.Stderr = os.Stderr, os.Stderr
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
	c.target.PID, c.target.StartTicks = command.Process.Pid, ticks
	if err := saveControlState(c.root, c.target); err != nil {
		return err
	}
	if _, err := write.Write([]byte{1}); err != nil {
		return err
	}
	write.Close()
	for {
		response, files, err := c.client(c.target).commandOnce(ctx, brokerRequest{Operation: "ping"})
		for _, file := range files {
			file.Close()
		}
		if err == nil && response.Ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (c *brokerControl) claim(ctx context.Context) (*os.File, error) {
	fd, err := unix.Open(filepath.Join(c.root, ".lock"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "broker-control-lock")
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return lock, nil
		}
		if err != unix.EWOULDBLOCK {
			lock.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			lock.Close()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (c *brokerControl) recover(ctx context.Context) error {
	lock, err := c.claim(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	data, err := os.ReadFile(filepath.Join(c.root, "state.json"))
	if err != nil {
		return err
	}
	var saved brokerControlIdentity
	if err := json.Unmarshal(data, &saved); err != nil || saved.PID < 2 || saved.StartTicks == 0 || saved.Sequence == 0 {
		return errors.New("invalid broker control recovery journal")
	}
	if saved.Sequence < c.target.Sequence {
		return errors.New("broker control journal regressed")
	}
	c.target = saved
	dead, err := brokerDead(saved.PID, saved.StartTicks)
	if err != nil {
		return err
	}
	if !dead {
		if err := c.validate(); err != nil {
			return err
		}
		response, files, err := c.client(saved).commandOnce(ctx, brokerRequest{Operation: "ping"})
		for _, file := range files {
			file.Close()
		}
		if err != nil || !response.Ready {
			return errors.Join(err, errors.New("replacement control not ready"))
		}
		return nil
	}
	// The dead controller's manifest must still match; corruption is not
	// permission to erase its journal and start another management process.
	if err := c.validateManifest(); err != nil {
		return err
	}
	return c.launch(ctx)
}

func saveControlState(root string, identity brokerControlIdentity) error {
	data, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	path := filepath.Join(root, ".state.new")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	err = errors.Join(writeErr, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(path, filepath.Join(root, "state.json")); err != nil {
		return err
	}
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (c *brokerControl) command(ctx context.Context, request brokerRequest) (brokerResponse, []*os.File, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return brokerResponse{}, nil, errors.New("broker control closed")
	}
	if c.failure != nil {
		return brokerResponse{}, nil, c.failure
	}
	response, files, err := c.client(c.target).commandOnce(ctx, request)
	if err == nil {
		return response, files, nil
	}
	dead, identityErr := brokerDead(c.target.PID, c.target.StartTicks)
	if identityErr != nil || !dead || ctx.Err() != nil {
		return response, nil, errors.Join(err, identityErr)
	}
	if err := c.recover(ctx); err != nil {
		c.failure = fmt.Errorf("broker control recovery failed; inspect journal: %w", err)
		return response, nil, err
	}
	// The owner retains all command state. Repeating an acknowledged-or-lost
	// add/activate/adopt/fence/retire uses the existing idempotency contracts.
	return c.client(c.target).commandOnce(ctx, request)
}

func (c *brokerControl) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	fd, err := unix.PidfdOpen(c.target.PID, 0)
	if err != nil {
		return
	}
	defer unix.Close(fd)
	ticks, err := processStart(c.target.PID)
	if err == nil && ticks == c.target.StartTicks {
		_ = unix.PidfdSendSignal(fd, unix.SIGTERM, nil, 0)
	}
}
