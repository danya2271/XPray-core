//go:build linux && !android

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/handoff"
	"golang.org/x/sys/unix"
)

type brokerStatus struct {
	PID          int    `json:"pid"`
	StartTicks   uint64 `json:"start_ticks"`
	SHA256       string `json:"sha256"`
	ConfigSHA256 string `json:"config_sha256"`
	Directory    string `json:"directory,omitempty"`
}

// An internal child entrypoint is also usable from the process test executable;
// it must run before testing parses the supervisor's command-line flags.
func init() {
	if path := os.Getenv("NODEX_INTERNAL_BROKER_CONTROL"); path != "" {
		if err := runBrokerControl(path); err != nil {
			fmt.Fprintln(os.Stderr, "NodeX broker control:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if path := os.Getenv("NODEX_INTERNAL_BROKER_MANIFEST"); path != "" {
		if err := runPacketBroker(path); err != nil {
			fmt.Fprintln(os.Stderr, "NodeX UDP broker:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
}

func runPacketBroker(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("invalid broker config path")
	}
	if err := handoff.PrivateSocketDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var config brokerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}
	if len(config.Bindings) == 0 || len(config.Bindings) > 128 {
		return errors.New("invalid broker socket count")
	}
	count := len(config.Bindings)
	if len(config.Direct) != 0 {
		count *= 5
	}
	if err := handoff.AwaitStartup(handoff.Manifest{StartupFD: 3 + count}); err != nil {
		return err
	}
	files := make([]*os.File, count)
	for i := range files {
		files[i] = os.NewFile(uintptr(3+i), "broker-public-udp")
		defer files[i].Close()
	}
	broker, err := newPacketBroker(config, files)
	if err != nil {
		return err
	}
	defer broker.close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return broker.serve(ctx, filepath.Join(filepath.Dir(path), "ctl.sock"))
}

func (s *supervisor) startBroker(config []byte) error {
	var parsed struct {
		Inbounds []struct {
			Tag    string
			Stream json.RawMessage `json:"streamSettings"`
		}
	}
	if err := json.Unmarshal(config, &parsed); err != nil {
		return err
	}
	streams := make(map[string]json.RawMessage)
	for _, inbound := range parsed.Inbounds {
		streams[inbound.Tag] = inbound.Stream
	}
	manifest := brokerConfig{}
	rand.Read(manifest.Nonce[:])
	var files []*os.File
	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()
	for _, binding := range s.bindings {
		if binding.transport != "hysteria" {
			continue
		}
		// Bind once in the management process, then transfer duplicates to
		// the persistent broker. There is no SO_REUSEPORT competing reader.
		settings, err := socketSettings(binding)
		if err != nil {
			return err
		}
		lc := internet.ManagedListenConfig(settings)
		network := "udp6"
		if net.ParseIP(binding.address).To4() != nil {
			network = "udp4"
		}
		packet, err := lc.ListenPacket(s.ctx, network, net.JoinHostPort(binding.address, fmt.Sprint(binding.port)))
		if err != nil {
			return err
		}
		public := packet.(*net.UDPConn)
		file, err := handoff.DuplicatePacketSocket(public)
		public.Close()
		if err != nil {
			return err
		}
		files = append(files, file)
		entry := brokerBinding{Tag: binding.tag, Address: binding.address, Port: binding.port, Native: binding.native, Stream: streams[binding.tag]}
		rand.Read(entry.Key[:])
		manifest.Bindings = append(manifest.Bindings, entry)
	}
	if len(files) == 0 {
		return nil
	}
	directory := filepath.Join(s.root, "broker")
	if err := os.Mkdir(directory, 0700); err != nil {
		return err
	}
	path := filepath.Join(directory, "manifest.json")
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := writePrivateSnapshot(path, data); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary := filepath.Join(directory, "daemon")
	hash, err := snapshotBinary(executable, binary)
	if err != nil {
		return err
	}
	read, write, err := os.Pipe()
	if err != nil {
		return err
	}
	defer read.Close()
	defer write.Close()
	command := exec.Command(binary)
	command.Env = append(os.Environ(), "NODEX_INTERNAL_BROKER_MANIFEST="+path)
	command.ExtraFiles = append(append([]*os.File{}, files...), read)
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
	s.brokerState = &brokerStatus{PID: command.Process.Pid, StartTicks: ticks, SHA256: hash, ConfigSHA256: configDigest(data)}
	s.broker = &brokerClient{path: filepath.Join(directory, "ctl.sock"), nonce: manifest.Nonce, pid: command.Process.Pid, startTicks: ticks}
	s.brokerStop = func() { command.Process.Signal(syscall.SIGTERM) }
	go command.Wait()
	if err := s.record("broker-starting", nil, nil, nil); err != nil {
		return err
	}
	if _, err := write.Write([]byte{1}); err != nil {
		return err
	}
	write.Close()
	ctx, cancel := context.WithTimeout(s.ctx, operationLimit)
	defer cancel()
	for {
		if response, _, err := s.broker.command(ctx, brokerRequest{Operation: "ping"}); err == nil && response.Ready {
			return s.attachBrokerControl(ctx)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (s *supervisor) recoverBroker(ctx context.Context, saved *brokerStatus) error {
	if saved == nil {
		return nil
	}
	directory := filepath.Join(s.root, "broker")
	if saved.Directory != "" {
		if filepath.Base(saved.Directory) != saved.Directory || !strings.HasPrefix(saved.Directory, "broker-recovery-") {
			return errors.New("invalid recovered broker directory")
		}
		directory = filepath.Join(s.root, saved.Directory)
	}
	if err := handoff.PrivateSocketDirectory(directory); err != nil {
		return err
	}
	fd, err := unix.PidfdOpen(saved.PID, 0)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			unix.Close(fd)
		}
	}()
	ticks, err := processStart(saved.PID)
	if err != nil || ticks != saved.StartTicks {
		return errors.New("UDP broker PID identity changed")
	}
	executable, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", saved.PID))
	if err != nil || executable != filepath.Join(directory, "daemon") {
		return errors.New("UDP broker executable path changed")
	}
	hash, err := fileDigest(fmt.Sprintf("/proc/%d/exe", saved.PID))
	if err != nil || hash != saved.SHA256 {
		return errors.New("UDP broker executable hash changed")
	}
	data, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil || configDigest(data) != saved.ConfigSHA256 {
		return errors.New("UDP broker launch config changed")
	}
	var manifest brokerConfig
	if err := json.Unmarshal(data, &manifest); err != nil {
		return err
	}
	client := &brokerClient{path: filepath.Join(directory, "ctl.sock"), nonce: manifest.Nonce, pid: saved.PID, startTicks: saved.StartTicks}
	if response, _, err := client.command(ctx, brokerRequest{Operation: "ping"}); err != nil || !response.Ready {
		return errors.Join(err, errors.New("UDP broker recovery was not acknowledged"))
	}
	s.broker, s.brokerState = client, saved
	s.brokerStop = func() { unix.PidfdSendSignal(fd, unix.SIGTERM, nil, 0); unix.Close(fd) }
	if err := s.attachBrokerControl(ctx); err != nil {
		// Validation failure closes this pidfd in the defer; do not retain a
		// closure that could later signal/close a reused descriptor number.
		s.brokerStop = nil
		return err
	}
	keep = true
	return nil
}
