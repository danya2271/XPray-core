//go:build linux && !android

// Command nodex-xray-supervisor is an opt-in TCP-only node canary. It is not
// installed as the production xray.service and cannot import unmanaged work.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/xtls/xray-core/transport/internet/handoff"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "NodeX supervisor:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("nodex-xray-supervisor", flag.ContinueOnError)
	runtime := flags.String("runtime", "", "Fresh absolute private runtime directory (0700).")
	binary := flags.String("binary", "", "Absolute path to a managed-capable Xray binary.")
	config := flags.String("config", "", "Absolute path to the generation's private JSON config.")
	probe := flags.String("probe", "", "Absolute path to a trusted new-tunnel verification executable.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 1 {
		return errors.New("use serve, recover, status, or upgrade")
	}
	if !filepath.IsAbs(*runtime) || filepath.Clean(*runtime) != *runtime {
		return errors.New("runtime requires a clean absolute path")
	}
	r := release{*binary, *config}
	switch flags.Arg(0) {
	case "status", "upgrade":
		return request(*runtime, flags.Arg(0), r)
	case "serve", "recover":
		return serve(*runtime, *probe, r, flags.Arg(0) == "recover")
	default:
		return errors.New("use serve, recover, status, or upgrade")
	}
}

func claimRuntime(root string) (*os.File, error) {
	if err := os.Mkdir(root, 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	if err := handoff.PrivateSocketDirectory(root); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil, errors.New("runtime directory must be owned by the supervisor user")
	}
	lock, err := os.OpenFile(filepath.Join(root, ".lock"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errors.New("runtime already claimed; recovery must not rebind live listeners")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		lock.Close()
		return nil, errors.New("runtime must be empty before first start")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, err
	}
	// Keep the marker after exit. Only explicit recovery can re-open it.
	return lock, nil
}

func serve(root, probe string, r release, recovering bool) error {
	if os.Geteuid() == 0 {
		return errors.New("run the canary under a dedicated unprivileged service account")
	}
	if !filepath.IsAbs(probe) || filepath.Clean(probe) != probe {
		return errors.New("a trusted absolute probe executable is required")
	}
	info, err := os.Stat(probe)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return errors.New("probe is not an executable regular file")
	}
	claim := claimRuntime
	if recovering {
		claim = claimRecovery
	}
	lock, err := claim(root)
	if err != nil {
		return err
	}
	defer lock.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	path := filepath.Join(root, "supervisor.sock")
	if recovering {
		// The supervisor lock is held. Replace only its obsolete management
		// socket; never remove a generation control socket or public origin.
		if info, err := os.Lstat(path); err == nil {
			if info.Mode().Type() != os.ModeSocket {
				return errors.New("unexpected file at supervisor socket path")
			}
			if err := os.Remove(path); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	socket, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return err
	}
	defer socket.Close()
	if err := os.Chmod(socket.Addr().String(), 0600); err != nil {
		return err
	}
	start := newSupervisor
	if recovering {
		start = recoverSupervisor
	}
	s, err := start(ctx, root, probe, r)
	if err != nil {
		return err
	}
	defer func() { cancel(); s.close() }()
	server := &http.Server{
		Handler: s.handler(), ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: time.Second,
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(socket) }()
	select {
	case <-ctx.Done():
		_ = server.Close()
		return nil
	case err := <-done:
		cancel()
		return err
	}
}

func (s *supervisor) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.status())
	})
	mux.HandleFunc("POST /upgrade", func(w http.ResponseWriter, req *http.Request) {
		var r release
		decoder := json.NewDecoder(http.MaxBytesReader(w, req.Body, 8192))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&r) != nil || decoder.Decode(new(any)) != io.EOF {
			http.Error(w, "invalid release request", http.StatusBadRequest)
			return
		}
		if err := s.upgrade(r); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"state": "accepted"})
	})
	return mux
}

func request(root, action string, r release) error {
	if err := handoff.PrivateSocketDirectory(root); err != nil {
		return err
	}
	socket := filepath.Join(root, "supervisor.sock")
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	method, path := http.MethodGet, "/status"
	var data []byte
	if action == "upgrade" {
		method, path = http.MethodPost, "/upgrade"
		var err error
		data, err = json.Marshal(r)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, "http://nodex.local"+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 65536))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("supervisor HTTP %d: %s", response.StatusCode, body)
	}
	fmt.Print(string(body))
	return nil
}
