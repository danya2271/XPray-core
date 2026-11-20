package splithttp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/transport/internet/handoff"
	"google.golang.org/protobuf/proto"
)

// Admission and completion share one atomic counter. Once settled, no
// public or forwarded request can reopen a session or begin proxying through
// this process. This accounting is absent from unmanaged listeners.
type handoffLifetime struct {
	work atomic.Uint64
	wait atomic.Bool
	wake chan struct{}
}

const (
	lifetimeSettled = uint64(1) << 63
	lifetimeStopped = uint64(1) << 62
)

func newHandoffLifetime() *handoffLifetime { return &handoffLifetime{wake: make(chan struct{}, 1)} }

func (l *handoffLifetime) enter() bool {
	for {
		work := l.work.Load()
		if work&(lifetimeSettled|lifetimeStopped) != 0 {
			return false
		}
		if l.work.CompareAndSwap(work, work+1) {
			return true
		}
	}
}

// A request holding a reference may also create a separately tracked session.
func (l *handoffLifetime) addSession() { l.work.Add(1) }

func (l *handoffLifetime) exit() {
	if l.work.Add(^uint64(0)) == 0 && l.wait.Load() {
		l.notify()
	}
}

func (l *handoffLifetime) settle() bool {
	l.wait.Store(true)
	for {
		work := l.work.Load()
		if work&lifetimeStopped != 0 {
			return false
		}
		if work == lifetimeSettled {
			return true
		}
		if l.work.CompareAndSwap(0, lifetimeSettled) {
			return true
		}
		<-l.wake
	}
}

func (l *handoffLifetime) close() {
	l.work.Or(lifetimeStopped)
	l.notify()
}

func (l *handoffLifetime) isSettled() bool {
	return l.work.Load() == lifetimeSettled
}

func (l *handoffLifetime) notify() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *Listener) StartHandoff(path string) error {
	if l.handler.life == nil || l.peerServer != nil {
		return errors.New("XHTTP listener is not eligible for handoff")
	}
	if err := handoff.PrivateSocketDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	config, err := proto.MarshalOptions{Deterministic: true}.Marshal(l.config)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(config)
	socket, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return err
	}
	if err := os.Chmod(path, 0600); err != nil {
		socket.Close()
		return err
	}
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	l.peerServer = &http.Server{Handler: l.HandoffHandler(), Protocols: protocols, ReadHeaderTimeout: 4 * time.Second,
		MaxHeaderBytes: l.config.GetNormalizedServerMaxHeaderBytes()}
	l.peerSocket = socket
	l.peerDigest = hex.EncodeToString(digest[:])
	go l.peerServer.Serve(socket)
	return nil
}

func (l *Listener) LinkHandoffPeer(path string) error {
	if l.peerServer == nil || l.handler.life.isSettled() {
		return errors.New("XHTTP handoff endpoint is unavailable")
	}
	if l.peerPath != "" && l.peerPath != path {
		return errors.New("cannot replace an XHTTP peer until it has drained and been unlinked")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode().Type() != os.ModeSocket || info.Mode().Perm() != 0600 {
		return errors.New("invalid private XHTTP peer socket")
	}
	probe := newHandoffPeer(path, false)
	defer probe.transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, handoffConfig, "http://xray-handoff.local/", nil)
	if err != nil {
		return err
	}
	response, err := probe.transport.RoundTrip(r)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent || response.Header.Get(handoffDigest) != l.peerDigest {
		return errors.New("XHTTP peer has incompatible session configuration")
	}
	l.peerPath = path
	return nil
}

func (l *Listener) HandoffPeerLinked() bool { return l.peerPath != "" }

func (l *Listener) SetHandoffAdmission(active bool) error {
	if l.peerPath == "" {
		if active {
			return nil // First generation, with no predecessor.
		}
		return errors.New("cannot pause an XHTTP inbound without a linked peer")
	}
	return l.SetHandoffPeer(l.peerPath, active)
}

func (l *Listener) UnlinkHandoffPeer() error {
	if l.peerPath == "" {
		return nil // Idempotent, including a lost control reply.
	}
	peer := l.handler.peer.Load()
	if peer == nil || !peer.acceptsNew {
		return errors.New("cannot unlink peer before admission has switched")
	}
	drained, err := peer.hasDrained()
	if err != nil {
		return err
	}
	if !drained {
		return errors.New("cannot unlink an XHTTP peer with live work")
	}
	l.ClearHandoffPeer()
	l.peerPath = ""
	return nil
}

// Drain keeps the peer lookup socket alive until the supervisor unlinks it on
// the active generation. An early Unlink cannot race the last forwarded body.
func (l *Listener) Drain() error {
	if !l.handler.life.settle() {
		return errors.New("XHTTP listener stopped before drain completed")
	}
	return l.server.Shutdown(context.Background())
}
