//go:build linux && !android

package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/handoff"
	"golang.org/x/sys/unix"
)

// The broker outlives the management process. Its memory owns Initial pins,
// routing keys, slots and packet channels; no state is reconstructed by binding
// a replacement socket after a supervisor failure.
type brokerConfig struct {
	Nonce    [32]byte              `json:"nonce"`
	Bindings []brokerBinding       `json:"bindings"`
	Direct   []brokerDirectRestore `json:"direct,omitempty"`
}

type brokerDirectRestore struct {
	Tag            string `json:"tag"`
	Slot           byte   `json:"slot"`
	ReservedBefore uint16 `json:"reserved_before"`
}

type brokerBinding struct {
	Tag     string          `json:"tag"`
	Address string          `json:"address"`
	Port    int             `json:"port"`
	Key     [16]byte        `json:"key"`
	Native  bool            `json:"native"`
	Stream  json.RawMessage `json:"stream"`
}

type packetIngress interface {
	Add(byte) (*os.File, error)
	Activate(byte) error
	FenceGeneration(context.Context, byte, [16]byte) (bool, error)
	Retire(byte, [16]byte) (bool, error)
	Abandon(byte) error
	Close() error
	Dropped() uint64
}

type nativePacketIngress interface {
	packetIngress
	AddDirectRing(byte) ([]*os.File, error)
	AdoptReceive([]byte) error
	ReleasePublic(byte, [16]byte) (bool, error)
	ExportDirect(byte) ([]*os.File, error)
	RestoreDirect(byte, []*os.File) error
}

type brokerPeer struct {
	files    []*os.File
	fence    [16]byte
	retired  bool
	admitted bool
}

type brokerEndpoint struct {
	config         brokerBinding
	public         *net.UDPConn
	ingress        packetIngress
	peers          map[byte]*brokerPeer
	current        byte
	direct         bool
	reservedBefore uint16
}

type packetBroker struct {
	mu        sync.Mutex
	nonce     [32]byte
	endpoints map[string]*brokerEndpoint
}

type brokerRequest struct {
	Nonce     [32]byte        `json:"nonce"`
	Operation string          `json:"operation"`
	Tag       string          `json:"tag,omitempty"`
	Slot      byte            `json:"slot,omitempty"`
	Token     [16]byte        `json:"token,omitempty"`
	Snapshot  json.RawMessage `json:"snapshot,omitempty"`
}

type brokerResponse struct {
	Error          string         `json:"error,omitempty"`
	Ready          bool           `json:"ready"`
	PID            int            `json:"pid"`
	StartTicks     uint64         `json:"start_ticks"`
	Current        byte           `json:"current"`
	Direct         bool           `json:"direct"`
	Binding        *brokerBinding `json:"binding,omitempty"`
	Drops          uint64         `json:"drops,omitempty"`
	ControlCapable bool           `json:"control_capable,omitempty"`
}

func newPacketBroker(config brokerConfig, files []*os.File) (*packetBroker, error) {
	restoring := len(config.Direct) != 0
	count := len(config.Bindings)
	expected := count
	if restoring {
		expected *= 5
	}
	if config.Nonce == [32]byte{} || count == 0 || count > 128 || len(files) != expected || (restoring && len(config.Direct) != count) {
		return nil, errors.New("invalid broker launch manifest")
	}
	b := &packetBroker{nonce: config.Nonce, endpoints: make(map[string]*brokerEndpoint)}
	keep := false
	defer func() {
		if !keep {
			b.close()
		}
	}()
	for index, binding := range config.Bindings {
		if binding.Tag == "" || b.endpoints[binding.Tag] != nil || binding.Key == [16]byte{} {
			return nil, errors.New("invalid or duplicate broker binding")
		}
		packet, err := net.FilePacketConn(files[index])
		if err != nil {
			return nil, err
		}
		public, ok := packet.(*net.UDPConn)
		if !ok || public.LocalAddr().String() != net.JoinHostPort(binding.Address, fmt.Sprint(binding.Port)) {
			packet.Close()
			return nil, errors.New("broker descriptor binding mismatch")
		}
		endpoint := &brokerEndpoint{config: binding, public: public, peers: make(map[byte]*brokerPeer), direct: binding.Native}
		b.endpoints[binding.Tag] = endpoint
		if err := tuneManagedPacketSocket(public); err != nil {
			return nil, err
		}
		if binding.Native {
			endpoint.ingress, err = makeNativePacketIngress(public, binding)
		} else {
			var stream conf.StreamConfig
			if err = json.Unmarshal(binding.Stream, &stream); err != nil {
				return nil, err
			}
			built, buildErr := stream.Build()
			if buildErr != nil {
				return nil, buildErr
			}
			settings, buildErr := internet.ToMemoryStreamConfig(built)
			if buildErr != nil {
				return nil, buildErr
			}
			var masked net.PacketConn = public
			if public.LocalAddr().(*net.UDPAddr).IP.IsUnspecified() {
				masked, err = handoff.PreservePacketDestination(public)
				if err != nil {
					return nil, err
				}
			}
			base := masked
			if settings.FinalMask != nil {
				for _, mask := range settings.FinalMask.UDPMasks() {
					if _, owns := mask.(interface{ HandleListen() }); owns {
						return nil, errors.New("broker network masks require explicit socket ownership integration")
					}
				}
				// Build the normal masks around the inherited socket once. A
				// second bind would split reception and lose CID ownership.
				masked, err = settings.FinalMask.WithPacketListener(func(context.Context, net.Addr) (net.PacketConn, error) { return base, nil }).ListenPacket(context.Background(), public.LocalAddr())
			}
			if err == nil {
				endpoint.ingress, err = handoff.NewUDPIngress(masked, binding.Key, 0, 4096, 30*time.Second)
			}
		}
		if err != nil {
			return nil, err
		}
		if restoring {
			saved := config.Direct[index]
			if !binding.Native || saved.Tag != binding.Tag || saved.ReservedBefore == 0 || saved.ReservedBefore > 256 || uint16(saved.Slot) >= saved.ReservedBefore {
				return nil, errors.New("invalid direct broker recovery binding")
			}
			base := count + index*4
			if err := endpoint.ingress.(nativePacketIngress).RestoreDirect(saved.Slot, files[base:base+4]); err != nil {
				return nil, err
			}
			endpoint.current, endpoint.reservedBefore = saved.Slot, saved.ReservedBefore
			endpoint.peers[saved.Slot] = &brokerPeer{admitted: true}
		}
	}
	keep = true
	return b, nil
}

// Match the existing native server public-socket policy before masks hide the
// descriptor from Go QUIC. Keep larger operator-provisioned buffers intact.
// This runs at bind/recovery only; private relay queue capacities do not change.
func tuneManagedPacketSocket(public *net.UDPConn) error {
	raw, err := public.SyscallConn()
	if err != nil {
		return err
	}
	var setupErr error
	err = raw.Control(func(fd uintptr) {
		for _, option := range []struct{ name, bytes int }{{unix.SO_RCVBUF, 4 << 20}, {unix.SO_SNDBUF, 1 << 20}} {
			current, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, option.name)
			if err != nil {
				setupErr = err
				return
			}
			if current < option.bytes*2 {
				if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, option.name, option.bytes); err != nil {
					setupErr = err
					return
				}
			}
		}
	})
	return errors.Join(err, setupErr)
}

func (b *packetBroker) close() {
	for _, endpoint := range b.endpoints {
		if endpoint.ingress != nil {
			endpoint.ingress.Close()
		}
		endpoint.public.Close()
		for _, peer := range endpoint.peers {
			for _, file := range peer.files {
				file.Close()
			}
		}
	}
}

func (b *packetBroker) command(ctx context.Context, request brokerRequest) (brokerResponse, []*os.File) {
	b.mu.Lock()
	defer b.mu.Unlock()
	response := brokerResponse{PID: os.Getpid()}
	response.StartTicks, _ = processStart(response.PID)
	fail := func(err error) (brokerResponse, []*os.File) { response.Error = err.Error(); return response, nil }
	if subtle.ConstantTimeCompare(request.Nonce[:], b.nonce[:]) != 1 {
		return fail(errors.New("broker identity mismatch"))
	}
	if request.Operation == "ping" {
		response.Ready = true
		response.ControlCapable = true
		return response, nil
	}
	e := b.endpoints[request.Tag]
	if e == nil {
		return fail(errors.New("unknown broker inbound"))
	}
	response.Binding, response.Current, response.Direct = &e.config, e.current, e.direct
	peer := e.peers[request.Slot]
	var err error
	switch request.Operation {
	case "status":
		response.Ready = true
	case "stats":
		response.Ready = true
		response.Drops = e.ingress.Dropped()
	case "checkpoint-direct":
		if !e.config.Native || !e.direct || e.current != request.Slot || peer == nil || peer.retired || peer.fence != [16]byte{} {
			return fail(errors.New("broker checkpoint requires stable native direct receive"))
		}
		files, err := e.ingress.(nativePacketIngress).ExportDirect(request.Slot)
		if err != nil {
			return fail(err)
		}
		public, err := handoff.DuplicatePacketSocket(e.public)
		if err != nil {
			for _, file := range files {
				file.Close()
			}
			return fail(err)
		}
		response.Ready = true
		return response, append([]*os.File{public}, files...)
	case "add":
		if peer == nil {
			if uint16(request.Slot) < e.reservedBefore {
				return fail(errors.New("recovered broker routing slot cannot be reused"))
			}
			var files []*os.File
			if e.config.Native {
				files, err = e.ingress.(nativePacketIngress).AddDirectRing(request.Slot)
				if err == nil {
					var transmit *os.File
					transmit, err = handoff.DuplicatePacketSocket(e.public)
					if err == nil {
						files = append(files, transmit)
					}
				}
			} else {
				var file *os.File
				file, err = e.ingress.Add(request.Slot)
				if err == nil {
					files = []*os.File{file}
				}
			}
			if err != nil {
				for _, file := range files {
					file.Close()
				}
				return fail(err)
			}
			peer = &brokerPeer{files: files}
			e.peers[request.Slot] = peer
		}
		if peer.retired {
			return fail(errors.New("retired broker slot cannot be reused"))
		}
		response.Ready = true
		return response, peer.files
	case "activate":
		if peer == nil || peer.fence != [16]byte{} || peer.retired {
			return fail(errors.New("broker slot cannot accept new work"))
		}
		err = e.ingress.Activate(request.Slot)
		if err == nil {
			e.current = request.Slot
			peer.admitted = true
		}
	case "abandon":
		if peer == nil {
			return fail(errors.New("unknown candidate channel"))
		}
		if peer.admitted || e.current == request.Slot {
			return fail(errors.New("cannot abandon admitted QUIC work"))
		}
		err = e.ingress.Abandon(request.Slot)
		if err == nil {
			peer.retired = true
			for _, file := range peer.files {
				file.Close()
			}
			peer.files = nil
			response.Ready = true
		}
	case "adopt":
		if !e.config.Native {
			return fail(errors.New("public receive adoption requires native ingress"))
		}
		err = e.ingress.(nativePacketIngress).AdoptReceive(request.Snapshot)
		if err == nil {
			e.direct = false
		}
	case "fence":
		if peer == nil || request.Token == [16]byte{} || (peer.fence != [16]byte{} && peer.fence != request.Token) {
			return fail(errors.New("invalid broker fence"))
		}
		response.Ready, err = e.ingress.FenceGeneration(ctx, request.Slot, request.Token)
		if response.Ready && err == nil {
			peer.fence = request.Token
		}
	case "retire":
		if peer == nil || peer.fence != request.Token || request.Token == [16]byte{} {
			return fail(errors.New("broker retirement requires matching fence"))
		}
		response.Ready, err = e.ingress.Retire(request.Slot, request.Token)
		if response.Ready && err == nil {
			peer.retired = true
			for _, file := range peer.files {
				file.Close()
			}
			peer.files = nil
		}
	case "release":
		if !e.config.Native {
			return fail(errors.New("direct receive requires native ingress"))
		}
		response.Ready, err = e.ingress.(nativePacketIngress).ReleasePublic(request.Slot, request.Token)
		if response.Ready && err == nil {
			e.direct = true
		}
	default:
		return fail(errors.New("unknown broker command"))
	}
	if err != nil {
		return fail(err)
	}
	if request.Operation == "activate" || request.Operation == "adopt" {
		response.Ready = true
	}
	response.Current, response.Direct = e.current, e.direct
	return response, nil
}

func peerCredentials(socket *net.UnixConn) (*unix.Ucred, error) {
	raw, err := socket.SyscallConn()
	if err != nil {
		return nil, err
	}
	var credential *unix.Ucred
	var credentialErr error
	err = raw.Control(func(fd uintptr) {
		credential, credentialErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	return credential, errors.Join(err, credentialErr)
}

func (b *packetBroker) serve(ctx context.Context, path string) error {
	return serveBrokerCommands(ctx, path, func(ctx context.Context, request brokerRequest) (brokerResponse, []*os.File, bool) {
		response, files := b.command(ctx, request)
		return response, files, request.Operation == "checkpoint-direct"
	})
}

func serveBrokerCommands(ctx context.Context, path string, command func(context.Context, brokerRequest) (brokerResponse, []*os.File, bool)) error {
	if err := handoff.PrivateSocketDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(path, 0600); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	// Commands are serialized; reuse the bounded request buffer instead of
	// allocating a MiB for every drain/status poll.
	wire := make([]byte, 1<<20)
	for {
		socket, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		// Serialize commands through their ACK. No per-packet management I/O.
		func() {
			defer socket.Close()
			socket.SetDeadline(time.Now().Add(5 * time.Second))
			peer, err := peerCredentials(socket)
			if err != nil || peer.Uid != uint32(os.Geteuid()) {
				return
			}
			n, _, flags, _, err := socket.ReadMsgUnix(wire, nil)
			if err != nil || flags&unix.MSG_TRUNC != 0 {
				return
			}
			var request brokerRequest
			decoder := json.NewDecoder(bytes.NewReader(wire[:n]))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&request) != nil {
				return
			}
			response, files, owned := command(ctx, request)
			if owned {
				defer func() {
					for _, file := range files {
						file.Close()
					}
				}()
			}
			data, err := json.Marshal(response)
			if err != nil {
				return
			}
			var rights []byte
			if len(files) != 0 {
				fds := make([]int, len(files))
				for i, f := range files {
					fds[i] = int(f.Fd())
				}
				rights = unix.UnixRights(fds...)
			}
			_, _, _ = socket.WriteMsgUnix(data, rights, nil)
		}()
	}
}

type brokerClient struct {
	path       string
	nonce      [32]byte
	pid        int
	startTicks uint64
	control    *brokerControl
}

func (c brokerClient) command(ctx context.Context, request brokerRequest) (brokerResponse, []*os.File, error) {
	if c.control != nil {
		if c.pid != c.control.owner.pid || c.startTicks != c.control.owner.startTicks || c.nonce != c.control.owner.nonce {
			return brokerResponse{}, nil, errors.New("broker control packet-owner identity mismatch")
		}
		return c.control.command(ctx, request)
	}
	return c.commandOnce(ctx, request)
}

func (c brokerClient) commandOnce(ctx context.Context, request brokerRequest) (brokerResponse, []*os.File, error) {
	var response brokerResponse
	connection, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unixpacket", c.path)
	if err != nil {
		return response, nil, err
	}
	socket := connection.(*net.UnixConn)
	defer socket.Close()
	stop := context.AfterFunc(ctx, func() { socket.Close() })
	defer stop()
	socket.SetDeadline(time.Now().Add(5 * time.Second))
	credential, err := peerCredentials(socket)
	if err != nil || credential.Uid != uint32(os.Geteuid()) || (c.pid != 0 && int(credential.Pid) != c.pid) {
		return response, nil, errors.New("unexpected UDP broker peer")
	}
	request.Nonce = c.nonce
	data, err := json.Marshal(request)
	if err != nil {
		return response, nil, err
	}
	if _, err := socket.Write(data); err != nil {
		return response, nil, err
	}
	wire, ancillary := make([]byte, 1<<20), make([]byte, unix.CmsgSpace(8*4))
	n, oobn, flags, _, err := socket.ReadMsgUnix(wire, ancillary)
	if err != nil {
		return response, nil, err
	}
	var files []*os.File
	keep := false
	defer func() {
		if !keep {
			for _, file := range files {
				file.Close()
			}
		}
	}()
	messages, err := unix.ParseSocketControlMessage(ancillary[:oobn])
	if err != nil {
		return response, nil, err
	}
	for _, message := range messages {
		fds, err := unix.ParseUnixRights(&message)
		if err != nil {
			return response, nil, err
		}
		for _, fd := range fds {
			unix.CloseOnExec(fd)
			files = append(files, os.NewFile(uintptr(fd), "broker-packet-channel"))
		}
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return response, nil, errors.New("truncated UDP broker response")
	}
	if err := json.Unmarshal(wire[:n], &response); err != nil {
		return response, nil, err
	}
	if response.Error != "" {
		return response, nil, errors.New(response.Error)
	}
	if response.PID != int(credential.Pid) || (c.startTicks != 0 && response.StartTicks != c.startTicks) {
		return response, nil, errors.New("UDP broker process identity changed")
	}
	keep = true
	return response, files, nil
}

func brokerToken(value string) ([32]byte, error) {
	var token [32]byte
	data, err := hex.DecodeString(value)
	if err != nil || len(data) != len(token) {
		return token, errors.New("invalid broker token")
	}
	copy(token[:], data)
	return token, nil
}
