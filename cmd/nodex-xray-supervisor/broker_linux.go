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
	Nonce    [32]byte        `json:"nonce"`
	Bindings []brokerBinding `json:"bindings"`
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
}

type nativePacketIngress interface {
	packetIngress
	AddDirectRing(byte) ([]*os.File, error)
	AdoptReceive([]byte) error
	ReleasePublic(byte, [16]byte) (bool, error)
}

type brokerPeer struct {
	files    []*os.File
	fence    [16]byte
	retired  bool
	admitted bool
}

type brokerEndpoint struct {
	config  brokerBinding
	public  *net.UDPConn
	ingress packetIngress
	peers   map[byte]*brokerPeer
	current byte
	direct  bool
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
	Error      string         `json:"error,omitempty"`
	Ready      bool           `json:"ready"`
	PID        int            `json:"pid"`
	StartTicks uint64         `json:"start_ticks"`
	Current    byte           `json:"current"`
	Direct     bool           `json:"direct"`
	Binding    *brokerBinding `json:"binding,omitempty"`
}

func newPacketBroker(config brokerConfig, files []*os.File) (*packetBroker, error) {
	if config.Nonce == [32]byte{} || len(files) == 0 || len(files) != len(config.Bindings) || len(files) > 128 {
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
	}
	keep = true
	return b, nil
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
	case "add":
		if peer == nil {
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
			wire := make([]byte, 1<<20)
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
			response, files := b.command(ctx, request)
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
}

func (c brokerClient) command(ctx context.Context, request brokerRequest) (brokerResponse, []*os.File, error) {
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
