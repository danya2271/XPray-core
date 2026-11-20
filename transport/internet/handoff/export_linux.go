package handoff

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"strings"

	"golang.org/x/sys/unix"
)

// ExportedListeners is a stable-generation recovery snapshot. The private
// control connection's peer credentials identify the exporting process.
type ExportedListeners struct {
	Manifest Manifest
	Files    []*os.File
	PID      int
}

func (e *ExportedListeners) Close() {
	for _, f := range e.Files {
		_ = f.Close()
	}
	e.Files = nil
}

func socketPeer(conn *net.UnixConn) (*unix.Ucred, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	var credential *unix.Ucred
	var credErr error
	err = raw.Control(func(fd uintptr) {
		credential, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	return credential, errors.Join(err, credErr)
}

func exportControl(conn net.Conn, g *Generation) {
	socket, ok := conn.(*net.UnixConn)
	if !ok {
		return
	}
	peer, err := socketPeer(socket)
	if err != nil || peer.Uid != uint32(os.Geteuid()) {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	// Recovery of a switching/draining or UDP generation needs additional
	// transactions. Never export a partial listener set as a complete one.
	if g.state != "active" || len(g.udp) != 0 {
		return
	}
	keys := make([]bindingKey, 0, len(g.bindings))
	for key := range g.bindings {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b bindingKey) int {
		if order := strings.Compare(a.tag, b.tag); order != 0 {
			return order
		}
		return a.port - b.port
	})
	manifest := Manifest{Control: g.control}
	files := make([]*os.File, 0, len(keys))
	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()
	fds := make([]int, 0, len(keys))
	for i, key := range keys {
		file, err := g.bindings[key].listener.File()
		if err != nil {
			return
		}
		files = append(files, file)
		fds = append(fds, int(file.Fd()))
		if key.port == 0 {
			manifest.Unix = append(manifest.Unix, UnixBinding{Tag: key.tag, Path: g.bindings[key].listener.Addr().String(), FD: i + 3})
		} else {
			manifest.TCP = append(manifest.TCP, TCPBinding{Tag: key.tag, Port: key.port, FD: i + 3})
		}
	}
	// The one-byte preamble carries all rights. JSON follows in the stream,
	// avoiding partial ancillary-data delivery with a large JSON write.
	if n, _, err := socket.WriteMsgUnix([]byte{'F'}, unix.UnixRights(fds...), nil); err != nil || n != 1 {
		return
	}
	_ = json.NewEncoder(socket).Encode(manifest)
}

// ExportListeners imports copies of the same listening sockets. It never
// changes admission, rebinds a port, or transfers any connected socket.
func ExportListeners(ctx context.Context, control ControlSocket) (*ExportedListeners, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", string(control))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	socket := conn.(*net.UnixConn)
	stop := context.AfterFunc(ctx, func() { socket.Close() })
	defer stop()
	credential, err := socketPeer(socket)
	if err != nil || credential.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("generation export must use the same service account")
	}
	if _, err := io.WriteString(socket, "export-listeners\n"); err != nil {
		return nil, err
	}
	result := &ExportedListeners{PID: int(credential.Pid)}
	keep := false
	defer func() {
		if !keep {
			result.Close()
		}
	}()
	var preamble [1]byte
	oob := make([]byte, unix.CmsgSpace(128*4))
	n, oobn, flags, _, err := socket.ReadMsgUnix(preamble[:], oob)
	if err != nil {
		return nil, err
	}
	messages, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, err
	}
	for _, message := range messages {
		fds, err := unix.ParseUnixRights(&message)
		if err != nil {
			return nil, err
		}
		for _, fd := range fds {
			unix.CloseOnExec(fd)
			result.Files = append(result.Files, os.NewFile(uintptr(fd), "xray-recovered-listener"))
		}
	}
	if n != 1 || preamble[0] != 'F' || flags&unix.MSG_CTRUNC != 0 ||
		len(result.Files) == 0 || len(result.Files) > 128 {
		return nil, errors.New("invalid or truncated generation listener export")
	}
	result.Manifest, err = ReadManifest(socket)
	if err != nil {
		return nil, err
	}
	// JSON is complete on EOF because the server closes the control connection.
	count := len(result.Manifest.TCP) + len(result.Manifest.Unix)
	if len(result.Manifest.UDP) != 0 || count != len(result.Files) || result.Manifest.Control != string(control) {
		return nil, errors.New("incomplete generation listener export")
	}
	keep = true
	return result, nil
}
