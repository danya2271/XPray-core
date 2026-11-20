package handoff

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"
)

const ProtocolVersion = 1

type Response struct {
	Protocol   int                            `json:"protocol"`
	State      string                         `json:"state"`
	Error      string                         `json:"error,omitempty"`
	UDP        map[string]UDPConnectionStatus `json:"udp,omitempty"`
	UDPManaged bool                           `json:"udp_managed,omitempty"`
}

type UDPConnectionStatus struct {
	Known bool   `json:"known"`
	Count uint64 `json:"connections"`
	// Diagnostic snapshot only; does not include all queued datagrams or
	// pending Initials. Backends may count the same connection twice.
	WorkKnown     bool   `json:"work_known,omitempty"`
	TransportWork uint64 `json:"transport_work,omitempty"`
}

// ListenControl never replaces an existing socket. The caller must create a
// fresh private directory for the generation under a trusted runtime path.
func ListenControl(path string, g *Generation) (net.Listener, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("handoff control socket requires a clean absolute path")
	}
	if err := PrivateSocketDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	go func() {
		for {
			conn, err := listener.AcceptUnix()
			if err != nil {
				return
			}
			serveControl(conn, g)
		}
	}()
	return listener, nil
}

func PrivateSocketDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("handoff sockets require a clean absolute directory path")
	}
	directory, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !directory.IsDir() || directory.Mode().Perm() != 0700 {
		return errors.New("handoff sockets require a private directory (0700)")
	}
	return nil
}

func serveControl(conn net.Conn, generation *Generation) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	input := bufio.NewScanner(conn)
	input.Buffer(make([]byte, 128), 4096)
	if !input.Scan() {
		return
	}
	if input.Text() == "export-listeners" {
		exportControl(conn, generation)
		return
	}
	if input.Text() == "quic-status" {
		state, counts := generation.UDPStatus()
		json.NewEncoder(conn).Encode(Response{Protocol: ProtocolVersion, State: state, UDP: counts, UDPManaged: len(counts) != 0})
		return
	}
	state, err := generation.Command(input.Text())
	response := Response{Protocol: ProtocolVersion, State: state, UDPManaged: len(generation.udp) != 0}
	if err != nil {
		response.Error = err.Error()
	}
	json.NewEncoder(conn).Encode(response)
}
