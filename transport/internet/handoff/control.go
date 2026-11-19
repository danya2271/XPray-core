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
	Protocol int    `json:"protocol"`
	State    string `json:"state"`
	Error    string `json:"error,omitempty"`
}

// ListenControl never replaces an existing socket. The caller must create a
// fresh private directory for the generation under a trusted runtime path.
func ListenControl(path string, g *Generation) (net.Listener, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("handoff control socket requires a clean absolute path")
	}
	directory, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !directory.IsDir() || directory.Mode().Perm() != 0700 {
		return nil, errors.New("handoff control socket requires a private directory (0700)")
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

func serveControl(conn net.Conn, generation *Generation) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	input := bufio.NewScanner(conn)
	input.Buffer(make([]byte, 64), 64)
	if !input.Scan() {
		return
	}
	state, err := generation.Command(input.Text())
	response := Response{Protocol: ProtocolVersion, State: state}
	if err != nil {
		response.Error = err.Error()
	}
	json.NewEncoder(conn).Encode(response)
}
