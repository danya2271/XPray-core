//go:build linux

package handoff

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

const tcpProcessTestEnv = "XRAY_TEST_HANDOFF_TCP_CHILD"

func TestInheritedTCPListenerAcrossProcesses(t *testing.T) {
	if value, ok := os.LookupEnv(tcpProcessTestEnv); ok {
		runTCPChild(t, value)
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	raw, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	file, err := raw.File()
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	start := func(generation string) (command func(string), wait func() error) {
		t.Helper()
		child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInheritedTCPListenerAcrossProcesses$")
		child.Env = append(os.Environ(), tcpProcessTestEnv+"="+generation)
		child.ExtraFiles = []*os.File{file}
		child.Stderr = os.Stderr
		in, err := child.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		out, err := child.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		wait = func() error {
			if child.ProcessState != nil {
				return nil
			}
			return child.Wait()
		}
		t.Cleanup(func() {
			in.Close()
			if child.ProcessState == nil {
				child.Process.Kill()
				_ = wait()
			}
		})
		scanner := bufio.NewScanner(out)
		command = func(action string) {
			t.Helper()
			if action != "ready" {
				if _, err := fmt.Fprintln(in, action); err != nil {
					t.Fatal(err)
				}
			}
			if !scanner.Scan() || scanner.Text() != action {
				t.Fatalf("generation %s did not acknowledge %s: %q (%v)", generation, action, scanner.Text(), scanner.Err())
			}
		}
		command("ready")
		return command, wait
	}
	old, oldWait := start("A")
	next, _ := start("B")
	dial := func() net.Conn {
		t.Helper()
		c, err := (&net.Dialer{}).DialContext(ctx, "tcp4", raw.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		c.SetDeadline(time.Now().Add(3 * time.Second))
		return c
	}
	check := func(c net.Conn, generation, value byte) {
		t.Helper()
		if _, err := c.Write([]byte{value}); err != nil {
			t.Fatal(err)
		}
		var got [2]byte
		if _, err := io.ReadFull(c, got[:]); err != nil || got != [2]byte{generation, value} {
			t.Fatalf("got %q, %v; expected %q", got, err, [2]byte{generation, value})
		}
	}
	a := dial()
	check(a, 'A', '1')
	old("pause")
	b := dial() // Arrives while both generations are paused.
	next("resume")
	check(b, 'B', '2')
	check(a, 'A', '3')
	a.Close()
	old("exit")
	if err := oldWait(); err != nil {
		t.Fatal(err)
	}
	check(b, 'B', '4')
}

func runTCPChild(t *testing.T, generation string) {
	t.Helper()
	if generation != "A" && generation != "B" {
		t.Fatal("invalid test child generation")
	}
	file := os.NewFile(3, "inherited TCP listener")
	listener, err := ImportTCPListener(file)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				var msg [1]byte
				for {
					if _, err := io.ReadFull(conn, msg[:]); err != nil {
						return
					}
					if _, err := conn.Write([]byte{generation[0], msg[0]}); err != nil {
						return
					}
				}
			}()
		}
	}()
	if generation == "A" {
		if err := listener.Resume(); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("ready")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		switch action := scanner.Text(); action {
		case "pause":
			err = listener.Pause()
		case "resume":
			err = listener.Resume()
		case "exit":
			fmt.Println(action)
			return
		default:
			t.Fatalf("bad command %q", action)
		}
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(scanner.Text())
	}
}
