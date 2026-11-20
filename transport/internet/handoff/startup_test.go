package handoff

import (
	"golang.org/x/sys/unix"
	"os"
	"testing"
)

func TestStartupRequiresExplicitCommit(t *testing.T) {
	for _, commit := range [][]byte{nil, {0}, {1}} {
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		if len(commit) != 0 {
			if _, err := write.Write(commit); err != nil {
				t.Fatal(err)
			}
		}
		write.Close()
		fd, duplicateErr := unix.Dup(int(read.Fd()))
		if duplicateErr != nil {
			t.Fatal(duplicateErr)
		}
		read.Close()
		err = AwaitStartup(Manifest{StartupFD: fd})
		// AwaitStartup owns this descriptor.
		if (err == nil) != (len(commit) == 1 && commit[0] == 1) {
			t.Fatalf("commit %v: %v", commit, err)
		}
	}
	if err := AwaitStartup(Manifest{StartupFD: 7, TCP: []TCPBinding{{FD: 7}}}); err == nil {
		t.Fatal("accepted aliased startup pipe")
	}
}
