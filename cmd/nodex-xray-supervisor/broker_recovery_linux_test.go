//go:build linux && !android

package main

import (
	"fmt"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestProcessStatusGoneRecognizesProcExitRace(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		gone bool
	}{
		{"present", nil, false},
		{"removed before open", &os.PathError{Op: "open", Path: "/proc/123/stat", Err: unix.ENOENT}, true},
		{"exited during read", &os.PathError{Op: "read", Path: "/proc/123/stat", Err: unix.ESRCH}, true},
		{"wrapped exit", fmt.Errorf("identity check: %w", unix.ESRCH), true},
		{"permission denied", &os.PathError{Op: "read", Path: "/proc/123/stat", Err: unix.EACCES}, false},
		{"other I/O failure", &os.PathError{Op: "read", Path: "/proc/123/stat", Err: unix.EIO}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if gone := processStatusGone(test.err); gone != test.gone {
				t.Fatalf("processStatusGone(%v)=%t, want %t", test.err, gone, test.gone)
			}
		})
	}
}
