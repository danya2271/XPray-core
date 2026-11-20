package handoff

import (
	"errors"
	"io"
	"os"
)

// AwaitStartup prevents an unjournaled candidate from initializing transport
// or accepting work. Closing the supervisor's pipe before commit fails closed.
func AwaitStartup(manifest Manifest) error {
	if manifest.StartupFD == 0 {
		return nil // Standalone handoff callers need not use the launch barrier.
	}
	fd := manifest.StartupFD
	if fd < 3 {
		return errors.New("invalid startup descriptor")
	}
	for _, binding := range manifest.TCP {
		if binding.FD == fd {
			return errors.New("startup descriptor aliases TCP listener")
		}
	}
	for _, binding := range manifest.Unix {
		if binding.FD == fd {
			return errors.New("startup descriptor aliases Unix listener")
		}
	}
	for _, binding := range manifest.UDP {
		if binding.FD == fd || binding.TransmitFD == fd {
			return errors.New("startup descriptor aliases UDP socket")
		}
		for _, ring := range binding.RingFDs {
			if ring == fd {
				return errors.New("startup descriptor aliases receive ring")
			}
		}
	}
	f := os.NewFile(uintptr(fd), "nodex-startup-gate")
	defer f.Close()
	var commit [1]byte
	if _, err := io.ReadFull(f, commit[:]); err != nil {
		return err
	}
	if commit[0] != 1 {
		return errors.New("invalid startup commit")
	}
	return nil
}
