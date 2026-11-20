//go:build !linux

package handoff

import "syscall"

// The managed-process pilot uses Linux inherited descriptors. Ordinary
// transport builds on other platforms retain their portable TCP primitive.
func validateListeningSocket(syscall.Conn) error { return nil }
