//go:build !linux

package handoff

import "net"

// The managed-process pilot uses Linux inherited descriptors. Ordinary
// transport builds on other platforms retain their portable TCP primitive.
func validateListeningSocket(*net.TCPListener) error { return nil }
