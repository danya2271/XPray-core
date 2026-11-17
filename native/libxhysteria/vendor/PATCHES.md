# Native Hysteria dependency patches

`quinn-proto` is based on the crates.io 0.11.19 release (MIT/Apache-2.0).
Its congestion interface exposes native Brutal pacing, exact lost-packet
accounting, a send-window check and a maximum number of datagrams per transmit.
The last hook permits GSO for BBR/Reno while restricting negotiated Brutal to
one datagram, so batching cannot bypass its byte budget.

The BBR startup condition compares the current congestion window with its
target, rather than comparing the dimensionless gain with a byte count.
ACK aggregation also skips excess-byte accounting until a nonzero bandwidth
estimate exists. Together these prevent an application-limited connection
from growing its congestion window with every acknowledged byte despite
having no bandwidth estimate. The vendored unit suite includes a regression
that acknowledges 10,000 sparse packets and checks the window stays bounded.
The original code grew it from 240,000 to 12,240,000 bytes in that test.

`h3-quinn` is copied from the crates.io 0.0.10 release (MIT). Two methods expose
the underlying Quinn send/receive streams only when no HTTP/3 operation is
pending. After Hysteria TCP classification and header parsing, the native
server uses these streams directly. HTTP/3 authentication and control streams
continue through the original adapter. The prefix consumed during
classification is replayed before the raw stream. No change to wire framing,
TLS verification, or QUIC ordering is made.

Both versions are pinned in the parent manifest and lockfile. Preserve the
included upstream licenses when distributing these sources.
