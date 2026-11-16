# Native Hysteria dependency patches

`quinn-proto` is based on the crates.io 0.11.19 release (MIT/Apache-2.0).
Its congestion interface exposes native Brutal pacing, exact lost-packet
accounting, a send-window check and a maximum number of datagrams per transmit.
The last hook permits GSO for BBR/Reno while restricting negotiated Brutal to
one datagram, so batching cannot bypass its byte budget. Existing fallback
controllers retain their original behavior.

`h3-quinn` is copied from the crates.io 0.0.10 release (MIT). Two methods expose
the underlying Quinn send/receive streams only when no HTTP/3 operation is
pending. After Hysteria TCP classification and header parsing, the native
server uses these streams directly. HTTP/3 authentication and control streams
continue through the original adapter. The prefix consumed during
classification is replayed before the raw stream. No change to wire framing,
TLS verification, or QUIC ordering is made.

Both versions are pinned in the parent manifest and lockfile. Preserve the
included upstream licenses when distributing these sources.
