# Codex Handoff: Native Hysteria Stability

Date: 2026-11-16
Workspace: `/home/danya2271/ProxVless/XPray-core`

## Current Request

The user reports that Hysteria on RU servers has become unreliable or appears
not to work, and that the client is also unstable. Continue by diagnosing and
fixing the native server path first, without assuming the latest logs prove a
single root cause. The user expects implementation, tests, and commits.

## Repository State

HEAD is `40b0b6be Allow native Hysteria handshake to complete`.

Recent relevant commits:

- `40b0b6be`: raise native client startup/fallback deadline to 11 seconds;
  Rust QUIC/auth startup allows 10 seconds and the Go bridge allows 12.
- `545fbd83`: estimate native server connection/flow limits from host or cgroup
  memory and `GOMAXPROCS`; server receive window set to 8 MiB.
- `0f7e7cfa`: native outbound startup fallback to Go with per-peer cooldown.

There is an unrelated, user-owned modification in
`transport/internet/splithttp/client.go`. Do not modify or commit it.
At handoff, no other uncommitted changes were present.

## Evidence From VPS Logs

- Recent native server startup selected
  `memory_bytes=929935360 memory_source=host gomaxprocs=1 max_connections=16
  max_flows=32`.
- Logs show multiple `authenticated role=server` events and server connection
  TX/RX counters increasing, including multi-megabyte transfers. Therefore the
  native inbound starts and some sessions authenticate and pass traffic; this
  does not establish that all clients/flows work.
- Older systemd journal entries explicitly reported `Failed with result
  'oom-kill'` and `status=9/KILL`.
- A later cgroup snapshot showed `MemoryMax=infinity`, `memory.events` counters
  at zero, and about 126 MB current usage. That later snapshot does not rule
  out a prior host-level OOM kill.
- Some client peers showed rising QUIC packet losses/RTT, but logs alone do not
  establish whether those are route quality or native regressions.

## High-Confidence Server Risk To Investigate

The native server limits are currently very restrictive on a one-CPU VPS:
16 concurrent QUIC connections and 32 total TCP/UDP flows per listener.
`Bus.max_flows` is shared across the native listener, not per client session.

In `native/libxhysteria/src/engine.rs`, the `ep.accept()` select branch is
guarded by `connection_slots.available_permits() != 0`. Once the limit is
reached, the engine stops polling `ep.accept()` entirely. New handshakes can
sit unanswered and time out, with no explicit saturation log. This can make
the server look completely dead during bursts even while established sessions
continue passing traffic.

The auto-tuner also caps both limits by `GOMAXPROCS` (`16 * CPUs` connections,
`32 * CPUs` flows), though CPU count is not a good concurrency limit by itself.
Do not simply raise all limits: this VPS previously had OOM kills.

Recommended next implementation:

1. Always poll `ep.accept()` on a native server. If no connection permit is
   available, explicitly reject/refuse that `Incoming` using Quinn's API and
   emit a rate-limited diagnostic with active/maximum connection counts. Do not
   silently stop accepting.
2. Add current/max active connection and flow counters to server diagnostics.
   Make flow-cap rejection observable too.
3. Revisit auto-tuning to provide useful concurrency on 1-GiB hosts while
   retaining a measured memory budget. Treat CPU count as a worker/performance
   input, not a hard cap on accepted sessions. Keep env overrides:
   `XRAY_HYSTERIA_SERVER_CONNECTIONS` and `XRAY_HYSTERIA_SERVER_FLOWS`.
4. Add an interop test that opens more than the default configured connection
   and flow limits, verifies excess connections are rejected promptly rather
   than hanging, and verifies the listener recovers when slots free.
5. Run Rust tests, Go native race tests, full interop tests, and Linux release
   build. Commit only task-owned paths; leave the `splithttp/client.go` change
   untouched.

Useful existing test:
`native/libxhysteria/interop/server_test.go`
contains `TestNativeServerBeyondPrototypeFlowCap`; inspect whether its
expectations still match the newly configured limits.

## Client Change Already Made

The fallback timeout is now 11 seconds, greater than the Rust 10-second
QUIC/auth deadline and less than the 12-second bridge startup timeout. This
prevents prematurely routing slow native startups through Go. Tests passed:

- `go test -race ./transport/internet/hysteria`
- `CGO_ENABLED=1 go test -race -tags xhysteria_native
  ./transport/internet/hysteria`

This does not establish end-to-end client stability on RU routes.

## Diagnostic / Rollback Notes

`XRAY_NET_DIAG=1` logs the selected server resource limits, authentication,
active QUIC peer counters, and native outbound fallback. Native server and Go
fallback are separate concerns: automatic fallback exists for outbound dials,
not for an already-started inbound listener.

For emergency rollback, `XRAY_HYSTERIA_NATIVE=0` forces Go for the whole Xray
process, including native-selected outbounds; prefer per-inbound config rollback
if preserving native outbound behavior is important. Do not make remote VPS
changes from this workspace.
