# Native Hysteria 2 migration plan

Status: corrected architecture proposal, checked against this checkout and
upstream sources on 2026-11-15. This document does not implement or enable a
native backend. Performance targets below require measurement.

## 1. Stack decision

Prefer Rust for a new `libxhysteria` implementation, subject to a working
interoperability and Brutal prototype. Build a Rust `staticlib` with a small C
ABI, independent native workers, `quinn-proto`, rustls, and an explicitly
selected crypto provider. Choose a maintained BLAKE2b-256 implementation.

This preference is an engineering judgment about maintaining parsers, buffer
ownership, and concurrent teardown. It is not a prediction that Rust will be
faster than C++, or that changing languages will outperform the existing Go
implementation. Both languages can implement the same batching and C ABI.

| Area | Rust / Quinn | C++ / lsquic |
| --- | --- | --- |
| UDP ownership | `quinn-proto` contains protocol logic without socket I/O; the high-level `quinn` API also exposes `AsyncUdpSocket` | Application supplies packet input and output |
| Brutal | Public controller interface exists, but exact pacing and packet-loss accounting need extensions in the inspected implementation | `struct cong_ctl_if` is internal; adding Brutal requires a maintained source patch or an upstream extension |
| HTTP/3 | Use a maintained HTTP/3/QPACK implementation with an adapter that can separate Hysteria streams | lsquic includes HTTP/3/QPACK; verify custom stream dispatch rather than assuming its HTTP mode accepts `0x401` |
| Memory safety | Keep parser and session code safe; audit the arena, OS handles, and FFI separately | Explicitly audit parser bounds, lifetimes, concurrency, and allocator pairing |
| Integration | Static library plus C ABI and CGO | Static library plus C ABI and CGO |

The Quinn controller and pacer were inspected at tag `quinn-0.11.12`. Its
controller reports a window; its internal pacer derives a rate from that window
and RTT. `ControllerMetrics.pacing_rate` is a reporting field, not a pacing
override. Loss callbacks expose lost bytes rather than the exact lost-packet
count required by the existing Brutal algorithm.

Budget an explicit patch for independently setting the pacing rate and obtaining
packet accounting. Prefer adding defaulted hooks that preserve existing
controllers. Pin the resulting revision, test the patch separately, and pursue
upstream support. Do not substitute a byte-based loss estimate without treating
that as an algorithm change.

C++ remains a valid alternative if the team prefers it. In that case, pin
lsquic, its submodules, and a compatible BoringSSL revision; add Brutal to
lsquic's controller selection and storage. The original
`lsquic_cong_ctl_if` public-plugin assumption is incorrect: the inspected
internal type is `struct cong_ctl_if`.

For either stack, prove HTTP/3 plus raw Hysteria stream dispatch before freezing
the dependency choice. Do not build a new TLS or QUIC implementation.

The inspected Quinn tag includes BBR, but its source labels that controller
experimental. Its existence does not establish equivalence to this fork's BBR
implementation and profiles. Test or port the required behavior explicitly.

## 2. Scope and existing dependencies

Move these operations into native workers:

- Tunnel UDP socket I/O, Salamander, QUIC/TLS, congestion control, and pacing.
- Hysteria authentication, TCP request/response framing, and UDP encapsulation.
- UDP fragmentation, reassembly, session expiration, and resource limits.

Keep Xray configuration, routing, user management, policy, logging, and traffic
statistics behind a thin Go adapter. Application UDP payloads cross that adapter
as distinct datagrams with their addresses. Routing arbitrary UDP through Xray
still requires Go involvement; moving Hysteria does not move all Xray outbounds
or the routing engine into native code.

The current integration points are:

- `transport/internet/hysteria/dialer.go`: socket setup, authentication,
  connection reuse, negotiated congestion control, and UDP session creation.
- `transport/internet/hysteria/hub.go`: HTTP/3 masquerading, authentication,
  stream dispatch, and listener setup.
- `proxy/hysteria/protocol.go`: TCP framing and UDP fragmentation/reassembly.
- `proxy/hysteria/server.go`: user context and `dispatcher.DispatchLink()`.
- `transport/internet/finalmask/salamander/`: Salamander is a separate mask,
  outside the two Hysteria packages.

The Hysteria congestion packages are also imported by XHTTP, MASQUE, xdrive, and
DoH. Keep those Go packages while they have consumers, or relocate them in a
separate change that updates every import. Native Hysteria alone will not remove
`github.com/apernet/quic-go` from `go.mod`.

Preserve the configuration schema and register an opt-in native backend during
migration. Existing transport names can remain compatibility adapters. Remove
the old implementation only after the supported feature matrix passes.

## 3. Protocol compatibility

### Authentication and HTTP/3

Use ALPN `h3`, real certificate validation, HTTP/3 control streams, SETTINGS, and
QPACK handling. A fixed byte array for `/auth` is insufficient for a server that
must accept independently implemented clients.

A literal-only QPACK encoder with no dynamic table can be an implementation
choice, but the decoder must handle encodings allowed by the advertised
settings, including Huffman strings. Let a maintained implementation handle
control streams, critical-stream lifetime, and protocol errors.

Authenticate `POST https://hysteria/auth`; process `Hysteria-Auth` and
`Hysteria-CC-RX`. A successful response includes status `233`, `Hysteria-UDP`,
and the server's receive-rate policy. Support the defined unknown/unlimited
values and the server's `auto` response through an explicit negotiation policy.
Client RX `0` means unknown, server RX `0` means unlimited, and server `auto`
requires adaptive congestion control. The current fork falls back to BBR on a
zero peer rate and parses `auto` as zero; preserve or deliberately revise that
policy, rather than treating those three wire values as interchangeable.

Do not hard-code authentication to QUIC stream ID 0, classify all IDs greater
than or equal to 4 as TCP, or stop handling HTTP/3 after authentication. Stream
direction, stream type, the initial application bytes, and authentication state
determine dispatch.

Unauthenticated requests retain configured HTTP/3 masquerading behavior.
Preserve the fork's `404`, file, proxy, and string modes, or explicitly reject
unsupported configurations while the native backend is opt-in. An invalid
password must not automatically produce a unique native-only response pattern.

### TCP

Decode incrementally across arbitrary QUIC read boundaries:

```text
request  = varint(0x401) | varint(address_len) | host:port
           | varint(padding_len) | padding
response = uint8(status) | varint(message_len) | message
           | varint(padding_len) | padding
```

Account for the TCP response before exposing raw response payload to Go.
Represent routing/open success and failure explicitly at the bridge. Match the
current routing/response timing deliberately; do not silently change it.

Preserve stream ordering, partial writes, FIN, independent read/write shutdown,
reset codes, cancellation, and deadlines. Bound header sizes, padding, and time
spent waiting for an incomplete header. Start with the checkout's existing
address/message/padding limits.

### UDP

Use QUIC DATAGRAM with this Hysteria payload:

```text
uint32_be(session_id) | uint16_be(packet_id)
| uint8(fragment_id) | uint8(fragment_count)
| varint(address_len) | host:port | payload
```

These are QUIC DATAGRAM payloads, not HTTP Datagram context-ID payloads.
Retain per-datagram addresses and boundaries in both bridge directions.

Key reassembly by `(connection, session_id, packet_id)`, and validate fragment
count, index, address consistency, duplicates, total size, and expiration.
Packet IDs alone are not unique across sessions or connections. Use bounded
storage and deterministic eviction; an unbounded hash table plus a timer is
insufficient.

The checkout currently allows 4096-byte UDP payloads, retains eight incomplete
packets per `Defragger`, and uses a 10-second fragment TTL. Preserve those
defaults initially; a 3-second TTL or larger datagram limit is a separately
tested behavior change. A connection-wide replacement needs per-session and
global budgets rather than one unrestricted table.

The 4096-byte payload cap is a fork limit, not a Hysteria protocol limit.
Specify the policy for larger and zero-length UDP payloads separately; the
current parser discards an empty payload. Include both cases in compatibility
tests instead of silently inheriting that restriction.

Derive outgoing fragment sizes from the current QUIC DATAGRAM payload limit,
including the Hysteria header and address. Account for Salamander overhead in
the outer path MTU. Preserve UDP enablement negotiation and idle expiration;
the protocol has no explicit UDP-session-close message.

## 4. Exact Brutal behavior

Port the local implementation in
`transport/internet/hysteria/congestion/brutal/brutal.go`, including the pacer in
`congestion/common/pacer.go`. The proposed formula changes its behavior.

Let `B` be the selected sending rate in **bytes per second**, `A` acknowledged
packets, and `L` lost packets in the five timestamped one-second buckets:

```text
ack_rate = 1                         if A + L < 50
ack_rate = max(A / (A + L), 0.8)     otherwise
pacing_bytes_per_second = B / ack_rate
cwnd_bytes = max(max_datagram_size, 2 * B * smoothed_rtt_seconds / ack_rate)
```

When loss compensation is disabled, `ack_rate = 1`. Before a usable RTT sample,
the current implementation returns a 10240-byte congestion window.

Consequently compensation is capped at 1.25 times `B`. There is no additional
1.25 multiplier, and clamping loss to 80% would implement a different cap.
Convert units explicitly at each native controller interface.

Preserve bucket rotation, monotonic timestamps, the minimum sample count,
burst budget, MTU updates, and pacing-deadline rounding. Count packet events,
not bytes divided by an assumed MTU. Test TCP traffic and DATAGRAM-only traffic.

Negotiate the local send limit against the peer's receive limit as the current
client/server do. Preserve BBR fallback and the explicit `force-brutal` and
loss-compensation settings; validate invalid zero-rate configurations.

Keep pacing independent of the congestion window. A deliberately large window
cannot safely encode the desired rate in stock Quinn's pacer. Preserve QUIC flow
control and anti-amplification limits even when Brutal permits sending.

## 5. Native I/O and workers

On Linux/Android, use nonblocking UDP and bounded `recvmmsg()`/`sendmmsg()`
batches where available. A batch size of 64 is a starting parameter, not a
throughput guarantee. Handle partial sends, `EINTR`, `EAGAIN`, truncated input,
ECN/local-address metadata, timers, and MTU changes.

Provide a separate Winsock implementation on Windows. Do not assume Linux batch
syscalls or `SO_REUSEPORT` behavior there. Disable unsupported GSO/GRO paths
explicitly until the Salamander adapter handles each UDP segment correctly.

Implement Salamander as the specified unkeyed BLAKE2b with a 32-byte output over
`PSK || salt`, followed by XOR with the repeated digest. It is neither keyed
BLAKE2 nor a truncated 64-byte BLAKE2b digest. Generate a fresh random 8-byte salt
for each outgoing datagram and support in-place decoding with checked lengths.

Start with a correct scalar XOR implementation and compiler optimization.
Introduce AVX2 only with runtime dispatch and a scalar fallback; validate any
NEON path separately. Benchmark hashing, crypto, copying, and syscalls before
assuming manual XOR SIMD is the bottleneck.

Start with one native worker and one connection owner. Scale using measured CPU,
memory, and connection counts. If adding Linux reuseport workers, define
connection-ID ownership and cross-worker forwarding for migration/rebinding;
reuseport alone does not solve QUIC connection routing. A single QUIC connection
does not automatically use every core.

## 6. Shared arena and ownership

This is native heap memory shared within one process, not necessarily an OS
shared-memory mapping. It reduces language transitions; it does not eliminate
QUIC encryption, reassembly, retransmission storage, or all memory copies.

Allocate with an explicit alignment/layout contract. Rust can use
`std::alloc::Layout`; C++ needs platform-specific aligned allocation and the
matching deallocator. Do not assume one `aligned_alloc()`/`free()` pair works
on every Windows toolchain.

Use separate RX and TX slab pools. Maintain one producer and one consumer for
each ring **per worker**:

| Ring | Producer | Consumer | Purpose |
| --- | --- | --- | --- |
| Commands | Go dispatcher | Native worker | Open, write, finish, reset, credit/config updates |
| Events | Native worker | Go dispatcher | Open/result, payload, FIN/reset, session/connection status |
| RX releases | Go dispatcher | Native worker | Return RX slabs after Go finishes reading |
| TX releases | Native worker | Go dispatcher | Return TX slabs after native references are gone |

Many Xray goroutines submit to the Go dispatcher through bounded Go queues.
They cannot all publish directly to an SPSC command ring. Multiple workers
cannot share one SPSC event ring.

Use configurable power-of-two ring capacities; 4096 slots is a candidate.
Size each release ring for at least its pool's maximum outstanding slabs.
Drain releases independently of full payload queues. Reserve control progress
so full data rings cannot prevent credit return, cancellation, or shutdown.
Ring capacity and slab count are independent parameters: 4096 slabs of 64 KiB
would consume 256 MiB for just one pool, which is not a sensible assumed mobile
default. Calculate and expose the actual per-worker memory budget.

RX ownership is `native free -> native filling -> published -> Go reading
-> RX release -> native free`. TX ownership is `Go free -> Go filling
-> published -> native consuming -> TX release -> Go free`.

Publishing a write is not permission to reuse its slab. If QUIC retains slab
references for retransmission, release waits until those references disappear.
If native code copies into transport-owned storage, release can follow that
copy. `TX_RELEASE` means reusable memory, not peer delivery.

Avoid the original shared bitmap in the first implementation. Its bit convention
was undefined: clearing a bit frees a slot only if a set bit means occupied.
Release rings make ownership explicit without a contended cross-language bitmap.

### ABI contract

Define a versioned C-compatible header and verify its layout from Go and native
code. Use fixed-width integers, explicit padding, offsets, and lengths. Use
`uint8_t` event kinds rather than compiler-dependent enum representations.
Do not expose C++ objects, Rust containers, Go pointers, or packed atomics.

The original mix of C++ enum syntax, C `_Atomic`, and GCC-only alignment
attributes is not a portable shared header. Define native atomic accessors over
a documented storage layout and restrict supported architectures/toolchains
until cross-language atomic interoperability is verified. Go uses `sync/atomic`;
native code uses the corresponding aligned atomic accesses, with acquire/release
publication. Never mix ordinary accesses with atomic accesses to the same index.
This is a platform ABI assumption, not a guarantee of either language's memory
model. Retain a batched FFI bridge as a correctness baseline if the required
contract cannot be established for a target. Rust shared storage also needs
interior mutability and scoped references; do not construct exclusive Rust
references spanning memory that Go can access concurrently. Neither the Go race
detector nor native sanitizers alone prove cross-language race freedom.

Check structure sizes, field offsets, alignment, lock-free atomic support,
endianness, ring index wraparound, and arena range arithmetic. A 64-byte
separation is an initial layout choice, not a universal cache-line guarantee.

Descriptors need a connection identity, a worker-local flow identity with
generation/reuse protection, a request correlation ID, slab/offset/length,
flags/error code, and address metadata. A QUIC stream ID alone repeats across
connections. A one-byte `addr_len` cannot represent the checkout's allowed
2048-byte addresses. Distinguish TCP flows from UDP sessions.

Include arena byte size, ABI version, capacities, pool geometry, and lifecycle
state in the view returned at initialization. Validate ranges before making
Go slices. The arena contains no retained pointers into Go-managed memory.

## 7. Notifications and batching

On Linux/Android, use two nonblocking, close-on-exec eventfds per worker, one for
each notification direction. Wrap the Go-owned notification endpoint with
`os.NewFile` only after nonblocking mode is set; read exactly eight bytes through
the pollable file API. Test registration, deadlines/cancellation, and closure on
the actual supported Go release.

Specify which component closes each descriptor and when native code stops
using it. Avoid duplicate owners closing the same handle. The arena remains
alive while either side can still access it.

Initially signal unconditionally once per published batch. Eventfd counters
coalesce notifications; an `EAGAIN` caused by counter saturation means a wake is
already pending. Drain all inbound/release queues before a blocking read; after
consuming a wake, return to draining the queues. Never consume a notification
and then sleep without checking for the corresponding published work.

Do not initially implement `if go_is_sleeping then signal`. That optimization
needs a separately proven arm/recheck/publish protocol to avoid lost wakeups.
Queues are the source of truth; a notification is only a hint to inspect them.

On Windows, use a connected loopback socket pair with the Go side represented
by `net.Conn`, and a native nonblocking endpoint. Define coalescing, writable
retry on `WouldBlock`, shutdown, and error handling. Do not assume that a raw
`CreatePipe` handle or arbitrary `os.NewFile` socket automatically obtains the
desired Go IOCP behavior.

For TCP, publish available bytes at the end of a ready batch, at a size cap, or
at a bounded latency deadline. Never wait to accumulate 32-64 KiB before serving
a small request. Apply per-flow fairness.

For UDP, batch descriptors while retaining individual datagram lengths and
addresses. Concatenating UDP payloads without framing destroys their semantics.

## 8. Go adapter and backpressure

Initially copy RX payload once into Xray-owned buffers, then return the native
slab through the RX release ring. Copy outgoing Xray buffers into TX slabs and
release the Go buffers once the copy succeeds. Track how much data is accepted
when a write spans several slabs.

This checkout has 4096-byte regular buffers and `buf.FromBytes()` for unmanaged
data; it has no `buf.NewWithUnmanaged()`. `Buffer.Release()` does not notify a
native owner when an unmanaged buffer is released. Wrapping a slab and returning
it immediately can therefore cause use-after-reuse.

End-to-end zero-copy RX is a later change requiring an audited release/ownership
mechanism across buffer splitting, merging, forwarding, and cancellation.
`unsafe.Slice` alone does not provide that mechanism.

Implement adapters for the existing connection and `buf.Reader`/`buf.Writer`
contracts, including deadlines and cancellation. Preserve inbound user identity,
VlessRoute, statistics, policy, and `DispatchLink()` metadata. Represent UDP
addresses through `Buffer.UDP` and preserve datagram boundaries.

Use bounded per-flow mailboxes and receive credits, including credit already
reserved for published/in-flight events. The global dispatcher must not block
on one slow stream. Pause native TCP application reads/credit advancement when
the consumer has no capacity; continue processing QUIC control traffic and other
flows. Apply explicit bounded drop/eviction policy to UDP.

Set per-worker, per-connection, per-flow, reassembly, and global memory budgets.
An arena bounds only its own storage; QUIC/TLS and Go queues have additional
memory use. Avoid claiming the whole engine performs no allocations.

## 9. Socket policy and lifecycle

Native-owned UDP must preserve the behavior currently reached through
`DialSystem`, `ListenSystemPacket`, external socket controllers, and finalmask.
Provide setup/control operations for source addresses, interface binding,
`SO_MARK`, DNS policy, and Android VPN protection/network binding before traffic
starts. These operations belong to setup, outside packet processing.

For Android VPN clients, ensure `VpnService.protect()` is applied to every
tunnel socket as required by the embedding application, including replacement
sockets after network changes. Do not assume every deployment supplies a
VpnService: the standalone client and embedded VPN client have different setup.

Choose and document native socket ownership, or create/configure a socket in Go
and hand off an independently owned descriptor with explicit transfer rules.
Never allow Go and native pollers to read the same UDP socket.

The fork also supports finalmask combinations and dialer-proxy/custom-dialer
paths that may not yield a native UDP socket. Port supported masks below QUIC,
keep an explicit compatibility backend, or reject unsupported native
configurations. Never silently bypass the selected outbound path. Likewise,
inventory TLS/ECH, ChromeParrot, QUIC parameters, BBR profiles, certificate
reload, user updates, and masquerading. Wire interoperability does not imply
matching the existing QUIC/TLS fingerprint or configuration behavior.

Use a versioned setup/teardown C ABI, conceptually:

```text
xhys_create(config, out_handle, out_arena_view)
xhys_start(handle)
xhys_request_stop(handle)
xhys_join(handle)
xhys_destroy(handle)
```

Copy setup inputs that must outlive a call. Catch recoverable Rust panics/C++
exceptions inside the ABI and return errors; fatal allocation failures or an
abort policy still terminate this monolithic process. Native workers are
independent threads, not process isolation.

Shutdown stops submissions, wakes both sides, cancels native work, drains or
invalidates pending events, joins workers, and waits for all Go arena users
before deallocation. Prevent concurrent destroy and arena access. A blocking
native join uses a regular CGO call rather than a Go syscall thread.

The target is no **per-packet CGO** calls or Go callbacks in native packet
processing. Initialization, setup, reconfiguration, and shutdown need multiple
calls; there is no requirement that the entire lifecycle fit into one call.

## 10. Build and packaging

Both Rust and C++ native backends require a CGO-enabled Go build and a matching
target linker. The current Linux/Windows/Docker instructions mostly use
`CGO_ENABLED=0`; Android already documents an NDK/CGO build. Update the build
matrix when implementation lands.

Build native static archives for the chosen targets, initially Linux amd64 and
arm64, Windows amd64, and Android arm64. Decide Windows arm64, Android x86_64,
and any 32-bit targets explicitly. The existing project's target list is wider
than this initial scope.

Pin Rust/C++ toolchains, Cargo dependencies or native submodules, crypto
providers, patches, and Android minimum API. Verify the chosen provider builds
on every target; using Rust does not guarantee all crypto code is Rust.

Specify libc/CRT linkage, C++ runtime linkage if applicable, CPU baselines,
symbol visibility, and Android PIC requirements. Static `libxhysteria` linkage
does not by itself produce a fully static Linux executable or eliminate system
library dependencies. Verify the resulting executable or embedding library with
platform tools and clean-machine smoke tests.

## 11. Implementation sequence and acceptance

1. Record a Go baseline: throughput, CPU per transferred byte, allocations/GC,
   RSS, small-request latency, UDP packets per second, and loaded latency on
   matching machines and network conditions. Capture both Hysteria directions.
2. Build a standalone native client and server with correct HTTP/3 auth,
   Salamander, TCP responses, and UDP framing. Test native-to-reference and
   reference-to-native using official Hysteria 2 and the supported Xray/sing-box
   versions. Record peer revisions and configuration.
3. Implement and validate the selected QUIC patch and exact Brutal port. Compare
   against the local Go implementation on packet traces: startup, fewer than
   50 samples, losses above 20%, bucket expiration, MTU changes, RTT changes,
   and compensation disabled. Verify DATAGRAM-only accounting and BBR fallback.
4. Add the bridge with separate ownership/release rings, one native worker,
   unconditional batch notifications, bounded queues, and a copying Go adapter.
   Exercise ring wraparound, saturation, slow consumers, cancellation, repeated
   reconnects, and teardown while buffers are outstanding on amd64 and arm64.
5. Integrate opt-in Xray inbound/outbound adapters, routing, user updates, socket
   policies, platform wakeups, and the supported feature/build matrix.
6. Test loss/reordering, IPv4/IPv6, rebinding, large/fragmented UDP, duplicate and
   malformed fragments, auth failure/masquerading, small TCP exchanges, and
   simultaneous TCP/UDP load. Verify deadlines and isolation between slow flows.
7. Compare native and Go builds under the same workload. Use `iperf3` for bulk
   traffic plus request/response and UDP packet-rate workloads. Remove the old
   Hysteria backend only after compatibility and measured performance pass.
   Pursue zero-copy leases, conditional wakes, SIMD, and extra workers only when
   profiling supports them.

A successful `/auth` handshake alone does not satisfy protocol acceptance.
Acceptance requires bidirectional TCP and UDP, fragmentation, shutdown,
resource bounds, and the intended platform/configuration behavior.

## 12. Sources

Repository references are relative to this checkout:

- [Brutal](../transport/internet/hysteria/congestion/brutal/brutal.go) and
  [pacer](../transport/internet/hysteria/congestion/common/pacer.go).
- [Hysteria protocol](../proxy/hysteria/protocol.go),
  [limits](../transport/internet/hysteria/config.go), and
  [protocol tests](../proxy/hysteria/protocol_test.go).
- [Xray buffers](../common/buf/buffer.go),
  [socket controllers](../transport/internet/system_dialer.go), and
  [build instructions](../README.md).

Upstream references describe the inspected code, not an implementation tested by
this document. Branch links can change; pin concrete revisions during the
prototype.

- [Hysteria 2 protocol specification](https://v2.hysteria.network/docs/developers/Protocol/).
- [lsquic internal congestion interface](https://github.com/litespeedtech/lsquic/blob/master/src/liblsquic/lsquic_cong_ctl.h),
  [controller selection](https://github.com/litespeedtech/lsquic/blob/master/src/liblsquic/lsquic_send_ctl.c),
  [public API](https://github.com/litespeedtech/lsquic/blob/master/include/lsquic.h),
  and [build/platform documentation](https://github.com/litespeedtech/lsquic/blob/master/README.md).
- [Quinn controller at quinn-0.11.12](https://github.com/quinn-rs/quinn/blob/quinn-0.11.12/quinn-proto/src/congestion.rs),
  [pacer](https://github.com/quinn-rs/quinn/blob/quinn-0.11.12/quinn-proto/src/connection/pacing.rs),
  [BBR implementation](https://github.com/quinn-rs/quinn/blob/quinn-0.11.12/quinn-proto/src/congestion/bbr/mod.rs),
  [protocol-only crate](https://github.com/quinn-rs/quinn/blob/quinn-0.11.12/quinn-proto/src/lib.rs),
  and [UDP socket adapter](https://docs.rs/quinn/0.11.12/quinn/trait.AsyncUdpSocket.html).
- [Go pollable file construction](https://go.dev/src/os/file_unix.go),
  [CGO pointer/build rules](https://pkg.go.dev/cmd/cgo),
  [Rust FFI](https://doc.rust-lang.org/nomicon/ffi.html), and
  [Android VpnService](https://developer.android.com/reference/android/net/VpnService).
