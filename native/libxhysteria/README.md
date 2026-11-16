# Embedded native Hysteria prototype

`libxhysteria.a` runs QUIC, TLS, HTTP/3 authentication, Salamander, TCP framing,
UDP fragmentation/reassembly, and congestion control on Rust-owned threads.
The Go adapter connects these flows to Xray routing, policy, and byte counters.
The normal Go backend remains available.

## Build and run on Linux

Requires Rust 1.88+, Cargo, Go matching `go.mod`, and a C compiler. From the
repository root:

```sh
bash native/libxhysteria/build-linux.sh
```

The output is `build_assets/xray-native`, with the Rust archive linked into the
executable. No separate Hysteria process or `libxhysteria.so` is used. System
libraries such as libc are still dynamically linked.

## Build on Android arm64

Install the Android NDK and the Rust standard library for Android:

```sh
rustup target add aarch64-linux-android
bash native/libxhysteria/build-android-arm64.sh "$ANDROID_NDK_ROOT"
```

The script accepts NDK directory, API level (default 26), and output path.
It builds the Rust archive for `aarch64-linux-android`, then links a PIE Android
executable with CGO and `-tags xhysteria_native`. It does not use the Linux
archive. `CARGO` and `GO` can select tool executables. Without `CARGO`, the script
prefers `~/.cargo/bin/cargo` when available.

In the sibling `bypclient` checkout, `./gradlew :app:buildXrayAndroid` uses this
script by default. The task accepts `-PandroidNdk=/path/to/ndk`, or uses the NDK
environment variables, or finds the newest installed NDK in the Android SDK.
Use `-PxrayCargo=/path/to/cargo` or `-PxrayGo=/path/to/go` to override tools,
and `-PxrayNative=false` for the old Go-only build. With `rebuildXray=true`,
the same build runs before assembling the APK.

`bypclient` enables the native backend by default for supported Hysteria profiles.
The **Settings > Network & DNS > Native Hysteria (Rust)** switch restores the Go
backend when turned off. Reconnect the VPN after changing it. The preference is
shared with the VPN process and isolated connectivity tests. Builds made with
`-PxrayNative=false` disable this switch and use Go.

The app sets `disableChromeParrot=true` and `XRAY_HYSTERIA_NATIVE=1` together,
deriving the process environment from its saved configuration even on a fast
restart. Profile QUIC idle/keepalive timers are passed to Rust. Remote port hopping
(`udphop` with `mode: "intervalRemote"`) runs in Rust, with or without Salamander.
Local port/IP hopping, unsupported masks, and proxy chains still select Go for
the entire Xray process; their settings are preserved, not silently dropped.
The VPN log identifies the selected backend and compatibility fallback reason.

### Remote port hopping

Keep the existing `finalmask.udp` configuration, for example:

```json
[
  {"type": "salamander", "settings": {"password": "example-secret"}},
  {"type": "udphop", "settings": {
    "mode": "intervalRemote", "interval": 120, "remotePorts": "12000-18000"
  }}
]
```

The server must already forward the configured UDP port range to its Hysteria
listener, as it does for the Go backend. This is not a multi-port native server.
The native client uses the same Go-created/protected UDP socket for every hop;
it does not create unprotected replacement sockets. QUIC keeps one logical peer
while the socket adapter changes the wire destination and accepts delayed
replies from earlier configured ports. The initial port is also selected from
the range, and successive choices differ when at least two distinct ports exist.
Intervals may be a fixed value or Xray's min/max range, with a minimum of five
seconds. Idle sockets choose their next port when they next send traffic.

The app logs `Hysteria backend: native (Rust), UDP port hopping`. Debug builds
also log the selected remote address when hopping. Client-side cascades and
`intervalLocal`, `perConnRemote`, or `remoteIPs` configurations still use Go.

## Enable the prototype

Enable individual inbounds/outbounds with `"native": true` in
`streamSettings.hysteriaSettings`, or set `XRAY_HYSTERIA_NATIVE=1` for the process.
`XRAY_HYSTERIA_NATIVE=0` forces Go even when JSON selects native, preserving the
Android preference and providing an operational rollback. Existing Hysteria JSON stays
valid, but `streamSettings.finalmask.quicParams.disableChromeParrot` must be true.
Without either selection, the executable uses the existing Go backend.

For a local client/server test, create a one-day localhost certificate in the
current directory:

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout key.pem -out cert.pem -days 1 -subj /CN=localhost \
  -addext subjectAltName=DNS:localhost -addext basicConstraints=critical,CA:FALSE
XRAY_LOCATION_CERT="$PWD" XRAY_HYSTERIA_NATIVE=1 build_assets/xray-native run -c native/libxhysteria/examples/server.json
```

Run the client in another terminal from the same directory:

```sh
XRAY_LOCATION_CERT="$PWD" XRAY_HYSTERIA_NATIVE=1 build_assets/xray-native run -c native/libxhysteria/examples/client.json
```

The client exposes SOCKS5 at `127.0.0.1:1080`, with TCP and UDP enabled. The
examples listen on loopback and contain test passwords. The commands set Xray's
certificate directory to the current directory. Server certificate files with
`oneTimeLoading: false` are checked every 30 seconds. A complete matching
certificate/key pair replaces certificates for new handshakes without interrupting
existing sessions. Invalid or incomplete updates retain the working certificate.
Inline PEM certificates and `oneTimeLoading: true` are also supported.

Linux builds include `-tags 'xhysteria_native xray_server'`. The server tag
restores the config builders, API CLI/services, statistics, metrics, observatory,
and server proxy registrations omitted by the Android distribution. It works
with a Go-only build as well; Android builds do not enable it.

Native server `/auth` validation uses Xray's current user validator over the
bridge. The established connection retains its authenticated `MemoryUser`, so
UUID routing aliases, policy, and per-user statistics stay in Go. `api adu/rmu`
updates subsequent authentication without restarting the inbound; existing
authenticated sessions keep their identity, matching the Go backend. Starting
with an empty user list is supported. Direct C ABI consumers can still select
the bounded static-user mode.

## Verify

```sh
bash native/libxhysteria/test-linux.sh
```

Tests exercise the embedded backend through Xray's real inbound/outbound
registration and routing. They cover native-to-Go, Go-to-native, native-to-native,
Salamander, TCP, fragmented 4096-byte UDP, authentication/TLS rejection, concurrent
TCP, and negotiated Brutal. Standalone `xhys-probe` and `xhys-echo` fixtures remain
available with `cargo build --locked --features probe --bins` in this directory.
The echo fixture is an echo endpoint, not a routing proxy.
Port-hopping tests route through a multi-port UDP forwarder against both Go and
native servers, with and without Salamander. The same TCP and fragmented UDP
flows must survive at least two destination-port changes with stable local
sockets; the base peer port is deliberately unreachable.
Pool tests mix TCP and fragmented UDP on shared sessions, check that closing one
flow preserves the others, and verify cleanup when the Xray instance stops.
Ring tests exercise full queues, counter wraparound, notification wakeups, and
buffer ownership after immediate slot reuse.
Cascade tests run four TCP transfers and two fragmented UDP flows through two
native servers, with Salamander and at least two port changes on both legs.
Both shared and socket bridges are exercised. UDP loss under load is permitted;
closed flows, corrupted payloads and stalled TCP transfers fail the test.
Startup tests distinguish native setup failures, handshake timeouts and caller
cancellation on both bridges.
Server tests also cover live user updates, UUID aliases, more than 1024 configured
users, 160 concurrent flows, and TLS certificate replacement with active streams.
To test a separately built NodeX server using DanVlessBot's actual config generator:

```sh
XHYS_SERVER_BINARY=/path/to/xray \
XHYS_NODEX_CONFIG_DUMP=/path/to/DanVlessBot/build/native_hysteria_config_test \
CGO_ENABLED=1 go test -tags 'xhysteria_native xhysteria_interop' \
  ./native/libxhysteria/interop -run '^TestNodeXNativeServerBinary$' -count=1 -v
```

## Session pooling and shared memory

Linux/Android amd64 and arm64 use the shared-memory bridge by default. Each
engine owns two bounded SPSC rings with 32 slots each and two nonblocking
`eventfd` notification descriptors. The arena is 4,460,864 bytes (about 4.25 MiB),
aligned to 64 bytes; storage layout version 2 is checked during setup. Engine
ABI version 3 adds authentication/certificate control frames and connection
ownership metadata. A dedicated
Go notification reader wakes both RX and blocked TX through Go netpoll. No CGO
call is made to transfer traffic. The socket bridge remains available for
comparison and rollback, and is the default on other supported architectures.

Payloads are copied across the ownership boundary: Rust copies commands out of
the ring, and Go copies events into pooled Xray buffers before releasing slots.
Those buffers then pass directly to `ReadMultiBuffer`; native arena pointers
never escape into Xray's forwarding buffers. This is not end-to-end zero-copy.
TCP writes are coalesced up to 64 KiB; client and server reads aggregate available
QUIC bytes without waiting to fill an entire chunk. Engine shutdown stops submissions,
wakes blocked operations, joins dispatchers and Rust, then releases the arena.

Each outbound lazily opens up to two QUIC sessions, sharing them across TCP and
UDP flows with the same Xray instance, transport settings, and peer. Closing a
flow preserves the session. There are 64 reserved flows per pooled session,
a global limit of 16 pooled outbound sessions including handshakes in progress,
and a 30-second idle timeout. A full pool waits for capacity with cancellation
and a 12-second timeout. The native handshake/auth timeout is ten seconds; the
additional bridge allowance lets Go receive its actual error. An earlier caller
deadline still takes precedence. Closing its Xray instance or losing the native
engine also retires sessions. A fixed negotiated Brutal rate restricts the pool to one
session so pooling does not multiply the configured bandwidth allowance. This
also covers an advertised RX limit when the local controller is BBR or Reno.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `XRAY_HYSTERIA_NATIVE` | unset | `1` forces native, `0` forces Go; unset uses JSON selection |
| `XRAY_HYSTERIA_BRIDGE` | shared on amd64/arm64 | `socket` restores socket IPC |
| `XRAY_HYSTERIA_POOL_SIZE` | `2` | `1..8` sessions per outbound; `0` restores per-flow engines |
| `XRAY_NET_DIAG` | unset | `1` logs roles, peers, authentication, QUIC counters, buffers and failure reasons |

With diagnostics enabled, `engine started ... role=server` confirms listener
startup; `authenticated role=server` confirms successful client authentication.
`connection role=client/server` reports RTT, TX/RX bytes, lost packets and the
congestion window every ten seconds. Increasing counters confirm traffic on
that peer. Startup errors identify the peer and phase (QUIC or HTTP/3 auth);
caller cancellation is reported separately from a native timeout. Error frames
survive shared-bridge shutdown instead of becoming a generic closed connection.

The existing Android native-backend preference still switches the entire backend
to Go. Pool and bridge overrides are process environment settings; no additional
Android preferences are required. Two active pooled sessions use about 8.5 MiB
of ring storage. These figures exclude QUIC buffers, per-flow queues, native
allocations, and Go heap usage, and the session limit does not include inbound
listeners or explicitly selected per-flow engines.

## Performance checks

```sh
bash native/libxhysteria/bench-linux.sh build_assets/native-perf
# Also sample Rust stacks with Linux perf, if permitted by the host:
XHYS_BENCH_PERF=1 bash native/libxhysteria/bench-linux.sh build_assets/native-perf
```

The harness compares Go, socket/shared bridges, and per-flow/pooled engines in
separate processes. It records throughput, process CPU time, Go allocations,
Go CPU/allocation profiles, and optional native stacks. See `PERFORMANCE.md`
for methodology, measured results, and limitations. Profiling also identified
and fixed an exact-size mismatch in Xray's regular buffer pool that had prevented
buffer reuse in both Go and native traffic paths.
Set `XHYS_BENCH_NATIVE_SERVER=1` to measure the native server; the default is Go.

The native transport now uses the same 8 MiB stream and 20 MiB connection receive
windows as Go Hysteria. Kernel UDP buffers request up to 8 MiB, subject to OS
limits, preserving larger existing buffers. Salamander and port hopping retain
batched receives and GRO segment metadata. Salamander delegates UDP send-error
handling and source-address selection to Quinn's socket adapter; transient send
failures no longer directly terminate a QUIC connection. GSO remains disabled.

## Prototype boundaries

The server uses one native engine per inbound listener, with up to four Rust
runtime worker threads (limited by available parallelism); clients retain one.
Defaults allow 256 concurrent server connections and 1024 active TCP/UDP flows
per listener, with bounded queues and backpressure. The Go client engine limit
remains 128; the outbound pool
reserves at most 64 per session. Per-flow credits bound
queued TCP data and apply backpressure; unread UDP datagrams are dropped when
receive credits run out. The older standalone ring experiment in
`src/ffi.rs` is separate from the active traffic ABI in `src/shared.rs`.

The prototype supports Linux and an Android arm64 cross-build. Android native
builds have been reported stable on a device; the new pooling/shared-memory
path still needs device validation, particularly sleep recovery and network
handover. Windows notification/socket integration is pending.
Go creates and configures tunnel UDP sockets with Xray's system dialer/listener,
then Rust duplicates the descriptor and owns packet I/O. This preserves socket
controllers during setup, including Android protection where the embedding app
provides it; network handover and replacement sockets still need validation.

Salamander and client-only remote port hopping are supported as UDP masks, and
only 404 HTTP masquerading is implemented. Unsupported masks, advanced TLS
options, verification-CA reload, and custom QUIC windows/stream limits are
rejected explicitly. Server UDP idle timeout accepts the configured value
(default 60 seconds). The native engine uses rustls/ring certificate
verification with bundled public roots and configured verification certificates;
it does not implement Xray's ECH or QUIC/TLS fingerprint behavior.

Brutal uses packet-count loss compensation and its own pacer. Rate negotiation
matches this fork's zero-rate/BBR fallback policy. `force-brutal` requires a
positive send rate. Adaptive BBR uses Quinn's experimental BBR controller; its
behavior does not reproduce Xray's BBR profiles. `reno` selects Quinn NewReno.

## Dependency patch

`vendor/quinn-proto` is the crates.io `quinn-proto 0.11.19` source at upstream
commit `8192ed399a26a6e8f0aba43d10ec8badaa5b905e`. Its MIT/Apache licenses are
retained. `Cargo.lock` pins the complete dependency graph.

Local changes add:

- An opt-in fallback for peers that omit `max_datagram_frame_size`, needed by
  this fork's Go client. Standard Quinn behavior remains the default.
- Defaulted controller hooks for exact lost-packet counts, pacing deadlines,
  and send-window admission. Existing controllers retain their default behavior.

The native tools enable the omitted-parameter fallback only for Hysteria. The
engine disables GSO while its custom pacer accounts individual transmissions.
