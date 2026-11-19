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
The core also supports remote IP-prefix hopping and `perConnRemote` initial
selection. The current bypclient compatibility gate still selects Go for these
new modes until its separate profile validator is updated.
The core supports local socket hopping and additional UDP masks through the
packet adapter below. The current bypclient compatibility gate still selects Go
for these profiles; proxy chains and unsupported settings also retain Go.
The VPN log identifies the selected backend and compatibility fallback reason.

### Remote address hopping

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
replies from earlier configured IP prefixes and ports. The initial port is selected from
the range, and successive choices differ when at least two distinct ports exist.
Intervals may be a fixed value or Xray's min/max range, with a minimum of five
seconds. Idle sockets choose their next port when they next send traffic.

`remoteIPs` accepts IPv4 or IPv6 CIDRs and selects a random host within each
prefix. With no `remotePorts`, the original port is preserved; with no
`remoteIPs`, the original IP is preserved. The actual UDP socket must support
the configured address family. IPv4-mapped prefixes are normalized to IPv4.
The configured IP addresses/port range must route to the same server; native
does not create listeners or forwarding rules for them.

`mode: "perConnRemote"` selects an IP/port once for each native QUIC session,
not for every proxied TCP/UDP flow. It never changes the destination afterward,
and its send path does not acquire the hopping mutex. When both internal
`Remote` and `RemoteOnce` flags are set, `Remote` retains interval hopping,
matching Go. `intervalLocal` uses the hybrid packet adapter described below.
Chained/custom socket dialers still require Go.

The app logs `Hysteria backend: native (Rust), UDP port hopping`. With
`XRAY_NET_DIAG=1`, the core also logs each selected remote address.

### UDP mask compatibility adapter

Plain UDP, Salamander, all six MKCP appearance headers (DNS, DTLS, SRTP, UTP,
WeChat, WireGuard), their ordered stacks, and supported remote hopping use direct
Rust socket I/O. A combined mask socket copies each QUIC datagram once into a
single lazily allocated send buffer, then masks it in place. It preserves separate
GSO salts/sequence numbers, GRO packet boundaries, and receive ECN/destination
metadata; it adds no batching timer or per-packet CGO call. DNS templates come
from the existing Go domain encoder at setup; Rust randomizes transaction IDs.
Header receive behavior matches Go's fixed-length stripping without enforcing
appearance bytes. A single Salamander retains its existing specialized path.

Direct chains are limited to 16 masks and 252 bytes of total expansion. PMTUD
reserves all mask overhead within an IPv6 1500-byte path, and `disableGSO` is
honored. Oversized chains and other masks use the existing Go `FinalMask`
PacketConn chain with Rust QUIC/HTTP/3/authentication; masks are never omitted.
That compatibility path is hybrid support, not fallback to quic-go. The direct
send buffer is capped at 64 KiB rather than retaining one scratch buffer per
mask. These are storage bounds, not a whole-process peak-RSS guarantee.

Mixed Go/native client/server tests cover all six MKCP appearance headers
(DNS, DTLS, SRTP, UTP, WeChat, WireGuard), MKCP original/AES-GCM, Sudoku,
custom UDP prefixes (including saved/returned state), standalone custom UDP,
noise packets, Lua noise scripts, and stacked Salamander masks. Existing mask
ordering, evaluation, delays and wire formats are preserved. Masks needing
privileged sockets or external services are not established by these tests.

Client-only local socket hopping uses the existing Go socket dialer for each
replacement, preserving socket-controller/Android protection hooks. Tests cover
local-only, local+remote IP/port, and local+remoteOnce profiles with header and
Salamander masks, against Go and native servers on both bridges. TCP and fragmented
UDP flows survive a source-port change without creating a replacement QUIC engine.
Android VPN handover has not been tested on a device.

Packet transfer uses a separate Unix datagram socketpair, two reusable 64 KiB Go
buffers and 64 KiB requested IPC socket buffers. Rust uses `sendmsg`/`recvmsg`
directly on Quinn's payload buffers with a stack metadata header; removing its
two scratch buffers saves approximately 128 KiB per packet-backed engine.
A bounded 32-entry peer cache eliminates address allocations on cache hits.
There is no CGO call per QUIC packet or unbounded user-space packet queue.

Rust can submit up to eight datagrams for one peer in an IPC message without a
batching delay. Go splits them before applying masks, preserving datagram
boundaries and Salamander salts. This is IPC batching, **not network UDP GSO**.
`disableGSO=true` disables the IPC batching too; Brutal still paces individual
sends. The 24-byte address header retains v1 for individual packets and uses
v2 for outgoing IPC batches. Go and Rust must be rebuilt together; the engine
FFI/shared-memory ABI has not changed. PMTUD remains disabled on this path and
QUIC MTU stays at 1200 before masking. Masks can expand the outer UDP payload
beyond the path MTU; neither batching nor the fixed MTU prevents fragmentation.

The base network socket now gets the same requested buffers as direct Rust I/O:
server receive/send 4 MiB/1 MiB, client receive/send 1 MiB/256 KiB. Setup runs
before wrapping masks and on every replacement/local-hopping dial through the
FinalMask callbacks. Linux doubles and caps these requests; actual values are
logged with `XRAY_NET_DIAG=1`. No sysctls are changed. Queue memory is allocated
as needed, is separate from Go allocations/RSS, and is not a whole-node memory
cap. Masks that create private sockets outside those callbacks are not covered.

Engine diagnostics report `packet_io=direct` with the native `mask_layers` count
or `packet_io=go_finalmask`; the latter reports zero native mask layers because
Go applies them. Adapter startup distinguishes `network_gso=false` from
`ipc_batch=true`.
Ten-second send-drop diagnostics distinguish reported `EMSGSIZE` from
`ENOBUFS`/`ENOMEM` and suppressed/short mask writes. An independent
`event=udp_bridge` counter reports drops due to exhausted Rust→Go flow credits
at engine level, not per connection. Silent fragmentation/path drops are not
captured by the send-error counter. The current adapter benchmarks and their
idle-latency regression versus Go are recorded in [PERFORMANCE.md](PERFORMANCE.md);
loss sampling and attribution are in [PACKET_LOSS.md](PACKET_LOSS.md).

UDP receive credits flush immediately when the Go per-flow queue drains rather
than waiting for a later datagram to complete a credit batch. Queued UDP bursts
still batch credits, TCP credits are unchanged, and no queue is enlarged. This
fixes a confirmed application-level drop source under saturated local load;
unread UDP flows still drop rather than block unrelated sessions.

Transient UDP send errors and suppressed/short mask writes count as send drops;
closed sockets and permanent mask errors stop the engine. Startup failure and
concurrent close release the PacketConn, Unix descriptors and relay goroutines.
Wildcard dual-stack sockets normalize received IPv4 peers to Quinn's mapped IPv6
form and do not invent destination-IP metadata. Scoped IPv6/zone addresses are
rejected explicitly by this packet adapter.

Focused compatibility checks (rebuild the release archive first):

```sh
native_hash=$(sha256sum native/libxhysteria/target/release/libxhysteria.a)
CGO_ENABLED=1 go test -race -tags 'xhysteria_native xhysteria_interop' \
  -ldflags="-buildid=xhysteria-${native_hash%% *}" ./native/libxhysteria/interop \
  -run '^TestNative(PacketMasks|PacketLocalHopping|HeaderRemoteHopping)$' -count=1 -timeout=300s
```

### HTTP masquerading

Native listeners support the existing `404`, `string`, `file`, and `proxy`
masquerade types.
The default 404 now returns the same text, content type, and `nosniff` header as
Go's `http.NotFoundHandler`. String masquerading forwards `masqString`,
`masqStringStatusCode`, and `masqStringHeaders`, including HEAD semantics,
bodyless 204/304 responses, and an HTTP Date header. Valid Hysteria authentication
still receives status 233; other HTTP/3 requests and invalid authentication
receive the configured cover response.

The response body is prepared once per engine and shared across requests and
connections with reference-counted bytes, rather than copied per request.
Active HTTP request tasks are bounded, retaining the ten-second request and
unauthenticated-session timeouts. Headers are validated before startup;
connection-specific HTTP/1 headers and trailer configurations are rejected,
as are mismatched Content-Length values and provisional 1xx status codes.
The default string status is 200; supported final codes are 200..999.
File/proxy requests keep QUIC, HTTP/3, and authentication in Rust while invoking
Go's existing `http.FileServer` and `httputil.ReverseProxy` through the bounded
flow bridge. This is hybrid cover-handler support, not a Go QUIC fallback.
Request/response bodies stream in 16 KiB chunks with a four-frame directional
window, sharing native flow admission and process send budgets. Large files and
uploads are not stored in startup JSON or buffered in full. The reverse proxy
also uses its existing pooled 32 KiB copy buffer. Those bounds do not represent
a hard cap on total Go/Rust/TLS/kernel memory per request.

File serving preserves HEAD, Range, conditional requests, redirects, directory
indexes, and missing-file responses. Proxying preserves path/query, host rewrite,
X-Forwarded settings, repeated headers, request/response trailers, informational
responses, HTTP/HTTPS upstreams, and Unix sockets. Upstream HTTPS verification
remains enabled unless `masqUrlInsecure` explicitly disables it. Each native
listener owns its upstream transport and closes idle connections on shutdown.
Successful Hysteria authentication bypasses the cover handler and returns 233.

HTTP/3 request field sections and native proxy upstream response headers are
limited to 32 KiB; serialized bridge metadata is limited to 69,632 bytes.
Connection-specific HTTP/1 headers never cross the HTTP/3 response bridge.
Invalid, conflicting, overflowing, or body-mismatched Content-Length values
are rejected before they can complete a malformed proxied stream.
The existing ten-second request/unauthenticated-connection timeouts remain.
Client cancellation stops the upstream request even after the upload completes;
early responses do not wait for an unread upload. The fixed Go quic-go baseline
replaces its trailer map at EOF, losing request trailers in ReverseProxy; native
updates the announced map in place and preserves them.

HTTP protocol upgrades/WebSocket tunneling, TCP finalmasks, custom/chained packet
dialers, Chrome Parrot, and advanced TLS/ECH settings remain unsupported.
The existing native setup limit remains 1 MiB of serialized JSON, including
the string body and TLS configuration; use streaming file/proxy masquerading
for larger cover pages instead of increasing the startup allocation.

Focused mixed-backend tests (rebuild the native archive first):

```sh
native_hash=$(sha256sum native/libxhysteria/target/release/libxhysteria.a)
CGO_ENABLED=1 go test -tags 'xhysteria_native xhysteria_interop' \
  -ldflags="-buildid=xhysteria-${native_hash%% *}" \
  ./transport/internet/hysteria ./native/libxhysteria/interop \
  -run 'TestNative(RemotePortHopping|StringMasquerade)$|TestEmbeddedNative(PortHopping|RemoteAddressHopping|HTTPMasquerade)$|TestEmbeddedHTTP(FileMasquerade|ProxyMasquerade|ProxyLifecycle)$' -count=1
```

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

For separating QUIC declared loss, local DATAGRAM queue evictions, and kernel
UDP drops, see [packet-loss diagnostics](PACKET_LOSS.md). The read-only sampler
works with an already running Xray; per-connection QUIC logging additionally
requires starting a diagnostic build with `XRAY_NET_DIAG=1`.

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
users, 160 concurrent flows, prompt capacity rejection and recovery on both bridges,
300 sequential short flows without exhausting QUIC stream credit, and TLS
certificate replacement with active streams. UDP idle-expiry tests verify that
ongoing traffic refreshes the deadline and that idle sessions return flow slots.
The Linux capacity test opens
1000 QUIC sessions for 500 distinct users in a separate server process, keeps 1000
TCP flows open, transfers 500 MiB in both directions with delayed readers, then
checks every flow for twelve seconds. Salamander is enabled by default. Run it
under an actual 1 GiB memory limit and one CPU quota with:

```sh
XHYS_CAPACITY_SYSTEMD=1 XHYS_CAPACITY_CASCADE=1 \
XHYS_CAPACITY_GOMEMLIMIT=256MiB CGO_ENABLED=1 go test \
  -tags 'xhysteria_native xhysteria_interop' ./native/libxhysteria/interop \
  -run '^TestNativeServer500Users$' -count=1 -v
```

The optional cgroup wrapper requires a user systemd session. The default test
still isolates server RSS without that wrapper. Cascade mode adds eight outbound
routes with two pooled native QUIC sessions each, then forwards every flow through
a separate exit server. Only the entry server is inside the measured cgroup;
the load generator and exit run outside it. The fixture defaults to `GOMEMLIMIT=128MiB`
and rejects entry-server RSS or cgroup peaks above 512 MiB. Go's limit does not
cover Rust allocations or kernel buffers. Readers start after two seconds while
uploads are still running, allowing flow control to apply backpressure.
`XHYS_CAPACITY_PROFILE=/absolute/directory` writes Go heap profiles and phase
counters from the entry process. `XHYS_CAPACITY_BACKEND=go` selects the Go
backend for both entry and exit with the same traffic and cgroup limits.
The native case uses sixteen pooled upstream sessions; Go uses its normal
single session per route (eight routes). `XHYS_CAPACITY_GOMEMLIMIT=256MiB`
overrides the fixture's Go runtime limit to measure the GC/memory tradeoff.
`XHYS_CAPACITY_MASK=0` disables Salamander on the client-facing entry leg
(cascade upstream links still use it), and
`XHYS_CAPACITY_SESSIONS=1..1310` selects a smaller debug workload.
`TestNativeServerBulkTransferWith100msRTT` checks that a 16 MiB upload and echo
can expand the initial receive window on a simulated 100 ms path.
`XHYS_WAN_BACKEND=go` repeats it using Go on both endpoints.
To test a separately built NodeX server using DanVlessBot's actual config generator:

```sh
XHYS_SERVER_BINARY=/path/to/xray \
XHYS_NODEX_CONFIG_DUMP=/path/to/DanVlessBot/build/native_hysteria_config_test \
CGO_ENABLED=1 go test -tags 'xhysteria_native xhysteria_interop' \
  ./native/libxhysteria/interop -run '^TestNodeXNativeServerBinary$' -count=1 -v
```

## Optional native allocation diagnostics

Normal builds retain Rust's system allocator without allocation counters.
For a diagnostic Linux build, enable the optional feature and relink Go with
the resulting archive hash so its linker cache cannot reuse the normal build:

```sh
cargo build --locked --release --features 'engine allocation-profile' --lib \
  --manifest-path native/libxhysteria/Cargo.toml
native_hash=$(sha256sum native/libxhysteria/target/release/libxhysteria.a)
CGO_ENABLED=1 go build -tags 'xhysteria_native xray_server' \
  -ldflags="-buildid=xhysteria-${native_hash%% *}" \
  -o build_assets/xray-native-allocation-profile ./main
XRAY_NET_DIAG=1 build_assets/xray-native-allocation-profile run -c /path/to/config.json
```

Server capacity diagnostics report live Rust allocations by power-of-two size
class when a class holds at least 1 MiB. GNU/Linux also reports allocator-owned
and retained free pages through `mallinfo2`. These are diagnostic counters, not
RSS or Go heap measurements, and add overhead. The anonymous shared-arena
mappings are outside these allocator counters; include them when measuring
whole-process RSS rather than interpreting smaller counters as freed memory.
Rebuild with `build-linux.sh`
without the optional feature before measuring or deploying a normal binary.

## Session pooling and shared memory

Linux/Android amd64 and arm64 use the shared-memory bridge by default. Each
engine owns two bounded frame rings with eight slots each and three nonblocking
`eventfd` notification descriptors. The arena is 1,639,744 bytes (about 1.56 MiB),
aligned to 64 bytes; storage layout version 6 and engine ABI version 5 are
checked during setup. Its zero-filled storage uses an anonymous private mapping,
not the system allocator. Only the header is initialized eagerly; unused
payload pages are not prefaulted. Linux huge-page advice prevents small arena
writes from committing adjacent large pages when supported. The mapping is
unmapped after the last arena reference or payload lease is released, rather
than retained in malloc's free lists. Virtual arena size and payload limits are
unchanged. Event publication is serialized with a short native
mutex; command publication uses a short Go mutex and an ordered queue fallback.
No lock is held over a blocked ring wait. Separate descriptors wake RX and
blocked TX directly through Go netpoll; a third wakes Rust. Coalesced pending
flags reduce redundant notification syscalls without delaying data. No CGO
call is made for normal payload publication or per-payload release. The last
Go arena owner uses a teardown CGO call to release its Rust reference.
The socket bridge remains available for
comparison and rollback, and is the default on other supported architectures.
Space notifications are armed only by writers that observe a full ring, then
recheck it before waiting. A consumer publishes the freed slot before checking
that flag, so concurrent draining cannot lose its wakeup. The Go eventfd paths
use nonblocking raw syscalls under `RawConn` descriptor lifetime protection;
`RawConn.Read` still waits through Go netpoll on EAGAIN and preserves deadlines.
Each event reader owns one reusable buffer and callback. Credit messages use
immutable byte tables instead of per-message payload allocations.
Rust handles wake descriptors and command dispatch in the same task. When a
control queue is full, it continues handling notification readiness so that
event-space backpressure cannot deadlock the engine.

Direct Go vectored writes borrow the caller's pointer array until publication;
only the ordered-queue fallback snapshots that array. Buffer ownership transfers
on successful submission, with exactly one release on failures or shutdown.
Fragmented UDP replies retain compact fragment allocations and publish the
address and fragments directly into one bridge frame instead of first joining
the payload in another allocation. Up to six fragments use stack-only slice
references. UDP transmit iterates fragments without allocating a fragment plan
and reserves each wire buffer once. The remaining compact-fragment and bridge
publication copies are intentional: retaining slices of large GRO allocations
would increase live memory, and the bridge needs independently owned storage.
DATA, DATAGRAM and credit routing is synchronous. Control frames also publish
without an async wait when their queue has capacity; only a full control queue
uses the notification-aware wait. The dispatcher still services event-space
notifications and yields after 32 commands or 256 KiB.

DATA and DATAGRAM payloads up to 16 KiB use independent leased storage: each
direction has sixteen 16 KiB slots and an atomic free bitmap. Publication copies
directly into one slot; consuming the descriptor does not copy the payload.
Rust wraps it in `Bytes::from_owner`, and Go exposes it through a releasable
Xray buffer. Releasing the last Rust clone or the Go buffer returns its slot.
Descriptor slots become reusable immediately, independently of payload lifetime.
An exhausted payload pool falls back to the existing inline ring/copy path,
never blocking unrelated flows or control frames on a retained lease. Larger
payloads and control frames also retain that fallback. This is not end-to-end
zero-copy: publication, socket bridges, QUIC wire encoding and `Read(p)` still
copy. The extra pool capacity is bounded at 512 KiB per shared engine; reduced
Go allocation traffic does not establish a lower whole-process RSS.
TCP writes are coalesced up to 16 KiB on both client and server;
reads aggregate available QUIC chunks directly into bridge frames, without a
per-flow scratch buffer or waiting to fill a frame. Each read batch holds at most
eight chunks. Engine shutdown stops submissions,
wakes blocked operations and joins dispatchers and Rust. Arena ownership survives
join until every outstanding Go/Rust payload lease is released. Go retains one
Rust arena reference rather than making a CGO call for each payload; a finalizer
is a safety net for abandoned Go buffers. Both Go and Rust must be rebuilt for
ABI version 5; old standalone C callers must be updated too.
Single Xray buffers and vectors of whole buffers are transferred without an
extra coalescing copy. Oversized TCP buffers copy their slices straight into
shared storage; queued borrowed writes first take an owned snapshot. UDP address
headers and payloads publish as separate slices in both runtimes, avoiding a
temporary combined payload. Eventfd notification happens after the command
publication lock is released. Rust writes owned chunks directly into Quinn and reads server TCP through
the underlying Quinn stream after parsing the Hysteria header. Data arriving
with that header is preserved. TCP uploads opportunistically write up to eight
already queued chunks / 128 KiB per Quinn call, with no payload coalescing or
wait for future frames. Extra chunks acquire local/process send reservations
nonblockingly; an unavailable reservation flushes the current batch before
waiting, avoiding a batch-induced budget deadlock. FIN/reset ordering is
preserved. A writer yields after 64 KiB of progress for other ready tasks.
Clients use eight frame credits with a return threshold of four; servers use
four with a threshold of two. A TCP batch returns credits after Quinn accepts
its data, combining returns up to the flow window, bounding their
TCP queues at 128 KiB and 64 KiB respectively. Payload publication never waits
to fill such a group. Power-of-two byte-pool classes avoid rounding 16 KiB frames
to 32 KiB and 64 KiB frames to 128 KiB. Nonstandard capacities are not retained
in those pools.
An explicit flow close resets its QUIC sending half so the peer releases stream
credit promptly. `CloseWrite` still sends FIN and allows the response to finish.
The server classifies bidirectional streams before passing HTTP requests to
HTTP/3; TCP streams do not accumulate in HTTP/3's request-completion table.

Each outbound lazily opens QUIC sessions shared across TCP and UDP flows with
the same Xray instance, transport settings, and peer. Automatic sizing retains
two sessions below eight Go CPUs and selects four with eight or more CPUs.
An explicit client worker budget above four raises that limit to the worker
budget, capped at eight. The two-session floor preserves the previous 128-flow
capacity on small CPUs. A single flow still opens only one session; extra sessions require
concurrently live flows. Closing a flow preserves the session. There are
64 reserved flows per pooled session,
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
| `XRAY_HYSTERIA_POOL_SIZE` | auto | Two/four automatically; explicit client workers above four allow up to eight; `1..8` overrides sessions per outbound; `0` restores per-flow engines |
| `XRAY_HYSTERIA_CLIENT_WORKERS` | `0` | `0`: automatic (up to four from `GOMAXPROCS`); `1..16`: explicit total client Rust workers, fixed by the first engine |
| `XRAY_HYSTERIA_SERVER_WORKERS` | `0` | `0`: automatic (up to four); `1..16`: explicit server runtime threads |
| `XRAY_HYSTERIA_SERVER_CONNECTIONS` | auto | Override native server QUIC connection cap (`1..4096`) |
| `XRAY_HYSTERIA_SERVER_FLOWS` | auto | Override native server TCP/UDP flow cap (`1..16384`) |
| `XRAY_NET_DIAG` | unset | `1` logs roles, peers, authentication, QUIC counters, buffers and failure reasons |

Server limits use the lower of host RAM and the process cgroup memory limit.
`GOMAXPROCS` selects up to four server workers and does not cap session counts.
The concurrency estimate reserves at least 60% of detected RAM (and at least
256 MiB), then splits the rest 40/60 between connection and flow allowances of
128 KiB each. A 1 GiB host selects 1310 connections and 1966 TCP/UDP flows; the
929,935,360-byte VPS selects 1135 and 1702. Unknown memory uses a 1 GiB estimate;
hosts with no remaining budget use 32 connections and 64 flows. These are admission
estimates, not preallocated buffers or a process RSS guarantee. Limits apply per
listener, so several listeners and other node services share the same host RAM.
Set either environment variable to override its cap independently.

Server receive windows start at 64 KiB per connection. After 64 KiB of TCP
traffic, a connection can reserve an expansion to 8 MiB from a shared receive
budget. Expanded credit remains reserved until that session closes. Each listener
has separate receive-expansion and send budgets of 1/32 of detected RAM, clamped
to 8..64 MiB (32 MiB each on a 1 GiB host). A process-wide send budget covers
retained TCP data from inbound and outbound engines together; a separate
process-wide receive budget covers inbound window expansions across listeners.
Each process budget is 1/16 of RAM, clamped to 8..128 MiB (64 MiB on 1 GiB).
The first engine using each process budget fixes its size until process exit.
Send reservations follow the lifetime of Quinn's retained byte buffers, including
retransmissions and shared slices; writers wait when a budget is full.
Outbound engines also have an 8 MiB local send budget and QUIC send window.
Server QUIC DATAGRAM send/receive queues are 32 KiB each and reassembly retains
at most eight incomplete packets per connection. These budgets cover their
respective buffers; kernel sockets, Go forwarding, and other allocations remain
part of the node's overall memory usage.

With diagnostics enabled, `engine started ... role=server` confirms listener
startup; `authenticated role=server` confirms successful client authentication.
`connection role=client/server` reports RTT, TX/RX bytes, lost packets and the
congestion window every ten seconds. Increasing counters confirm traffic on
that peer. Startup errors identify the peer and phase (QUIC or HTTP/3 auth);
caller cancellation is reported separately from a native timeout. Error frames
survive shared-bridge shutdown instead of becoming a generic closed connection.
`event=capacity` reports active/maximum connections and flows, rejected counts,
and listener/process receive-window reservations and process send usage every
ten seconds. Saturated connections are
explicitly refused while established sessions continue; rejection diagnostics
are limited to one per five seconds. TCP flow overflow resets only the new
stream. Unauthenticated connections and incomplete handshakes have ten-second
deadlines.

The existing Android native-backend preference still switches the entire backend
to Go. Pool and bridge overrides are process environment settings; no additional
Android preferences are required. Two active pooled sessions use about 2.13 MiB
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

The `BenchmarkNativePacket*` family compares both-Go against both-native
endpoints with Salamander + WireGuard masks, shared bridge and pool size one.
It includes echo transfer, continuous upload/download, idle TCP/UDP latency,
saturated TCP latency and paced TCP latency. Unlike the older mixed-endpoint
harness, its Go mode cannot accidentally retain a native server. Go allocation
metrics exclude native heap, arenas and kernel memory; use isolated processes
with fixed transferred byte counts for RSS comparisons.

For server comparisons with the same client, in isolated processes:

```sh
bash native/libxhysteria/bench-server.sh build_assets/native-server-perf
```

The harness alternates server order and covers Salamander on/off, single and
parallel request/response traffic, continuous upload/download, TCP latency in
idle and under four background transfers, and UDP latency. `PacedLatency`
offers the same 64 MiB/s background traffic to each backend and reports the
achieved rate; `LoadedLatency` saturates each backend. Latency output
includes p50/p95/p99. `XHYS_BENCH_PACED_MIBPS=32` selects a lower common offered
load; accepted values are 1..4096. Environment overrides `XHYS_BENCH_TIME`,
`XHYS_BENCH_COUNT`, `XHYS_BENCH_SCENARIOS`, `XHYS_BENCH_CLIENTS` and
`XHYS_BENCH_MASKS` select a smaller matrix. Native archive hashes and the tracked
source diff against HEAD, including staged changes, are recorded beside the raw
results. Untracked source files must be staged before recording that diff.
`XHYS_BENCH_AFFINITY=0` pins
all Go and Rust threads in each sample to CPU 0. This differs from
`XHYS_BENCH_CPUS=1`, which only sets Go parallelism. The CPU must be in the
process's allowed affinity set. `XHYS_BENCH_GO_OPENCLOSE=1` retries the Go
short-flow case that is otherwise explicitly skipped after previous failures.
`XHYS_BENCH_TRACE=1` logs short-flow exchanges taking over 100 ms.
CPU includes both endpoints;
Go allocations exclude Rust allocations and the shared arena.

The native client uses an 8 MiB stream receive window. Its connection window is
1/512 of detected RAM, clamped to 2..8 MiB (2 MiB on a 1 GiB node).
Direct C ABI clients default to 8 MiB unless `receive_buffer_bytes` is set.
Server connections use the smaller initial window and shared expansion budget
described above; their stream limit remains 8 MiB. Kernel UDP buffer targets are
4 MiB RX / 1 MiB TX per server socket and 1 MiB RX / 256 KiB TX per client
socket, subject to OS limits. These replace larger existing settings; Linux
reports twice the requested value. They are limits, not immediate allocations.
Salamander and port hopping retain
batched receives. Upstream sockets whose configured connection receive window
is at most 2 MiB disable kernel UDP GRO and report one receive segment to Quinn,
reducing each endpoint receive workspace from about 2.88 MiB to 46 KiB. This is
done after wrapping the socket; failure to disable GRO preserves the original
segment count and workspace. Other sockets preserve GRO segment metadata.
Salamander delegates UDP send-error
handling and source-address selection to Quinn's socket adapter; transient send
failures no longer directly terminate a QUIC connection. BBR/Reno can use GSO.
Each Salamander segment receives its own system-random salt and BLAKE2b-256
mask. Hashing uses runtime SIMD detection and a precomputed password prefix;
the portable implementation remains available. OS-random bytes are prefetched
in bounded blocks. MTU discovery reserves salt overhead; `disableGSO` and
`disablePathMtuDiscovery` remain effective. Negotiated Brutal limits transmits
to one datagram so batching cannot bypass its pacer. See
`vendor/PATCHES.md` for the pinned dependency changes.

## Prototype boundaries

The server uses one native engine per inbound listener, with a dedicated
runtime and its explicit worker setting. Outbound engines share a process
pool of single-worker Rust runtimes, assigned round-robin when an engine
starts. An engine's QUIC driver, streams and bridge tasks stay in its selected
runtime; distinct engines can run concurrently across the pool. This avoids
cross-worker scheduling and connection-lock contention within one engine,
but does not pin an OS thread to a particular CPU or provide load balancing
between engines after assignment.
Go configures up to four total pool workers from GOMAXPROCS; the first shared
engine fixes that count. `XRAY_HYSTERIA_CLIENT_WORKERS=8` enables eight client
workers and, unless separately overridden, up to eight sessions per outbound.
Set the variable before the first client engine starts. The eight-physical-core
client fixture confirms that all eight Rust workers process traffic with eight
active sessions. It also measures higher CPU per exchange and peak RSS than the
four-worker automatic mode; eight workers are an explicit throughput/latency
tradeoff rather than a CPU-saving default. See `PERFORMANCE.md` for the isolated
client measurements. A single flow stays on one QUIC connection and cannot use
eight QUIC drivers at once. The low-level `process_workers` field accepts 0..16;
zero selects one worker. Splitting the runtime does not add worker threads,
but each shard owns a separate reactor and scheduler. A single hot engine
uses one Rust worker; skewed engine workloads can leave other shards idle.
Each engine has an independently joined task and owns its own UDP socket and
bridge arena. The process pool remains alive between engine lifetimes.
With `XRAY_NET_DIAG=1`, `event=client_runtime` reports the selected shard,
shard count, per-shard workers and fixed total worker count.
Endpoint ownership closes connections on early failure; authentication-driver
tasks abort when startup is cancelled or their engine stops.
TLS configuration prefers TLS 1.3 AES-128-GCM while retaining the other
provider-supported cipher suites; this is not a compatibility restriction or
a claim of equal acceleration on every CPU.
Incoming bidirectional stream credit starts at eight on the server and rises
to 64 after the second TCP stream. Native clients accept no server-initiated
bidirectional streams; both sides allow eight unidirectional HTTP/3 streams.
These stream counts reduce idle bookkeeping without reducing the pooled
64-flow allowance.
TCP header scratch storage is released after parsing. Server UDP dispatch
starts only when the first authenticated datagram arrives. Fragment cleanup
timers run only while reassembly is pending, and each UDP session sleeps until
its activity-based expiry instead of waking every second.
Server connection and flow caps are selected from host/cgroup memory,
up to 4096 connections and 16384 active TCP/UDP flows per listener. The Go client engine limit
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

Salamander, the six MKCP appearance headers and their stacks, client-only remote
IP/port hopping, and once-per-session remote selection have direct Rust support;
other UDP masks and local socket hopping use the bounded hybrid adapter above.
HTTP masquerading supports 404/string
directly in Rust and streaming file/proxy through bounded Go cover handlers.
Unsupported mask/socket combinations, advanced TLS
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
- Receive-stream defragmentation reuses its metadata backing storage instead of
  allocating a second heap, and shrinks oversized sparse storage after merging.
  Closed or reset streams retain at most 128 metadata slots; active chunk limits,
  receive windows and duplicate/overlap handling remain unchanged.

The native tools enable the omitted-parameter fallback only for Hysteria. The
engine disables GSO while its custom pacer accounts individual transmissions.
