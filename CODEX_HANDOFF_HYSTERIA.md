# Codex Handoff: Native Hysteria Memory and Capacity

Date: 2026-11-17
Workspace: `/home/danya2271/ProxVless/XPray-core`

## Request and Scope

Reduce memory, improve Hysteria performance and reliability, and investigate
bottlenecks for 500 users on a 1 GiB RU node. Compare the resulting backend with
Go, including memory, throughput, CPU and latency. The requested 2.5x improvement
in every metric is not universally achieved. The user authorized commits.
Do not push or modify remote VPSs.

The previously user-owned `transport/internet/splithttp/client.go` change was
committed separately as `3cc7b38a` at the user's request. Neighboring DanVlessBot
and bypclient worktrees contain unrelated changes. kettuRay has a user-owned
`cores/slipstream-client` change. Preserve these neighboring changes. Generated
binaries and measurements belong in ignored `build_assets` or `/tmp`.

## Production Evidence

The November 16 kernel logs confirm two global OOM kills, at 21:41:12 and
22:12:49 +03:00. The second killed the main Xray at approximately 283 MiB RSS;
the first killed it at approximately 339 MiB. There was no swap.
`olcrtc invoked oom-killer` identifies the allocating task, not the main memory
consumer. The node reports 929,935,360 usable bytes and runs Docker/containerd,
an approximately 95 MiB Erlang process, another Xray, agents and other services.

Immediately before the second OOM, the native listener had about 75 connections,
150 flows and zero capacity rejections. Receive expansion reservations consumed
57,802,752 of 58,120,960 bytes. The same process also hosted native upstream
sessions whose buffers were outside the old listener budget.

Some upstreams timed out during QUIC/auth startup and fell back to Go.
Loss counters are cumulative; they are not packet-loss percentages. Neither
timeouts nor path loss have been established as the cause of the global OOM.

## Implemented Changes

- Always poll server accepts and explicitly refuse excess connections. Reject
  excess TCP flows with a stream reset and recover when slots become available.
  Handshakes and unauthenticated sessions have ten-second deadlines.
- Admission limits depend on host/cgroup memory, including parent slices.
  CPU selects workers, not connection counts. A 1 GiB host selects 1310
  connections / 1966 flows; the logged RU host selects 1135 / 1702.
- Shared arenas shrink from 4,460,864 to 1,115,456 bytes. Storage layout is 5;
  the engine ABI remains 4. Rebuild Rust and Go together.
- TCP frames are 16 KiB. Server/client queues use four/eight frame credits,
  returned in groups of two/four. QUIC reads hold at most eight chunks and
  publish available payload directly through vectored shared-ring writes.
- Send permits follow the last retained Quinn `Bytes` reference, including
  retransmission slices. Both inbound and outbound engines share a process
  send budget. Server receive expansion also has a process-wide budget.
  Process budgets use the first initialized limit until process exit.
- Listener send/receive-expansion budgets use RAM/32, clamped to 8..64 MiB.
  Process budgets use RAM/16, clamped to 8..128 MiB. Client local send budget
  and QUIC send window are 8 MiB. Normal client connection RX is RAM/512,
  clamped to 2..8 MiB; low-level C ABI clients default to 8 MiB.
- Server RX starts at 64 KiB and may reserve an expansion to 8 MiB after
  receiving 64 KiB. The reservation lasts until connection closure. Process RX
  budgets cover inbound expansions, not the base client receive windows.
- Server/client kernel UDP targets are 4 MiB/1 MiB RX and 1 MiB/256 KiB TX,
  respectively. Server QUIC DATAGRAM queues are 32 KiB, with at most eight
  incomplete reassemblies per connection.
- Common byte pools use power-of-two classes from 2 KiB through 128 KiB.
  Previously a 16 KiB frame occupied 32 KiB. Oversized nonstandard arrays are
  no longer retained in a smaller pool.
- Vendored BBR compares congestion-window bytes with its target rather than
  comparing a gain coefficient with bytes, and skips ACK excess accounting
  before a bandwidth estimate exists. A regression reproduces growth from
  240,000 to 12,240,000 bytes in the original implementation.
- Explicit flow closure resets the QUIC sending half; FIN remains reserved
  for half-close. This removes approximately 0.9-second stream-credit stalls
  during sequential short flows. A 300-flow regression covers both bridges.
- TCP classification happens before HTTP/3 request admission. Only HTTP
  streams enter HTTP/3's request-completion table, avoiding retained TCP IDs.
- Small-node outbound sockets with configured RX at most 2 MiB disable kernel
  UDP GRO after wrapping and report one receive segment to Quinn, reducing the
  receive workspace from roughly 2.88 MiB to 46 KiB per endpoint. A failed
  `setsockopt` preserves the original segment count rather than truncating data.
- Outbound engines share a process Tokio runtime with up to four workers
  selected from GOMAXPROCS; the first client fixes that count. Each inbound
  listener keeps its dedicated runtime. Sharing inbound and outbound runtime
  workers was tested and rejected because saturated p99 latency increased.
- Authentication-driver tasks abort on early startup errors or engine stop.
  Endpoint ownership closes QUIC connections on early return. Worker joins are
  independent, and panics in either bridge are converted to engine failures.
- Servers initially advertise eight bidirectional streams and raise this to 64
  after the second TCP stream. Clients accept no server-initiated bidirectional
  streams; both roles advertise eight unidirectional streams.
- TCP header/response scratch storage is released immediately after parsing.
  Server UDP dispatch starts on the first authenticated datagram, fragment
  cleanup runs only with pending reassembly, and UDP flow timers sleep until
  their activity-based expiry instead of polling each second.
- TLS prefers AES-128-GCM without removing other supported suites. Salamander
  XOR handles eight bytes at a time while preserving its wire format and salts.
- Optional `allocation-profile` instrumentation reports live Rust allocations
  by size class and, on GNU/Linux, allocator retention. Normal builds do not
  enable it. A mimalloc experiment increased peak memory and was removed.
- The latency follow-up adds waiting-writer flags at header offsets 48 and 52,
  leaving the arena size unchanged. Sequentially consistent flag/ring rechecks
  preserve wakes during concurrent draining. Consumers notify space only when
  a writer has armed its wait; uncontended traffic avoids those eventfd writes.
- Shared publication creates its `Notify::notified()` future before inspecting
  the ring but registers a waiter only if it must actually wait. `notify_waiters`
  generations preserve a wake between future creation and first poll.
- Go eventfd reads/writes use nonblocking raw syscalls under RawConn protection;
  reads still wait through netpoll on EAGAIN, honor deadlines and unblock on
  close. Per-reader buffers/callbacks are reused. Go/Rust credit payloads use
  immutable byte tables, avoiding control-payload allocations and byte-pool use.

## Capacity Measurements

The fixture isolates the entry server in a 1 GiB cgroup with CPUQuota=100%,
GOMAXPROCS=1 and GOMEMLIMIT=256MiB. Load generation and the exit are outside
that cgroup. It opens 500 identities, 1000 authenticated QUIC sessions and
1000 TCP flows; transfers 500 MiB duplex with readers delayed two seconds;
then verifies every stream for twelve seconds. Eight upstream routes use
sixteen native pooled sessions, or Go's eight normal sessions.

Final same-harness means of two fresh samples per backend, alternating
Go/native then native/Go, without heap-profile sampling:

| Metric | Go | Native |
| --- | ---: | ---: |
| Duplex MiB/s | 20.16 | 85.76 |
| Transfer seconds | 25.061 | 5.830 |
| Idle RSS MiB | 186.6 | 142.6 |
| Peak cgroup memory MiB | 286.0 | 261.0 |
| Peak RSS MiB | 295.1 | 279.3 |
| Final RSS MiB | 272.6 | 279.3 |
| RSS two seconds after closing all sessions MiB | 272.7 | 277.0 |
| Cgroup CPU seconds | 18.732 | 8.079 |
| Heartbeat p50 microseconds | 2,171.9 | 1,081.9 |
| Heartbeat p99 microseconds | 6,895.2 | 2,339.7 |

All four samples pass. Mean cascade duplex throughput is 4.25x, idle RSS 24%
lower, peak cgroup memory 8.7% lower and CPU work 57% lower. Transfer time
includes the mandatory two-second reader delay but excludes setup and the
twelve-second heartbeat phase. Final native RSS is still 2.5% higher, with
allocator pages retained after connection closure.

Native individual rates are 85.83/85.69 MiB/s; Go is 22.20/18.12. Go heartbeat
p99 varies from 4037.2 to 9753.1 microseconds, native from 2096.2 to 2583.2.
Means of two runs do not prove a stable latency factor. Upstream session counts
reflect each backend's normal pooling and are not identical. Historical 128 MiB
Go-limit samples in PERFORMANCE.md are not measurements of this final build.
Go GC tuning affects the comparison; no production or device guarantee follows.

Earlier development measurements of 533 MiB and 442 MiB helped locate buffer
over-allocation. Their upload/read sequencing differs from the final harness,
so they are not controlled before/after comparisons.

## Validation and Reproduction

Passed: 314 vendored Quinn tests, 44 engine tests (also with allocation profiling
enabled), Go race tests for bytespool,
buf, xhysteria and hysteria, complete native interop including NodeX-generated
config, capacity rejection/recovery, shared/socket bridges, UDP activity/expiry,
sequential short flows and port hopping. Final synthetic 100 ms RTT transfers
of 16 MiB each way passed. Three fresh whole-fixture cgroup samples averaged
14.69 MiB/s duplex for Go and 14.35 for native; the ranges overlap. There is no
demonstrated native WAN speed advantage in that fixture.

Final one-logical-CPU matrix: three fresh two-second samples per combination,
Salamander, CPU affinity 0, GOMAXPROCS=1, common offered load 32 MiB/s. Native/
native Transfer is 109.76 MB/s versus Go/Go 80.83 (1.36x), upload 142.54 versus
77.25 (1.85x). Idle TCP p99 is 244.2 versus 140.6 microseconds; UDP p99 is 233.1
versus 151.8. Go wins idle latency. At achieved load approximately 31.95 MiB/s,
native TCP p99 is 1169 versus Go 1282 microseconds (9% lower). Mixed backends
often regress. The requested universal 2.5x in memory/latency/speed is not met.

The broader `go test ./common/...` fails in unrelated packages: missing
`resources/geosite.dat` and `resources/geoip.dat`, log tests without registered
logging, and a reflect JSON test. Do not describe that whole suite as passing.

Builds:

```sh
bash native/libxhysteria/build-linux.sh
bash native/libxhysteria/build-android-arm64.sh \
  /home/danya2271/Android/Sdk/ndk/30.0.14904198 26 \
  build_assets/xray-native-android-arm64
```

Android is a CLI PIE build, not a replacement for bypclient's user-modified
`libxray.so`. No device test or remote deployment has been performed.

Capacity comparison:

```sh
XHYS_CAPACITY_BACKEND=go XHYS_CAPACITY_GOMEMLIMIT=256MiB \
  XHYS_CAPACITY_SYSTEMD=1 XHYS_CAPACITY_CASCADE=1 \
  CGO_ENABLED=1 go test -tags 'xhysteria_native xhysteria_interop' \
  ./native/libxhysteria/interop -run '^TestNativeServer500Users$' -count=1 -v
```

Repeat with `XHYS_CAPACITY_BACKEND=native`. `XHYS_CAPACITY_PROFILE` writes heap
profiles and phase counters. The test rejects peaks above 512 MiB.

The fresh-process Go/native performance matrix and methodology are recorded in
`native/libxhysteria/PERFORMANCE.md`; raw output is under
`build_assets/native-delivery-20261117` (three fresh two-second matrix samples,
CPU affinity 0, Salamander, plus two capacity samples and three WAN samples per
backend). `summary.json` records arithmetic means and individual capacity/WAN
observations. The earlier `native-go-release-20261117` milestone uses older code;
`native-tuned-20261117` predates lazy UDP dispatch.
The `native-optimized-20261117` shared-server-runtime experiment was rejected
for saturated tail latency and is not the final implementation.
The earlier `native-go-comparison-20261117` run had
Go parallelism 1 but allowed Rust threads on other CPUs and preceded the
short-flow close fix; do not present it as a physical one-CPU comparison.
The separate original-HEAD/native
comparison is under `build_assets/native-memory-final-20261117`.

## Latest Latency Follow-up

See PERFORMANCE.md's latest latency section. All code, tests and builds for the
follow-up have passed. Fresh baseline/current/Go samples are in
`build_assets/native-latency-20261117/delivery-*.txt` and
`delivery-summary.json`: three two-second processes per case, Salamander,
GOMAXPROCS=1, all threads pinned to CPU 14, alternating backend order.
CPU 0 became busy with unrelated work; these are separate from the CPU-0 matrix.

| Metric | Native before | Native current | Go |
| --- | ---: | ---: | ---: |
| Idle TCP p99 microseconds | 309.6 | 284.4 | 185.8 |
| Idle UDP p99 microseconds | 288.8 | 272.4 | 256.4 |
| Paced TCP p99 microseconds | 1,185.3 | 1,170.7 | 1,407.7 |
| Paced achieved MiB/s | 31.93 | 31.84 | 31.95 |
| Saturated TCP p99 microseconds | 5,239.7 | 5,188.3 | 8,075.3 |
| Saturated achieved MiB/s | 121.57 | 121.07 | 74.01 |
| Transfer MB/s | 108.07 | 107.94 | 80.42 |
| Transfer Go B/op | 5,898 | 5,379 | 43,506 |
| Transfer Go allocs/op | 195.0 | 165.3 | 1,203.0 |

Observed native idle TCP p99 falls 8.1%, UDP p99 5.7%, and idle TCP allocations
22 to 16. Native still loses idle latency to Go. At near-equal 32 MiB/s load,
native p99 is 16.8% lower than Go. Saturated delivered rates differ. Do not claim
a universal 2.5x latency win or statistical significance from three processes.

Latest single native 500-user capacity validation: 90.94 MiB/s, 259.4 MiB entry
cgroup peak, 277.7 MiB final/peak RSS, 270.8 MiB RSS two seconds after close,
7.570 CPU seconds; heartbeat p50/p99 988.0/3597.4 microseconds. All passed. Its
p99 is higher than the earlier native samples; do not turn the idle improvement
into a claim of better 500-user tail latency. No fresh repeated Go capacity
comparison accompanies this last validation.

More frequent I/O polling, a multithread scheduler for a one-worker listener,
and higher priority for small TCP writes were tested and removed because they
did not consistently improve latency. Per-wait raw-read closures were replaced
with reusable per-reader storage to remove their allocation overhead.

## Latest Allocation Follow-up

The final implementation additionally changes the regular Go buffer pool to
store `*[4096]byte`, avoiding boxed slice-header allocations at `Put`, and reuses
one callback/byte-count field per `ReadVReader`. Public buffer APIs and ownership
rules stay intact. Focused tests verify zero-allocation regular reuse and readv
recovery after deadlines, repeated reads and EOF. These fixes apply to Go too.

`build_assets/native-max-20261117/summary.json` records three fresh two-second
samples per before/current/Go case, CPU affinity 14, GOMAXPROCS=1, Salamander.
Native Transfer: 110.06 -> 113.02 MB/s; Go current 84.87. Native Transfer Go
allocation bytes 5436 -> 4260; allocation count 167.3 -> 115.0. Upload/download
decrease about 1%; latency is effectively unchanged. Against Go, current native
Transfer/upload/download ratios are 1.33x/1.83x/1.70x. Go remains faster idle.
Paced p99 is 1137us native vs 1233us Go at achieved 31.85/31.95 MiB/s.

Latest capacity pair (one fresh sample each with current buffers): native
89.96 MiB/s, 254.7 MiB cgroup peak, 272.5 MiB final RSS, 7.782 CPU seconds;
Go 24.24 MiB/s, 280.6 MiB peak, 269.7 MiB final RSS, 16.607 CPU seconds.
Observed throughput ratio 3.71x, but native heartbeat p99 4182.5us is higher
than Go 3923.5us. Do not claim lower tail latency from this capacity pair.

TCP command batching and caller-slice shared publication were measured and
removed: no consistent win across the routing/latency workloads. Final changes
contain neither prototype.

kettuRay integration is complete in its commit `dde0370`. Its Linux build
embeds Rust-enabled Xray, defaulting to published Xray revision `9bef1a20`.
Compatible direct hy2 profiles and HTTP probes select Rust; sing-box supplies
TUN. Windows/insecure/unsupported settings and chained exits keep compatibility
backends. Actual local TLS/Salamander HTTP traffic verifies Rust startup without
Go fallback. Core race tests and the Linux Wails app build pass. Preserve its
user-owned `cores/slipstream-client` change outside commits.

## Latest Flow-Credit Follow-up

TCP tasks retain their receive-credit semaphore from registration, removing
the flow-map mutex, ID lookup and per-frame `Arc` clone/drop on publication.
Removing a flow still closes its semaphore. A regression exhausts credits,
closes/re-registers a flow ID and verifies the old handle cannot publish.
UDP lookup, shared layout 5, engine ABI 4 and memory budgets are unchanged.

Go `Conn.waitWrite` tries ready credits and direct shared publication before
creating a deadline timer. Errors, expired deadlines and engine shutdown are
checked first; blocked operations keep their deadline-change/close select and
queued-frame accounting. Tests verify zero allocations on the ready-credit and
direct-publication paths with a future deadline, and failed ready operations
do not consume credits.

`build_assets/native-flow-20261117` contains source/binary hashes, implementation
patch, the runner, raw data and summaries. Three fresh two-second samples per
before/current/Go routing cell rotate backend order on CPU 14, GOMAXPROCS=1,
Salamander, 32 MiB/s paced load, without simultaneous builds. Before uses the
`ed2b49a0` allocation-follow-up binary. Current paced p99 is 1009.7us versus
1048.0us before (3.7% lower), Go 1114.7us, at approximately equal delivered
31.94 MiB/s. Idle TCP p99 is unchanged: 230.6 -> 231.2us, Go 145.5us.
Transfer/upload/download rates are 116.71/155.13/151.52 MB/s versus before
117.95/154.97/150.30 and Go 91.46/87.24/89.20. Transfer CPU falls about 1%.
No universal or statistically established gain follows from three short runs.

Three fresh native 500-user samples per version pass; before/current means:
101.09/100.52 MiB/s, entry cgroup peak 262.7/264.4 MiB, post-close RSS
278.1/280.5 MiB, CPU 6.873/6.799 seconds, heartbeat p99 1684.9/1744.5us.
There is no demonstrated capacity-memory or capacity-tail improvement.
One fresh Go sample gives 26.56 MiB/s, 308.4 MiB cgroup peak, 268.4 MiB
post-close RSS, 16.578 CPU seconds, heartbeat p99 3891.2us. The native/Go
rate ratio is 3.78x in this fixture; upstream session counts differ as usual.

Passed: 45 Rust engine tests with and without allocation profiling, all-target
Clippy with warnings denied, Go race tests for xhysteria/hysteria/full interop
(capacity run separately), packaged NodeX config/API/traffic, native/Go interop,
both bridges, deadlines, short-flow capacity recovery, hopping, fragmented UDP,
and the synthetic 100ms RTT transfer. Linux amd64 and Android arm64/API 26 CLI
artifacts are rebuilt. kettuRay was not changed or rebuilt during this stage.

## Packet-Loss Follow-Up

Packet-loss follow-up on 2026-11-17: see `native/libxhysteria/PACKET_LOSS.md`.
Added per-native-QUIC send/loss/MTU and DATAGRAM queue eviction diagnostics,
Go loss deltas, a read-only Linux per-process UDP sampler, and native/Go saturated
single-leg/cascade UDP sequence tests. Fixed a confirmed native BBR max-filter
expiration bug; a 10x bandwidth decrease previously retained the peak for
30 rounds. Regression tests cover falling rates and application-limited samples.
Current desktop Xray had zero kernel UDP/socket drops over 60 seconds, with no
QUIC diagnostic environment enabled. Local native load delivered 8000/8000 UDP
messages; Go lost 0.8–3.35% in the four matching saturated single-core scenarios.
Twelve native 10s samples showed 4,288,480 sent QUIC packets and no declared loss
or observed queue/MTU drops. This is loopback evidence, not VPS/WAN verification.
Artifacts: `build_assets/packet-loss-20261117/`. No production VPN/VPS restart,
sysctl changes, or deployment occurred. Compatibility/masking request remains
pending beyond the address-hopping/string-HTTP stage described below.
All 181 vendored Quinn tests, 45 engine tests, all-target Clippy, Go race/short
interop, Go-only Hysteria, and eight loaded/four idle loss scenarios passed.
Linux diagnostic Xray rebuilt; Android not rebuilt in this stage. A subsequent
unmixed 30-second live sample also showed zero kernel/Xray/physical-NIC drops.

## Native Masking Follow-Up

Native `udphop` now supports `remoteIPs` CIDRs, IP-only hopping with the original
port, combined IP/port selection, and `remoteOnce` (`perConnRemote`). Once-only
selection is per QUIC session and has a lock-free fixed destination. Combined
Remote/RemoteOnce flags preserve interval hopping like Go. Go normalizes CIDRs
and validates addresses/ports/timers; Rust independently validates serialized
prefixes and IPv4 socket compatibility. Logical QUIC peers and protected local
sockets remain unchanged, including GRO/GSO metadata and delayed replies.
Ten Rust hopping tests cover prefix validation, mapped/IPv6 families, once-only
selection, stable sockets and delayed GRO replies. Wire tests cover Go/native
server × Salamander combinations, with engine-count assertions preventing
fallback false positives. The combined IP/port fixture must bind the full
Cartesian product: native correctly samples IP and port independently; the
first two-front diagonal-only fixture caused expected startup/transfer failures.
The corrected twenty-case address suite passed with Go race enabled, including
four IPv6 wire cases. The four original remote-port cases also passed.

Native HTTP masquerading now supports `string` and a Go-compatible default
404 response, with status/headers/body, HEAD, bodyless 204/304 and Date handling.
Prepared response bodies are shared across requests/connections, not copied.
The active HTTP request-task count is bounded, preserving ten-second request
and unauthenticated-session limits. Invalid/header-injection, connection-specific
HTTP/1 headers, trailers, mismatched Content-Length and provisional 1xx settings
fail before startup. Twenty-four HTTP interop scenarios compare
Go/native × Salamander × six cover-response types, including 640 KiB bodies,
invalid/valid auth and authenticated TCP/fragmented UDP.
`httpdate` 1.0.3 is the only new dependency; other locked versions are preserved.
The CGO configuration remains capped at 1 MiB of serialized JSON, including
the cover body; this stage does not remove that existing startup bound.
Validation: all 54 native engine tests, all-target Clippy with warnings denied,
the full Go race/short hysteria+xhysteria+interop suite, focused HTTP race tests,
and Go-only Hysteria tests passed. Linux amd64 and Android arm64/API 26 binaries
are at `build_assets/xray-native-masking` and
`build_assets/xray-native-masking-android-arm64`. No runtime/service restart,
deployment, sibling-repo modification, or commit occurred in this stage.

Remaining masking work after the streaming stage below: arbitrary UDP/TCP finalmask
adapters, local socket/IP hopping, custom/chained packet dialers, Chrome Parrot,
and advanced TLS/ECH. bypclient's separate Kotlin gate still forces Go for new
IP/once modes; it and kettuRay were not modified or deployed in this stage.

## Streaming HTTP Cover Handlers

Native listeners now support HTTP `file` and `proxy` masquerading without
falling back to Go QUIC. Rust owns QUIC/H3/auth and forwards only cover requests
over existing bounded flows to the same Go FileServer/ReverseProxy handlers as
the Go backend. Shared/socket bridges use optional frame kinds 20/21/22 for
request/response metadata and trailers; rebuild Rust and Go together. The public
ABI remains version 4, shared layout 5. There are no new dependencies this stage.
Default 404/string retain their Rust fast path without a Go cover flow or
cancellation-monitor allocation.

Bodies stream in 16 KiB chunks with four directional credits, native flow
admission, and engine/process send budgets; proxy copies use the existing 32 KiB
pool. Large files/uploads are not serialized into the 1 MiB startup JSON limit.
H3 request field sections and native upstream response headers are limited to
32 KiB; bridge metadata is bounded at 69,632 bytes. These are local buffer bounds,
not a whole-process RSS guarantee. Native listeners clone/own proxy transports
and close idle upstream connections on shutdown.
Content-Length is independently validated for both bridge directions, including
conflicts, overflow, and short/long bodies; early responses and HEAD/204/304 do
not require reading or sending disallowed bodies.

The bridge preserves HEAD, Range, conditional file requests, redirects/indexes,
path/query, host/X-Forwarded settings, repeated response headers, request/response
trailers, informational responses, HTTP/HTTPS/Unix upstreams, and explicit
HTTPS-insecure settings. Auth still returns 233 without invoking cover handlers.
Request/unauthenticated-session deadlines remain ten seconds. Request cancellation
now watches Quinn's send-side stopped future and promptly cancels Go upstream
work even after upload EOF; download completion does not wait for unread uploads.
Invalid bridge requests close immediately instead of holding a flow until timeout.
The pinned Go quic-go server replaces req.Trailer at EOF, losing request trailers
when ReverseProxy copies the announced map. Native mutates that map in place;
shared handler Rewrite preserves its live map. The upstream Go bug is documented,
not patched in the module cache.

Validation: 57 Rust engine tests, all-target Clippy with warnings denied,
focused bridge Go race tests repeated ten times, and all 30 streaming HTTP
wire scenarios passed. File cases cover Go/native × Salamander × shared/socket;
proxy cases cover Go/native × HTTP/HTTPS/Unix × forwarding settings. Lifecycle
cases cover request trailers, informational responses, cancel, early response,
default TLS verification, and excessive upstream response headers. Approximately
2 MiB files and full-duplex uploads
exercise bodies larger than the startup JSON limit. Engine-count assertions
prevent native fallback false positives. The full Go race/short suite across
hysteria/xhysteria/interop passed; the final native transport adjustment is also
checked by the focused HTTP suite. Existing 404/string interop remains covered.
Linux amd64 and Android arm64/API 26 binaries are rebuilt at the masking paths
above. No deployment, runtime/service restart, sibling edit, or commit occurs.

Remaining after this HTTP stage: HTTP protocol upgrades/WebSocket tunneling,
UDP/TCP masks, local hopping, custom/chained PacketConn, Chrome Parrot, advanced TLS/ECH,
and sibling client gates. No new throughput/latency factor is established here.

## UDP Mask Packet Adapter — November 17, 2026

Additional UDP finalmasks and client-only local socket hopping now use the existing
Go PacketConn wrappers while native Rust continues QUIC/H3/authentication. Plain
UDP, single Salamander and supported remote hopping retain direct Rust I/O.
Packet transport uses a separate Unix SOCK_DGRAM pair with 24-byte address/length
metadata, reusable 64 KiB buffers and 64 KiB requested kernel socket buffers.
The engine ABI/shared layout stay unchanged; optional JSON packet_bridge and
packet_local fields require matching Go/Rust builds. Network descriptor family
and type are validated before wrapping them as UDP or Unix datagrams.

Fixed native-client startup on dual-stack wildcard sockets: Quinn maps outgoing
IPv4 addresses to IPv6, so the adapter now maps incoming peers equivalently and
omits unknown destination-IP metadata. Prior client transfer tests succeeded only
by falling back to Go; current mixed-backend tests explicitly assert native
engine counts, preventing that false-positive. Client hostname peers are resolved
and normalized before native startup. IPv6 zone addresses are rejected explicitly.

Transient kernel UDP send errors and short/suppressed mask writes are counted,
not fatal; permanent errors and closure stop the engine. Optional ten-second
diagnostics expose packet-bridge send drops. Startup failure, external socket
closure and concurrent Engine.Close release relays/sockets/native handles.
There are no per-QUIC-packet CGO callbacks or unbounded user-space queues.

Initial validation: 61 Rust tests and all-target Clippy; 105 mixed Go/native mask
scenarios with -race (15 mask configurations times seven backend/bridge combinations),
including TCP and fragmented UDP; 12 local hopping scenarios against Go/native
servers on shared/socket bridges, covering local-only, local+remote IP/port and
local+remoteOnce with stacked header/Salamander. Flow continuity and observed
source-port replacement are asserted without reconnecting QUIC engines. The mask
matrix additionally repeats twice with pooling disabled and per-TCP/UDP native
engine-count assertions, so neither flow can hide a Go fallback.
Relay tests repeat under -race and include maximum IPv4 UDP datagrams, ENOBUFS,
bad local/peer/TLS config, context cancellation, and concurrent shutdown.

This compatibility path adds copies/syscalls and retains existing mask allocation
and delay behavior. GSO/PMTUD are disabled and QUIC MTU is fixed at 1200 before
mask expansion. It is not a guarantee against outer fragmentation or network
loss, and no 2.5x or other new performance ratio is claimed. Android VPN handover,
privileged/external-service UDP masks, TCP finalmask, custom/chained packet
dialers, Chrome Parrot, advanced TLS/ECH and sibling gates remain unvalidated or
unsupported. Linux amd64 and Android arm64/API 26 masking binaries are rebuilt at
the paths above; HTTP masquerade and native transfer/concurrency regressions also
pass under -race. No deployment, runtime restart, sibling edit or commit occurs.

## Hybrid Packet Overhead and Loss — November 17, 2026

The subsequent adapter stage removes Rust send/receive scratch buffers and
mutexes. `sendmsg` sends a stack header plus Quinn payload; `recvmsg` receives
directly into Quinn storage, drops malformed/truncated IPC frames and preserves
Tokio readiness. This saves roughly 128 KiB per packet-backed engine. The Go
32-entry direct-mapped peer cache owns stable address copies (zero allocations
on hits; eviction does not mutate previously returned pointers).

Up to eight same-peer QUIC datagrams share one outgoing IPC message, with no
batching wait timer. Go splits them into separate mask writes, including a short
last segment; a transient failure does not discard later segments. This is not
network UDP GSO. `disableGSO=true` opts out, while Brutal remains individually
paced. Header v1 stays for single packets; v2 uses the existing 24 bytes with
stride-u16/length-u16 fields. Only Rust→Go sends batches. Optional JSON
`packet_batch` joins `packet_bridge`/`packet_local`; Go and Rust must be rebuilt
together although engine/shared ABI is unchanged. PMTUD remains off at inner
MTU 1200; outer masks can still exceed path MTU.

The generic path had missed the direct Rust base-socket buffer setup. New
`FinalMask.WithPacketConnSetup` returns a copy, runs before masks and on each
replacement/local hop, and leaves the original untouched. Native config uses
`TunePacketSocket`: requested server RX/TX 4 MiB/1 MiB, client 1 MiB/256 KiB,
matching direct Rust. Linux doubles/caps requests; diagnostics log actual sizes
and errors. No sysctls are changed. Busy kernel queues can retain more memory;
this is not a peak-RSS reduction or whole-node OOM protection. Private sockets
constructed outside FinalMask callbacks are not covered automatically.

The saturated header-mask loss matrix after setup: native one-leg 0% for 256
and 4096 bytes; two-leg 0.35%/0%, with zero observed kernel drops. The two-leg
256-byte repeats miss 0.40%/0.15%; one later diagnostic run misses 0%. Go rows
miss 0.80–2.90% and have 3,464–17,699 process-owned UDP socket drops. The kernel
sampler includes persistent echo sockets, excludes closed/replaced sockets,
and is not per-QUIC loss attribution. Remaining native missing replies are
not retrospectively localized. An engine-level `event=udp_bridge` counter now attributes
exhausted receive-credit drops separately from QUIC DATAGRAM and socket losses.
The latest diagnostic run has zero samples in every counter, not proof of
loss-free production behavior.

The next repeated diagnostic matrix localizes 12/2000 one-leg and 1/2000
two-leg 256-byte missing replies to exactly matching server UDP credit drops.
Go now flushes outstanding receive credits when a UDP per-flow queue drains:
sparse traffic no longer waits for the half-window batch, queued UDP bursts
still batch, and TCP batching/windows stay unchanged. No buffers are enlarged.
An unread Rust flow still drops UDP explicitly and increments the counter.
After this fix, all four native loss rows plus two two-leg/256-byte repeats
deliver 2000/2000 with zero kernel and sampled credit drops; saturated TCP
background is 101.21/101.18 MiB/s one-leg and 49.93/49.51 two-leg. This is not
a production zero-loss guarantee.

Latest hybrid transfer median: native 100.85 MB/s versus Go 86.50 MB/s (~1.17×);
preceding socket-tuned runs showed ~1.14×. Go allocations 12,128 B/op and 394
allocs/op versus 27,262/724. Native CPU remains 1.978 ms/op versus Go 1.404.
Latest idle TCP p50/p99 native 137.9/264.3 µs versus Go 87.7/131.3; UDP
130.4/253.1 versus Go 86.6/144.9 µs. No idle-latency win versus Go or universal
2.5× is demonstrated. The unmodified Go baseline also improves across samples;
do not attribute all timing differences to the credit fix. Runs use
GOMAXPROCS=1, no affinity, three one-second repetitions; do not combine them
with direct-path tables. Go allocation counts exclude Rust and kernel memory.

Current Rust unit tests: 64 (including IPC batching/truncation and credit-drop
isolation). The mask matrix now has 112 combinations (the prior 105 plus seven
`disableGSO` cases), alongside 12 local-hopping cases; both pass under `-race`.
The wider interop run passes native/Go traffic, Brutal, remote IP/port/once
hopping, HTTP file/proxy/string cover handlers and lifecycle, pool/capacity
recovery, dynamic users, short-stream credit recovery, UDP idle expiry and
100 ms RTT transfer. It takes 282 seconds; the initial 180-second runner limit
expired during remote hopping, and the full rerun passes with a 600-second
limit. Rust all-target Clippy and format checks pass, as do all native transport
and finalmask package race tests. The Go-only run passes for hysteria and
finalmask packages; xhysteria is intentionally build-tagged out without native
CGO and must not be listed as a Go-only test package. After the UDP credit fix,
the 112-mask/12-local-hop matrix, traffic/concurrency, Brutal, cascade, cover
handlers/lifecycle, pooling and UDP expiry pass again under -race (110 seconds).
Focused metadata/credit/socket-policy tests repeat five times under -race.
Linux and Android binaries include the final credit fix. Base-socket setup tests
verify before-mask ordering, repeat dials/listens, failed opens, unchanged
original callbacks and actual buffer policy under host clamping. Details/raw
benchmarks: `PERFORMANCE.md`, `PACKET_LOSS.md`, ignored
`build_assets/hybrid-packet-20261117/`. Linux/Android masking binaries at the
existing paths are rebuilt together, not deployed. No commit or sibling edits.

## Remaining Limits

This work does not bound every allocation in the Xray process or the whole
node. GOMEMLIMIT covers Go runtime memory, not Rust or kernel memory.
Receive expansion is bounded but is not redistributed from idle sessions.
Allocator page retention and idle TCP/UDP bridge scheduling remain targets.
Process RX expansion budgets exclude the base client receive windows, and each
listener has its own admission estimate rather than a whole-process RSS cap.
Loopback timing does not model real RU paths, loss, high-bandwidth WAN links
or Android battery/thermal behavior. Existing upstream startup timeouts still
need remote path/configuration evidence. Avoid promising that every metric
beats Go or that a 1 GiB host with all its other services is OOM-proof.
