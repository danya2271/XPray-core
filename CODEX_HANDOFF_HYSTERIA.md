# Codex Handoff: Native Hysteria Memory and Capacity

Date: 2026-11-17
Workspace: `/home/danya2271/ProxVless/XPray-core`

## Request and Scope

Reduce memory, improve Hysteria performance and reliability, and investigate
bottlenecks for 500 users on a 1 GiB RU node. Compare the resulting backend with
Go, including memory, throughput, CPU and latency. The requested 2.5x improvement
in every metric is not universally achieved. The user authorized commits.
Do not push or modify remote VPSs.

The user-owned change in `transport/internet/splithttp/client.go` is unrelated:
preserve it and exclude it from commits. Neighboring DanVlessBot and bypclient
worktrees also contain unrelated changes. Generated binaries and measurements
belong in ignored `build_assets` or `/tmp`.

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
