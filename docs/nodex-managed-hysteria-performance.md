# Managed Hysteria performance investigation

## Native ingress batching and counter follow-up, November 21, 2026

Baseline is `af2494ea`. This follow-up keeps the packet-ring ABI, queue and
socket capacities, flow-credit policy, receive ownership and worker counts
unchanged. In particular, ring head/tail publication and waiting-flag rechecks
retain the **SeqCst wakeup handshake**; the experimental weaker ordering is not
part of this patch.

The production changes are limited to existing-batch work:

* Cache the consumer head within a producer batch, refreshing it with Acquire
  when the conservative cached position would report a full ring. Publish the
  completed batch with the existing SeqCst tail/waiting-flag handshake.
* Empty receives do not rewrite the head. A receive that consumes frames,
  including invalid frames, still publishes progress and wakes an armed writer.
* Aggregate receive and queued-byte/backpressure counters once per batch.
  Invalid tails still count as send failures, while already queued frames
  contribute their bytes and packet counts.
* Skip peer permutation for Unix forwarding and one-packet ring forwarding;
  same-peer ring batches copy in their existing order. Partial acceptance
  still returns a mask in the original packet positions.
* Track retained RX packets with a bitmap rather than rescanning all 32 slots
  for every pending/fill decision. Accepted and failed packets clear both
  their held entry and bitmap bit; unaccepted packets remain in the RX buffers.

### Controlled baseline versus final server matrix

Three fresh-process samples per version, core count and workload, alternating
order before/final, final/before, before/final. Both versions use the **same**
`interop-final.test` client/echo fixture; only the server executable changes.
The server/supervisor/echo are pinned to logical CPUs `0` or `0-3`, with
one/four server workers, and the client to `8-15`, with eight Go workers and
four native workers/pool sessions. These are local CPU-affinity limits, not
separate weak/four-core physical servers or a memory-limited VPS.

Managed native shared RX/direct TX starts with direct receive, then measures
mixed overlap: sixteen TCP flows on A and sixteen on B. Masks are Salamander
plus WireGuard header, with the existing public-socket policy. Each transfer
sample completes 32768 duplex 64 KiB exchanges; each small-message sample
completes 200000 64-byte exchanges. The strict relay/public UDP drop gate is
enabled. CPU below is generation CPU plus supervisor/echo CPU, normalized per
completed exchange; RSS is the generation process's peak, not whole-node RAM.

| Workload / logical server CPUs | Metric | Baseline | Final |
| --- | --- | ---: | ---: |
| 64 KiB duplex / 1 | MB/s | 228.26 | 229.78 |
| | Total server CPU, µs/exchange | 525.48 | 522.63 |
| | TCP p99, ms | 35.908 | 35.642 |
| | Generation peak RSS, MiB | 103.10 | 102.80 |
| 64 B / 1 | Total server CPU, µs/exchange | 42.30 | 41.85 |
| | TCP p99, ms | 2.616 | 2.577 |
| | Generation peak RSS, MiB | 91.46 | 91.68 |
| 64 KiB duplex / 4 | MB/s | 700.80 | 700.32 |
| | Total server CPU, µs/exchange | 645.26 | 646.97 |
| | TCP p99, ms | 12.531 | 12.403 |
| | Generation peak RSS, MiB | 109.10 | 111.90 |
| 64 B / 4 | Total server CPU, µs/exchange | 77.90 | 77.25 |
| | TCP p99, ms | 2.180 | 2.132 |
| | Generation peak RSS, MiB | 93.90 | 96.31 |

All values are medians of three runs. Single-CPU bulk CPU decreases 0.54%
and throughput increases 0.67%; four-CPU bulk throughput changes -0.07% and
CPU +0.26%. Small-message CPU decreases 1.08%/0.83%. These are small changes,
not a substantial speedup or a universal improvement. Four-CPU peak RSS is
about 2.6% higher in both workloads; no memory reduction is established.

For context, four-CPU throughput ranges overlap: 699.83–706.03 before versus
696.74–704.71 MB/s after. Four-CPU bulk p99 ranges are 12.019–12.904 before
and 12.165–13.198 ms after; a lower median is not a bound on latency or proof
of a stable tail-latency gain. The earlier V2/final comparison also moved
four-CPU bulk performance in the opposite direction. It is retained as an
intermediate experiment, not substituted for this baseline comparison.

All **24/24** matrix samples pass and report zero ingress, queue, send and
public UDP drops. Separate final-candidate application acceptance repeats
deliver **36000/36000** distinct 1200-byte UDP replies: three 6000-reply runs
per core count under 32 unpaced duplex TCP background flows, without application
retransmission. Achieved background rates are 194.40–196.75 MiB/s on one CPU
and 506.75–517.83 MiB/s on four. Both continuous-load drain checks also pass,
with 47154/123716 exchanges and zero relay/public-socket drops. These local
results do not establish zero WAN loss or an OOM fix.

### Reproduction and validation record

Raw data, baseline identity, comparison scripts and build/test logs are in
ignored `build_assets/ingress-opt-20261121/`. The controlled matrix uses
`compare-baseline-final.sh`, `compare-baseline-final.log` and
`baseline-cpu*-r*-{before,final}-{transfer,latency}.log`; `results.json`
records individual observations and medians. Earlier `compare`, `compare-fast`
and `compare-final` files describe intermediate comparisons. The experimental
`atomic.patch`/`xray-atomic` are not the shipped ordering.

126 release Rust engine unit tests pass, including new cached-head refresh,
same-peer/short-final-segment grouping, partial queue acceptance masks on
Unix/ring paths, invalid-tail counter accounting and held-bitmap assertions.
The existing concurrent 10000-frame ring wakeup/closure test remains enabled.
Production-library Clippy passes with warnings denied. Strict all-target
Clippy fails on the pre-existing `type_complexity` lint at
`receive_owner.rs:327`; the complete target check passes with only that lint
allowed. The unrelated fixture is not changed or silently counted as passing
the strict check.

Fresh-linked Go `-race` tests pass for `transport/internet/xhysteria`,
`transport/internet/handoff` and `cmd/nodex-xray-supervisor`. External-generation
cases require their explicit binary environment and are not covered by an
unconfigured package run. The Linux release executable is rebuilt from the
final source as `xray-validated`; fresh saturated UDP/drain runs against it
are recorded separately in `validated-cpu*.log`: both deliver 6000/6000 UDP
replies and complete drain with zero relay/public UDP drops, with
35853/105317 loaded exchanges. Their achieved TCP background rates are
194.64/449.30 MiB/s, respectively. They are additional final-build acceptance
checks, not extra samples in the baseline performance matrix. Source changes
since the measured candidate are tests, assertions and a comment only.

Android arm64/API 26 also builds as `xray-android-arm64`, using NDK
30.0.14904198. This validates compilation/linking, not device operation.
Artifact hashes are recorded in `artifact-hashes-resume.txt`. No deployment
or process restart on a remote VPS is performed.

## Lossless backpressure fixes and qualification, November 20, 2026

Two independently reproduced loss mechanisms are fixed:

* Native ingress previously consumed public datagrams after both the ring and
  its bounded queue filled, discarding the unqueued tail. It now reports which
  original packets were accepted and retains the rest in the **existing** RX
  buffers. Admission is captured at receive time; GRO segments and per-peer
  ordering survive retries. Fence, retirement and public-RX release cannot
  overtake a held batch. Other generation groups in that batch can progress.
* Go ingress previously discarded a datagram when the private Unix send returned
  `EAGAIN`. The normal nonblocking fast path remains; on backpressure it retains
  that datagram and retries through Go's netpoll. Admission changes do not wait
  for the write, but the receive fence does. Closing ingress interrupts the
  pending write.

The stronger application UDP test also exposed loss **without managed ingress**:
native direct delivered 1998/2000 replies, with exactly two
`credit_dropped_total` events and zero QUIC receive/send, relay and public-socket
drops. Native's Rust-to-Go bridge waited only 2 ms for flow credit and then
silently discarded the application datagram. It now waits for returned credit
or flow closure on both client/server and shared/socket bridges. The existing
bounded flow windows are unchanged; closing a flow wakes the pending acquire.

No ring, private queue, public socket buffer or flow window is enlarged by these
fixes. No retry timer, application retransmission or per-datagram goroutine is
added. Hard send errors and kernel overflow remain real losses and are counted;
zero queue drops alone is **not** an acceptance criterion.

### Completed local checks

The measured binary includes both ingress fixes and the native credit fix.
Server/generation/echo CPU sets were `0` / `0-3`; client was `8-15`, with native
client pool size four. Masks were Salamander plus WireGuard header. The public
managed socket used the existing native buffer policy, not a larger test-only
buffer. All TCP background flows exchanged 64 KiB in both directions.

* **12/12** nonpaced application UDP checks: three runs each for native ring
  and Go Unix ingress, on one/four server cores. Every run delivered
  **6000/6000** distinct 1200-byte replies, with zero relay and public-socket
  drops. There were 32 continuous unpaced TCP background flows, and an
  eight-packet UDP response window, without sleep pacing or retransmission.
  A's UDP flow stayed open before/after cutover; B used a separate client.
* **4/4** paced UDP checks: both server backends, one/four cores, each
  **2000/2000** replies and zero relay/public-socket drops.
* **18/18** long strict TCP checks: three runs for native direct, native mixed
  A+B and Go mixed A+B, on each core count, 32768 exchanges per run. Every
  completed run reported zero relay and public-socket drops. Mixed means
  sixteen TCP flows remain on A and sixteen use B, rather than replacing A.
* Native Unix ingress without shared ring/direct TX also passed one-core
  nonpaced **6000/6000** and paced **2000/2000** checks with zero counted drops.
* Go `-race` handoff/supervisor/Hysteria suites and 121 Rust engine unit tests
  passed; engine Clippy passed with warnings denied.
  The native-tagged Go `-race` suite also passed, including xhysteria's
  stalled-consumer fixture. That fixture now verifies delivery and order of all
  768 retained datagrams after receiver resumption instead of expecting drops
  as a signal that the queue is full.
* Real supervisor lifecycle tests passed for Go and native: two consecutive
  updates, retained A/B streams, natural drain, direct-RX reacquisition on
  native, SIGKILL of the supervisor during switching/draining, successful
  recovery and failed-probe rollback. The persistent broker stayed alive.
  These do **not** test recovery after killing the broker.
* Drain under **continuous saturated B traffic** passed on one/four cores:
  32 live B TCP flows continued while A remained open through the ingress
  fence, then closed naturally. Both runs reached verified drain and returned
  B to direct RX without reconnecting B or reporting relay/kernel drops
  (47210 / 123684 completed exchanges respectively). The test additionally
  requires at least 128 exchanges after the receive transfer.
* After the final rebuild, one additional nonpaced UDP run for each
  backend/core-count combination again delivered **6000/6000**, with zero relay
  and public-socket drops. TCP/REALITY supervisor generation/probe tests passed.

Long TCP medians from the completed three-round matrix:

| Server cores | Variant | Duplex MB/s | Exchange p99, ms |
| --- | --- | ---: | ---: |
| 1 | Native direct | 235.93 | 32.177 |
| 1 | Native mixed A+B | 227.89 | 35.062 |
| 1 | Go mixed A+B | 121.61 | 75.589 |
| 4 | Native direct | 641.60 | 10.582 |
| 4 | Native mixed A+B | 709.47 | 12.346 |
| 4 | Go mixed A+B | 320.59 | 29.451 |

The one-core native overlap median is about 3.4% below direct, not zero
overhead. Mixed uses two generation processes and two client QUIC pools;
the four-core throughput difference cannot be attributed to relay efficiency
alone. Lifecycle tests ran simultaneously on disjoint CPU sets `16-23`;
shared cache/memory and local-machine noise remain. This is loopback evidence,
not WAN, IPv6, arbitrary UDP flood or production qualification.

A separate **ordinary Go direct** one-core control failed the strict kernel
gate (`public_udp_drops=1032930`, relay drops zero). The masked ordinary Go
listener and explicitly tuned managed public socket did not share the same
buffer policy; this is not a valid direct-versus-managed overhead comparison.
The failure is retained rather than silently counted as a passing TCP test.
Native application-credit failures from before the fix are also retained.

### Reproduction and limits

Build both the server and interop driver against the same native archive
(including `--features engine`). Start from a clean environment, using the
existing native linker-normalization helper when needed:

```sh
export XHYS_HANDOFF_SERVER_BINARY=/path/to/xray-native
export XHYS_HANDOFF_SERVER_CPUS=0 XHYS_HANDOFF_SERVER_PROCS=1
export XHYS_HANDOFF_MODE=managed XHYS_HANDOFF_BACKEND=native
export XHYS_HANDOFF_TUNE_PUBLIC=1 XHYS_HANDOFF_REQUIRE_NO_DROPS=1
export XHYS_HANDOFF_PACKET_BATCH=1 XHYS_HANDOFF_NATIVE_INGRESS=1
export XHYS_HANDOFF_SHARED_RX=1 XHYS_HANDOFF_DIRECT_TX=1 XHYS_HANDOFF_DIRECT_RX=1
export XRAY_HYSTERIA_NATIVE=1 XRAY_HYSTERIA_CLIENT_WORKERS=4 XRAY_HYSTERIA_POOL_SIZE=4
taskset -c 8-15 /path/to/interop.test \
    -test.run '^TestHandoffSaturatedUDP$' -test.cpu 8 -test.count 3 -test.v
taskset -c 8-15 /path/to/interop.test \
    -test.run '^TestHandoffDrainUnderSaturatedLoad$' -test.cpu 8 -test.v
XHYS_HANDOFF_BENCH_MIXED_GENERATIONS=1 taskset -c 8-15 /path/to/interop.test \
    -test.run '^$' -test.bench '^BenchmarkHandoffServerTransfer$' \
    -test.benchtime 32768x -test.cpu 8 -test.count 3 -test.v
```

For four cores use `SERVER_CPUS=0-3`, `SERVER_PROCS=4`. For Go server set
`BACKEND=go` and unset `PACKET_BATCH`, `NATIVE_INGRESS`, `SHARED_RX`, `DIRECT_TX`
and `DIRECT_RX` variables (all prefixed `XHYS_HANDOFF_`). For native Unix ingress
retain `PACKET_BATCH` and `NATIVE_INGRESS` but unset the three ring/direct flags.
The driver must use `xhysteria_native,xhysteria_interop` build tags. Diagnostic
mode `XHYS_HANDOFF_DIAGNOSTIC=1` preserves server engine logs.

Backpressure removes deliberate discard of already-read packets, not UDP's
finite-capacity limits. A permanently stalled receiver can still overflow the
kernel/QUIC receive buffer under unbounded offered load. Go's sole public reader
can head-of-line block on a stalled generation; native groups already retained
in the same batch can progress, but the next batch waits. Native per-connection
UDP reception can wait on one flow's credit. These tradeoffs are explicit,
not an infinite zero-loss guarantee or proof of all-mode production readiness.

Raw logs, failed controls and binary hashes are retained locally under
`build_assets/handoff-lossless-20261120/` in the primary XPray checkout;
source fixtures and this report are committed. Historical failures below
describe older builds and are not erased by these results.

## Historical loss accounting and rejected direct-RX experiment, November 20, 2026

The load fixture now snapshots Linux public-socket `sk_drops` and receive queue
bytes separately from relay drops. It resolves the exact bind and pins its
inode; a missing/rebound socket or regressed counter is an error, not zero
loss. This is test-only `/proc/self/net/udp` inspection, with no per-packet
production overhead. A focused test deliberately fills a small unread UDP
socket and proves kernel overflow is reported.

Use `XHYS_HANDOFF_REQUIRE_NO_DROPS=1` for a strict TCP benchmark gate: it fails
on either relay or public UDP drops. Paced UDP already requires every sequence
and now checks both counters too. A successful ordinary TCP benchmark can
hide packet loss behind QUIC retransmission; it is not a release acceptance.
The strict one-core ordinary-native run passed, while overlap failed with
2217 relay drops and zero kernel drops. Logs: `/tmp/nodex-strict-{ordinary,overlap}.log`.

A single exploratory recheck with a temporary direct-RX experiment showed
zero kernel drops in direct, overlap and post-retirement on four/one cores.
Overlap still reported 30 relay drops on four cores and 4182 on one. This
supports targeting the relay, not merely its public socket buffer, for those
runs. Logs: `/tmp/nodex-kernel-recheck-20261120/`.

The direct-RX experiment bypassed Initial filtering for entirely short-header
GRO batches and avoided cloning an unchanged waker. An interleaved three-round
post-retirement comparison (32 flows, 64 KiB, 8192 exchanges, CPU sets 0–3/0
and client 8–15) produced:

| Cores | Variant | MB/s | Exchange p99, ms | Server CPU, µs/exchange |
| --- | --- | ---: | ---: | ---: |
| 4 | Current native managed direct | 581.08 | 21.496 | 689.54 |
| 4 | Experimental short-header path | 589.55 | 22.766 | 694.91 |
| 1 | Current native managed direct | 218.34 | 42.358 | 539.11 |
| 1 | Experimental short-header path | 220.37 | 36.404 | 534.19 |

All twelve completed with zero counted public/relay drops. The throughput
difference is only ~1–1.5%, four-core CPU is not improved, and one four-core
experiment had a substantial outlier. **The experiment was rejected and is
not in the production code.** GRO/Initial regression tests are retained.
Logs: `/tmp/nodex-fast-rx-compare-20261120/`. These are local measurements,
not proof that managed direct has no overhead or saturated overlap is fixed.

## Historical pre-fix lifecycle and acceptance status, November 20, 2026

The historical experiments below predate the integrated persistent broker,
safe engine drain and return-to-direct receive lifecycle. Those statements
about missing lifecycle support describe the old experiment, not today's
implementation; see `nodex-managed-lifecycle.md`.

At the time of the measurements in this section, saturated-overlap loss was
**not fixed**. The user's completed
Android comparison (three runs, fresh B client, 32 TCP flows, 64 KiB exchanges,
Salamander/WireGuard header, matched public socket policy) reports:

| Saturated variant | Duplex MB/s | Exchange p99, ms | Drops |
| --- | ---: | ---: | --- |
| Native direct, 4 server cores | 586 | 21.7 | Baseline |
| Native ring, 4 server cores | 548 | 28.5 | 0–356 per run |
| Native direct, 1 server core | 219 | 56 | Baseline |
| Native ring, 1 server core | 197 | 79 | Still present |
| Go direct, 1 server core | 109 | 51 | Baseline |

Fresh-B **paced** UDP tests completed on both four and one server cores:
ring and receive-lease variants each delivered 6000/6000 replies over three
runs with zero counted ingress drops. Ordinary native/Go relays failed strict
checks. This is moderate paced traffic, **not saturated UDP acceptance**.

Our lifecycle checkout also produced single exploratory fresh-B samples:
4 cores direct/ring/lease 564.33/483.14/486.50 MB/s with ring/lease drops
163/398; 1 core 230.61/195.53/193.06 MB/s with drops 2172/2010.
These are not medians and ran on a shared local machine. Do not treat them as
a controlled comparison with the user's matrix. The raw exploratory logs are
local `/tmp/nodex-final-load/`, not committed release evidence.

The fixture accepts `XHYS_HANDOFF_BENCH_AFTER_DRAIN=1` (managed + shared RX +
direct TX): switch to B, close A's client, wait for verified natural drain,
require `receive-direct`, and only then start measurement. This is separate
from fresh-B overlap (`XHYS_HANDOFF_BENCH_NEW_GENERATION=1`). A completed
transfer test alone does not prove post-drain throughput matches native.

The completed local interleaved three-round post-drain matrix used the same
native binary, 32 flows, 64 KiB exchanges and 4096 exchanges per run. Server
CPU sets were 0–3 / 0, client 8–15. Every run required `receive-direct` before
measurement; all twelve passed with zero counted ingress drops:

| Server cores | Variant | Median MB/s | Median exchange p99, ms |
| --- | --- | ---: | ---: |
| 4 | Ordinary native direct | 560.51 | 24.438 |
| 4 | Native after drain/direct reacquire | 561.78 | 24.830 |
| 1 | Ordinary native direct | 230.68 | 49.846 |
| 1 | Native after drain/direct reacquire | 218.99 | 52.964 |

The one-core result is about 5% lower, not zero overhead. This fixture retains
the already-drained old process for diagnostics; the actual supervisor also
stops/removes it after the certificate. Further isolated post-retirement
measurements are needed. Local raw logs:
`/tmp/nodex-postdrain-final-20261120/`. Earlier failed fixture attempts are
excluded, retained at `/tmp/nodex-postdrain-20261120/` and
`/tmp/nodex-postdrain-{verified,fixed}.log`; they failed before measurement.
These runs do not erase the saturated **overlap** failures above.

`XHYS_HANDOFF_BENCH_AFTER_RETIRE=1` additionally requires the old generation's
`drained` certificate, sends SIGTERM and waits for its successful exit before
measuring B. A second interleaved three-round matrix (same CPU sets, workload
and binary) completed all twelve runs with zero counted ingress drops:

| Server cores | Variant | Median MB/s | Median exchange p99, ms | Server total CPU, µs/exchange |
| --- | --- | ---: | ---: | ---: |
| 4 | Ordinary native direct | 545.29 | 41.713 | 741.74 |
| 4 | Native after old process exit | 547.35 | 23.969 | 729.79 |
| 1 | Ordinary native direct | 230.52 | 66.558 | 511.56 |
| 1 | Native after old process exit | 220.83 | 51.598 | 532.31 |

CPU is the median per-run sum of generation plus supervisor/echo CPU. On one
core throughput remains ~4.2% lower and CPU/exchange ~4.1% higher even after
old process exit. The short-run p99 baseline varies across these separate
matrices; it does not establish a managed latency advantage. This is local
loopback evidence, not WAN or production acceptance. Logs:
`/tmp/nodex-retired-final-20261120/`.

Production defaults remain unchanged. The historical performance gate included
saturated overlap with no additional relay drops and controlled post-drain
direct baselines on one/four cores. Do not increase queues, weaken strict
loss assertions, or confuse QUIC retransmission with preserving every
application datagram.

## Grouping interleaved peers within an existing receive batch

Native ingress now groups packets for the same peer within a batch already
routed to one generation/local destination. Per-peer order is unchanged;
no wait timer, heap allocation, wire ABI change or queue enlargement is added.
The fixed 32-element stack batch lets the existing segmented ring frame
represent interleaved peer bursts without consuming a separate slot for each
packet. Tests prove 32 alternating packets for two peers occupy two slots,
retain sequence/destination metadata, and do not change the Unix relay path.

An interleaved three-round **fresh-B overlap** comparison against the previous
lifecycle binary used 32 TCP flows, 64 KiB exchanges, 8192 exchanges per run,
server CPU sets 0–3 / 0 and client 8–15. Both drivers/generations were built
with the respective native archive. Medians:

| Cores | Native ring variant | MB/s | Exchange p99, ms | Ingress drops | Server CPU, µs/exchange |
| --- | --- | ---: | ---: | ---: | ---: |
| 4 | Before grouping | 525.36 | 29.785 | 257 | 790.21 |
| 4 | Grouped peers | 525.95 | 29.349 | 68 | 783.93 |
| 1 | Before grouping | 201.18 | 54.424 | 4456 | 596.71 |
| 1 | Grouped peers | 198.14 | 57.595 | 2061 | 606.48 |

The four-core drop median is ~74% lower, the one-core median ~54% lower.
But every grouped saturated run still dropped packets (4 cores: 60–81;
1 core: 1889–2329). One-core throughput is ~1.5% lower and CPU/exchange ~1.6%
higher. This is a measured trade-off, **not saturated loss-free acceptance**.
Raw logs: `/tmp/nodex-ring-grouping-20261120/`.

The grouped build also delivered 6000/6000 paced UDP replies over three
runs on each of four/one server cores, with zero counted ingress drops and
both generations live. The lifecycle/recovery test ran concurrently with
these paced checks; do not use their latency values as an isolated performance
comparison. The saturated matrix ran sequentially without builds in flight.
Local evidence copies and binary hashes are retained under
`build_assets/handoff-lifecycle-20261120/` in the Android workspace; logs are
not committed. Loss-free paced traffic does not cancel the saturation failures.

## Historical experiments

The original managed Hysteria relay is not suitable for enabling native NodeX
updates by default. The reported 32-flow, 64 KiB duplex regression was reproduced
locally: about 270 MB/s, 102 ms p99, and 78,695 relay drops for 8192 exchanges.
Neither the cheap CID classifier nor normal-start tests demonstrate acceptable
managed throughput or tail latency.

## Experimental paths

The worktree adds explicit, opt-in experiments, not production activation:

- `packet_batch` in the generation manifest permits the native engine to use
  segmented Unix frames, up to eight UDP datagrams per frame. It defaults to
  false for existing manifests. Frames preserve peer address and datagram
  boundaries, including a short final datagram. The Go endpoint can decode
  segmented frames too; unmodified native engines must not receive them.
- `xhysteria.NewNativeIngress` is a Linux-only experimental relay using the
  existing Rust mask implementation and batched public UDP I/O. The Go wrapper
  performs setup and control only. The receiver owns Initial/0-RTT pins and
  acknowledges admission changes between batches. Neighboring datagrams for
  the same peer/generation can share one vectored Unix frame. There is no wait
  to fill a frame. The generation worker uses `recvmmsg` on Linux/Android.
- Optional `transmit_fd` gives a native worker a duplicate of the public UDP
  descriptor for sending replies directly with its configured masks and GSO.
  Only the ingress receives from that descriptor. Its bound address must match
  the ingress; an unsupported backend or mask configuration is rejected. The
  default remains relay egress, and ordinary native startup remains unchanged.
- Optional `ring_fds` passes a sealed memfd and two nonblocking eventfds to a
  Linux native generation. `NativeIngress.AddRing` creates a fixed SPSC receive
  ring (32 frames, approximately 2 MiB mapped capacity) instead of Unix packet
  sends. Frames may contain up to 32 equal-stride datagrams with a short last
  segment; the Unix format retains its limit of eight. Data is copied into the
  ring and then into Quinn buffers, **not** advertised as zero-copy. Publication
  and wakeups are coalesced per ready batch, with no fill timer. Go QUIC rejects
  this manifest; omitted `ring_fds` retains the compatible Unix path.

The native relay now retries transient Unix socket backpressure with a separate
bounded queue per generation (512 KiB and 256 frames). The healthy fast path
still borrows receive buffers without allocation; only queued frames are copied.
Each generation gets an independent readiness turn, so a stalled worker does
not block admission changes or another worker. Queue overflow remains packet
loss rather than unbounded memory or blocking the public receive owner.
Directional counters separate queue overflow, send errors and egress failures.
They are approximate telemetry, **not** a drain certificate.

At saturation this still causes loss and QUIC retransmissions. It is an
unresolved performance defect, not a successful zero-overhead implementation.
This ingress has no recovery protocol or QUIC retirement certificate and is
not connected to the node supervisor or TUI activation path.

## Exploratory results, November 20, 2026

These are **single samples**, not medians or release acceptance results. All
use 32 TCP flows exchanging 64 KiB, 8192 completed exchanges, server/supervisor/
echo on physical CPUs 0–3 and client on 8–15, four native client workers and
four sessions, Salamander plus WireGuard header, and a public UDP buffer policy
matching direct native. MB/s sums both directions. Some builds ran concurrently
with exploratory tests; an interleaved repeated matrix is still required.

| Experimental variant | MB/s | Exchange p99, ms | Relay drops |
| --- | ---: | ---: | ---: |
| Original managed relay | 270.23 | 101.592 | 78,695 |
| Segmented native egress through Go relay | 284.94 | 170.660 | 75,400 |
| Rust ingress, individual worker ingress frames | 441.10 | 91.338 | 204,499 |
| Rust ingress, grouped worker ingress frames | 507.12 | 54.131 | 137,235 |
| Grouped Rust ingress, direct native UDP replies | 481.29 | 46.214 | 104,974 |
| Direct replies plus worker `recvmmsg` | 494.10 | 65.001 | 108,990 |

No row meets the requirement of retaining direct native performance with low
tail latency. The direct-native figures in the user's three-sample comparison
are 613 MB/s and 20.3 ms p99. These new single-sample experiments must not be
substituted for that controlled baseline. A user-space Unix relay still costs
CPU, queue capacity and scheduling. The packet data path, including overload
behavior, needs further work before seamless Hysteria activation is enabled.

The local user-space `perf` recording completed, but the stripped Xray binary
does not provide reliable per-function attribution. It does not prove a sole
cause. A symbolized profile, directional drop accounting, and repeatable load
measurements are required before making another performance claim.

## Reproduction

Build the native archive, Xray and benchmark driver from the same checkout.
Use `native/libxhysteria/link-go.sh` when the local Go linker requires normalized
Rust archives. The interop driver is test-only and never creates public node
services. Example after building it as `/tmp/handoff-load.test`:

```sh
XHYS_HANDOFF_SERVER_BINARY=/tmp/xray-native \
XHYS_HANDOFF_SERVER_CPUS=0-3 XHYS_HANDOFF_SERVER_PROCS=4 \
XHYS_HANDOFF_MODE=managed XHYS_HANDOFF_BACKEND=native \
XHYS_HANDOFF_TUNE_PUBLIC=1 XHYS_HANDOFF_PACKET_BATCH=1 \
XHYS_HANDOFF_NATIVE_INGRESS=1 XHYS_HANDOFF_DIRECT_TX=1 \
XRAY_HYSTERIA_NATIVE=1 XRAY_HYSTERIA_CLIENT_WORKERS=4 \
XRAY_HYSTERIA_POOL_SIZE=4 taskset -c 8-15 /tmp/handoff-load.test \
  -test.run '^$' -test.bench '^BenchmarkHandoffServerTransfer$' \
  -test.benchtime 8192x -test.cpu 8 -test.count 1 -test.timeout 120s
```

Choose disjoint physical CPU sets appropriate to the host. Omit the native
ingress, batch and direct-TX flags for the original managed path; set mode to
`direct` for ordinary startup. The same fixture has `TestHandoffUnderLoad` to
check live switching with background TCP and paced UDP, including missing
sequence counts. A completed TCP benchmark alone does not certify zero UDP
loss, safe Hysteria drain, wildcard binding, or supervisor recovery.

The first `TestHandoffUnderLoad` run of the direct-TX/`recvmmsg` experiment
**failed**: 1985/2000 paced UDP replies arrived, with 7463 relay drops.
Three replies were missing before the switch and twelve afterwards; both
generations still reported live QUIC connections. Background duplex TCP was
98.91 MiB/s, UDP p99 1.631 ms and maximum 23.810 ms. Those latency numbers
describe received replies only, not a loss-free result. This failure is retained
as evidence that the experimental path cannot be enabled as a seamless updater.

## Bounded backpressure retry, November 20, 2026

Three new load-switch tests with retry all passed: 2000/2000 paced UDP replies
each, zero relay drops, old/new generations both live, and 127.25–127.52 MiB/s
background duplex TCP. Received UDP p99 was 1.405–1.469 ms. Peak queued data
was 126080–130439 bytes, not the 512 KiB limit. Tests additionally fill one
generation's socket and queue, verify another generation can still activate,
check retry order and segment boundaries, and check bounded overflow.

An interleaved three-round matrix then ran direct, previous native relay, and
retry relay sequentially without builds running. Medians of each metric:

| Variant | MB/s | TCP p99, ms | Relay drops | Total server CPU, µs/exchange |
| --- | ---: | ---: | ---: | ---: |
| Direct native | 624.98 | 20.201 | 0 | 661.04 |
| Previous native relay + direct TX | 494.59 | 53.239 | 105384 | 830.43 |
| Native relay + retry + direct TX | 510.47 | 31.547 | 12546 | 800.80 |

The same 32-flow/64 KiB/8192-exchange, server CPUs 0–3 and client CPUs 8–15
setup was used. Total server CPU is the per-run sum of Xray and supervisor/echo
CPU, then its median. Retry improves tail latency and reduces drops, but the
full-load queue reaches its bound. All remaining counted drops in that matrix
are queue overflow, not Unix send errors. Do not enlarge queues and call this
fixed; managed throughput still trails direct by about 18%.

## Shared receive ring, November 20, 2026

The ring avoids the per-frame Unix `sendmmsg`/`recvmmsg` data path. It validates
the sealed size, header ABI, distinct eventfd descriptors and one-shot consumer
claim. Atomic indices protect each slot; tests exercise full/empty transitions,
wraparound, malformed and short frames, 32 segment boundaries, partial batch
publication, duplicate consumer rejection, and wakeup on peer closure.
The ingress uses one runtime thread and drains ready worker queues before
reading another public batch, while prioritizing control commands.

The final interleaved three-round matrix (same CPUs, flows and exchange count)
produced the following medians:

| Variant | MB/s | TCP p99, ms | Relay drops | Total server CPU, µs/exchange |
| --- | ---: | ---: | ---: | ---: |
| Direct native | 607.22 | 20.456 | 0 | 670.35 |
| Shared RX + direct TX | 544.10 | 32.765 | 99 | 737.29 |

The full-load ring variant still drops 99–140 datagrams per run when the
fallback queue fills. It remains about 10% below direct throughput and about
10% above direct CPU cost per exchange; TCP tail latency is not acceptable.
This is progress over the original relay, **not** release acceptance.
Do not compare these sequential local matrices as though they were one
simultaneous controlled benchmark or substitute them for the user's baseline.

Three load-switch tests of the verified ring build each received 2000/2000 UDP
replies with zero relay drops, old/new QUIC generations alive, and
127.51–127.61 MiB/s background TCP. UDP p99 was 1.287–1.335 ms. The fallback
queue was unused in those tests. This does not erase the saturation failure.

Add `XHYS_HANDOFF_SHARED_RX=1` to the reproduction environment to use the ring.
It requires the native ingress, batch capability and native server; direct TX
is independent. There is no production supervisor or TUI activation for it.
An ordinary receiver close wakes the sender, but SIGKILL and supervisor
reconnect still need an explicit persistent ownership protocol. A ring head/
tail snapshot is not proof that Quinn handshakes or sessions have drained.

Further throughput work should establish safe exclusive public UDP receive
ownership, so stable single-generation operation can avoid a permanent relay.
Do not simply let two processes read the same public socket: pending Initials,
already-received packets, admission acknowledgement and rollback all need
fences before that transition can be called seamless.

## Direct receive lease, November 20, 2026

An additional explicit canary path now starts the initial native generation
with exclusive public receive ownership (`direct_receive` in the manifest).
It requires a native shared receive ring and inherited direct-transmit socket.
`NewDormantNativeIngress` retains the public descriptor without registering it
in a reactor. Merely skipping `recv` on an already registered descriptor was
not sufficient: readiness events in the idle reactor still cost CPU.

Before enabling the relay, the caller requests `release-receive TAG:PORT` from
the private generation control socket. Native code excludes an in-progress
public read, switches future reads to its packet view, wakes Quinn, and returns
an idempotent snapshot of pending client-chosen Initial/0-RTT routes. The bounded
pin table rejects new unpinnable handshakes instead of starting untracked work.
The snapshot is bound to the CID key, local address and generation slot; it is
private control data and must not be logged or exposed publicly.

`AdoptReceive` validates and installs that snapshot before registering the UDP
socket and enabling the ingress receive loop. Only then may the caller activate
the new generation. A lost acknowledgement means uncertain ownership: retain
both processes and retry the same operation; never synthesize a snapshot or
kill the old process. There is no automatic ownership recovery after SIGKILL.

The interop fixture exercises this sequence with `XHYS_HANDOFF_DIRECT_RX=1`
in addition to shared RX, direct TX and native ingress flags. This is a one-way
direct-to-relay handoff, **not** a complete node updater. The reverse transition
is intentionally unavailable until old QUIC/handshake/queue retirement can be
proven. After a switch, persistent relay performance limitations still apply.

Two additional data-path defects were found during this work:

- Exporting `net.Conn.File().Fd()` could clear `O_NONBLOCK` on the shared socket
  description and leave a live Go ingress blocked in `recvfrom`, including on
  close. Packet export now uses `F_DUPFD_CLOEXEC` through `SyscallConn.Control`
  and preserves file status flags. The ownership test verifies this, and ten
  `handoff -race` suite repetitions completed without the hang.
- Managed native direct TX unnecessarily forced PMTU discovery off and MTU
  1200. It now preserves the configured native policy when sending directly;
  relayed TX keeps the conservative fixed MTU because it lacks public PMTU
  feedback. The inherited public socket buffer policy is unchanged.

An interleaved three-round matrix after these fixes measured stable operation:

| Variant | MB/s median | TCP p99, ms median | Counted relay drops | Total server CPU, µs/exchange median |
| --- | ---: | ---: | ---: | ---: |
| Ordinary native | 593.75 | 21.677 | 0 | 673.49 |
| Managed native, exclusive direct RX | 603.41 | 21.635 | 0 | 668.11 |

Same 32 TCP flows, 64 KiB, 8192 exchanges, server CPUs 0–3 and client CPUs
8–15. Throughput ranges were 587.11–615.76 and 581.11–609.22 MB/s respectively.
These overlapping samples support near-direct steady-state performance in this
fixture, not a claim that managed is faster or zero-overhead on every host.
Zero relay counters in this mode are expected because the ingress is dormant;
they do not measure kernel UDP loss. Switching and saturation must be tested
separately, and the post-switch relay still fails full-load acceptance.

Three direct-to-relay load-switch runs of the same dormant-ingress build passed:
2000/2000 paced UDP replies each, zero relay drops before and after the switch,
old/new generations both live, and 127.46–127.68 MiB/s background duplex TCP.
UDP p99 was 1.398–3.948 ms (not uniformly 1.4 ms); maximum was
13.852–27.350 ms. Both release and adoption acknowledgements were retried with
the identical snapshot. These results still do not certify saturated traffic,
unbounded handshake duration, rollback, reverse ownership or supervisor loss.
