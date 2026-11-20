# Managed Hysteria performance investigation

## Current lifecycle and acceptance status, November 20, 2026

The historical experiments below predate the integrated persistent broker,
safe engine drain and return-to-direct receive lifecycle. Those statements
about missing lifecycle support describe the old experiment, not today's
implementation; see `nodex-managed-lifecycle.md`.

The remaining saturated-overlap loss is **not fixed**. The user's completed
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

Production defaults remain unchanged. Performance acceptance still includes
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
