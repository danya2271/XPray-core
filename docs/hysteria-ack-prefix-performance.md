# Hysteria ACK-prefix optimization and qualification

Date: November 23, 2026. Baseline: `5a0afcdc`.

## Accepted production change

Quinn's send buffer formerly inserted every acknowledgement into a range tree
and immediately removed that entry when it advanced the first unacknowledged
byte. The accepted fast path consumes this prefix directly **only when no
out-of-order acknowledgements are queued**. All other acknowledgements retain
the original range-set algorithm.

Both paths call the same prefix-consumption helper. The original payload
ownership, partial-chunk accounting, retransmissions, stream offsets and
per-segment metadata shrink policy are preserved. A deterministic differential
test compares 64 scenarios of duplicate/gapped acknowledgements and new writes
against the old implementation, including exact queue capacity. Additional
tests cover wrapped deque storage and last-byte payload-owner release.

No receive/send window, queue capacity, QUIC stream limit, runtime worker count,
wire ABI, allocator or packet-ring wakeup ordering changes. The previously
qualified third-party client STOP_SENDING and half-close fixes remain intact.
The shared-bridge production fast-path experiment was removed; its new
full-queue/close regression remains.

## Demonstrated local benefit

The ignored `benchmark_batched_ack_metadata` test contains both the old
reference algorithm and the new implementation in **one executable**.
`XHYS_ACK_BENCH_REFERENCE=1` selects the reference; without the variable it uses
the production implementation. Seven alternating-order fresh-process samples
per algorithm run on logical CPU 0. Each sample repeats the operation 1024
times; buffer construction and destruction are outside the timed ACK interval.
The table reports medians, not whole-transport speedup factors.

| Chunks | ACK shape | Before, ns | After, ns | Change |
| ---: | --- | ---: | ---: | ---: |
| 1 | Full prefix | 67 | 35 | -47.8% |
| 1 | Partial prefix | 56 | 25 | -55.4% |
| 1 | Reordered | 129 | 147 | +14.0% |
| 8 | Full prefix | 146 | 118 | -19.2% |
| 8 | Partial prefix | 123 | 95 | -22.8% |
| 8 | Reordered | 227 | 233 | +2.6% |
| 128 | Full prefix | 700 | 685 | -2.1% |
| 1024 | Full prefix | 4455 | 4536 | +1.8% |
| 8192 | Full prefix | 56539 | 56859 | +0.6% |

An isolated, thread-local allocation counter in an **ignored copy of the test
crate** confirms that the one-chunk full/partial prefix makes zero allocation
requests instead of one 192-byte allocation. The reordered one-chunk operation
still allocates once in both implementations. For eight chunks, full/partial
ACKs request 32 bytes rather than 224 bytes, with one allocation request rather
than two. The remaining request is the unchanged deque resize. This counts
temporary allocation requests, not retained heap, RSS or a new allocator in
production.

The small sequential ACK has a demonstrated local benefit. Reordered ACK
timing is mixed and includes a 14% increase in the smallest microcase, despite
the fallback algorithm being retained. Code layout and dispatch can affect
these timings; the unchanged fallback does not justify calling every case
faster. No universal latency improvement is claimed.

Reproduce using the tagged Quinn-proto test executable:

```sh
XHYS_ACK_BENCH_REFERENCE=1 taskset -c 0 proto.test --ignored --nocapture \
  --exact connection::send_buffer::tests::benchmark_batched_ack_metadata
taskset -c 0 proto.test --ignored --nocapture \
  --exact connection::send_buffer::tests::benchmark_batched_ack_metadata
```

## End-to-end transport checks

Five alternating-order fresh processes per variant and workload, both-native
client/server, shared bridge, pool size one, Salamander plus WireGuard header.
Client, server and TCP/UDP target are in the same measured process pinned to
CPUs4–7 with GOMAXPROCS4 and four native workers per role. Concurrent functional
and capacity checks were confined to different physical cores0–3. This is a
local loopback comparison, not independent physical VPSs or a WAN path.

Upload/download samples complete a fixed 512MiB and include the receiver's
completion acknowledgement. TCP/UDP idle samples complete20000 64-byte
exchanges; paced TCP completes10000 exchanges with a common offered background
load. RSS comes from `wait4` peak usage of the complete benchmark process and
includes client, server and target. It is not just Go allocations or Rust
metadata capacity.

| Workload | Metric | Before median | After median |
| --- | --- | ---: | ---: |
| Upload | MB/s | 369.85 | 377.06 |
| | Process CPU, µs/64KiB | 421.08 | 439.31 |
| | Process peak RSS, MiB | 87.40 | 80.67 |
| Download | MB/s | 330.83 | 330.80 |
| | Process CPU, µs/64KiB | 493.18 | 501.30 |
| | Process peak RSS, MiB | 56.12 | 56.25 |
| Idle TCP | p50/p99, µs | 135.60 / 194.10 | 134.10 / 189.40 |
| | Process peak RSS, MiB | 40.16 | 40.01 |
| Idle UDP | p50/p99, µs | 125.30 / 181.00 | 117.70 / 170.70 |
| | Process peak RSS, MiB | 40.12 | 39.94 |
| Paced TCP | p50/p99, µs | 143.40 / 567.10 | 133.80 / 485.60 |
| | Achieved background, MiB/s | 60.96 | 63.16 |
| | Process peak RSS, MiB | 43.57 | 43.77 |

Upload speed changes+1.95%, download is effectively unchanged. Upload CPU
increases4.33% and download CPU1.65%; this is not an across-metrics improvement.
Lower sampled latency and upload RSS are observations on a shared host, not
proof that bypassing one tree allocation explains the entire difference.
Achieved paced background is not identical. These checks do not establish a
stable throughput, latency or RSS factor; the accepted claim is the narrowly
measured sequential-ACK allocation/work reduction.

## Capacity and correctness

Three fresh-process samples per baseline/candidate, alternating order:
500 identities, 1000 authenticated QUIC sessions and TCP flows, 500MiB duplex,
two-second delayed reads and a heartbeat phase. The server uses logical CPU0,
GOMAXPROCS1 and GOMEMLIMIT128MiB; parent fixture uses CPUs0–3/GOMAXPROCS4.
No 1GiB cgroup is applied, and Go's memory limit does not bound native or kernel
memory. Medians:

| Generation metric | Before | After |
| --- | ---: | ---: |
| Idle RSS, MiB | 138.3 | 135.7 |
| Peak RSS, MiB | 223.5 | 221.8 |
| Duplex transfer, MiB/s | 151.65 | 153.98 |
| Heartbeat p99, µs | 2670.3 | 2554.7 |
| Process CPU seconds | 4.371 | 4.280 |

All six samples pass the512MiB RSS guard. The overlapping small changes are
not an OOM guarantee or evidence of a production-wide memory reduction.

Validation of the accepted source:

* 320 release /321 debug Quinn-proto tests pass, with three ignored benchmarks.
  Quinn-proto library/tests Clippy passes with warnings denied.
* 129 release native-engine tests pass, including publication backpressure,
  concurrent wakeups and close interruption. Production-library Clippy passes.
* Fresh-linked Go race tests pass for xhysteria, Hysteria, handoff, supervisor
  and the Hysteria proxy. Binary archive hashes invalidate the Go linker cache.
* Single-session quic-go regressions pass:192 simultaneously live streams,
  1200 closes without reconnection, finite startup allowance. Flow-cap
  rejection/recovery, short-flow credit reuse, packet masks and100ms RTT
  bulk transfer pass.
* Four strict native UDP scenarios (direct/cascade,256/4096-byte payloads)
  deliver2000/2000 each:8000/8000 total, no sampled kernel drops or duplicates.
  A real-time soak of the new ACK variant was not run; the earlier fifteen-minute
  passes qualify the retained stream-lifecycle fix, not a fresh ACK-variant soak.

The repository-wide Go failures and all-target Clippy limitation reported in
earlier work are not repaired by this optimization. Do not describe this
targeted qualification as a clean full-repository test run.

## Earlier experiments and broader goal

The user's objective remains maximum Hysteria throughput, minimum latency and
low RSS. A local win is still useful, but must not be presented as satisfying
all three goals. Earlier experiments in this round changed ACK shrink timing,
merged queued ACK ranges directly, or bypassed shared-bridge waiter setup.
They were reverted because results were mixed, including latency or RSS
regressions. Their binaries and failed setup/cache-comparison logs remain in
`build_assets/ack-batching-20261122/`, `ack-fastpath-20261122/`,
`ack-combined-20261122/` and `shared-fastpath-20261122/`.

A matched three-run both-native/both-Go baseline comparison on this host
measured upload392.06/204.06MB/s and download334.45/206.08MB/s. Native still
lost idle TCP p99=166.0/144.6µs and upload peak RSS80.31/41.04MiB. These are
different runs from the accepted-patch table, not an additional patch-versus-Go
matrix. Lower Go allocation counts alone do not establish lower process RAM.

The prior idle perf profile has zero lost samples but approximately48% of
sampled CPU in unresolved kernel addresses across Go/Rust threads; it does not
justify a precise kernel-function or sleeping-lock attribution.

Accepted raw data, reference microbenchmark, allocation-counter copy, build
and test logs are in ignored `build_assets/ack-prefix-20261123/`.
No deployment, VPS restart, allocator replacement or neighboring-repository
change was performed. Android builds are compilation/linking checks only.
