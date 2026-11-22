# Bounded, lazy QUIC receive-state reuse

Date: November 23, 2026. Baseline: `829b79d1`.

## Production change

After an incoming QUIC stream closed, Quinn reused its `Recv` object by
attaching it to another stream ID that the peer had permission to open. Those
IDs need not ever be used. A session that had handled a burst of simultaneous
streams could therefore keep all their receive objects and small assembler
metadata allocations alive while idle.

The receive-object cache now holds at most **32 objects per QUIC connection**.
Allowed but unused stream IDs contain no receive object. Cached objects are
assigned only when DATA, RESET_STREAM, application reads or STOP_SENDING
actually access a stream. An already-open stream takes a direct path through
the lookup helper without taking and reconstructing its map entry.

Only closed receive objects are evicted. Active streams are not capped by the
cache size. The advertised QUIC stream limits, stream-credit replenishment,
connection/stream receive windows, native flow admission and process budgets
remain unchanged. `Recv::reinit` still resets offsets, reset/stop state and
assembler metadata before reuse; it releases payload references. The previous
ACK-prefix fast path and third-party client lifecycle fixes are preserved.

## Reproduced post-burst retention

The ignored `benchmark_idle_receive_cache` fixture constructs 128 protocol
stream states. Each permits 256 incoming unidirectional streams, receives
sixteen gapped 64-byte static chunks per stream, then stops and resets every
stream. It keeps all 128 states alive after closing the streams.

The fixture sums `size_of::<Recv>()` and retained assembler metadata capacity
for cached objects, and samples its process `VmRSS`. Static payloads isolate
receive-object/metadata retention; this is not a TLS/QUIC network capacity test.
The baseline bounded-cache regression fails because receive objects remain
attached to allowed stream IDs.

Five alternating-order fresh-process samples per version on logical CPU0:

| Post-burst metric | Baseline median | Final median |
| --- | ---: | ---: |
| Retained receive objects | 32768 | 4096 |
| Object + assembler metadata storage, MiB | 32.25 | 4.03 |
| Fixture process RSS, MiB | 50.99 | 22.48 |
| Lifecycle work per protocol state, µs | 421.15 | 318.98 |

Object/metadata storage decreases **87.5%**, or 28.22MiB across this fixture.
Its RSS decreases approximately 56%, and lifecycle work approximately24%.
These results target idle sessions following a receive-stream burst. They do
not predict a 56% RSS reduction in an ordinary running Xray or prove that a
1GiB node is OOM-proof. The stream-ID tables themselves remain allocated.

The final candidate includes a direct path for already-open receive objects.
The first cache prototype reconstructed the entry on every access and showed
an approximately8% download-rate decrease in its initial transport matrix.
That prototype is not the final implementation; its measurements remain in
`build_assets/recv-cache-20261123/`.

## Ordinary transport qualification

Five alternating-order fresh processes per baseline/final workload. Both
client and server use native Hysteria, shared bridge, pool size one and
Salamander plus WireGuard header masks. The complete client/server/echo
benchmark process runs on CPUs4–7 with GOMAXPROCS4 and four native workers per
role. Functional and capacity checks run on different physical cores0–3.
This is a shared-host loopback comparison, not two separate VPSs.

Upload and download each complete512MiB, including the receiver's completion
acknowledgement. Idle TCP/UDP complete20000 64-byte exchanges; paced TCP
completes10000 exchanges under a common offered background load. RSS is the
complete benchmark process's `wait4` peak, including client, server and target.
Values are medians:

| Workload | Metric | Before | Final |
| --- | --- | ---: | ---: |
| Upload | MB/s | 364.42 | 363.03 |
| | Process CPU, µs/64KiB | 438.59 | 457.57 |
| | Process peak RSS, MiB | 83.77 | 78.19 |
| Download | MB/s | 326.92 | 317.70 |
| | Process CPU, µs/64KiB | 502.04 | 522.54 |
| | Process peak RSS, MiB | 62.25 | 55.42 |
| Idle TCP | p50/p99, µs | 139.00 / 205.90 | 139.10 / 202.60 |
| | Process peak RSS, MiB | 40.08 | 40.07 |
| Idle UDP | p50/p99, µs | 131.80 / 201.80 | 127.30 / 191.90 |
| | Process peak RSS, MiB | 39.97 | 40.08 |
| Paced TCP | p50/p99, µs | 145.20 / 588.20 | 148.20 / 608.10 |
| | Achieved background, MiB/s | 60.31 | 60.26 |
| | Process peak RSS, MiB | 43.44 | 43.44 |

The change is accepted as a **post-burst retention reduction**, not an
across-metrics speedup. In this series, upload changes -0.38%, download -2.82%,
and bulk CPU increases about4.3%/4.1%. Bulk RSS is lower, idle latency is mixed
and paced p99 increases3.38%. These tradeoffs are recorded rather than replaced
by the more favorable isolated-memory result. Five shared-host samples do not
establish universal throughput, latency or RSS factors.

## 1000-session capacity checks

Three alternating-order fresh-process samples per version authenticate500
identities in1000 real QUIC sessions, each with a TCP flow. They transfer500MiB
duplex with delayed readers and run a heartbeat phase. Server affinity is CPU0,
GOMAXPROCS1, GOMEMLIMIT128MiB; parent fixture uses CPUs0–3/GOMAXPROCS4. No1GiB
cgroup is applied. Go's memory limit does not bound Rust or kernel memory.

| Server metric | Before median | Final median |
| --- | ---: | ---: |
| Idle RSS, MiB | 137.6 | 135.4 |
| Peak RSS, MiB | 221.4 | 219.6 |
| Duplex transfer, MiB/s | 143.86 | 140.72 |
| Heartbeat p99, µs | 2724.5 | 2770.0 |
| Process CPU seconds | 4.594 | 4.619 |

All six samples pass the fixture's512MiB RSS guard. This low-per-session-flow
case does not reproduce the large post-burst cache, and its small RSS change
is not the isolated fixture's 28.22MiB saving.

## Correctness and build validation

Four new regression tests verify:

* closed-object cache count/capacity and unchanged stream-credit replenishment;
* reuse of the same object only after an allowed stream is accessed;
* fresh read, stop and reset accesses reuse objects with cleared state;
* unread payload owners are released and a subsequent256-stream burst remains
  possible despite the32-object cache.

The exact final source passes324 release and325 debug Quinn-proto tests, with
four ignored benchmarks, and Quinn-proto library/tests Clippy with warnings
denied. Native engine passes129 release tests and production-library Clippy.
Fresh-linked Go race tests pass for xhysteria, Hysteria, handoff, supervisor and
the Hysteria proxy.

Direct quic-go client checks pass on both shared/socket bridges with
Salamander:192 simultaneously live streams,1200 closes without reconnect,
finite startup allowance and a small window after serial churn. Native
flow-cap rejection/recovery, short-flow stream credits, packet-mask cases and
synthetic100ms RTT bulk transfer also pass.

Strict native UDP tests deliver2000/2000 in each direct/cascade ×256/4096-byte
scenario:8000/8000 total, zero sampled kernel drops and duplicates. These are
local tests, not proof of zero production WAN loss. No new fifteen-minute
real-time soak was run for this cache variant.

Linux amd64 and Android arm64/API26 builds pass. Android validation covers
compilation/linking only. The older repository-wide Go failures and unrelated
all-target engine Clippy limitation are not repaired or described as passing.

## Artifacts and reproduction

Final logs, same-fixture baseline/candidate executables, individual observations
and summary are under ignored `build_assets/recv-cache-fast-20261123/`.
The initial prototype and expected baseline regression failure are under
`build_assets/recv-cache-20261123/`. Preserve the first Clippy failures from
fixture development; the corrected final checks are separate logs.

```sh
cargo test --locked --release --no-default-features --features rustls-ring \
  --lib --manifest-path native/libxhysteria/vendor/quinn-proto/Cargo.toml \
  --target-dir native/libxhysteria/target/proto-tests \
  benchmark_idle_receive_cache -- --ignored --nocapture
```

No remote deployment, restart, allocator-policy change or neighboring-repository
edit was performed. Normal commits/pushes publish this task to the four
`origin` branches without rewriting branch-specific history.
