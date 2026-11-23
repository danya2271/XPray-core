# Hysteria on one and two physical cores: experiment results

Measurements: November 23, 2026. Finalized: November 24, 2026.
Baseline: `19052c8ebd3650026fa6b2b5d71fb877d1541fce`.

## Outcome

The runtime remains at the baseline. None of the candidates in this round
demonstrated a sufficiently balanced improvement in throughput, CPU, latency
and memory to retain in production. The round adds a reproducible A/B driver,
its regression tests, and the measurements below.

The final candidate retained at most four empty send-buffer descriptors.
Repeated write/transmit/ACK CPU cost fell **55.5%**, and an independently loaded
managed server improved throughput by **0.46% on one core / 0.89% on two**.
These are useful local wins. Combined client/server workloads were mixed,
including lower bulk throughput and worse paced latency; the candidate was
therefore reverted. These observations do not establish causality for every
small difference, but they do not justify a general low-core speedup claim.

Linux's native archive, interop harness and standalone Xray binary were
restored byte-for-byte to baseline. Android arm64 was rebuilt from the same
baseline source. Runtime workers, flow/window limits, queue policies, UDP
buffers and SeqCst wakeup handshakes have no changes from this round.

## Method and boundaries

The host is a single-socket Xeon E5-2698 v3 with 16 physical cores and 32 SMT
threads. CPUs 4 and 5 are distinct physical cores. Unrelated host load remains
uncontrolled. Task builds and functional checks did not overlap timed matrices.
All main tables use medians of three alternating paired fresh-process runs.
They are observations, not confidence intervals or minimum-speed guarantees.

The combined fixture pins client, native server and TCP target together to
CPU 4 or CPUs 4,5. GOMAXPROCS and native workers **per role** equal that core
budget. Both versions use pool=2, shared bridges, Salamander + WireGuard and
32 TCP flows:

- Duplex: 8192 checked 64 KiB exchanges in both directions, 1 GiB aggregate.
- Upload/download: 512 MiB per direction, including receiver completion.
- Paced: 10,000 64-byte probes alongside an offered 64 MiB/s duplex load.

CPU covers the timed interval of the complete process. Bulk CPU is per 64 KiB
operation; duplex CPU is per exchange with 64 KiB in each direction. Paced CPU
is per probe and includes concurrent background work. Peak RSS includes setup
and both roles. Calibration engines finish before the next setup; socket
counters retain exact identities. Samples with UDP drops remain in summaries.

Throughput marked MB/s uses decimal bytes; background rates and memory marked
MiB use binary units. Duplex throughput sums both directions and is not a
one-direction upload or download result.

## Bounded empty send-metadata cache

The old send buffer frees its `VecDeque<Bytes>` metadata at full ACK. Repeated
small writes then allocate the same small deque again. The candidate retains
up to four empty descriptors, **128 bytes per drained open send stream on
amd64**, without keeping payload owners. Large queues still shrink at the old
quarter-capacity threshold, with a minimum of four slots. Discard/reset and
stream destruction free the deque. No struct fields or payload copies are added.

Partial ACKs, final offsets, retransmission ranges and stream credit behave
as before. Regression tests verify final-byte payload release, wrapped deque
behavior, bounded reuse and full release on discard. Differential tests compare
payload/ACK/offset/retransmission behavior. Exact-capacity equality was replaced
by a bound because caching intentionally changes subsequent geometric growth.
The initial assertion failure and successful rerun remain in raw logs.

### Repeated small-write microbenchmark

Seven alternating old/new repeats use the same release executable on CPU 4.
Each repeat performs 524,288 write/transmit/ACK cycles. The reference frees the
empty deque after each cycle, simulating the previous full-drain policy.

| Metric | Previous policy | Cached metadata |
| --- | ---: | ---: |
| Median ns per cycle | 32.444 | 14.429 |
| Empty retained metadata bytes per send stream | 0 | 128 |

The 55.5% reduction applies to this allocation-heavy loop. It is not an overall
Hysteria CPU or throughput factor. The reference executes the candidate's ACK
path plus an explicit final shrink; it is an approximation of the former policy.

### Combined client/server/target: one and two cores

| Cores | Workload | MB/s before → cache | Process CPU µs/op | p99 µs | Peak RSS MiB | Samples with UDP drops before/cache |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 1 | duplex | 179.92 → 172.73 | 686.92 → 714.77 | 64833.00 → 61629.00 | 65.51 → 65.32 | 0/0 |
| 1 | upload | 142.10 → 139.36 | 425.71 → 434.42 | — | 135.80 → 134.24 | 0/0 |
| 1 | download | 182.56 → 182.85 | 342.72 → 342.60 | — | 83.26 → 85.54 | 0/0 |
| 1 | paced | — | 235.84 → 245.04 | 4689.00 → 4742.00 | 54.63 → 54.45 | 0/0 |
| 2 | duplex | 309.07 → 306.02 | 759.15 → 766.40 | 47032.00 → 47159.00 | 68.28 → 65.89 | 0/0 |
| 2 | upload | 294.16 → 289.76 | 386.42 → 390.13 | — | 178.89 → 175.06 | 0/0 |
| 2 | download | 327.80 → 327.16 | 362.10 → 362.58 | — | 89.37 → 90.02 | 0/2 |
| 2 | paced | — | 290.46 → 287.38 | 2226.00 → 2370.00 | 55.43 → 52.50 | 0/0 |

Paced attained background: one core **64.21 → 64.28 MiB/s**; two cores
**64.16 → 63.89 MiB/s**. One-core duplex throughput changed −4.0%, upload
−1.9%; two-core upload changed −1.5% and paced p99 +6.5%. Two of the 24
candidate samples had kernel UDP drops; none of the 24 baseline samples did.
This three-pair matrix does not qualify either a universal regression factor
or a loss-free gain. All minima, maxima and individual counters remain recorded.

### Managed server with an independent baseline load generator

The unchanged baseline client runs on CPUs 8–15 with GOMAXPROCS=8, four native
client workers and pool=4. Server generations, ingress/supervisor and TCP echo
share CPU 4 or CPUs 4,5, with one/two server workers. There are 32 flows across
two live generations. Transfer uses 32,768 duplex 64 KiB exchanges; latency
uses 200,000 64-byte exchanges. Public UDP and ingress drop gates are strict.
CPU below sums server generations and supervisor/echo, excluding the client.

| Server cores | Workload | MB/s before → cache | Total server-side CPU µs/op | p99 µs | Server peak RSS MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | transfer | 230.05 → 231.11 | 525.19 → 522.21 | 35685.00 → 34300.00 | 102.70 → 101.70 |
| 1 | latency | — | 41.35 → 41.19 | 2542.00 → 2537.00 | 92.44 → 90.36 |
| 2 | transfer | 412.00 → 415.68 | 573.43 → 567.45 | 23975.00 → 21885.00 | 108.70 → 106.50 |
| 2 | latency | — | 63.32 → 62.67 | 3519.00 → 3507.00 | 94.48 → 95.22 |

All 24 samples have zero public UDP and ingress drops. Bulk CPU medians fall
0.57%/1.04% and p99 improves in this fixture. These small gains do not erase
the combined-workload results. Server RSS excludes the separate supervisor/echo
process; individual measurements for that process remain in the JSON.

### Four/eight-core controls

Four native workers per role, pool=2/4 respectively, combined fixture on
CPUs 4–7 / 4–11. These controls use the same parameters for both versions.

| Cores | Workload | MB/s before → cache | Process CPU µs/op | p99 µs | Peak RSS MiB | Samples with UDP drops before/cache |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 4 | duplex | 449.40 → 442.03 | 927.23 → 955.37 | 34587.00 → 36916.00 | 71.58 → 69.35 | 0/0 |
| 4 | upload | 432.62 → 426.90 | 470.44 → 473.48 | — | 181.98 → 182.80 | 0/0 |
| 4 | download | 448.06 → 447.27 | 453.31 → 458.68 | — | 90.37 → 90.67 | 0/0 |
| 4 | paced | — | 265.68 → 280.19 | 1341.00 → 1314.00 | 56.92 → 56.12 | 0/0 |
| 8 | duplex | 685.99 → 665.06 | 1126.18 → 1148.64 | 19911.00 → 21465.00 | 72.21 → 70.08 | 0/0 |
| 8 | upload | 626.53 → 613.73 | 572.77 → 587.45 | — | 213.77 → 215.27 | 3/3 |
| 8 | download | 685.28 → 674.43 | 523.81 → 522.28 | — | 102.80 → 102.77 | 0/0 |
| 8 | paced | — | 252.95 → 268.40 | 758.30 → 814.50 | 57.14 → 55.29 | 0/0 |

There are three nonzero-drop samples per version across these 48 controls.
Eight-core paced p99 changes 758.3 → 814.5 µs at attained background
63.71 → 63.75 MiB/s; four-core attained background is 64.04 → 64.14 MiB/s.
The controls also fail to demonstrate a balanced general improvement.

### One-core server: 500 users / 1000 sessions

Three alternating pairs, 1000 authenticated QUIC sessions and TCP flows.
The server uses CPU 4, GOMAXPROCS=1 and GOMEMLIMIT=128 MiB; the whole test
runs on CPUs 4–7. Peak server RSS must remain below 512 MiB.

| Metric | Baseline median | Cache median |
| --- | ---: | ---: |
| Idle server RSS, MiB | 138.800 | 135.600 |
| Buffered server RSS, MiB | 215.100 | 212.700 |
| Peak server RSS before close, MiB | 225.100 | 222.300 |
| Server RSS after close, MiB | 222.800 | 221.800 |
| 500 MiB duplex transfer, MiB/s | 159.760 | 157.800 |
| Heartbeat p99, µs | 2561.400 | 2693.200 |
| Whole server lifetime CPU, seconds | 4.185 | 4.251 |

All six runs pass the memory guard. Peak-before-close RSS falls about 2.8 MiB,
while server lifetime CPU rises about 1.6% and heartbeat p99 about 5.1%.
The cache's metadata allowance is up to 128 bytes for each drained open send
stream; observed process RSS differences include allocator and Go heap effects.
This does not establish an intrinsic memory reduction from retaining metadata.

## Other rejected experiments

- Combining up to four already queued TCP DATA frames, bounded to 64 KiB and
  without waiting, improved one-core upload **137.13 → 181.09 MB/s** and CPU
  **442.152 → 344.483 µs/64 KiB** over seven pairs. Paced p99 worsened
  **3946 → 4383 µs**, and upload peak RSS **131.55 → 155.35 MiB**.
  Two-core upload was **284.73 → 292.04 MB/s**, download
  **321.74 → 320.35 MB/s**; candidate/baseline had five/two drop samples.
  Server-only variants also remained mixed. All read-batching code was reverted.
- Avoiding a connection mutex when frames were already ready saved about 3%
  in its microbenchmark, but one-core download changed
  **187.38 → 142.51 MB/s** and paced p99 **3732 → 4609 µs** over three pairs.
- A last-range lookup and then in-place `BTreeMap` ACK-range extension were
  both reverted. The latter saved **37–54%** in insertion microbenchmarks, but
  combined one/two-core upload/download changed approximately **−1…−4%**.
  Four-core paced p99 changed **916 → 1205 µs**. A fresh baseline rebuild
  matched the original baseline byte-for-byte. Its 120 final transport and
  42 micro observations are in [range-rejected.json](hysteria-low-core-20261123/range-rejected.json).

These are separate experiments; their results and validation do not qualify
the cache candidate. Intermediate source snapshots and failed/aborted runs
remain under the ignored artifact directory.

## Validation and restoration

The rejected send-cache candidate passed:

- Quinn-proto: 325 release and 326 debug tests, five ignored manual benchmarks.
- Engine library: 130 release tests, one ignored benchmark.
- Quinn-proto release library Clippy with `-D warnings`.
- Fresh native Go race tests for xhysteria, hysteria, handoff, supervisor and proxy.
- One/two-core packet masks, concurrent transfers, returned stream capacity,
  rejection/recovery at capacity, and bulk transfers with 100 ms RTT.
- Masked third-party client checks through shared/socket bridges on both budgets:
  192 concurrent streams, 1200 close/reopen cycles, finite advertised limit.
- Strict UDP delivery under TCP load: **8000/8000 per core budget**, direct and
  cascade, 256/4096-byte payloads, zero sampled kernel drops or duplicates.
- Six capacity runs and Android arm64/API 26 build using NDK 30.0.14904198.

After reverting the candidate, the Linux native archive, interop harness and
standalone Xray binary match baseline hashes exactly. Android arm64/API 26 was
rebuilt from the restored source. No device execution, WAN benchmark, new
15-minute stream soak, production deployment or full-repository green claim
follows from these checks.

The committed A/B driver passed seven focused unit tests and the seven existing
scaling tests. A live one-core pair exercised all four workloads (eight fresh
processes). Resumption reused all eight logs; hashes, sample logs and summaries
matched. Manifest rejection, failed exit, pool validation, timeout process-group
cleanup and drop retention are covered by focused tests.

## Reproduction and evidence

Build both revisions using `link-go.sh` and a content-derived native archive
build ID so Go's cache cannot reuse a stale archive. Run:

```sh
python3 -B native/libxhysteria/bench-low-core.py \
  --before /absolute/path/interop-before.test \
  --after /absolute/path/interop-after.test \
  --cpus 4,5 --cores 1,2 --repeats 3 \
  --output build_assets/low-core-reproduction
```

The driver records binary/driver hashes, relevant inherited environment,
physical affinity, commands, host load average and individual metrics.
Resumption requires the same manifest and successful invocations. Failed logs
remain preserved and stop the run. A separate output directory is required
after fixing a failed run or changing a method. Drop samples remain in
median/min/max summaries and have explicit nonzero sample counts; they never
qualify as loss-free runs.

[send-cache-rejected.json](hysteria-low-core-20261123/send-cache-rejected.json)
contains all 120 final transport observations, 14 micro samples, six capacity
runs, strict UDP checks, individual bulk commands and binary hashes. The
[historical candidate patch](hysteria-low-core-20261123/send-cache-rejected.patch)
can be applied to baseline in a disposable checkout to rebuild the experiment;
use `git apply --unidiff-zero` for this zero-context patch. It is excluded from
the final runtime. The microbenchmark has an `XHYS_SEND_CACHE_REFERENCE=1`
reference mode.

Raw logs, temporary source snapshots, intermediate candidates and large binaries
remain ignored in `build_assets/low-core-20261123/`. The live driver smoke is
validation only and is excluded from the three-pair performance tables.
