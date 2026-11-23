# Hysteria throughput target: 400 MB/s upload, 370 MB/s download

Date: November 23, 2026. Baseline: `4fd935d5`.

## Qualified worker profile: original 512 MiB workload

The target is reached by median with **one native runtime worker per role**,
using the existing implementation and the original 512 MiB fixture.
Seven fresh-process paired samples alternate one/four workers, keeping the
same binary, four CPU cores, GOMAXPROCS4, packet masks, QUIC session count
and workload. No task builds or functional tests run concurrently.

| Workload | Metric | 4 workers/role | 1 worker/role | Change |
| --- | --- | ---: | ---: | ---: |
| Upload | MB/s | 385.16 | **409.60** | +6.35% |
| | Process CPU µs/64KiB | 422.734 | 399.601 | -5.47% |
| | Peak process RSS MiB | 90.80 | 70.87 | -21.95% |
| Download | MB/s | 350.24 | **429.85** | +22.73% |
| | Process CPU µs/64KiB | 489.036 | 397.151 | -18.79% |
| | Peak process RSS MiB | 55.09 | 51.38 | -6.74% |
| Idle TCP | p99 µs | 192.2 | 189.5 | -1.40% |
| Idle UDP | p99 µs | 181.7 | 180.5 | -0.66% |
| Paced TCP | p99 µs | 438.8 | 401.8 | -8.43% |
| | Attained background MiB/s | 31.61 | 31.76 | +0.47% |

Upload meets400MB/s in5/7tuned samples (range392.70–412.79); download
meets370MB/s in7/7 (418.76–439.48). The goal is qualified by median,
not as a minimum in every run.
These are loopback observations of a single QUIC session, not a production
multi-user aggregate throughput or WAN/Android performance guarantee.
No universal RSS reduction factor follows from these complete-process peaks.

Set the profile before creating the native runtimes:

```sh
export XRAY_HYSTERIA_CLIENT_WORKERS=1
export XRAY_HYSTERIA_SERVER_WORKERS=1
```

Each role's variable applies in the process hosting that role. The shared
client runtime uses the setting of its first initialization. Global runtime
defaults are unchanged. The many-user server's default is not reduced on
the basis of this single-session fixture.

The initial three-sample search also measured two workers per role:
416.87MB/s upload,394.68MB/s download,398.916/414.485µs CPU per64KiB.
Three workers gave393.29/373.98MB/s; four389.77/369.63. Those search samples
are separate from the seven paired qualification samples.

Reproduce the paired512MiB throughput and latency matrix:

```sh
XHYS_BENCH_AFFINITY=4-7 XHYS_BENCH_CPUS=4 XHYS_BENCH_COUNT=7 \
  bash native/libxhysteria/bench-throughput.sh build_assets/throughput-reproduction
```

The script builds a fresh harness with a content-derived native archive ID,
uses the existing packet-mask fixture, alternates one/four workers in fresh
processes, and preserves every observation, SHA-256, methodology and median/
range summary. Set `XHYS_BENCH_BINARY` to an existing absolute harness path
to reuse it; `XHYS_BENCH_LATENCY=0` selects bulk only.
The output directory refuses to overwrite previous results.

## Initial experiment conditions

The target is interpreted as decimal MB/s, matching the existing Go benchmark
output. It applies here to one both-native client/server loopback connection,
shared bridge, one pooled QUIC session, Salamander plus WireGuard header,
four native workers per role, GOMAXPROCS4 and CPUs4–7.
Client, server and TCP target run in the same measured process.
This is not a WAN/VPS or Android-device throughput guarantee.

Each initial bulk sample transfers1GiB (16384operations of65536bytes), including
receiver completion, in a fresh process. Five paired samples per version
alternate execution order. CPU is the whole measured process's timed work
per64KiB. `wait4` reports complete-process peakRSS. Hardware counters record
user instructions/cycles, including setup/teardown, with100% active time.
No task builds or other functional/load tests run during bulk measurements.
Unrelated user processes are left untouched.

This round's fresh baseline is already above both targets by median:
428.98MB/s upload and386.78MB/s download in the first five-pair matrix.
The previous round's accepted-runtime medians384.65/349.76MB/s used512MiB
transfers under different host conditions. Both the changed sample length
and host variation matter; their difference from this round must not be
attributed to a new code change. All optimization comparisons below use
paired observations from this round.

## Rejected candidates and sample-length qualification

All three production candidates were rejected and fully reverted.
The measurements in this section use unchanged runtime `4fd935d5`.
The 1 GiB run reaches the targets by median, but the original 512 MiB
fixture with four workers does not. The larger sample must not substitute
for the user's target in the previous fixture.

Pooling the ten baseline samples across both five-pair matrices, with no
baseline observation removed:

| Metric | Upload | Download |
| --- | ---: | ---: |
| Target, MB/s | 400 | 370 |
| Median, MB/s | **427.325** | **387.475** |
| Minimum, MB/s | 412.36 | 357.38 |
| Maximum, MB/s | 434.77 | 408.07 |
| Samples meeting target | 10/10 | 9/10 |
| Median CPU, µs/64KiB | 380.347 | 448.126 |
| Median peak processRSS, MiB | 90.70 | 58.67 |

The download minimum is below370MB/s. This is a median throughput result,
not a guaranteed per-run minimum. All ten upload samples meet400MB/s.
The five baseline downloads from the second matrix are384.87–391.06MB/s,
median388.17; the slower357.38sample from the first matrix is retained.
Different host conditions between rounds do not establish a code speedup.

Five additional baseline samples use the original fixed 512 MiB workload,
with all other fixture settings retained and no concurrent task workload:
upload **382.57 MB/s**, download **357.56 MB/s** by median. Neither direction
meets its target in any of these five original-size samples. This is the
four-worker control result, not a passed goal under those settings.
Original-size and long-run results are retained separately; none is omitted.

## Profile

Fresh baseline download profile:5187 user-CPU samples, zero lost samples.
Its instrumented2GiB transfer returns383.51MB/s and441.464µs CPU/64KiB.
Combined AES-GCM assembly is a major cost, with the two largest sample shares
7.46% and5.19%; Go memmove4.11%; BLAKE2b compression2.22%+2.04%;
packet construction2.20%. These are sampling shares, not predicted speedups.
Full call graphs and raw profile are retained in the artifact directory.

## Rejected receive-batch expansion

The first candidate raised each TCP receive array from8 to16already-available
QUIC chunks. It added no wait for packets and kept16KiB shared frames and
existing credit/queue limits, but could transiently hold more receive chunks.
All130engine tests passed.

| Workload | Metric | Baseline median | 16-chunk candidate median |
| --- | --- | ---: | ---: |
| Upload | MB/s | 428.98 | 424.99 |
| | Process CPU µs/64KiB | 378.769 | 384.250 |
| | PeakRSS MiB | 89.48 | 89.73 |
| Download | MB/s | 386.78 | 348.06 |
| | Process CPU µs/64KiB | 451.367 | 505.611 |
| | PeakRSS MiB | 58.60 | 56.61 |

Download also retires more instructions in the candidate:19.401→19.670billion
per fixed1GiB run. The candidate was rejected and `engine.rs` restored
exactly to the baseline. Its source, binaries, checks and every observation
remain under `batch16-*`/`*-batch16*` in the ignored artifact directory.

## Rejected last-segment send-buffer lookup candidate

Quinn's stream send buffer retains old application-write chunks until ACK.
Previously `get` scanned all retained chunks from the front even when the
requested data belonged to the newest write. Check that last chunk directly,
using its existing length and the stream's existing end offset. All other
lookups retain the original scan.

This is different from the rejected mutable cursor experiment: there is no
cursor, additional field, ACK/reset bookkeeping or retained reference.
The send-buffer size remains112bytes on amd64. No buffer allocation,
payload ownership, stream credit, queue size, backpressure, wire format or
SeqCst wakeup-handshake change is introduced.

A differential regression compares the old scan after8192writes, including
zero-length chunks, repeated partial prefix ACKs, deque rotation, boundaries,
out-of-range lookups, discard and subsequent writes. All325release Quinn
tests pass, with five ignored fixtures.

Five alternating same-binary micro samples,131072lookups per case:

| Retained chunks | Position | Reference ns/lookup | Candidate ns/lookup |
| ---: | --- | ---: | ---: |
| 1 | Only chunk | 5.535 | 4.609 |
| 8 | Head | 5.388 | 6.541 |
| 8 | Middle | 8.953 | 10.061 |
| 8 | Tail | 12.112 | 4.598 |
| 64 | Head | 5.472 | 6.576 |
| 64 | Middle | 44.483 | 43.619 |
| 64 | Tail | 75.098 | 4.779 |
| 512 | Head | 5.365 | 6.693 |
| 512 | Middle | 265.979 | 266.749 |
| 512 | Tail | 525.966 | 4.582 |

The extra check costs about1ns in the head cases. The tail micro improvement
is not an end-to-end throughput factor. The ignored
`connection::send_buffer::tests::benchmark_last_segment_lookup` contains
both algorithms; `XHYS_LOOKUP_REFERENCE=1` selects the original scan.

Five paired end-to-end samples against the same existing baseline:

| Workload | Metric | Baseline median | Last-segment candidate median |
| --- | --- | ---: | ---: |
| Upload | MB/s | 427.18 | 420.51 |
| | Process CPU µs/64KiB | 380.638 | 385.624 |
| | PeakRSS MiB | 92.94 | 94.62 |
| Download | MB/s | 388.17 | 366.99 |
| | Process CPU µs/64KiB | 447.817 | 477.032 |
| | PeakRSS MiB | 67.00 | 57.36 |

Download instructions also increase19.309→19.463billion per fixed1GiB run.
Despite a clear tail-lookup micro win, this candidate fails the transport
goal and is rejected. `send_buffer.rs` and its experimental tests were
restored exactly to the baseline; source and the test executable remain
archived in the ignored artifacts. Neither the earlier mutable cursor nor
this immutable tail check is present in production.

## Rejected runtime-dispatched AVX2 XOR

An isolated AVX2 helper handled payloads of at least512bytes, with runtime
CPU detection and the existing scalar/SSE path retained on all other CPUs
and small packets. Independent tests covered unaligned offsets0–31,
short tails,511/512/513byte dispatch boundaries, buffers through65536bytes
and unchanged surrounding storage.131engine tests passed, with two ignored
benchmarks.

Seven same-binary hash-plus-XOR micro samples improved1200byte packets
160.40→150.66ns (-6.1%) and1452byte packets168.49→155.73ns (-7.6%).
The64/256byte fallback cases were near unchanged. A preliminary isolated
prototype also improved common large packets.

Five paired512MiB transport samples at four workers/role nevertheless gave
upload384.78→383.77MB/s, CPU422.976→420.581µs/64KiB; download359.97→350.21MB/s,
CPU475.164→488.644µs. User instructions declined9.309→9.158billion upload
and9.826→9.726billion download, but that work reduction did not improve
download speed. No specific cause of the elapsed/CPU regression was established.
The helper, feature dispatch and test changes were fully reverted.
All experimental source, tests, binaries and observations remain archived.

## Restored-runtime validation

After reverting both candidates, all runtime source matches `4fd935d5`.
A fresh Linux rebuild is byte-identical to the baseline Xray used in the
measurements (SHA-256
`0e567bcb6557ab9b2f314fec2343e0f8c263638242a7033c940a4a6b2e48fdeb`).
The final interop executable also remains the original measured baseline.

Fresh checks pass:130release engine tests,324release and325debug Quinn-proto
tests, with1/4/4ignored fixtures respectively; selected-library Clippy with
warnings denied; Go race tests for xhysteria,hysteria,handoff,supervisor
and proxy/hysteria.

Shared/socket masked third-party quic-go tests preserve192simultaneous
streams,1200sequential recycles in one QUIC connection and the finite
untransmitted-handle window. Packet-mask compatibility, concurrent transfers,
short-flow credit recovery, rejection/recovery at the flow cap and16MiB
transfers in each direction with100msRTT pass.

Strict direct/cascade UDP under TCP load,256/4096byte payloads:8000sent,
8000received, zero sampled kernel UDP drops and zero duplicates.

Three1000-session capacity samples pass the512MiB RSS guard with
GOMEMLIMIT128MiB and one server CPU. Medians:idle serverRSS135.5MiB,
peak222.3MiB,500MiB duplex160.43MiB/s, heartbeatp992819.4µs,
serverCPU4.175seconds. These are fresh observations of the unchanged
runtime, not a baseline/candidate capacity improvement. No1GiB cgroup
is applied in these runs.

No fresh15minute soak or Android device measurement is part of this round.

The one-worker profile additionally passes fresh shared/socket masked
third-party stream-limit/churn tests, the mask/concurrency/cap-recovery/
short-flow/100msRTT group and a second strict8000/8000UDP check, with zero
sampled kernel drops/duplicates.

`bench-throughput.sh` passes Bash syntax checking and ShellCheck.
Both reuse-binary and build-harness paths complete an actual one-repeat
bulk matrix. The newly built harness matches the original baseline binary
byte-for-byte. Its smoke result409.97/438.35MB/s for one worker is recorded
separately and is not substituted for the seven-pair qualification result.

## Artifacts

Ignored `build_assets/throughput-400-370-20261123/` contains the baseline
commit, baseline/candidate executables, profiles, all per-repeat observations,
counter CSVs, source snapshots, scripts and JSON summaries. Baseline interop
SHA-256:
`263af5d82a9f4b49477e6a36a3b2d917c2188b2ea3bc5250324125d715a37ae2`.
Last-segment candidate interop:
`d352194c6e68ec402679480ea91a7afc1d6d53c99aeaa6571c4e72e9da3357f6`.

No production deployment, remote restart or neighboring-repository edit is
part of this work. Existing unrelated repository-wide test/Clippy failures
are not described as passing.
