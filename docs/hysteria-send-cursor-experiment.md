# Hysteria send-buffer lookup experiment: rejected

Date: November 23, 2026. Baseline: `9c39305b`.

The cursor was **not adopted**. It improves lookup microbenchmarks, but the
repeat managed-server matrix regresses bulk throughput and CPU on one core.
The source remains archived; production send-buffer code is unchanged.

## Candidate change

Quinn formerly scanned every retained stream-data chunk from the front for
each packet fragment. A stream can retain many chunks until acknowledgement,
so sequential packet construction repeatedly traversed the same prefix.

Remember the last chunk's index and absolute stream offset. Forward lookups
resume there; backward retransmission lookups start at the current retained
prefix. Any nonempty prefix acknowledgement resets the cursor, including a
partial first-chunk acknowledgement and consumption of queued ACK ranges.
The one-chunk case has a direct path. Discard resets the cursor while keeping
the stream's final offset.

The candidate cursor stores two integers, not payload references: 16 additional bytes
per send buffer on amd64. It introduces no allocation, payload copy, queue,
batching delay or waiting policy. Existing buffer capacity, flow control,
payload-owner release, stream limits, STOP_SENDING/half-close handling and
packet-ring SeqCst wakeup ordering are preserved.

Differential tests cover wrapped deque storage, partially acknowledged
chunks, backward and out-of-range lookups, queued ACK ranges, discard and new
writes. Another test compares 64 deterministic scenarios, each containing
512 writes, prefix acknowledgements and eight random lookups per write.

## Local lookup measurements

Seven alternating-order samples compare the original scanning algorithm and
the cursor in one release test executable. Each sample times 64 independent
buffers containing 16 KiB chunks, read in fragments of at most 1200 bytes.
Buffer creation and destruction are outside the timed interval. The
retransmit scenario adds one backward lookup per chunk after the sequential
pass. These are send-buffer timings, not whole-transport speedup factors.

| Retained chunks | Lookup pattern | Before, ns/buffer | After, ns/buffer | Change |
| ---: | --- | ---: | ---: | ---: |
| 1 | Sequential | 106 | 76 | -28.3% |
| 8 | Sequential | 1190 | 868 | -27.1% |
| 64 | Sequential | 44185 | 6830 | -84.5% |
| 512 | Sequential | 2134028 | 54418 | -97.4% |
| 1 | Sequential then backward | 113 | 81 | -28.3% |
| 8 | Sequential then backward | 1274 | 961 | -24.6% |
| 64 | Sequential then backward | 47911 | 10242 | -78.6% |
| 512 | Sequential then backward | 2189940 | 245299 | -88.8% |

`XHYS_LOOKUP_REFERENCE=1` selects the reference algorithm in the ignored
`connection::send_buffer::tests::benchmark_sequential_send_lookup` test.
Both paths use the same cursor-containing struct, so this comparison isolates
lookup work rather than measuring the struct-size change.

## Transport measurement method

Baseline and candidate executables are freshly linked against their respective
Rust archives. The candidate's SHA-256 matches the earlier experiment:

| Artifact | SHA-256 |
| --- | --- |
| Baseline Xray | `880dd2c68387017161c8d60b40cce14c60fc5e93f23aecdec91e7b13ea5c9335` |
| Candidate Xray | `1627b86cba8a3bfdd00bc259ace729b3dfd7306f063183271951b088bc8c0eef` |
| Baseline interop | `225ca3e59f06999cd792daa7a7e6393fca754bc49fccb2f2bad3e184369d27c5` |
| Candidate interop | `512a2316d2291a2fcad4d8544040a18da93b614ae45e7430a151845b958e4a1b` |

Both-native loopback measurements alternate baseline/candidate order over five
fresh-process samples per workload, with shared bridge, one pooled connection,
Salamander plus WireGuard header, CPUs4–7 and four native workers per role.
Upload/download each complete 512 MiB including receiver completion.
Idle TCP/UDP each complete 20000 64-byte exchanges. Paced TCP completes
10000 exchanges with a common offered background load. Process CPU and peak
RSS include both client/server and the target. CPU on bulk transfers is
reported per 64 KiB, not per byte or per second.

The separate managed-server matrix uses three alternating samples on one
and four logical server CPUs, mixed generations with 16 TCP flows each,
32768 duplex 64 KiB exchanges or 200000 small exchanges. Generation,
supervisor and echo CPU are added together; the load generator is pinned to
different physical cores. Relay/public UDP drop gates are strict.

Additional latency samples use seven alternating repetitions at a lower
offered 32 MiB/s, reporting attained background throughput alongside p99.
Capacity uses three alternating samples per version: 1000 authenticated
QUIC sessions, 500 MiB duplex, one server CPU, GOMEMLIMIT128MiB and a
512MiB RSS guard. It is not a 1GiB cgroup measurement.

## Repeat transport results and decision

Five repeated samples per version:

| Workload | Metric | Baseline | Cursor |
| --- | --- | ---: | ---: |
| Upload | MB/s | 399.30 | 402.96 |
| | CPU µs/64KiB | 427.005 | 412.249 |
| | Peak RSS MiB | 82.98 | 89.38 |
| Download | MB/s | 352.31 | 351.61 |
| | CPU µs/64KiB | 466.078 | 476.221 |
| | Peak RSS MiB | 56.12 | 54.91 |
| Idle TCP | p99 µs | 172.2 | 175.9 |
| Idle UDP | p99 µs | 158.7 | 166.4 |
| Paced TCP | p99 µs | 487.5 | 509.9 |
| | Attained background MiB/s | 60.60 | 60.09 |

Three repeated managed-server samples per version:

| Server CPUs | Exchange | Metric | Baseline | Cursor |
| ---: | --- | --- | ---: | ---: |
| 1 | 64KiB duplex | MB/s | 240.29 | 227.00 |
| 1 | 64KiB duplex | Total CPU µs/exchange | 505.35 | 529.07 |
| 1 | 64KiB duplex | p99 µs | 33264 | 42943 |
| 1 | 64B | Total CPU µs/exchange | 40.66 | 40.31 |
| 1 | 64B | p99 µs | 2523 | 2481 |
| 4 | 64KiB duplex | MB/s | 751.79 | 748.43 |
| 4 | 64KiB duplex | Total CPU µs/exchange | 606.95 | 610.53 |
| 4 | 64KiB duplex | p99 µs | 11719 | 11913 |
| 4 | 64B | Total CPU µs/exchange | 73.94 | 73.65 |
| 4 | 64B | p99 µs | 2106 | 2117 |

All 24 server samples pass strict drop gates. The earlier identical candidate
had upload382.05→393.44MB/s and one-core server230.44→227.57MB/s. The repeat
one-core comparison regresses in each pair:219.21→210.67,240.29→227.00,
241.02→235.58MB/s. These observations do not establish universal regression
factors, but they prevent accepting the cursor for the current CPU/throughput
goal. Its microbenchmark benefit alone is insufficient here.

Seven repeat latency samples at the lower offered32MiB/s are mixed:
TCPidle p99180.9→174.9µs, UDP166.8→171.6µs, paced434.2→430.4µs,
with attained background31.71→31.70MiB/s. This is not an across-metrics win.

All six capacity samples pass512MiB: median idleRSS135.5→135.5MiB,
peak221.9→220.0MiB, duplex139.46→150.52MiB/s and
heartbeatp99=3730.5→2834.2µs. This separate capacity fixture does not overturn
the managed bulk result or establish general RSS/latency improvements.

326 release/327 debug Quinn-proto tests pass, with five ignored fixtures;
129 engine tests, production-library Clippy, freshly linked Go race tests,
shared/socket third-party192live/1200churn, masks/shortflows/cap recovery and
100msRTT pass. Strict direct/cascade256/4096 UDP returns8000/8000 with zero
sampled kernel drops/duplicates. Linux and Androidarm64/API26 build.
No fresh15minute cursor soak or production deployment was performed.

## Other rejected masking experiments

The same CPU-optimization round profiled native upload with 2392 user-CPU
samples and no lost samples. BLAKE2b contributed approximately 4.43% combined;
libc copies approximately 5.9%. These profile shares do not predict an equal
end-to-end speedup.

Merging Salamander payload copy and XOR passed reference-byte checks but
regressed common 64–1200-byte packets. Several variants were measured; even
the final rotated-key version regressed ordinary packets by roughly 8–24%.
Its gains on 4000-byte packets did not justify adopting it for ordinary QUIC
traffic. All production changes from that experiment were reverted.

Parallel BLAKE2b hashing of four existing GSO segments also passed 131 engine
tests, including independent digest equality, long-password fallback,
multiple layers, short tails, salt offsets and counter wrapping. Five
same-binary micro samples showed common 1200–1452-byte packets regressing
roughly 12–26% with a 13-byte password. Hash-job setup and changed traversal
outweighed parallel compression in this fixture. This experiment was also
reverted. Neither experiment adds memory or latency to production code.

## Artifacts

Candidate checks and repeated observations are recorded under ignored
`build_assets/send-cursor-final-20261123/`. Earlier identical-candidate
observations, micro samples and the baseline CPU profile remain under
`build_assets/send-lookup-20261123/`. Rejected masking code and raw results
remain under `build_assets/mask-fused-20261123/` and
`build_assets/salamander-batch-20261123/`, including failed development checks.

No production deployment, remote restart or neighboring-worktree edit is
part of this qualification. Android checks cover compilation/linking.
