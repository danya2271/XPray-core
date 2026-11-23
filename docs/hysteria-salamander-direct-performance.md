# Hysteria single-block Salamander hashing

Date: November 23, 2026. Baseline: `9c39305b`.

## Change and compatibility

For passwords of 4–120 bytes, the Salamander hash input fits in one 128-byte
BLAKE2b block, including its eight-byte salt. Store that password prefix and
call the existing SIMD library's one-shot hash, using shared immutable
BLAKE2b-256 parameters. This avoids cloning, updating and finalizing a
streaming hash state on every sent or received packet.

The wire calculation is still **unkeyed BLAKE2b-256(PSK || salt)**.
There is no change to the password, salt generation/consumption, XOR,
layer order, GSO/GRO segmentation or packet format. Processing remains
immediate: this change does not wait for another packet.

Passwords longer than120bytes keep the previous prehashed streaming
algorithm. Its template is boxed to keep the ordinary mask object small.
On amd64, the inline Salamander object shrinks224→136bytes (-88bytes).
A long-password mask additionally owns a224byte streaming state allocation,
so its total storage increases; the ordinary size reduction is not a claim
of universal lower RSS. Shared hash parameters contain no password or salt.
There is no allocation per packet in either path.

Independent BLAKE2 reference tests cover key lengths4,13,119,120,121,127,
128,129,255,1024, payload lengths1–1452,128 varying salts per boundary case
and cloned templates. Existing vectors, random-salt round trips and segmented
packet checks remain in place.

QUIC send-buffer production code, stream limits, third-party STOP_SENDING
cleanup, half-close behavior, flow windows, queue budgets and SeqCst ring
wakeup ordering are unchanged. The send-cursor experiment was rejected;
its separate report is `hysteria-send-cursor-experiment.md`.

## Same-binary microbenchmark

Seven alternating fresh processes per algorithm, pinned to CPU0.
`benchmark_single_packet_hashing` contains the original streaming reference
and the production path in one release engine test executable.
`XHYS_HASH_REFERENCE=1` selects the reference. Each case processes131072
independent salt values, hashes and XORs a reused packet; allocation and
template initialization are outside the timed interval.

| Password bytes | Payload bytes | Streaming ns/packet | Direct ns/packet | Change |
| ---: | ---: | ---: | ---: | ---: |
| 13 | 64 | 151.12 | 140.03 | -7.3% |
| 13 | 1200 | 186.49 | 184.81 | -0.9% |
| 13 | 1452 | 205.63 | 189.87 | -7.7% |
| 120 | 64 | 138.07 | 131.79 | -4.5% |
| 120 | 1200 | 186.73 | 174.43 | -6.6% |
| 120 | 1452 | 186.64 | 182.30 | -2.3% |
| 121 | 64 | 266.03 | 262.78 | -1.2% |
| 121 | 1200 | 305.68 | 307.53 | +0.6% |
| 121 | 1452 | 310.66 | 316.46 | +1.9% |
| 1024 | 64 | 265.35 | 264.13 | -0.5% |
| 1024 | 1200 | 293.32 | 289.08 | -1.4% |
| 1024 | 1452 | 298.45 | 298.80 | +0.1% |

The long-password algorithm is unchanged; its timings include enum dispatch
and pointer access. Do not extrapolate these local measurements to an equal
whole-Hysteria CPU or throughput factor. An earlier isolated hash-only
prototype showed7–9% savings but did not include XOR; the table above is the
complete measured hash-plus-XOR path.

An additional standalone compilation of the unchanged production methods
checks the actual25byte transport-fixture password, with both template
references passed through `black_box`. Seven alternating samples give
148.76→135.05ns for64byte payloads,186.46→180.83ns for1200bytes and
196.62→185.79ns for1452bytes (-9.2%/-3.0%/-5.5%).
Its source and observations are `fixture-packet.rs`, `fixture-packet.log`
and `fixture-packet-summary.json` in the final artifact directory.
This separate compilation supplements the engine microbenchmark; it is
not the executable used for end-to-end transport measurements.

## End-to-end method

The final candidate Xray SHA-256 is
`0e567bcb6557ab9b2f314fec2343e0f8c263638242a7033c940a4a6b2e48fdeb`;
its interop executable is
`263af5d82a9f4b49477e6a36a3b2d917c2188b2ea3bc5250324125d715a37ae2`.
Baseline hashes are
`880dd2c68387017161c8d60b40cce14c60fc5e93f23aecdec91e7b13ea5c9335`
and
`225ca3e59f06999cd792daa7a7e6393fca754bc49fccb2f2bad3e184369d27c5`.
Content-based build IDs prevent Go from reusing a stale Rust archive.

Five alternating fresh processes per version/workload, both-native loopback,
shared bridge, one pooled session, Salamander plus WireGuard header,
CPUs4–7, four native client and server workers. Upload/download each complete
512MiB including receiver completion. Idle TCP/UDP each complete20000
64byte exchanges. Paced TCP completes10000 exchanges with a common offered
background load. Report attained background throughput with latency.
CPU and peak RSS include the complete process: client, server and echo.

Three alternating managed-server repetitions per version on one and four
logical server CPUs, load generator on other physical cores. Each run uses
32flows split between two generations,32768duplex64KiB exchanges or200000
small exchanges, with strict relay/public UDP drop gates.
Total server CPU includes generation, supervisor and echo processes.

Seven additional alternating latency samples per version use a lower offered
32MiB/s and30000exchanges. Three alternating capacity samples per version use
1000authenticated QUIC sessions,500MiBduplex, one server CPU,
GOMEMLIMIT128MiB and a512MiB RSS guard, without a1GiB cgroup.

## Initial observations

The first five-run matrix overlapped functional checks/builds on different
physical cores during part of its execution. It had large bulk variability:
upload325.43–386.80MB/s baseline and261.65–393.26candidate;
download299.98–355.64baseline and244.56–364.69candidate. Those observations
remain recorded. CPU affinity prevents direct core sharing between the
fixtures, but does not isolate the whole CPU package or unrelated desktop
processes. No single cause of the variation was established.

| Workload | Metric | Baseline median | Candidate median |
| --- | --- | ---: | ---: |
| Upload | MB/s | 379.64 | 384.38 |
| | Process CPU µs/64KiB | 434.092 | 421.130 |
| | Peak RSS MiB | 86.51 | 89.11 |
| Download | MB/s | 350.56 | 326.05 |
| | Process CPU µs/64KiB | 477.814 | 523.746 |
| | Peak RSS MiB | 67.62 | 63.84 |
| Idle TCP | p99 µs | 183.9 | 178.3 |
| Idle UDP | p99 µs | 180.0 | 172.4 |
| Paced TCP | p99 µs | 490.8 | 544.5 |
| | Attained background MiB/s | 63.28 | 60.44 |

The first managed-server one-core matrix gave234.86→230.20MB/s and
516.93→522.15µs total CPU per duplex exchange. Each successive pair moved
closer:224.52→213.35,234.86→230.20,237.62→237.48MB/s.
This motivated another matched bulk comparison after all functional/build
work had finished; the original observations are not discarded.

The four-core matrix ran after that validation work finished. Three medians:

| Exchange | Metric | Baseline | Candidate |
| --- | --- | ---: | ---: |
| 64KiB duplex | MB/s | 751.09 | 755.51 |
| | Total CPU µs/exchange | 608.46 | 606.12 |
| | p99 µs | 13376 | 11925 |
| | Peak server RSS MiB | 112.70 | 112.00 |
| 64B | Total CPU µs/exchange | 73.80 | 73.07 |
| | p99 µs | 2113 | 2105 |
| | Peak server RSS MiB | 95.64 | 97.11 |

All24initial server samples pass strict relay/public UDP drop gates.

All six1000-session capacity samples pass512MiB. Medians:
idleRSS137.2→135.6MiB, peak221.2→221.0MiB, duplex149.75→152.45MiB/s,
heartbeatp992844.6→2759.7µs and serverCPU4.488→4.454seconds.
These small changes are local observations, not general savings factors.

## Bulk control after validation finished

Five new alternating samples per version, on the same CPUs4–7 and fixed
512MiB workload, with no concurrent task builds, functional tests or other
task benchmarks. Existing unrelated desktop processes were not changed.
`perf stat` counts user instructions and cycles for the complete benchmark
process, including setup/teardown; both hardware events ran100% of the time
without multiplexing. The benchmark's CPU metric covers the timed workload.
Software context-switch/migration counters in raw perf output are filtered
to user mode by the host and are not interpreted.

| Workload | Metric | Baseline median | Candidate median | Change |
| --- | --- | ---: | ---: | ---: |
| Upload | MB/s | 379.85 | 384.65 | +1.26% |
| | Process CPU µs/64KiB | 429.710 | 425.457 | -0.99% |
| | User instructions, billions/run | 9.35438 | 9.29993 | -0.58% |
| | User cycles, billions/run | 7.99407 | 7.91354 | -1.01% |
| | Peak RSS MiB | 90.20 | 88.29 | -2.13% |
| Download | MB/s | 343.24 | 349.76 | +1.90% |
| | Process CPU µs/64KiB | 505.851 | 493.325 | -2.48% |
| | User instructions, billions/run | 9.91626 | 9.85737 | -0.59% |
| | User cycles, billions/run | 8.92251 | 8.76311 | -1.79% |
| | Peak RSS MiB | 55.62 | 54.38 | -2.24% |

Upload instruction-count ranges do not overlap in these samples:
9.340–9.360billion baseline versus9.285–9.313billion candidate.
Download instruction-count ranges overlap. This supports a small measured
reduction in whole-process work; it does not make the microbenchmark's
1–8% reduction a whole-transport factor. Initial and control matrices have
different conditions and remain separate in the raw data and report.
RSS changes are small and not a general memory reduction guarantee.

Five additional one-core managed-server control pairs, also after all other
task benchmarks/checks finished:
throughput243.90→244.16MB/s, total CPU497.842→497.337µs per duplex exchange,
p9932.711→33.913ms and peak serverRSS101.7→102.9MiB.
All10samples pass strict drop gates. Throughput/CPU are essentially unchanged
in this fixture; one-core tail latency does not improve. This change is not
presented as a general weak-server speedup.

Seven additional latency samples at the lower offered32MiB/s:
TCPidle p99174.4→172.5µs, UDP163.2→165.6µs, paced409.7→413.0µs,
with attained background31.70→31.49MiB/s. Ordinary latency is essentially
unchanged/mixed; a general latency improvement is not demonstrated.

## Validation

130 release engine tests and20 release core tests pass, with one ignored
manual benchmark in each configuration. Unchanged Quinn-proto passes324
release/325 debug tests, with four ignored fixtures. Production-library
Clippy and freshly linked Go race tests pass for xhysteria,hysteria,handoff,
supervisor and proxy/hysteria.

Shared/socket Salamander single-session tests preserve192live streams and
1200stream recycles, plus the finite untransmitted-handle window. Packet
masks, concurrent transfers, stream-cap rejection/recovery, short flows and
the100msRTT16MiB-per-direction fixture pass. Strict direct/cascade UDP with
256/4096byte payloads returns8000/8000, zero sampled kernel drops/duplicates.
Linuxamd64 and Androidarm64/API26 build and link successfully.

No new15minute soak or Android device runtime measurement was performed.
Older repository-wide Go failures and the unrelated all-target engine Clippy
limitation are not described as passing or repaired.

## Artifacts and scope

Source, exact binary hashes, raw measurements and checks:
ignored `build_assets/salamander-direct-20261123/`. Reference streaming
and direct paths are both reproducible through the ignored microbenchmark.
The preliminary hash-only code/results remain in
`build_assets/salamander-batch-20261123/direct-hash.*`.

Rejected copy/XOR fusion, four-packet hash batching and send-cursor candidates
remain archived with their failures and measurements. They are not part of
this production change. No remote deployment/restart or neighboring-worktree
modification was performed. Android checks cover compilation/linking only.
