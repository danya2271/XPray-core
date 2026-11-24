# Hysteria per-frame work: queues, credits, publication and metadata

Date: November 24, 2026. Baseline: `6cfd8baf`.

## Scope

The user requested the five remaining targets: diagnose the two one-core
upload modes, finish the queued `Conn.waitWrite` fast path, reduce credit/budget
work, amortize bridge/connection locking, and reduce copies/owner metadata.
Cryptography and QUIC packet/ACK algorithms are outside this experiment.

## Outcome and limits

Accepted as a bounded reduction in per-frame allocation and publication work,
not as a universal transport improvement. The measured ready-state candidate
is published unchanged; the earlier mutex candidate is superseded and retained
as historical evidence.

The future-deadline queued send, eventfd callback and payload-buffer metadata
paths remove respectively three, two and one allocations in the isolated
fixtures. Grouping two/four ready native frames saves 5.85%/8.48% in the
publication microbenchmark. Payload copies into the shared arena remain.

Transport results are mixed and all initial regressions remain included:

- One-core paced p99 improves 4554→3581 µs at similar attained background load.
  Two-core paced p99 worsens 1769→1828 µs.
- Across all nine one-core upload pairs, medians are 183.32→183.92 MB/s and
  means 165.98→166.76 MB/s, with four slow observations per revision. The
  original packetization/scheduling trigger remains unresolved.
- All nine two-core duplex pairs improve throughput 319.84→322.04 MB/s but
  worsen bulk p99 43734→47977 µs. All seven eight-core duplex pairs worsen
  throughput 731.82→703.11 MB/s, CPU 1049.94→1087.12 µs/op and
  p99 20876→23371 µs. The additional four pairs alone do not cancel this
  combined regression.
- Managed-server transfer improves 254.34→257.93 MB/s on one core and
  458.14→466.58 MB/s on two cores. These server-isolated results do not
  establish a general scaling win.
- At 1000 sessions, peak-before-close RSS is 219.50→222.40 MiB,
  throughput 167.03→166.03 MiB/s and heartbeat p99 3795→3938.8 µs.
  There is no demonstrated intrinsic RSS or capacity-latency improvement.

The regression envelope above is a limitation of this accepted work-reduction
patch, not an optimization guarantee. No ARM performance, WAN improvement,
new 15-minute soak, full-repository test success or production deployment is
claimed. Android validation is a build only. The independent supervisor
gone-process correction is committed separately as `cffb8e6f`.

Publication preserves the existing branch-specific regular Go buffer size:
4 KiB on `android`, 8 KiB on `node-main`, `nodex-supervisor-pilot` and
`nodex-seamless-update`. The numeric A/B evidence and source hashes describe
the measured `android` workspace; they are not measurements of the server
branches with their different regular-buffer setting. The new release and
metadata logic is applied without overwriting that setting.

## Diagnosis before changes

Eight baseline runs transfer 1 GiB each with Go CPU, memory, mutex and block
profiles enabled. Five reach 191.81–193.71 MB/s with 36–37 allocations per
64 KiB operation; three reach 136.03–137.13 MB/s with 53 allocations/op.
These are instrumented observations, not acceptance throughput measurements.
Go's unresolved native CPU labels are not used to attribute Rust function cost.

A separate baseline build adds only per-engine DATA-size counters to the Go
reader. Eight fresh runs again reproduce both modes. The six fast observations
publish approximately 100–101 thousand DATA frames per GiB, averaging
10.6–10.7 kB/frame (decimal); the two slow observations publish approximately
207 thousand, averaging 5.2 kB/frame. Exact byte averages and
bucket boundaries are in the evidence. Calibration engines are recorded
separately; the actual transfer is identified by its byte total.

This establishes a correlation between the slow mode and roughly twice the
per-frame work. It does not establish the original scheduling/packetization
trigger. Increasing the ready-chunk array alone does not eliminate both modes.

The fixture calls `SetDeadline` on its `cnc.Connection` wrapper, whose deadline
methods do not forward deadlines. Therefore the queued-send timer fix below
must not be presented as the cause or cure of the observed upload modes.
The timer regression and paired microbenchmarks explicitly set a native
connection deadline instead.

## Candidate

- `Conn.waitWrite` attempts an immediately available writer-queue slot before
  allocating a deadline timer. Expired deadlines, closed connections, changed
  deadlines and reservation cleanup retain explicit checks.
- Go's eventfd writer reuses its `RawConn.Control` callback and synchronizes
  its error result. This removes the per-signal captured closure/error
  allocations while preserving descriptor-close protection, EINTR/EAGAIN
  behavior and pending-flag error rearming.
- Shared Rust DATA publication copies already-read QUIC chunks directly into
  up to four bounded frames under one publisher lock and one head publication.
  Credits are acquired together only when already available. With one ready
  credit it publishes one frame immediately. There is no wait to accumulate
  more data, and control/UDP publication retains its existing path.
- The native TCP receive array holds up to 16 ready Quinn chunks rather than
  eight. Each `read_chunks` still returns as soon as any data is ready.
  This can reduce repeated connection-state locking and short DATA frames;
  it is not a universal solution to the bimodality.
- Go payload slots reuse one `buf.ReusableBuffer` each instead of allocating
  an owned buffer wrapper for every frame. Metadata reuse is synchronized
  by a per-slot Go ready state, independently of native atomics. Release marks
  all Go metadata writes complete before returning the native slot; the next
  read acquires that state and rejects duplicate live leases without adding
  an owner reference.
  `Buffer.Release` makes its callback the last operation on owned metadata so
  that returning the slot cannot race a trailing metadata write.

Payload copies into the shared arena remain. No all-copy-free transport is
claimed. Reuse retains no released payload and is bounded to 16 metadata
entries per bridge, with a small fixed metadata cost. The receive workspace
change adds eight `Bytes` descriptors per TCP receive future and can change
how many already-received chunks remain held during backpressure; RSS is
therefore measured rather than inferred.

Wire/storage ABI6,16 KiB DATA maximum,ring capacity8,payload slots16,
flow credits,local/process byte budgets,worker defaults,Tokio intervals and
SeqCst wakeup handshakes retain their baseline settings.

## Isolated screening

Before metadata reuse, two candidates compare the new timer/eventfd/shared
publication paths with eight or 16 receive descriptors. There are eight
fresh upload observations per version and three per version for one-core
paced latency,two-core download and eight-core duplex:51transport observations.
All are retained, including the bimodal upload results.

The 16-descriptor variant was selected for the next test stage because its
three-pair screening improves paced p99 and eight-core throughput, while the
eight-descriptor variant does not improve the low-core screening consistently.
These samples do not qualify the final metadata-reuse variant; its independent
before/after matrix and acceptance checks follow.

## Method

The host is an Intel Xeon E5-2698 v3,16physical cores/32SMTthreads.
One/two-core combined client/server/TCP-target runs use CPU4 or CPUs4,5,
GOMAXPROCS and native workers per role equal to that budget,pool2,32flows,
shared bridges,Salamander+WireGuard. Five paired fresh-process repetitions
alternate version/core order. Duplex is8192checked64KiB exchanges each way;
upload/download transfer512MiB including receiver completion. Paced latency
is10000×64B probes with offered64MiB/s duplex background.

Four/eight-core controls use CPUs4–7/4–11,four native workers per role,pool2/4.
An independent managed-server comparison uses an unchanged baseline generator
on CPUs8–15 and server generations,supervisor/ingress and echo on CPU4 or4,5.
No task builds/tests overlap timed matrices. Unrelated host activity is
uncontrolled. Nonzero-drop benchmark observations stay in all summaries.


## Replacing the first metadata synchronization candidate

The initial per-slot mutex variant completes247transport observations,
6capacity runs,98micros and24instrumented diagnostics. Its one/two-core
throughput/CPU mostly improve,but all seven eight-core duplex pairs retain
a mixed result:732.19→706.12MB/s,CPU1049.042→1087.848µs/op,p9920140→21195µs.
The four additional pairs alone improve throughput,so this does not identify
a fixed cache regression or its cause. The no-cache variant also has mixed
results. All initial,additional and component observations remain in
[mutex-candidate.json](hysteria-bridge-work-20261124/mutex-candidate.json).

The final candidate replaces the per-slot mutex with a ready CAS/store. This
removes two lock/unlock pairs per lease cycle and avoids returning a native
slot while still holding its Go metadata mutex. The new state synchronizes
Go metadata before native reuse and detects a duplicate live lease. The
common buffer callback remains the last metadata operation. Exact-source
race,lifetime,transport,capacity and Android checks and new paired measurements
have completed for this version. Earlier mutex-candidate throughput does not
qualify the final candidate. Four paired repetitions also compare baseline,
mutex and ready-state candidates on2coreduplex/4paced/8duplex/8paced;these cases
were selected from the earlier concerns before the final candidate's matrix.
One-core upload was added after the new matrix again showed a mode imbalance.


## Final measurements

Decimal MB/s;duplex aggregates both directions. Arrows compare medians,
not minimum guarantees. Combined-process CPU/RSS include client/server/target.

### One/two-core combined fixture: five paired repetitions

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB | Allocs/64KiB op or probe | Drop samples before/after |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | :---: |
| 1 | duplex | 183.21 → 186.10 | 676.14 → 666.19 | 62051.00 → 65207.00 | 64.14 → 64.29 | 89.00 → 70.00 | 0/0 |
| 1 | upload | 183.85 → 145.45 | 336.43 → 416.67 | — | 160.93 → 126.37 | 37.00 → 33.00 | 0/0 |
| 1 | download | 193.13 → 198.35 | 324.05 → 315.62 | — | 133.88 → 86.18 | 40.00 → 32.00 | 0/2 |
| 1 | paced | — | 228.07 → 215.00 | 4554.00 → 3581.00 | 52.83 → 52.89 | 26.00 → 15.00 | 0/0 |
| 2 | duplex | 320.27 → 322.16 | 736.40 → 732.74 | 43734.00 → 49444.00 | 66.02 → 66.90 | 95.00 → 72.00 | 0/0 |
| 2 | upload | 296.04 → 295.81 | 384.67 → 384.27 | — | 173.52 → 169.36 | 39.00 → 29.00 | 2/0 |
| 2 | download | 338.05 → 338.81 | 353.91 → 352.02 | — | 94.06 → 87.80 | 43.00 → 33.00 | 3/0 |
| 2 | paced | — | 300.01 → 293.58 | 1769.00 → 1828.00 | 53.06 → 53.86 | 25.00 → 13.00 | 0/0 |

The initial one-core upload matrix again places more candidate observations
in the slow mode. Four further fresh paired runs compare all three binaries.
No initial sample is removed. Arithmetic means and mode counts accompany the
median because a one-observation change can move a bimodal median abruptly.

| One-core upload observations | Median MB/s baseline → candidate | Mean MB/s | CPU µs/op | Below160MB/s baseline/candidate |
| --- | ---: | ---: | ---: | :---: |
| additional4pairs | 143.00 → 184.11 | 152.65 → 174.45 | 422.18 → 334.28 | 3/1 |
| all9pairs | 183.32 → 183.92 | 165.98 → 166.76 | 337.37 → 334.69 | 4/4 |

Four additional paired runs repeat exactly this
workload/settings/binary pair after all other task builds/tests/diagnostics.
The original results remain above and in the joint summary.

| Two-core duplex observations | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| additional4pairs | 317.36 → 321.56 | 730.72 → 727.38 | 40433.50 → 47481.50 | 65.62 → 67.26 |
| all9pairs | 319.84 → 322.04 | 734.79 → 730.03 | 43734.00 → 47977.00 | 66.02 → 67.13 |

Attained paced background:

- 1 core(s): 64.34 → 64.45 MiB/s.
- 2 core(s): 64.05 → 64.09 MiB/s.

### Four/eight-core controls: three paired repetitions

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB | Allocs/64KiB op or probe | Drop samples before/after |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | :---: |
| 4 | duplex | 487.99 → 493.06 | 864.24 → 861.90 | 31085.00 → 33267.00 | 68.91 → 67.62 | 99.00 → 73.00 | 0/0 |
| 4 | upload | 462.91 → 472.45 | 444.72 → 438.07 | — | 178.72 → 173.03 | 39.00 → 29.00 | 0/0 |
| 4 | download | 488.17 → 487.07 | 422.52 → 422.22 | — | 86.66 → 88.17 | 49.00 → 35.00 | 0/0 |
| 4 | paced | — | 256.20 → 264.48 | 1266.00 → 1264.00 | 54.98 → 54.90 | 22.00 → 12.00 | 0/0 |
| 8 | duplex | 731.82 → 700.31 | 1053.72 → 1094.35 | 21705.00 → 23865.00 | 70.09 → 68.24 | 101.00 → 72.00 | 0/0 |
| 8 | upload | 649.71 → 669.18 | 556.00 → 545.03 | — | 203.20 → 205.86 | 42.00 → 30.00 | 3/3 |
| 8 | download | 719.80 → 768.46 | 491.48 → 465.15 | — | 101.86 → 89.95 | 51.00 → 33.00 | 0/0 |
| 8 | paced | — | 914.12 → 925.47 | 2868.00 → 2828.00 | 56.11 → 56.00 | 34.00 → 20.00 | 0/0 |

### Predefined additional checks and synchronization isolation

Concerns from the earlier mutex candidate predefine four additional paired
runs of2coreduplex/4paced/8coreduplex/8corepaced. A one-core upload case is
added after the new five-pair matrix again exhibits a mode imbalance. The frozen mutex-candidate
binary is also measured in rotating order in every case. It has the same
native receive/publication,timer,eventfd and metadata-reuse changes,but uses
a per-slot mutex instead of the final ready CAS/store. This comparison
isolates synchronization without conflating the other requested changes.
Every initial result remains above;no slow or nonzero-drop sample is removed.

| Cores | Workload | Variant | N additional | Median MB/s | CPU µs/op | p99 µs |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| 1 | upload | before | 4 | 143.00 | 422.18 | — |
| 1 | upload | mutex | 4 | 185.22 | 332.34 | — |
| 1 | upload | after | 4 | 184.11 | 334.28 | — |
| 2 | duplex | before | 4 | 317.36 | 730.72 | 40433.50 |
| 2 | duplex | mutex | 4 | 319.30 | 725.69 | 46101.00 |
| 2 | duplex | after | 4 | 321.56 | 727.38 | 47481.50 |
| 4 | paced | before | 4 | — | 363.25 | 1688.50 |
| 4 | paced | mutex | 4 | — | 376.90 | 1784.00 |
| 4 | paced | after | 4 | — | 393.85 | 1840.50 |
| 8 | duplex | before | 4 | 723.87 | 1034.79 | 19135.50 |
| 8 | duplex | mutex | 4 | 730.18 | 1035.27 | 21932.00 |
| 8 | duplex | after | 4 | 730.83 | 1014.93 | 21717.00 |
| 8 | paced | before | 4 | — | 821.72 | 2370.50 |
| 8 | paced | mutex | 4 | — | 810.84 | 2698.50 |
| 8 | paced | after | 4 | — | 808.49 | 2244.50 |

All seven exact baseline/candidate pairs per control case:

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 4 | paced | — | 264.91 → 272.20 | 1360.00 → 1332.00 | 54.98 → 54.71 |
| 8 | duplex | 731.82 → 703.11 | 1049.94 → 1087.12 | 20876.00 → 23371.00 | 70.58 → 68.55 |
| 8 | paced | — | 828.77 → 813.16 | 2666.00 → 2648.00 | 56.05 → 55.51 |

### Managed server with an unchanged separate baseline generator

Three paired repetitions per case;server CPU sums generations and supervisor/
echo,excluding the client. All24observations pass strict ingress/publicUDP-drop
gates. Transfer32768duplex64KiB exchanges;latency200000×64B,32flows across two
live generations. Server RSS covers server generations.

| Server cores | Workload | MB/s baseline → candidate | Total server CPU µs/op | p99 µs | Server peak RSS MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | transfer | 254.34 → 257.93 | 474.95 → 468.13 | 39639.00 → 38224.00 | 103.90 → 104.50 |
| 1 | latency | — | 40.11 → 39.85 | 2428.00 → 2409.00 | 92.61 → 92.82 |
| 2 | transfer | 458.14 → 466.58 | 515.09 → 504.76 | 19585.00 → 18475.00 | 107.30 → 106.50 |
| 2 | latency | — | 62.52 → 62.11 | 3477.00 → 3380.00 | 95.34 → 95.89 |

### Capacity: 500 users,1000QUIC sessions and TCP flows

Three alternating pairs,server CPU4/GOMAXPROCS1,GOMEMLIMIT128MiB;fixture on
CPUs4–7. All six runs pass the512MiB server peak-RSS guard.

| Metric | Baseline median | Candidate median |
| --- | ---: | ---: |
| server_idle_rss_mib | 137.00 | 136.40 |
| server_buffered_rss_mib | 212.80 | 213.20 |
| server_peak_rss_mib | 219.50 | 222.40 |
| server_after_close_rss_mib | 218.30 | 218.00 |
| duplex_transfer_mib_per_second | 167.03 | 166.03 |
| heartbeat_p99_us | 3795.00 | 3938.80 |
| server_process_cpu_seconds | 4.20 | 4.21 |

## Microbenchmarks

All useCPU4,seven repetitions. Timer-policy before/after executables use the
same benchmark and native-library-independent queued path,1000000iterations.
The eventfd reference copies the previous callback algorithm in the same
executable;both variants perform real nonblocking writes,excluding pending-flag
coalescing. Metadata uses a one-entry channel to preserve escaped buffer-owner
behavior;it excludes the ready-state synchronization used by the actual bridge.

| Benchmark | Revision | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| BenchmarkConnQueuedSend/no_deadline | after | 251.40 | 0 | 0 |
| BenchmarkConnQueuedSend/future_deadline | after | 316.70 | 0 | 0 |
| BenchmarkConnQueuedSend/no_deadline | before | 301.80 | 0 | 0 |
| BenchmarkConnQueuedSend/future_deadline | before | 776.20 | 248 | 3 |
| BenchmarkEventSignal/legacy | same-binary case | 195.10 | 32 | 2 |
| BenchmarkEventSignal/reused | same-binary case | 160.00 | 0 | 0 |
| BenchmarkOwnedBufferMetadata/allocate | same-binary case | 122.70 | 64 | 1 |
| BenchmarkOwnedBufferMetadata/reuse | same-binary case | 61.66 | 0 | 0 |

Native publication uses65536groups per size,normal allocator,16KiB/frame.
The reference acquires one credit and publishes one frame at a time;candidate
uses the grouped path. Both copy payload and drain parsed payload owners.
The fixture does not rearm Go's eventfd pending flag each group,so this
isolates credit/locking/publication/copy work without claiming syscall savings.

| Ready frames/group | Individual ns/frame | Grouped ns/frame | Reduction |
| ---: | ---: | ---: | ---: |
| 1 | 500.909 | 499.696 | 0.24% |
| 2 | 763.704 | 719.033 | 5.85% |
| 4 | 760.451 | 696.002 | 8.48% |

## Instrumented before/after DATA-size diagnostics

Eight separate fresh runs per version,1GiB upload,one core. Counters describe
the actual server transfer;calibration engines remain separately recorded.
The builds add only reader counters/close-time output to their respective
production sources. Instrumented throughput is excluded from acceptance.

| Version | Repeat | MB/s | DATA frames | Mean bytes/frame | ≤4096B frames |
| --- | ---: | ---: | ---: | ---: | ---: |
| before | 1 | 190.94 | 101276 | 10602.14 | 5654 |
| before | 2 | 137.22 | 206751 | 5193.41 | 118778 |
| before | 3 | 137.66 | 206878 | 5190.22 | 119211 |
| before | 4 | 190.73 | 100295 | 10705.84 | 5053 |
| before | 5 | 191.54 | 100984 | 10632.79 | 5466 |
| before | 6 | 189.42 | 101426 | 10586.46 | 5988 |
| before | 7 | 191.20 | 101157 | 10614.61 | 5901 |
| before | 8 | 191.55 | 100548 | 10678.90 | 4935 |
| after | 1 | 136.01 | 202216 | 5309.88 | 122414 |
| after | 2 | 134.23 | 207558 | 5173.21 | 127304 |
| after | 3 | 135.29 | 206489 | 5200.00 | 126466 |
| after | 4 | 137.09 | 200482 | 5355.80 | 120834 |
| after | 5 | 137.46 | 200840 | 5346.26 | 120702 |
| after | 6 | 137.63 | 200867 | 5345.54 | 120864 |
| after | 7 | 136.50 | 202247 | 5309.06 | 122127 |
| after | 8 | 136.63 | 203085 | 5287.16 | 122938 |

## Validation

- 137release/137debug Rust engine/library tests pass;4manual benchmarks ignored.
- Release engine-library Clippy passes with`-D warnings`.
- Fresh native Go race tests pass for common/buf,xhysteria,hysteria,handoff,
 supervisor and proxy packages. New tests cover deadline/queue cleanup,
 zero-allocation notification,concurrent pending rearm,zero-allocation slot
 metadata reuse,concurrent slot/UDP metadata reuse,and callback-time rebinding.
- The first supervisor race run fails the lost-ACK recovery fixture with
  `/proc/<pid>/stat: no such process`. The controller deliberately exits before
  ACK;Linux can returnESRCH after successful pidfd_open. The old liveness check
  handledENOENT only. A separate supervisor fix recognizes both gone-process
  errors while retaining permission/other-I/O failures. The initial failure
  remains in the dataset;20focused regression groups and the fresh full package
  race run pass after the correction. The earlier plain10repeat did not
  reproduce the timing window and is not presented as proof of its absence.
- Rust tests cover frame bytes/boundaries,ring wrap,inline fallback,one-credit
 progress/close,and multiple flow payloads.
- One/two-core masks,concurrent transfers,HTTP file/proxy/lifecycle,short-flow
 credits,capacity recovery and100msRTT bulk checks pass without skips.
- Masked third-party shared/socket clients pass192concurrent streams,
 1200close/reopen and advertised finite-window checks on both budgets.
- Strict direct/cascadeUDP256/4096B under load passes8000/8000per budget,
 zero sampled kernel drops/duplicates.
- Managed saturatedUDP passes6000/6000per budget,zero ingress/publicUDPdrops;
 retained-connection and saturated-drain tests pass.
- Six1000-session capacity runs pass the512MiB guard;Androidarm64/API26 builds
 withNDK30.0.14904198. No device performance/new15minute-soak/deployment claim.

All192final/recheck,20mutex-isolation and51historical-screening transport
observations,6capacity runs,98micro
observations,24instrumented diagnostic runs,validation,source/binary hashes
and reproduction scripts:[results.json](hysteria-bridge-work-20261124/results.json).
Raw profiles and binaries remain ignored in`build_assets/bridge-work-atomic-20261124/`.
