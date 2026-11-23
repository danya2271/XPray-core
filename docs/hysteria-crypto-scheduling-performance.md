# AES-GCM and scheduler investigation

Date: November 24, 2026. Baseline: `a8f604ee`.
Runtime baseline is unchanged from `4fd935d5`.

## Outcome

Production keeps ring and both Tokio intervals at 7. AWS-LC is slower in
the local QUIC crypto comparison. Raising the I/O event interval worsens
two-core paced latency; raising only the global queue interval substantially
regresses one-core upload. No runtime patch from these experiments is retained.
The useful deliverables are the standalone crypto comparison and recorded
measurements, including all overload observations.

## AES-GCM provider comparison

Production currently uses ring through rustls and prioritizes
`TLS13_AES_128_GCM_SHA256`. The measured host is an Intel Xeon E5-2698 v3,
with 16 physical cores and AES-NI. Changing the cipher preference alone
cannot speed up this already-selected AES-128-GCM path.

A separate executable compares ring 0.17.14 and AWS-LC 1.19.0
(aws-lc-sys 0.47.0), using rustls 0.23.46 QUIC packet/header key interfaces.
The application manifest, lockfile and production crypto provider are unchanged.

Seven alternating same-binary repeats run on physical CPU 4. Each observation
performs 262,144 payload-encrypt + payload-decrypt + one header-protection
cycles. Keys/storage are created before timing; packet numbers increase
within each key. Both directions of cross-provider payload/header compatibility
and tamper rejection pass at all four sizes.

| Payload bytes | ring median ns/cycle | AWS-LC median ns/cycle | AWS-LC time change |
| ---: | ---: | ---: | ---: |
| 64 | 241.916 | 289.266 | +19.57% |
| 512 | 525.184 | 570.240 | +8.58% |
| 1200 | 903.304 | 956.072 | +5.84% |
| 1452 | 1060.425 | 1107.742 | +4.46% |

AWS-LC is slower for every tested size on this host. No whole-transport
AWS-LC improvement is claimed and the production provider remains ring.
These are synthetic packet crypto observations, excluding QUIC construction,
socket I/O, scheduling and TLS setup. They do not predict results on ARM,
newer x86 CPUs, or a different provider version.

Reproduction: [standalone crypto benchmark](../native/libxhysteria/tools/crypto-bench/README.md).

### ARM remains unmeasured

The Xeon result does not select a winner on ARM. Both providers contain
separate ARM implementations; their relative cost requires measurements on the
actual CPU, with its available crypto instructions, OS and power behavior.
No ARM performance comparison has been performed in this round. Keeping the
existing ring default is not an ARM performance qualification.

The available host is x86_64 and ADB reports no connected devices. The same
standalone benchmark was successfully cross-built for Android arm64/API26
with NDK30.0.14904198, including both providers. Its local artifact is
`build_assets/crypto-scheduling-20261124/crypto-bench-android-arm64`,
SHA256 `85a454b5d3128528dcf3c2bdc7ff5fe5a707e7b7dda3165f2174403a480de00e`.
This is build validation only. The binary must run on the target device
before claiming a provider win there. An emulator run would not establish
native ARM performance.

## Queues and wakeup audit

The shared bridge already has a bounded eight-descriptor ring, independent
payload leases, coalesced eventfd notifications, and separate writer-space
notifications. Go drains up to 32 queued frames without an intentional wait;
Rust's command pump bounds a turn to 32 frames or 256 KiB. Upload relays
yield after 64 KiB and receive relays after 256 KiB. DATA/credit commands
already bypass the control queue.

The atomic pending exchanges and the full-ring waiting handshakes synchronize
the producer and consumer. Replacing an exchange with a load-and-skip is not
accepted as a safe optimization: it would require a separate lost-wakeup proof
and stress tests. No such ordering change is part of this investigation.
Increasing queue/frame bounds previously improved upload at the cost of
latency and RSS, so that rejected batching patch is not reused.

Runtime construction explicitly sets Tokio `event_interval(7)` and
`global_queue_interval(7)` on dedicated and shared client runtimes.
Tokio's local documentation explains the tradeoff: a larger event interval
reduces synchronization/syscall overhead when task polls are short, while
delaying discovery of external events under sustained runnable work.

The isolated scheduling candidate changes only **event interval 7→31**
in both constructors. Global queue interval, workers, byte budgets, queue
capacities and SeqCst publication/waiting handshakes remain unchanged.
It is measured separately from the crypto microbenchmark, using the unchanged
ring provider and identical native worker settings.

## Scheduler measurements

Both candidates use three alternating paired fresh-process observations for
each of four workloads on one and two physical cores: 48 samples per candidate.
The client, native server and TCP target share CPU 4 or CPUs 4,5; GOMAXPROCS and
native workers per role equal that budget. Pool=2, 32 flows, shared bridge,
Salamander + WireGuard. Calibration engines drain before setup; stream receivers
confirm delivery. Task builds and crypto measurements do not overlap these runs.
Unrelated host activity is uncontrolled.

Duplex exchanges 64 KiB per direction 8192 times; upload/download transfer
512 MiB per direction. Paced probes are 10,000×64 bytes alongside offered
64 MiB/s duplex background. CPU is timed whole-process µs per operation;
RSS is whole-process peak including setup and both roles. Duplex MB/s sums
directions. Nonzero-drop samples stay in medians and explicit counts.

Three repeats and host variability do not establish causality for each small
difference. These are screening experiments, not production qualification.

### I/O event interval 7→31;global queue remains7

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB | Drop samples baseline/candidate |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 1 | duplex | 180.58 → 184.31 | 683.17 → 671.55 | 66138.00 → 65490.00 | 63.39 → 63.81 | 0/0 |
| 1 | upload | 185.62 → 187.39 | 332.55 → 329.15 | — | 150.82 → 159.18 | 0/0 |
| 1 | download | 190.88 → 194.89 | 327.91 → 321.14 | — | 130.73 → 84.04 | 1/0 |
| 1 | paced | — | 239.45 → 241.12 | 4643.00 → 4503.00 | 52.77 → 54.52 | 0/0 |
| 2 | duplex | 321.87 → 316.67 | 729.09 → 741.87 | 44730.00 → 48897.00 | 68.17 → 66.32 | 0/0 |
| 2 | upload | 301.39 → 299.47 | 376.80 → 377.05 | — | 178.57 → 180.02 | 0/0 |
| 2 | download | 333.64 → 328.43 | 355.68 → 359.89 | — | 96.29 → 87.43 | 1/1 |
| 2 | paced | — | 303.98 → 296.86 | 1876.00 → 2100.00 | 52.62 → 53.24 | 0/0 |

Paced attained background medians:

- 1 core(s): 64.25 → 64.27 MiB/s.
- 2 core(s): 64.17 → 64.32 MiB/s.

### Global queue interval 7→31;I/O events remain7

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB | Drop samples baseline/candidate |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 1 | duplex | 180.47 → 181.28 | 685.21 → 682.41 | 64634.00 → 65021.00 | 63.81 → 63.66 | 0/0 |
| 1 | upload | 184.69 → 143.69 | 333.74 → 420.89 | — | 160.97 → 133.01 | 0/0 |
| 1 | download | 192.70 → 194.81 | 324.92 → 321.34 | — | 132.74 → 124.93 | 0/1 |
| 1 | paced | — | 236.41 → 234.75 | 4625.00 → 4636.00 | 52.97 → 52.66 | 0/0 |
| 2 | duplex | 318.56 → 319.14 | 737.09 → 731.62 | 46029.00 → 49064.00 | 68.38 → 66.59 | 0/0 |
| 2 | upload | 299.68 → 298.72 | 379.68 → 378.46 | — | 173.47 → 179.85 | 1/0 |
| 2 | download | 334.56 → 343.31 | 354.13 → 348.08 | — | 93.33 → 86.67 | 1/0 |
| 2 | paced | — | 295.62 → 306.67 | 2205.00 → 2039.00 | 54.64 → 51.98 | 0/0 |

Paced attained background medians:

- 1 core(s): 64.12 → 64.30 MiB/s.
- 2 core(s): 64.05 → 64.09 MiB/s.

The event-interval candidate's two-core paced p99 changes **1876→2100 µs**
(+11.94%); duplex CPU also increases. Its one-core bulk gains do not justify
changing the general runtime setting. The global-queue candidate changes
one-core upload **184.69→143.69 MB/s** and CPU **333.742→420.886 µs/64 KiB**.
Its two-core paced p99 improves **2205→2039 µs**, while CPU per probe increases
**295.624→306.671 µs**. Both candidates are rejected.

Each matrix contains two baseline and one candidate nonzero-drop samples.
All are retained. Neither matrix proves a production loss-free guarantee.
Peak-RSS differences include setup, Go heap behavior and host variation; a
single three-pair result is not attributed to a change in retained storage.

## Loaded server profiles with a separate client

Two diagnostic baseline runs use 65,536 duplex 64 KiB exchanges, a load
generator on CPUs 8–15, and server generations plus supervisor/echo on CPU 4
or CPUs 4,5. A profiler attaches only after the same two server PIDs have
remained alive for four seconds. It samples their processes and supervisor/echo
for eight seconds using `cpu-clock:u` at 499 Hz. The generator is excluded.
This reduces setup/calibration contamination; recording is independent of
the unprofiled scheduler comparisons.

The profiling build retains symbols. Its complete `.text` section matches
the stripped baseline byte-for-byte, SHA256
`94fae78c2a13ef7cf8dee735bd9b30fa3eb9817382d2ed9da32ac1cde0ca8f15`.
Perf's automatic Go pclntab handling mislabels some Rust addresses, including
AES routines, even with symbols present. These labels are not used. Instead,
each first sampled instruction address is mapped into exact function extents
from `nm -n -S -C --defined-only` for its executable. Stack-inclusive
percentages are not inferred. Initial stripped profiles remain diagnostic
artifacts, excluded from the table.

| Resolved user-CPU sample group | 1 server core | 2 server cores |
| --- | ---: | ---: |
| AES-GCM and AES header protection | 15.80% | 15.27% |
| Quinn connection protocol functions | 13.52% | 13.78% |
| BLAKE2b | 3.98% | 3.84% |
| Go scheduler selection functions | 2.55% | 1.94% |
| Go channels/select | 1.08% | 1.47% |
| Tokio scheduler functions | 1.04% | 1.73% |
| Go memmove/memclr | 1.47% | 2.23% |

The profiles contain 2588/4846 samples. The largest resolved individual
function, `_aesni_ctr32_ghash_6x`, has **10.01% / 9.14%**.
About **14.84% / 15.68%** remain unresolved, mainly libc. Therefore these
groups are incomplete; they cannot establish the total copy, scheduler or
crypto cost. They are user-CPU shares, excluding kernel CPU and time blocked.
Each profile has one observation, not a repeat-based performance estimate.
Both instrumented fixtures pass strict zero ingress/public-UDP drop gates.
Their throughput is excluded from candidate acceptance.

During the recording window, threads present in both snapshots accumulate:

| Stable-thread accounting | 1 core | 2 cores |
| --- | ---: | ---: |
| Voluntary context switches | 188760 | 409371 |
| Involuntary context switches | 61166 | 159398 |
| Stable threads | 24 | 37 |
| New threads excluded from delta totals | 1 | 2 |

These are aggregate `/proc` thread counters around an approximately
eight-second recording, covering server generations and supervisor/echo.
They do not distinguish necessary from avoidable switches, or measure
eventfd/futex call counts. Counts alone cannot prove that a particular
notification is redundant.

## Practical optimization direction

The current evidence supports retaining ring on this Xeon. It does not
justify custom AES/GHASH assembly, weaker encryption, or changing the
provider globally. Fewer underfilled encrypted packets and less packet
construction work may help, but require packet-size/packet-rate measurements;
that benefit is not established by this experiment.

For scheduling, the useful next target is work transferred per delivered
byte: queue transitions, buffer ownership, credit handling and cross-runtime
wakes. The existing bounds should remain the starting point. A worthwhile
candidate should reduce measured work/switches per MiB while keeping attained
load, paced p99 and UDP delivery comparable. Increasing Tokio intervals or
enlarging queues has not established that improvement.

## Validation and artifacts

- The standalone crypto executable builds with its locked dependencies and
  passes release Clippy with `-D warnings`.
- The final source runs all 56 timing observations and cross-provider
  payload/header checks, including corrupted payload and wrong-AAD rejection.
  This validation run is recorded separately from the initial 56 observations.
- The event-interval candidate passes 130 release engine tests, one ignored.
  Both scheduler candidates complete all 48 transport screening observations.
- The original source is restored. Linux native archive SHA256 matches
  baseline `2a1cd8dcabb972412c9521ce3e8b11927b41859225d841351610d0a9dea697d6`.
  Android archive remains unchanged at
  `5c06dfe74c4bf05528c3e27de8040eef72183d6434a7655796e974e985b3d3a9`.
- No runtime change is deployed; no new Android device, WAN, 15-minute soak,
  full-repository test or four-plus-core acceptance claim is made.

[Results and profile summaries](hysteria-crypto-scheduling-20261124/results.json)
include all 96 scheduler observations, both sets of 56 crypto observations,
per-invocation commands, manifests, binary hashes, symbol groups, and thread
snapshots. Historical zero-context candidate patches use
`git apply --unidiff-zero` in a disposable baseline checkout and are excluded
from the runtime.

Raw recordings, stripped-profile attempts, exact scripts, binaries, build logs
and failed/intermediate diagnostics remain ignored in
`build_assets/crypto-scheduling-20261124/`.
