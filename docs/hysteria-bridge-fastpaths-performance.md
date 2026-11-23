# Bridge queue, notification and route fast paths

Date: November 24, 2026. Baseline: `ab53428e`.

## Requested changes

1. A queued `Engine.send` first attempts an immediately available writer-queue
   slot after the deadline and close checks. It allocates a deadline timer only
   when the queue is actually full and the operation must wait.
2. Reserving/filling the Go writer queue no longer notifies Rust before any
   new ring descriptor is published. Direct publication still notifies
   immediately. The writer still notifies after publication and before
   waiting on a full ring.
4. Audit item 4: route lookup work is reduced in both bridge directions. The single Go
   reader caches its last connection, checks its close channel before reuse,
   and resolves misses/control/lifecycle operations through the protected
   flow table. The Rust shared command reader groups at most eight already
   published noncontrol frames, takes the flow table once and resolves each
   consecutive run of the same flow ID once. A single-frame group retains
   the individual routing path.

There is no wait to accumulate a group. A control frame ends collection so
it keeps its place relative to DATA, CREDIT and FIN. The existing turn limits
remain 32 descriptors /256 KiB. The Rust group holds no cached sender after
the lock is released; deleting a flow still closes its receiver and credit
semaphore. The Go cache holds at most one connection per reader and cannot
reuse a closed route for a newly registered ID.

Shared storage ABI, ring capacity, payload slots, flow windows, credit batching,
worker budgets, Tokio intervals, encryption and SeqCst waiting handshakes retain
their baseline settings. The earlier shared-payload/send-budget owner fusion
is included in both before and after builds.

## Checks before measurement

Focused tests cover:

- Immediate queue sends allocate no timer; an expired deadline publishes
  nothing, and a full queue still times out without leaking a reservation.
- A reserved but unpublished queue entry neither advances the command ring
  nor arms a native notification; actual publication does both.
- A closed cached connection is not used after deletion, ID reuse or engine
  shutdown.
- Grouped Rust routing matches individual routing for DATA, CREDIT, FIN,
  unknown IDs, close, overflow, receiver closure and repeated registration.
- Existing concurrent direct/queued DATA+FIN publication and full-ring,
  close/deadline, payload ownership and HTTP tests still run.

## Measurement method

The host is an Intel Xeon E5-2698 v3 with 16 physical cores and 32 SMT threads.
The main before/after matrix pins the combined client/native-server/TCP-target
process to CPU4 or CPUs4,5; GOMAXPROCS and native workers per role equal that
budget. Pool=2,32flows,shared bridges,Salamander+WireGuard. Five paired fresh
process repetitions alternate version and core-budget order.

Duplex uses8192checked64KiB exchanges per direction (1GiB aggregate).
Upload/download each transfer512MiB and wait for all receivers. Paced latency
uses10000×64B probes with offered64MiB/s duplex background; attained load is
recorded. CPU is timed whole-process CPU per operation; peak RSS includes setup
and both native roles. Calibration engines drain before setup, socket counters
retain exact identities, and nonzero-drop samples remain in all summaries.

Four/eight-core controls use four native workers per role, pool2/4,32flows,
three paired repeats and the same four workloads. The independent managed
server comparison keeps an unchanged baseline generator on CPUs8–15 while
server generations, supervisor/ingress and echo share CPU4 or CPUs4,5.
Builds, functional checks and other task benchmarks do not overlap timed
matrices. Unrelated host activity is uncontrolled.


## Final measurements

All throughput below is decimal MB/s. Duplex is aggregate across both directions.
Each arrow compares medians; these are local measurements, not a universal speed factor.
CPU and RSS include both roles for combined-budget tests.

### Initial one/two-core comparison

Five paired repetitions for each workload:

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB | Drop samples baseline/candidate |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 1 | duplex | 181.36 → 181.16 | 680.95 → 682.42 | 61063.00 → 63012.00 | 65.49 → 63.82 | 0/0 |
| 1 | upload | 183.72 → 143.84 | 335.23 → 419.85 | — | 149.25 → 131.97 | 0/0 |
| 1 | download | 190.05 → 193.67 | 329.22 → 322.97 | — | 84.17 → 83.39 | 0/0 |
| 1 | paced | — | 236.34 → 230.19 | 4585.00 → 4441.00 | 54.53 → 53.32 | 0/0 |
| 2 | duplex | 317.52 → 318.43 | 730.62 → 730.14 | 45705.00 → 41991.00 | 68.14 → 66.48 | 0/0 |
| 2 | upload | 298.78 → 296.27 | 381.48 → 378.05 | — | 174.34 → 169.05 | 0/1 |
| 2 | download | 333.85 → 334.55 | 354.79 → 352.62 | — | 89.91 → 91.16 | 0/0 |
| 2 | paced | — | 296.75 → 294.56 | 2286.00 → 2146.00 | 54.20 → 54.39 | 0/0 |

Attained paced background medians:

- 1 core(s): 64.33 → 64.26 MiB/s.
- 2 core(s): 64.18 → 64.22 MiB/s.

### Initial four/eight-core controls

Three paired repetitions, four native workers per role and pool2/4:

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB | Drop samples baseline/candidate |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 4 | duplex | 492.35 → 490.27 | 860.92 → 863.16 | 35431.00 → 34503.00 | 70.01 → 68.47 | 0/0 |
| 4 | upload | 465.07 → 465.70 | 433.98 → 431.58 | — | 181.27 → 182.67 | 0/0 |
| 4 | download | 517.35 → 489.64 | 403.52 → 418.92 | — | 82.03 → 88.68 | 0/0 |
| 4 | paced | — | 264.47 → 259.80 | 1250.00 → 1218.00 | 54.50 → 54.95 | 0/0 |
| 8 | duplex | 726.46 → 669.98 | 1043.35 → 1135.81 | 21557.00 → 27585.00 | 70.03 → 68.98 | 0/0 |
| 8 | upload | 654.53 → 669.94 | 543.71 → 536.17 | — | 217.88 → 213.69 | 3/3 |
| 8 | download | 714.43 → 729.73 | 493.57 → 480.24 | — | 101.32 → 102.12 | 0/0 |
| 8 | paced | — | 850.66 → 892.95 | 2545.00 → 2712.00 | 56.12 → 57.21 | 0/0 |

### Predefined additional checks of the initial concerns

The initial one-core upload median was 183.72→143.84MB/s; the same baseline
binary also produced two samples near143MB/s, while the candidate produced
one near183MB/s. The apparent regression is not a simple fixed speed ratio.
Initial four-core download and eight-core duplex/paced controls also warranted
repeat checks. Five additional pairs per case use the exact same binaries and
settings, after all other task builds and tests. No original result is removed.

Additional five pairs only:

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB | Drop samples baseline/candidate |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 1 | upload | 183.17 → 183.80 | 336.64 → 335.10 | — | 161.18 → 149.79 | 0/0 |
| 4 | download | 477.83 → 484.25 | 424.60 → 417.31 | — | 91.13 → 89.09 | 1/0 |
| 8 | duplex | 723.94 → 724.37 | 1048.10 → 1039.10 | 21660.00 → 20176.00 | 70.73 → 70.33 | 0/0 |
| 8 | paced | — | 945.16 → 919.65 | 2923.00 → 2853.00 | 57.95 → 57.78 | 0/0 |

All original plus additional pairs: ten for one-core upload, eight for each control:

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB | Drop samples baseline/candidate |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 1 | upload | 183.44 → 144.23 | 335.93 → 418.87 | — | 150.53 → 133.10 | 0/0 |
| 4 | download | 481.75 → 485.09 | 422.48 → 418.12 | — | 88.80 → 89.08 | 1/0 |
| 8 | duplex | 725.20 → 674.88 | 1045.72 → 1127.43 | 21608.50 → 26701.00 | 70.27 → 69.65 | 0/0 |
| 8 | paced | — | 908.82 → 906.80 | 2688.50 → 2721.00 | 57.80 → 57.50 | 0/0 |

### Component isolation

Six additional fresh-process repetitions compare baseline,Go-only,Rust-only
and the full candidate on one-core upload and eight-core duplex. Variant order
rotates and reverses; all diagnostic builds finish before the timed runs.
Go-only contains requested items1/2 and the Go last-flow cache; Rust-only has
the grouped native route with baseline Go. Both production sources are restored
byte-for-byte before timing. These are diagnostic builds, not extra runtime
configuration flags. Exact variant hashes and every observation are retained.

| Cores | Workload | Variant | N | Median MB/s | Min–max MB/s | CPU µs/op | p99 µs |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | upload | before | 6 | 141.70 | 134.54–185.52 | 426.02 | — |
| 1 | upload | go | 6 | 145.47 | 140.04–185.83 | 415.43 | — |
| 1 | upload | rust | 6 | 162.72 | 142.78–184.53 | 379.40 | — |
| 1 | upload | after | 6 | 184.51 | 142.37–184.99 | 333.85 | — |
| 8 | duplex | before | 6 | 738.09 | 701.16–747.17 | 1036.58 | 20055.00 |
| 8 | duplex | go | 6 | 740.41 | 727.99–748.17 | 1022.98 | 18210.00 |
| 8 | duplex | rust | 6 | 734.99 | 731.43–742.26 | 1034.86 | 19556.50 |
| 8 | duplex | after | 6 | 740.59 | 713.30–748.51 | 1028.86 | 17889.50 |

All observations with the exact baseline/full-candidate binary pair, including
the six diagnostic repetitions above:16pairs for one-core upload,14for
eight-core duplex. Other diagnostic variants are not merged into this table.
Initial,repeat and diagnostic phases stay separate in the JSON.

| Cores | Workload | MB/s baseline → candidate | CPU µs/op | p99 µs | Peak RSS MiB | Drop samples baseline/candidate |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 1 | upload | 183.03 → 163.99 | 336.87 → 376.94 | — | 149.45 → 142.00 | 0/0 |
| 8 | duplex | 728.62 → 727.75 | 1042.89 → 1042.56 | 21290.00 → 19956.00 | 70.49 → 68.73 | 0/0 |

One-core upload observations below160MB/s (descriptive mode count; no exclusions):

- before: 7/16 observations.
- after: 8/16 observations.

Arithmetic mean over all16one-core upload observations per revision: 165.34 → 163.57MB/s (-1.07%).

### Managed server, separate unchanged baseline generator

Three paired repetitions. Server generations, supervisor/ingress and TCP echo
share CPU4 or CPUs4,5. The unchanged generator runs on CPUs8–15, GOMAXPROCS8,
four native workers, pool4,32flows across two live generations. Transfer uses
32768duplex64KiB exchanges; latency200000×64B. CPU sums server generations
and supervisor/echo, excluding the client. RSS includes server generations.
All24samples pass strict zero ingress/publicUDP-drop gates.

| Server cores | Workload | MB/s baseline → candidate | Total server CPU µs/op | p99 µs | Server peak RSS MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | transfer | 250.12 → 250.84 | 480.71 → 479.43 | 36787.00 → 38064.00 | 103.70 → 103.80 |
| 1 | latency | — | 40.73 → 40.39 | 2486.00 → 2476.00 | 92.45 → 92.41 |
| 2 | transfer | 451.10 → 457.93 | 521.01 → 513.41 | 19457.00 → 18119.00 | 108.30 → 106.90 |
| 2 | latency | — | 63.27 → 63.30 | 3544.00 → 3526.00 | 92.83 → 95.39 |

### One-core server capacity: 500 users,1000 QUIC sessions and TCP flows

Three alternating pairs. Server CPU4/GOMAXPROCS1,GOMEMLIMIT128MiB; fixture and
generator on CPUs4–7. All six runs pass the512MiB server peak-RSS guard.
Server CPU is process-lifetime CPU, not only the bulk interval.

| Metric | Baseline median | Candidate median |
| --- | ---: | ---: |
| server_idle_rss_mib | 136.80 | 137.10 |
| server_buffered_rss_mib | 213.70 | 212.40 |
| server_peak_rss_mib | 222.20 | 221.90 |
| server_after_close_rss_mib | 218.10 | 217.60 |
| duplex_transfer_mib_per_second | 164.33 | 166.99 |
| heartbeat_p99_us | 3683.60 | 3652.40 |
| server_process_cpu_seconds | 4.23 | 4.18 |

## Microbenchmarks

Seven alternating same-binary Rust reference/grouped pairs pinned toCPU4,
262144groups per size,normal allocator,fixed eight-slot array,static payload.
Reference routes each frame individually; candidate uses `route_batch`.
Medians include frame construction, routing and receiver drain, and exclude
the shared ring parser/collection loop and network.

| Already-ready frames/group | Individual ns/frame | Grouped ns/frame | Reduction |
| ---: | ---: | ---: | ---: |
| 1 | 124.121 | 112.390 | 9.45% |
| 2 | 101.316 | 90.033 | 11.14% |
| 4 | 98.672 | 79.766 | 19.16% |
| 8 | 97.515 | 75.630 | 22.44% |

The group1 path calls the original route method, so its difference is not
evidence of lock amortization. It includes dispatch/codegen/timing effects.
The2–8frame numbers establish the bounded-group micro benefit, not a matching
percentage improvement for transport.

Current Go harness,CPU4,1000000iterations,seven repetitions per case:

| Case | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| BenchmarkEngineQueuedSend/no_deadline | 149.400 | 0 | 0 |
| BenchmarkEngineQueuedSend/future_deadline | 223.300 | 0 | 0 |
| BenchmarkFlowReadLookup/table | 16.770 | 0 | 0 |
| BenchmarkFlowReadLookup/last_flow | 9.033 | 0 | 0 |

Table versus last-flow lookup is a same-executable comparison. Queued-send
timings describe only the candidate: no old timer-policy timing reference was
measured. The available-queue allocation test directly verifies zero allocations
with a future deadline, and full-queue deadline/error checks preserve cleanup.

## Validation

- 134release and134debug Rust engine/library tests pass;3manual benchmarks ignored.
- Release engine-library Clippy passes with `-D warnings`.
- Fresh native Go race tests pass for xhysteria,hysteria,handoff,supervisor and
  proxy packages.
- One/two-core masks,concurrency,HTTP file/proxy/lifecycle,short-flow credit,
  capacity recovery and100msRTT bulk checks pass without skips.
- Masked third-party clients pass192concurrent streams,1200close/reopen and
  advertised finite-window checks,shared/socket bridges,on both budgets.
- Strict direct/cascade UDP256/4096B under load passes8000/8000per budget,
  zero sampled kernel drops and duplicates.
- Managed saturated UDP passes6000/6000per budget,zero ingress/publicUDPdrops;
  retained-connections and saturated-drain checks also pass.
- All six1000-session capacity runs pass the512MiB server RSS guard.
- Androidarm64/API26 builds with NDK30.0.14904198;device performance unmeasured.

No WAN/device/new15minute-soak/full-repository-green/deployment claim.
An eight-core upload benchmark with kernel drops is not loss-free qualification.

All192main/recheck and48diagnostic transport observations,sixcapacity runs,
56Rust and28Go micro observations,
exact bulk commands,reproduction scripts,source/binary hashes and validation:
[results.json](hysteria-bridge-fastpaths-20261124/results.json).
Raw logs and large binaries stay ignored in `build_assets/bridge-fastpaths-20261124/`.

## Acceptance and limits

Accepted for the requested reduction in redundant timer allocation, premature
notification and repeated route locking/lookups. Allocation/lifetime tests pass;
the same-binary lookup and grouped-route micros establish the local CPU benefit.
Neither the end-to-end speed factor nor a material RSS reduction is established.

One-core upload remains an explicit concern. The initial5pairs give
183.72→143.84MB/s; the additional5give183.17→183.80, and the component-isolation
series gives141.70→184.51. Both individual components reproduce the two modes.
All16exact-binary pairs give median183.03→163.99MB/s and mean165.34→163.57;
7/16baseline and8/16candidate observations are below160MB/s. The cause of the
mode change is unresolved. These data do not establish an intrinsic21.7%
slowdown, and they do not qualify this patch as an upload improvement.

After all14same-binary pairs,eight-core duplex is728.62→727.75MB/s with
p9921290→19956µs and essentially unchanged CPU; the initial large regression
does not persist across the full evidence. Eight-core paced p99 over eight
pairs is2688.5→2721µs, so latency is not universally lower.
Managed two-core throughput451.10→457.93MB/s,CPU521.012→513.409µs/op and
p9919457→18119µs improve in the three-pair server-only comparison. Managed
one-core bulk p9936787→38064µs worsens while throughput is essentially flat.
The six1000-session capacity runs have peak RSS222.2→221.9MiB; the0.3MiB
difference is not a demonstrated intrinsic memory reduction.

All nonzero-drop observations remain in summaries, including one candidate
two-core upload,one baseline four-core download recheck,and all three
eight-core upload samples per revision. Strict UDP/drain acceptance is a
separate set of passing checks. Existing memory/window/queue bounds and wakeup
handshakes retain baseline settings.
