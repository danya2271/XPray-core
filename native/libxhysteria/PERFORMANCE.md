# Native Hysteria performance checks

Packet-loss sampling, the native BBR bandwidth-peak expiration fix, and the
read-only production diagnostic procedure are documented separately in
[PACKET_LOSS.md](PACKET_LOSS.md). Its saturated loopback results do not establish
packet-loss rates on deployed servers or a before/after WAN improvement.

The initial sections are historical measurements from earlier implementations.
The November 17 delivery, latency follow-up and allocation follow-up sections
describe successive implementations and their comparison boundaries. The
allocation follow-up describes the earlier direct Salamander comparison. The
November 18 direct header/Salamander chain section is the latest paired result;
its pinned before/after run is distinct from the unpinned historical adapter
tables. Do not combine different stages or benchmark setups.

Measurements were taken on November 15, 2026 on Linux amd64, an Intel Xeon
E5-2698 v3 at 2.30 GHz, with Go 1.27.1-X:nodwarf5 and Rust 1.99.0. The host
exposes 32 logical CPUs; the benchmark uses `GOMAXPROCS=4`. Rust uses the release
profile with thin LTO. These are short loopback measurements, not Android or
WAN speed guarantees.

## Reproduce

From the repository root:

```sh
XHYS_BENCH_PERF=1 bash native/libxhysteria/bench-linux.sh build_assets/native-perf
```

Omit `XHYS_BENCH_PERF=1` if Linux perf is unavailable. The script builds one
native-enabled test executable and runs each scenario/mode in a separate
process, three one-second repetitions by default. Override duration, repetitions,
or Go parallelism with `XHYS_BENCH_TIME`, `XHYS_BENCH_COUNT`, and
`XHYS_BENCH_CPUS`. Avoid concurrent builds or other load during measurement.
Raw benchmark text, Go CPU/allocation profiles, profile summaries, and optional
native-inclusive perf stacks stay in the ignored output directory. The final
local run used `build_assets/native-perf-release`.

All modes use the same Go Hysteria server, Salamander, Xray's actual outbound
registration/routing, and a local TCP echo target. `go` selects the original Go
client; `socket_perflow` and `shared_perflow` create an engine per connection;
`socket_pool2` and `shared_pool2` lazily share up to two engines. Pool size zero
reproduces the old session lifecycle, with the current common buffer fixes.

`OpenClose` measures dialing, exchanging four bytes, and closing a connection,
after warmup. The Go baseline repeatedly failed with `closed pipe` during
sustained rapid open/close in this fixture; that case is explicitly skipped,
and has no comparison number. Long-flow and parallel Go baselines complete.
`Transfer` reuses one connection for 64 KiB writes and their echoes. `Parallel`
uses four workers, each with its own long-lived connection and the same 64 KiB
exchange. Throughput counts both directions, 131,072 bytes per operation, in
decimal MB/s; it is not one-way link throughput.

Process CPU time comes from `getrusage(RUSAGE_SELF)` and includes Go, Rust,
the local Go server, and echo target. CPU milliseconds per operation describe
total work across threads, not a core utilization percentage. Go `B/op` and
allocation profiles exclude Rust allocations and the native arena. The logged
peak RSS is a process high-water mark including setup, not steady-state memory.
Go CPU profiles may contain unsymbolized native samples; Linux perf supplies
Rust symbols. The short perf run is suitable for identifying candidates, not
precise per-function cost estimates.

## Results

Arithmetic means of three repetitions, after the buffer and dispatcher fixes:

| Client mode | Transfer MB/s | Transfer CPU ms/op | Transfer Go B/op | Parallel MB/s | Parallel CPU ms/op | Parallel Go B/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Go | 157.32 | 2.360 | 46,480 | 208.64 | 2.158 | 50,225 |
| Socket, per flow | 149.57 | 2.557 | 52,279 | 280.01 | 2.579 | 62,342 |
| Socket, pool 2 | 142.90 | 2.647 | 53,249 | 258.93 | 2.389 | 55,484 |
| Shared, per flow | 153.56 | 2.500 | 31,873 | 292.47 | 2.455 | 35,501 |
| Shared, pool 2 | 152.26 | 2.513 | 31,909 | 266.09 | 2.306 | 35,158 |

| Client mode | Open/exchange/close ms/op | CPU ms/op | Go B/op |
| --- | ---: | ---: | ---: |
| Go | skipped: `closed pipe` | unavailable | unavailable |
| Socket, per flow | 2.207 | 3.478 | 200,521 |
| Socket, pool 2 | 0.609 | 0.996 | 35,188 |
| Shared, per flow | 2.634 | 3.956 | 193,674 |
| Shared, pool 2 | 0.659 | 1.021 | 26,764 |

The default shared/pool configuration reduced short-flow wall time about 3.35x
against socket/per-flow and reduced its CPU work about 71%. Compared at the
same pool size, shared memory reduced Go allocation bytes about 40% for the
long-flow case and 37% for parallel transfer. Initial shared/pool measurements
before buffer/dispatcher reuse were about 1.60 MB allocated per transfer;
the final run was about 31.9 KB. The common buffer fix benefits both client and
server paths in this local harness, including the Go baseline.

Long-flow throughput was similar to Go, with no demonstrated CPU advantage in
that scenario. Parallel shared/pool throughput exceeded Go by about 28%, but
total process CPU time per operation was about 7% higher. Relative to four
independent socket engines, the two-session shared pool used about 11% less CPU
per operation while its parallel throughput was about 5% lower. These short
runs do not establish small percentage differences as statistically significant;
they demonstrate the latency/allocation gains and the session-count tradeoff.

## Optimizations and tradeoffs

Profiling the initial shared/pool implementation attributed about 96% of Go
allocation bytes to the generic byte-pool allocator. Regular Xray buffers had
`Size=4096`, but the selected byte-pool class allocated 8192 bytes; managed
`Release` accepted only capacity 4096. Consequently those payload arrays never
returned to their pool. An exact-size pool restores reuse for both the Go and
native backends. A stale consumed-buffer test now uses `Size` instead of a
hardcoded old capacity.

The shared-memory path also transfers owned pooled buffers directly into
Xray's `ReadMultiBuffer`, batches bridge submissions, reuses the dispatch batch,
and coalesces available TCP payload. It still copies payload at the native/Go
ownership boundary. There are no traffic CGO calls and no claim of end-to-end
zero-copy.

Pooling primarily removes repeated native-thread startup, socket setup,
QUIC/TLS handshakes, and HTTP/3 authentication for short flows. It also bounds
the number of native workers. Two sessions can reduce peak throughput compared
with four independent native engines in a parallel workload. Use
`XRAY_HYSTERIA_POOL_SIZE=0` for the old per-flow behavior or `1..8` for a chosen
pool limit. Fixed negotiated Brutal rates restrict pooling to one session so
the configured bandwidth allowance is not multiplied.

One shared engine allocates 4,460,864 bytes of ring storage; two active sessions
use about 8.5 MiB. This excludes QUIC windows, flow queues, native allocations,
and Go memory. The global limit of 16 pooled outbound sessions covers in-flight
startup, but does not cover inbound listeners or explicitly selected per-flow
engines. Engine statistics expose live engines, starts, and arena bytes for
diagnostics and lifecycle tests.

Device validation remains necessary for the new pool/shared-memory build,
including speed, CPU use, memory, sleep recovery, and network handover. Earlier
native Android builds being stable does not establish those results for this
change. UDP correctness and port hopping are covered by interoperability tests;
this benchmark does not measure UDP throughput.

## Server follow-up, November 16, 2026

The harness now accepts `XHYS_BENCH_NATIVE_SERVER=1`. The same short loopback
workload was repeated three times with the corrected batched Salamander/hopping
receives, server read coalescing, and Go-equivalent QUIC receive windows. Results
in `build_assets/native-server-fix/server-0.txt` and `server-1.txt`:

| Client | Server | Transfer MB/s | Parallel MB/s |
| --- | --- | ---: | ---: |
| Go | Go | 163.29 | 220.47 |
| Shared, pool 2 | Go | 157.68 | 280.40 |
| Go | Native | 127.08 | 176.64 |
| Shared, pool 2 | Native | 117.00 | 195.68 |

The native server remains slower than Go in this fixture. These figures do not
demonstrate recovery of the reported WAN speed regression or compare the native
server before and after the fixes. No production throughput improvement is
claimed. Trying one server runtime worker reduced CPU work but also reduced
parallel throughput; the existing limit of four workers remains unchanged.
Cascade tests exercise two native servers under concurrent TCP and fragmented
UDP traffic through two port changes on both legs, using each bridge. That is
a bounded interoperability check, not a long-running VPS stability test.

## Optimized bridge, November 16, 2026

Implementation: `f1bc3bdf`. Run the isolated server harness:

```sh
XHYS_BENCH_MASKS=1 XHYS_BENCH_TIME=2s XHYS_BENCH_COUNT=3 \
  bash native/libxhysteria/bench-server.sh build_assets/native-server-f1bc3bdf
```

The following arithmetic means cover three fresh processes per combination,
alternating server order. All use Salamander, standard BBR, the shared bridge
where native is selected, and `GOMAXPROCS=4`. Continuous upload/download count
one direction and include the receiver's final acknowledgement. Transfer and
Parallel count both directions as before. Go allocation bytes and total process
CPU include both endpoints; they cannot isolate the server's own CPU or RSS.

| Client | Server | Transfer MB/s | Parallel MB/s | Upload MB/s | Download MB/s |
| --- | --- | ---: | ---: | ---: | ---: |
| Go | Go | 159.83 | 213.07 | 173.58 | 166.29 |
| Go | Native | 143.77 | 217.84 | 178.34 | 282.63 |
| Native, pool 2 | Go | 169.14 | 284.36 | 330.91 | 170.81 |
| Native, pool 2 | Native | 205.98 | 471.43 | 385.64 | 325.56 |

With the native client held constant, changing only the server improved
Parallel by 66%, download by 91%, upload by 17%, and Transfer by 22%. Process
CPU per operation decreased from 1.958 to 1.374 ms in Parallel and from 1.195
to 0.603 ms in download; Go allocation bytes decreased from 28,225 to 5,277 and
from 13,073 to 2,399 respectively. End-to-end native/native exceeded Go/Go by
2.21x in Parallel and 2.22x in upload. Those are different-client comparisons,
not proof that the server alone is twice as fast. With a Go client, native
Transfer remains slower and total CPU work is higher in several workloads.

| Client | Server | TCP idle p50/p99 us | TCP loaded p50/p99 us | UDP p50/p99 us |
| --- | --- | ---: | ---: | ---: |
| Go | Go | 128.8 / 218.4 | 455.3 / 1195.0 | 113.5 / 204.7 |
| Go | Native | 141.2 / 258.4 | 496.6 / 1101.3 | 130.8 / 240.7 |
| Native, pool 2 | Go | 144.3 / 248.4 | 389.2 / 1017.2 | 129.9 / 233.2 |
| Native, pool 2 | Native | 138.7 / 246.6 | 489.6 / 1105.3 | 136.1 / 245.0 |

The latency requirement is not met universally. Native/native improves idle
TCP latency against the same client and Go server, but UDP and loaded TCP still
regress. Loaded latency uses four unthrottled background transfers, so the
faster backend also processes more background traffic; this is a saturation
test, not a comparison at equal offered load. Long-running WAN/device tests,
equal-load latency and packet-loss scenarios remain necessary. Raw output,
archive hash, revision and source diff are in the ignored result directory.

Changes measured here include GSO with independent Salamander salts,
runtime-dispatched BLAKE2b, fewer shared-memory task hops and wakeups, grouped
credits, raw Quinn TCP handoff after header parsing, and fewer buffer copies.
Brutal stays restricted to one datagram per transmission. Payload batching uses
only already available data, with no batching timer.

## Equal-load latency follow-up

Combining the native wake-descriptor reader with command dispatch removes one
task handoff. It continues servicing notification readiness while a control
queue is full; a regression test explicitly blocks both that queue and the
event ring. `BenchmarkNativePacedLatency` offers 64 MiB/s of background traffic
on four connections, and reports actual background throughput. Three two-second
repetitions with Salamander achieved 63.94-64.00 MiB/s on average:

| Client | Server | TCP idle p50/p99 us | TCP at 64 MiB/s p50/p99 us | UDP p50/p99 us |
| --- | --- | ---: | ---: | ---: |
| Go | Go | 128.3 / 215.5 | 141.6 / 466.1 | 112.6 / 211.7 |
| Go | Native | 136.0 / 235.6 | 158.2 / 513.8 | 130.0 / 236.4 |
| Native, pool 2 | Go | 141.3 / 247.9 | 150.9 / 484.9 | 132.8 / 269.0 |
| Native, pool 2 | Native | 137.2 / 251.0 | 148.7 / 412.6 | 135.2 / 245.2 |

Raw results and archive hash: `build_assets/native-server-reactor-latency`.
With the same native client, equal-load TCP p99 improved about 15%; idle TCP
p99 remained similar. UDP median remained slightly higher, while UDP p99
improved in this run. Go-client results still regress, and these measurements
do not demonstrate universally lower latency or a universal 2x server speedup.

## November 17, 2026: initial bounded-memory milestone (historical)

The memory/capacity work adds smaller shared arenas and frame queues, byte-pool
size classes without 2x over-allocation, bounded retained QUIC send data across
engines, bounded server receive-window expansion, smaller client receive windows
on small nodes, and role-specific kernel UDP buffer targets. Admission limits
now use memory rather than CPU count. See the README for exact budgets.

Two other defects surfaced during verification. Application-limited BBR could
increase its congestion window with cumulative acknowledged traffic despite a
zero bandwidth estimate. The startup comparison and ACK excess accounting are
fixed, with a vendored regression test. Explicit bridge flow closure also used
to leave a QUIC sending half to finish gracefully. Sequential short flows then
waited approximately 0.9 seconds for stream credit around each exhausted stream
window. Explicit closure now resets the sending half; intentional half-close
still sends FIN. A 300-flow test covers both shared and socket bridges.
TCP streams are classified before HTTP/3 request admission, preventing their IDs
from accumulating in HTTP/3's request-completion bookkeeping.

The milestone backend matrix uses three fresh processes per combination, alternating
server order, with and without Salamander. Both native and Go use the current
common buffer fixes. `GOMAXPROCS=1` is combined with CPU affinity 0 so all native
and Go threads, both endpoints and the echo target share one logical
CPU. This differs from the earlier runs that limited only Go parallelism.
The capacity fixture instead constrains only the entry server to one CPU quota
and 1 GiB; its load generator and exit run outside the measured cgroup.

```sh
XHYS_BENCH_AFFINITY=0 XHYS_BENCH_CPUS=1 \
XHYS_BENCH_TIME=1s XHYS_BENCH_COUNT=3 \
XHYS_BENCH_SCENARIOS='OpenClose Transfer Parallel Upload Download Latency LoadedLatency PacedLatency UDPLatency' \
XHYS_BENCH_CLIENTS='go shared_pool2' XHYS_BENCH_MASKS='0 1' \
bash native/libxhysteria/bench-server.sh build_assets/native-go-release-20261117
```

The host is the same Xeon E5-2698 v3. Each throughput sample lasts one second.
The native client uses its normal two-session pool and shared bridge; Go uses
its existing session reuse. Transfer/Parallel count bytes in both directions;
Upload/Download count one direction, all in decimal MB/s. CPU work includes
both endpoints and Rust. Go allocation metrics exclude Rust allocations and
shared arena storage. `LoadedLatency` saturates each backend with different
achieved traffic rates; `PacedLatency` attempts a common 64 MiB/s load and must
be read together with the achieved background rate.

Go short-flow benchmarks were retried explicitly with
`XHYS_BENCH_GO_OPENCLOSE=1`; both Go and native servers produced
`io: read/write on closed pipe` with the Go client. Those cases have no valid
latency comparison and are explicitly skipped in the matrix. Native-client
short-flow measurements remain available.

### Throughput, CPU and allocations

Arithmetic means of three fresh processes per cell. CPU and Go allocation
columns describe one Transfer operation (64 KiB each way).

With Salamander:

| Client | Server | Transfer MB/s | Parallel MB/s | Upload MB/s | Download MB/s | CPU ms/op | Go B/op | Go allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Go | Go | 83.49 | 82.16 | 78.07 | 81.22 | 1.452 | 43,658 | 1,204 |
| Go | Native | 66.18 | 68.28 | 41.50 | 84.66 | 1.796 | 26,428 | 746 |
| Native | Go | 67.95 | 65.67 | 53.56 | 42.92 | 1.748 | 25,352 | 738 |
| Native | Native | 108.00 | 106.51 | 130.91 | 126.53 | 1.133 | 5,998 | 197 |

Without Salamander:

| Client | Server | Transfer MB/s | Parallel MB/s | Upload MB/s | Download MB/s | CPU ms/op | Go B/op | Go allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Go | Go | 110.21 | 110.15 | 108.80 | 116.46 | 1.052 | 31,088 | 839 |
| Go | Native | 88.95 | 86.76 | 65.86 | 112.10 | 1.327 | 20,306 | 547 |
| Native | Go | 98.70 | 99.40 | 117.44 | 71.44 | 1.195 | 18,208 | 521 |
| Native | Native | 119.49 | 120.73 | 146.30 | 140.43 | 1.019 | 5,962 | 195 |

On the Salamander workload native/native improves Transfer by about 29%,
Parallel by 30%, upload by 68% and download by 56% over Go/Go on one CPU.
Transfer CPU work falls about 22%, and Go allocation bytes fall about 86%.
Without masking, gains are smaller: roughly 8% Transfer and 34% upload.
The mixed-backend cases often regress on one CPU. In particular, a native
server with a Go client does not demonstrate a universal speed or CPU advantage.

Native-client open/exchange/close averages 1.017 ms against a native server and
1.084 ms against Go with Salamander; without it the means are 0.992 and
1.060 ms. The Go-client cases failed as described above, so these are comparisons
of servers with the same native client, not a comparison against a Go short-flow
baseline.

### Latency

All values below are microseconds. Background rates are aggregate MiB/s actually
achieved, not the configured load. Mean p99 values are means of the per-process
p99 observations.

With Salamander:

| Client | Server | Idle TCP p50/p99 | Saturated TCP p99 / background MiB/s | Paced TCP p99 / background MiB/s | UDP p50/p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| Go | Go | 94.4 / 139.9 | 7,317 / 76.8 | 4,236 / 63.7 | 92.7 / 148.8 |
| Go | Native | 157.4 / 214.4 | 7,854 / 70.6 | 6,044 / 59.6 | 154.9 / 238.8 |
| Native | Go | 156.6 / 217.1 | 10,537 / 63.4 | 8,038 / 58.1 | 148.7 / 231.9 |
| Native | Native | 171.9 / 235.7 | 5,819 / 114.4 | 1,966 / 61.8 | 156.9 / 227.5 |

Without Salamander:

| Client | Server | Idle TCP p50/p99 | Saturated TCP p99 / background MiB/s | Paced TCP p99 / background MiB/s | UDP p50/p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| Go | Go | 94.9 / 136.7 | 7,153 / 104.7 | 2,353 / 62.9 | 93.9 / 164.5 |
| Go | Native | 155.4 / 209.5 | 5,682 / 90.3 | 2,959 / 62.1 | 153.4 / 228.6 |
| Native | Go | 153.0 / 212.8 | 7,494 / 89.7 | 3,243 / 63.8 | 146.7 / 222.9 |
| Native | Native | 167.3 / 230.1 | 5,254 / 130.0 | 1,754 / 63.6 | 152.1 / 214.2 |

Go/Go wins idle TCP and UDP latency. Native/native serves more saturated traffic
and has lower observed tail latency under load. The paced comparison is only
approximately equal: some combinations fail to sustain the offered 64 MiB/s.
The loaded rows must not be used as proof of latency at identical delivered
load. These findings do not support a claim of universally improved latency.

### Capacity and memory methodology

The entry server handles 500 distinct identities, 1000 authenticated QUIC
connections and 1000 live TCP streams. Eight routes forward through an exit
server, using sixteen pooled native upstream sessions or Go's eight normal
sessions. Every stream uploads 256 KiB and receives its echo, totaling 500 MiB
duplex. Readers start after two seconds while uploads may still be pending.
All streams then exchange heartbeats for twelve seconds. The same cgroup limits,
Salamander and traffic apply to both backends.

`GOMEMLIMIT=128MiB` is a Go runtime limit; it does not include Rust allocations.
Its effect on the live Go heap and collection work must be considered when
comparing CPU time. `XHYS_CAPACITY_GOMEMLIMIT` supports a higher-limit comparison.
Only the entry server's cgroup supplies memory peak and CPU usage. A separate
RSS high-water value may differ from cgroup memory accounting. A passing
1 GiB cgroup test does not establish that Xray plus all other services fit on
the reported RU host, which previously killed Xray at approximately 283 MiB RSS
during global OOM.

Earlier development runs decreased from roughly 533 to 442 MiB while narrowing
buffer size classes; heap profiles attributed about 94% of the intermediate
Go live heap to byte-pool arrays. Subsequent smaller windows and read batches
restored backpressure. Upload/read sequencing changed at that point, so these
development numbers are not a controlled before/after capacity comparison.

Two fresh capacity samples per backend, without heap-profile sampling, with
`GOMEMLIMIT=128MiB`:

| Entry backend | Setup s | Idle RSS MiB | Peak cgroup MiB | Final RSS MiB | Entry CPU s | Traffic + heartbeats s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Go | 2.494 | 162.8 | 288.7 | 196.9 | 26.925 | 36.351 |
| Native | 1.143 | 155.4 | 333.8 | 318.2 | 10.508 | 20.651 |

All samples passed. The native process uses about 61% less CPU and completes
the traffic/heartbeat phase about 43% sooner at this Go memory limit, but its
peak cgroup memory is about 16% higher. Its post-transfer RSS is also higher.
Native Go heaps are much smaller, but that does not mean total native RSS is
smaller: native allocations and retained allocator pages remain outside the
Go heap accounting.

Single diagnostic runs at `GOMEMLIMIT=256MiB`, including heap-profile sampling:

| Entry backend | Peak cgroup MiB | Final RSS MiB | Entry CPU s | Traffic + heartbeats s | GC cycles at final sample |
| --- | ---: | ---: | ---: | ---: | ---: |
| Go | 291.8 | 269.1 | 18.188 | 32.185 | 88 |
| Native | 333.2 | 321.7 | 10.598 | 20.785 | 12 |

An additional Go 128 MiB profiling run observed 248 GC cycles at its final
sample, versus 88 with 256 MiB. Raising the Go limit reduces its collection
pressure and CPU work while retaining more Go memory after traffic. Therefore
the 61% CPU advantage from the 128 MiB fixture must not be treated as an
advantage over an optimally configured Go process. At 256 MiB the diagnostic
native run used about 42% less CPU; these are single samples, not a tuning study.

### Simulated WAN

Three fresh runs per backend, Salamander, 100 ms RTT, 16 MiB upload plus its
16 MiB echo, with the complete test inside a 1 GiB / 100%-CPU cgroup. There is
no injected packet loss or bandwidth cap. Both endpoints and the UDP delay relay
are included; this is not the capacity fixture's isolated-server accounting.

| Backends | Mean duplex MiB/s | Observed range MiB/s |
| --- | ---: | ---: |
| Go / Go | 18.83 | 17.82–20.13 |
| Native / Native | 16.74 | 15.95–18.07 |

Both passed the transfer correctness and completion checks. Native has no
demonstrated WAN speed advantage in this fixture. Its small-node receive-window
settings trade memory for bandwidth-delay capacity; real high-BDP and lossy
paths still need separate measurement.

All raw matrix, capacity and simulated-WAN results live in ignored
`build_assets/native-go-release-20261117`, alongside the native archive hash,
source diff and affinity settings. Earlier raw results in
`build_assets/native-go-comparison-20261117` predate the short-flow close fix
and did not pin Rust threads to one CPU. The separate historical native
comparison in `build_assets/native-memory-final-20261117` has that same affinity
limitation.

### Verification boundaries

The vendored Quinn suite passed 314 tests, the engine passed 39 tests, and Go
race checks passed for bytespool, buf, xhysteria and hysteria. Full interop
passed with shared/socket bridges, port hopping, capacity recovery, dynamic
users, TLS replacement and the actual NodeX-generated server configuration.
Linux amd64 and Android arm64/API 26 CLI builds succeeded.

The broader `go test ./common/...` remains unsuccessful in unrelated packages:
geodata tests lack `resources/geosite.dat` and `resources/geoip.dat`; log tests
receive no registered log output; a reflect JSON test also fails. These were
not changed to make an unrelated suite green.

No remote deployment, Android-device performance measurement, real RU route
measurement or WAN loss experiment was performed. Receive-window expansion
reservations remain held until session closure. The process budgets do not
cover every native allocation, Go allocation or kernel socket, and do not
guarantee a bound on whole-node RSS.

## November 17, 2026: final delivery comparison

This section supersedes the initial bounded-memory milestone above. The final
implementation uses 64 KiB initial server receive windows, eight-chunk QUIC
reads, shared outbound runtimes with dedicated inbound runtimes, compact
small-node outbound receive workspaces, and lazy server UDP dispatch. Header
scratch storage is released after parsing, and UDP expiry follows activity
without per-second polling. Optional allocation instrumentation is disabled
for every measurement below. No allocator or host sysctl tuning was applied.

All final raw results, the archive hash, methodology, and source snapshot are in
`build_assets/native-delivery-20261117`. `summary.json` contains arithmetic means
and individual capacity/WAN observations. These remain local ignored artifacts;
this document preserves the results for other checkouts.

### Final one-CPU matrix

The same Xeon E5-2698 v3 host and Go/Rust toolchains are used. Three fresh
two-second processes per cell alternate server order. CPU affinity 0 pins Go,
Rust, both endpoints and the echo target to one logical CPU; `GOMAXPROCS=1`.
Salamander is enabled. Native uses its normal shared bridge and two-session
client pool; Go uses its existing session reuse. Both include the current common
buffer-pool fixes. There is no concurrent build during measurement.

```sh
XHYS_BENCH_PACED_MIBPS=32 XHYS_BENCH_AFFINITY=0 \
XHYS_BENCH_TIME=2s XHYS_BENCH_COUNT=3 XHYS_BENCH_CPUS=1 \
XHYS_BENCH_SCENARIOS='Transfer Parallel Upload Download Latency LoadedLatency PacedLatency UDPLatency' \
XHYS_BENCH_CLIENTS='go shared_pool2' XHYS_BENCH_MASKS=1 \
bash native/libxhysteria/bench-server.sh build_assets/native-delivery-20261117
```

Throughput is decimal MB/s; Transfer/Parallel count both directions, while
Upload/Download count one direction. CPU and Go allocation columns describe a
Transfer operation of 64 KiB each way. CPU includes native work; Go allocation
bytes exclude Rust allocations and shared arenas.

| Client | Server | Transfer MB/s | Parallel MB/s | Upload MB/s | Download MB/s | CPU ms/op | Go B/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Go | Go | 80.83 | 79.92 | 77.25 | 79.09 | 1.501 | 43,508 |
| Go | Native | 66.36 | 65.00 | 41.68 | 89.45 | 1.784 | 26,274 |
| Native | Go | 67.48 | 67.20 | 58.02 | 42.95 | 1.759 | 25,239 |
| Native | Native | 109.76 | 109.90 | 142.54 | 137.55 | 1.095 | 5,904 |

Native/native achieves 1.36x Transfer and 1.85x upload, not a universal 2.5x.
Transfer CPU work is about 27% lower and Go allocation bytes about 86% lower.
The allocation reduction is not a measurement of total process memory. Mixed
combinations often regress, especially upload with a Go client/native server.

Latency values are microseconds. Each p99 is the mean of the three process p99
observations. Background rates are actual aggregate MiB/s.

| Client | Server | Idle TCP p50/p99 | Saturated TCP p99 / background MiB/s | Paced TCP p99 / background MiB/s | UDP p50/p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| Go | Go | 95.1 / 140.6 | 8,261 / 74.36 | 1,282 / 31.96 | 93.7 / 151.8 |
| Go | Native | 159.9 / 218.8 | 9,086 / 66.67 | 2,274 / 31.94 | 157.9 / 239.7 |
| Native | Go | 158.5 / 225.8 | 9,583 / 67.30 | 2,414 / 31.95 | 154.0 / 239.7 |
| Native | Native | 178.0 / 244.2 | 5,253 / 121.83 | 1,169 / 31.95 | 163.4 / 233.1 |

Go/Go remains better for idle TCP and UDP latency. Native/native has about 36%
lower saturated TCP p99 at its higher delivered load. With a common offered
32 MiB/s and essentially equal achieved rates, its TCP p99 is about 9% lower.
The saturated row is not an equal-delivered-load latency comparison.

### Final 500-user cascade

Two fresh samples per backend alternate Go/native, then native/Go. The entry
alone is in a 1 GiB cgroup with `CPUQuota=100%`, `GOMAXPROCS=1`, and
`GOMEMLIMIT=256MiB`; the generator and exit are outside it. This quota is not
the matrix's CPU affinity. There are 500 identities, 1000 authenticated QUIC
sessions and 1000 open TCP flows. Eight routes use sixteen native upstream
sessions or Go's normal eight sessions. All links use Salamander.

Each stream sends 256 KiB and receives its echo, totaling 500 MiB duplex.
Transfer time includes the mandatory two-second delayed reader start, but
excludes setup and the following twelve seconds of per-flow heartbeats.
Heap profiling is disabled. This is a cascade comparison with each backend's
normal pooling, not a comparison with identical upstream session counts.

```sh
XHYS_CAPACITY_BACKEND=go XHYS_CAPACITY_GOMEMLIMIT=256MiB \
XHYS_CAPACITY_SYSTEMD=1 XHYS_CAPACITY_CASCADE=1 \
build_assets/native-delivery-20261117/interop.test \
  -test.run '^TestNativeServer500Users$' -test.v -test.timeout=2m
```

Repeat with `XHYS_CAPACITY_BACKEND=native`. Build the test binary first with
`bench-server.sh`, or use the README's `go test` invocation after rebuilding the
Rust archive. All four final samples pass the 512 MiB RSS/cgroup ceiling.

| Metric | Go mean | Native mean |
| --- | ---: | ---: |
| Setup seconds | 1.476 | 1.066 |
| Transfer seconds | 25.061 | 5.830 |
| Duplex MiB/s | 20.16 | 85.76 |
| Idle entry RSS MiB | 186.6 | 142.6 |
| Buffered entry RSS MiB | 266.3 | 238.3 |
| Peak entry RSS MiB | 295.1 | 279.3 |
| Peak entry cgroup MiB | 286.0 | 261.0 |
| Final entry RSS MiB | 272.6 | 279.3 |
| Entry RSS two seconds after close MiB | 272.7 | 277.0 |
| Entry cgroup CPU seconds | 18.732 | 8.079 |
| Heartbeat p50 microseconds | 2,171.9 | 1,081.9 |
| Heartbeat p99 microseconds | 6,895.2 | 2,339.7 |

Individual observations expose the variation rather than hiding it in a mean:

| Backend/sample | Duplex MiB/s | Peak cgroup MiB | Heartbeat p99 microseconds | Final RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| Go 1 | 22.20 | 302.1 | 4,037.2 | 278.3 |
| Go 2 | 18.12 | 269.8 | 9,753.1 | 266.9 |
| Native 1 | 85.83 | 264.3 | 2,583.2 | 282.9 |
| Native 2 | 85.69 | 257.7 | 2,096.2 | 275.7 |

The mean duplex-rate ratio is 4.25x; both individual native rates exceed both
Go rates by more than 2.5x. Idle RSS is about 24% lower, peak cgroup memory about
8.7% lower, and entry CPU work about 57% lower. Mean heartbeat p99 is about 66%
lower, but the Go tail varies substantially; two runs do not establish a stable
universal p99 factor. Final RSS is about 2.5% higher for native, and allocator
pages remain retained after closing all sessions. Peak RSS and cgroup accounting
are distinct, not interchangeable memory estimates.

### Final simulated WAN

Three fresh samples per backend alternate order. The whole fixture, including
both endpoints and the UDP relay, is in a 1 GiB / 100%-CPU cgroup with
`GOMAXPROCS=1` and `GOMEMLIMIT=256MiB`. Salamander, simulated 100 ms RTT, and
16 MiB upload plus 16 MiB echo are used. No loss or bandwidth cap is injected.

```sh
XHYS_WAN_BACKEND=native systemd-run --user --quiet --scope \
  --property=MemoryMax=1G --property=CPUQuota=100% \
  env GOMAXPROCS=1 GOMEMLIMIT=256MiB \
  build_assets/native-delivery-20261117/interop.test \
  -test.run '^TestNativeServerBulkTransferWith100msRTT$' -test.v -test.timeout=1m
```

Repeat with `XHYS_WAN_BACKEND=go`.

| Backends | Mean duplex MiB/s | Individual MiB/s |
| --- | ---: | --- |
| Go / Go | 14.69 | 15.43, 13.40, 15.23 |
| Native / Native | 14.35 | 15.61, 12.76, 14.67 |

All samples pass. The ranges overlap; this does not demonstrate a native WAN
speed advantage. Historical WAN rates above belong to earlier builds and must
not be combined with these observations.

### Final validation and unresolved bottlenecks

The final engine suite passes 42 tests, also with `allocation-profile` enabled.
The unchanged BBR patch passes the 314-test vendored Quinn suite. Go race tests
pass for bytespool, buf, xhysteria and hysteria. Full interop passes, including
shared/socket bridges, dynamic users, TLS rotation, port hopping, 300 sequential
short flows, UDP activity/idle slot recovery, 500-user capacity and the actual
NodeX-generated configuration. Linux amd64 and Android arm64/API 26 CLI builds
succeed. The unrelated broad `common/...` failures described above remain.

Next targets are idle TCP/UDP bridge scheduling, allocator page retention after
traffic, receive-window expansion held by idle sessions, and throughput with
mixed Go/native endpoints or high-BDP/lossy paths. Sharing the server runtime
with outbound engines worsened saturated p99 and was rejected; mimalloc
increased peak memory and was removed. Larger frames and fewer credits also
regressed throughput or caused stalls and were not retained.

This is not a hard whole-process memory cap. Base client RX, kernel buffers,
Go forwarding and other allocations are outside the process expansion/send
budgets. Other services on the RU host caused global memory pressure; a passing
isolated cgroup test does not make that node OOM-proof. No remote deployment,
real RU-path comparison, Android-device benchmark or bypclient library
replacement is included.

## November 17, 2026: latency follow-up (latest)

The final delivery implementation above is the baseline for this follow-up.
Shared storage layout advances from 4 to 5, without changing the 1,115,456-byte
arena or engine ABI 4. Go and Rust must be rebuilt together.

Full-ring writers arm a waiting flag and recheck the ring before sleeping.
Consumers publish the freed slot before inspecting the flag and send space
notifications only when a writer is waiting. Sequentially consistent ordering
of the flag/recheck pair prevents missed wakes during concurrent draining.
The Rust publication path creates its `Notified` future before inspecting the
ring; it enters the waiter list only on actual backpressure, with
`notify_waiters` generations preserving intervening notifications.

Go uses nonblocking raw syscalls for eventfd I/O under RawConn descriptor
lifetime protection. EAGAIN still sleeps in Go netpoll rather than spinning.
Each reader retains its own buffer and callback instead of allocating a new
closure for each wait. Go and Rust credit messages reference immutable tables;
the shared decoder no longer allocates a forwarding buffer for credit events.
Payload frames still copy into owned storage before releasing ring slots.

### Paired latency measurement

Three fresh two-second processes per cell, alternating baseline/current/Go
order, with Salamander and `GOMAXPROCS=1`. All Go and Rust threads, endpoints and
echo targets are pinned to logical CPU 14. This CPU was selected after CPU 0
became heavily loaded by unrelated work. There is no concurrent build during
the measurements. Raw results and individual observations are in
`build_assets/native-latency-20261117/delivery-*.txt` and
`delivery-summary.json`.

The baseline/current columns both use native client and server, the shared
bridge and the normal two-session client pool. Go means Go at both endpoints.
All three binaries include the common byte-pool fixes. These observations must
not be treated as the earlier CPU-0 matrix or combined with those samples.
There is still background host noise; three runs do not establish significance
or a guaranteed tail-latency bound.

| Metric | Native before | Native current | Go |
| --- | ---: | ---: | ---: |
| Idle TCP p50 microseconds | 179.7 | 171.1 | 95.8 |
| Idle TCP p99 microseconds | 309.6 | 284.4 | 185.8 |
| Idle UDP p50 microseconds | 164.3 | 158.5 | 94.9 |
| Idle UDP p99 microseconds | 288.8 | 272.4 | 256.4 |
| Paced TCP p99 microseconds | 1,185.3 | 1,170.7 | 1,407.7 |
| Paced achieved background MiB/s | 31.93 | 31.84 | 31.95 |
| Saturated TCP p99 microseconds | 5,239.7 | 5,188.3 | 8,075.3 |
| Saturated achieved background MiB/s | 121.57 | 121.07 | 74.01 |
| Transfer MB/s | 108.07 | 107.94 | 80.42 |
| Transfer CPU ms/op | 1.101 | 1.057 | 1.489 |
| Transfer Go B/op | 5,898 | 5,379 | 43,506 |
| Transfer Go allocs/op | 195.0 | 165.3 | 1,203.0 |
| Idle TCP Go B/op / allocs/op | 490 / 22 | 384 / 16 | 1,223 / 38 |
| Idle UDP Go B/op / allocs/op | 913 / 39 | 809 / 33 | 2,378 / 72 |

Compared with native before, observed idle TCP p99 is 8.1% lower and UDP p99
5.7% lower. TCP p50 is 4.8% lower and UDP p50 3.5% lower. Transfer rate differs
by -0.1%, while its CPU work falls about 4%, Go allocation bytes about 9%, and
Go allocation count about 15%. Idle TCP exchanges use six fewer Go allocations.
These allocation values exclude Rust allocations and arena storage.

At essentially equal achieved load near 32 MiB/s, current native TCP p99 is
16.8% below Go; the improvement relative to native before is only 1.2%.
The saturated p99 is 35.8% below Go while delivering more traffic; that row is
not an equal-delivered-load comparison. Go still wins idle TCP and UDP latency.
There is no demonstrated 2.5x idle-latency advantage over Go.

An earlier five-process CPU-14 comparison, before the raw eventfd reader,
observed native idle TCP p99 decrease 268.9 to 259.7 microseconds and UDP p99
253.1 to 245.2. A subsequent three-process reader comparison reduced TCP p99
259.9 to 254.0 and UDP p99 245.0 to 242.9. The absolute tails vary across
measurement windows, so these stages are not added to manufacture a total gain.
The final table uses only the contemporaneous baseline/current/Go samples.

Scheduler experiments with more frequent I/O polling or a multithread scheduler
for a one-worker listener did not give a consistent latency gain. The latter
worsened saturated p99. Giving small TCP writes higher stream priority also
failed to produce a consistent gain. All three experiments were removed.
An initial raw-read callback allocated on each wait; that version was replaced
with the per-reader buffer and callback before the final comparison.

### Capacity and verification after latency changes

One fresh native capacity validation uses the same 500 identities, 1000 QUIC
sessions, 1000 TCP flows, 500 MiB duplex, two-second delayed readers, twelve
seconds of heartbeats, eight cascade routes, 1 GiB entry cgroup, 100% CPU quota
and `GOMEMLIMIT=256MiB` as above:

| Metric | Current native |
| --- | ---: |
| Idle entry RSS MiB | 139.8 |
| Duplex MiB/s / transfer seconds | 90.94 / 5.498 |
| Heartbeat p50/p99 microseconds | 988.0 / 3,597.4 |
| Peak entry cgroup MiB | 259.4 |
| Final/peak entry RSS MiB | 277.7 |
| Entry RSS two seconds after close MiB | 270.8 |
| Entry cgroup CPU seconds | 7.570 |

The validation passes; it is one native observation, not a fresh repeated Go
capacity comparison. Its heartbeat p99 is higher than the two pre-follow-up
native samples above despite lower CPU work. The load-test tail is variable,
and the idle microbench improvement does not prove a lower 500-user p99.
Allocator retention and whole-node OOM limits remain unresolved.

The Rust suite now passes 44 tests, including waiting-writer flags and
uncontended notification suppression, also with allocation profiling enabled.
Clippy passes with warnings denied. Go race checks pass for bytespool, buf,
xhysteria and hysteria. Shared tests cover full queues, wraparound, concurrent
publication, close wakeups, read deadlines and recovery, and credit bytes after
slot reuse. Full native interop and the NodeX-generated configuration pass.
Linux amd64 and Android arm64/API 26 CLI builds succeed. The vendored BBR code
is unchanged from its 314-test validation. No remote or device test is included.

## November 17, 2026: allocation follow-up

CPU and allocation profiles of the latency-follow-up binary are recorded in
`build_assets/native-max-20261117`. Native-inclusive `perf` sampling of a
five-second Salamander Transfer case attributes about 13% of user-space cycles
to the main AES-GCM assembly routine, 4% to Go `memmove`, and 4% to BLAKE2b
compression. These are sampled user-space proportions, not wall-time fractions
or a precise end-to-end attribution. Go allocation profiles show regular buffer
constructors, buffer returns and the readv callback among recurring allocations.

Two changes are retained:

- The regular 4096-byte buffer pool stores `*[4096]byte` rather than `[]byte`.
  Returning a slice through `sync.Pool` used to allocate a boxed slice header;
  returning an array pointer avoids that allocation. `New`, `StackNew`,
  `NewExisted` and `Release` keep their existing ownership rules and public API.
- Each `ReadVReader` retains one callback and its byte-count result instead of
  allocating a capturing closure/result on every vector read. Each operation
  resets its count before waiting; timeout/EOF recovery is regression-tested.

The regular-buffer reuse benchmark reports 0 B/op and 0 allocs/op over three
one-second samples. A focused allocation regression checks this property.
These shared Go changes benefit both Go and native Hysteria.

### Paired throughput and latency

Three fresh two-second processes per cell, alternating before/current/Go order,
Salamander, `GOMAXPROCS=1`, and all threads/endpoints pinned to CPU 14. The
before column is the latency-follow-up native build; current is native with the
two buffer changes. The Go column uses the same new buffer changes. No build
runs concurrently with measurement. Results are arithmetic means;
`delivery-*.txt` and `summary.json` contain raw samples and aggregation under
`build_assets/native-max-20261117`.

Rates are decimal MB/s; Transfer counts 64 KiB each way. The CPU/allocation rows
describe one Transfer operation and include both endpoints. Go allocation
metrics exclude Rust allocations and arena storage.

| Metric | Native before | Native current | Go current |
| --- | ---: | ---: | ---: |
| Transfer MB/s | 110.06 | 113.02 | 84.87 |
| Upload MB/s | 145.40 | 143.46 | 78.57 |
| Download MB/s | 141.75 | 140.17 | 82.26 |
| Transfer CPU ms/op | 1.077 | 1.055 | 1.429 |
| Transfer Go B/op | 5,436 | 4,260 | 27,187 |
| Transfer Go allocs/op | 167.3 | 115.0 | 723.0 |
| Idle TCP p50/p99 microseconds | 172.1 / 242.3 | 173.6 / 244.4 | 96.1 / 154.7 |
| Idle UDP p50/p99 microseconds | 158.7 / 228.1 | 159.9 / 228.2 | 97.5 / 166.4 |
| Paced TCP p99 microseconds | 1,130 | 1,137 | 1,233 |
| Paced achieved background MiB/s | 31.97 | 31.85 | 31.95 |
| Saturated TCP p99 microseconds | 5,029 | 5,033 | 7,521 |
| Saturated achieved background MiB/s | 124.10 | 123.67 | 77.15 |

Native Transfer allocations fall about 31%, allocation bytes 22%, and CPU work
2%; rate rises about 3% in this short series. Upload/download rates fall about
1%, and idle/tail latency does not improve. The result is an allocation
improvement, not a universal speed or latency increase. Against current Go,
native Transfer is 1.33x, upload 1.83x and download 1.70x. Go still wins idle
latency. Native paced p99 is 7.8% below Go at approximately equal delivered load;
saturated delivered rates differ and must not be treated as equal-load latency.
Three runs do not establish statistical significance or a universal factor.

Two additional prototypes were measured and removed. Combining up to four
already available TCP commands into a Quinn write improved upload/download
about 2%, but regressed request/response throughput and CPU work. Borrowing the
caller's slice for synchronous shared publication avoided a potential copy but
did not consistently improve the real routing path, which already supplies
owned MultiBuffers. Neither experiment is present in the final code.

### Capacity after common-buffer changes

One fresh sample per backend, native then Go, with both using the current
common-buffer changes. The established fixture uses 500 identities, 1000
authenticated QUIC sessions and open TCP flows, eight cascade routes, sixteen
native or eight Go upstream sessions, 500 MiB duplex, two-second delayed readers,
twelve seconds of heartbeats, and only the entry inside a 1 GiB/100%-CPU cgroup.
Both set `GOMAXPROCS=1` and `GOMEMLIMIT=256MiB`, with Salamander.

| Metric | Native | Go |
| --- | ---: | ---: |
| Duplex MiB/s | 89.96 | 24.24 |
| Idle entry RSS MiB | 139.3 | 172.6 |
| Buffered entry RSS MiB | 230.9 | 257.1 |
| Peak entry RSS MiB | 272.5 | 295.6 |
| Peak entry cgroup MiB | 254.7 | 280.6 |
| Final entry RSS MiB | 272.5 | 269.7 |
| Entry RSS two seconds after close MiB | 271.2 | 269.8 |
| Entry cgroup CPU seconds | 7.782 | 16.607 |
| Heartbeat p50/p99 microseconds | 1,105.7 / 4,182.5 | 2,227.9 / 3,923.5 |

Both pass. The observed rate ratio is 3.71x, entry cgroup peak about 9% lower
and entry CPU work 53% lower. Native final RSS is about 1% higher and heartbeat
p99 about 7% higher. This single pair is not evidence of improved capacity-tail
latency or a repeated measurement of the earlier 4.25x result. Whole-node OOM
guarantees and allocator page-retention limits remain unchanged.

Focused Go race checks and full native interop pass, including readv deadline
recovery/repeated reads/EOF, both bridges and the packaged NodeX configuration.
Linux amd64 and Android arm64/API 26 CLI builds succeed. Rust, shared ABI and
vendored Quinn are unchanged from the latency follow-up.

## November 17, 2026: flow-credit fast paths (latest)

TCP forwarding tasks retain the receive-credit semaphore returned at flow
registration. Publishing each TCP frame no longer locks the flow map, hashes
its ID, or clones/drops the semaphore `Arc`. Flow removal still closes that
semaphore, waking blocked publishers. A regression exhausts credits, closes
the flow, registers its ID again, and verifies that the retained old handle
cannot publish into the replacement flow. UDP retains its existing lookup.
Shared storage layout 5, engine ABI 4 and byte budgets are unchanged.

Go `Conn.waitWrite` checks connection errors, the current deadline and engine
shutdown before attempting an immediate credit receive. It also attempts direct
shared publication before creating a deadline timer. Blocked operations retain
the existing selectable close/deadline-change path and queued-frame accounting.
Regressions cover ready credits after shutdown/expiry and zero allocations for
ready credits and direct shared writes with a future deadline.

### Paired routing benchmarks

Raw samples, binary hashes, the implementation patch, runner and summaries are
in `build_assets/native-flow-20261117`. Before is the allocation-follow-up binary
from `ed2b49a0`; current includes only these two optimizations. Go uses the
current common-buffer implementation. Three fresh two-second processes per
cell rotate before/current/Go order, all threads/endpoints pinned to logical
CPU 14, `GOMAXPROCS=1`, Salamander and 32 MiB/s offered paced load. No build
runs concurrently with the routing matrix. Values are arithmetic means.
Transfer counts 64 KiB each way; Go allocations exclude Rust and shared arenas.

| Metric | Native before | Native current | Go current |
| --- | ---: | ---: | ---: |
| Transfer MB/s | 117.95 | 116.71 | 91.46 |
| Upload MB/s | 154.97 | 155.13 | 87.24 |
| Download MB/s | 150.30 | 151.52 | 89.20 |
| Transfer CPU ms/op | 1.006 | 0.996 | 1.330 |
| Transfer Go B/op | 4,292 | 4,285 | 27,167 |
| Transfer Go allocs/op | 116.0 | 115.7 | 723.0 |
| Idle TCP p50/p99 microseconds | 159.4 / 230.6 | 158.6 / 231.2 | 89.5 / 145.5 |
| Idle UDP p50/p99 microseconds | 147.4 / 216.9 | 148.3 / 215.6 | 88.8 / 151.2 |
| Paced TCP p99 microseconds | 1,048.0 | 1,009.7 | 1,114.7 |
| Paced achieved background MiB/s | 31.94 | 31.94 | 31.95 |
| Saturated TCP p99 microseconds | 4,720 | 4,633 | 7,297 |
| Saturated achieved background MiB/s | 133.20 | 134.33 | 85.64 |

Observed paced p99 decreases 3.7%, with all three current samples below the
before samples. Idle latency is effectively unchanged and still worse than Go.
Transfer rate decreases 1.1%, upload is unchanged, download increases 0.8%,
and Transfer CPU work decreases 1.0%. These short samples do not establish
statistical significance or a universal improvement. Native/current-Go
Transfer/upload/download ratios are 1.28x/1.78x/1.70x; paced p99 is 9.4% lower
at approximately equal delivered load. Saturated delivered loads differ.

### Capacity check

Three fresh native samples each before/current, in alternating pair order
current/before, before/current, current/before; one fresh Go sample between
the first and second pairs. Only the entry is in a 1 GiB/100%-CPU cgroup;
generator/exit remain outside. The existing 500-user fixture opens 1000 QUIC
sessions and 1000 TCP flows, eight cascade routes, sixteen native or eight Go
upstreams, transfers 500 MiB duplex with a two-second reader delay, then checks
twelve seconds of heartbeats. `GOMAXPROCS=1`, `GOMEMLIMIT=256MiB`, Salamander.
Raw samples and `capacity-summary.json` are in the same artifact directory.

| Metric | Native before mean | Native current mean | Go single sample |
| --- | ---: | ---: | ---: |
| Duplex MiB/s | 101.09 | 100.52 | 26.56 |
| Idle entry RSS MiB | 143.0 | 143.7 | 180.3 |
| Buffered entry RSS MiB | 240.3 | 237.1 | 268.8 |
| Peak entry RSS MiB | 280.1 | 282.6 | 312.8 |
| Peak entry cgroup MiB | 262.7 | 264.4 | 308.4 |
| Entry RSS two seconds after close MiB | 278.1 | 280.5 | 268.4 |
| Entry cgroup CPU seconds | 6.873 | 6.799 | 16.578 |
| Heartbeat p50/p99 microseconds | 932.5 / 1,684.9 | 894.5 / 1,744.5 | 2,059.1 / 3,891.2 |

All samples pass. Current versus before: duplex rate decreases 0.6%, cgroup
peak increases 0.6%, CPU decreases 1.1%, heartbeat p99 increases 3.5%. This
stage does not demonstrate a capacity-memory or capacity-tail improvement.
Current native mean versus the one Go sample is 3.78x throughput with 14%
lower cgroup peak and 59% lower CPU, but native retains more RSS after close.
Different upstream counts and the limited samples prevent a universal factor.

### Desktop integration

The sibling kettuRay commit `dde0370` builds and embeds native Rust Hysteria on
Linux, using published Xray commit `9bef1a20` by default. Compatible direct
Hysteria 2 profiles and their HTTP checks/probes select Xray's Rust transport;
sing-box remains the TUN frontend. Windows, insecure TLS, unsupported
ALPN/obfuscation and chained Hysteria exits retain their compatibility paths.
The real local TLS/Salamander traffic test confirms shared Rust startup without
Go fallback. Its core race tests and Linux Wails build pass.

No remote deployment, running application restart or Android-device change was
performed. `XPRAY_SOURCE_DIR` can build a local Xray checkout without modifying
its Git state; no unpublished revision is pinned as a remote build default.

## Hybrid UDP-mask adapter follow-up — November 17, 2026 (historical)

This measures Salamander plus a WireGuard appearance header, which selects the
Go PacketConn mask chain with native Rust QUIC rather than direct Rust UDP I/O.
Both endpoints use the same backend; shared flow bridges and pool size one are
selected. The harness asserts exactly two native engines to exclude fallback.
Go 1.27.1-X:nodwarf5, Rust 1.99.0, release Rust, `GOMAXPROCS=1`, the same Xeon
E5-2698 v3, three one-second repetitions, without CPU affinity. Other benchmark
and load tests were not run concurrently. Values below are medians. Short runs
and host scheduling variation limit the precision of before/after comparisons.

Transfer is a 64 KiB bidirectional TCP echo, so MB/s counts both directions.
`process-cpu-ns/op` includes Rust threads; `B/op` and `allocs/op` cover only Go
allocations and cannot establish a total-process memory advantage.

| Implementation | Transfer MB/s | Process CPU ms/op | Go bytes/op | Go allocations/op |
|---|---:|---:|---:|---:|
| Previous native adapter | 85.56 | 2.44 | 18,210 | 521 |
| Scatter/cache native adapter | 84.31 | — | 12,180 | 395 |
| Socket-tuned native adapter, before UDP credit flush | 92.63 | 2.146 | 12,084 | 391 |
| Current Go mask path | 86.50 | 1.404 | 27,262 | 724 |
| Current native adapter | 100.85 | 1.978 | 12,128 | 394 |

Current native is approximately 1.17× Go throughput here, but consumes about
41% more process CPU per exchange. The preceding socket-tuned run showed 1.14×,
so no precise single multiplier should be extrapolated. Versus the previous
native adapter the latest run shows about 18% higher throughput, 19% less CPU,
33% fewer Go allocation bytes and 24% fewer Go allocations. CPU/throughput also
improved in the unmodified Go baseline across runs, so these differences are
not entirely attributable to the final credit change. This does not demonstrate
the requested universal 2.5×.
Rust scatter/gather also removes two 64 KiB scratch buffers per packet engine;
this is a storage reduction, not a measured peak-RSS reduction. Cache hits
measure zero allocations versus two allocations for uncached address decoding.

| Implementation | Idle TCP ns/op | p50 µs | p95 µs | p99 µs |
|---|---:|---:|---:|---:|
| Current Go mask path | 91,820 | 87.7 | 111.5 | 131.3 |
| Current native adapter | 147,132 | 137.9 | 202.2 | 264.3 |

Native is still substantially slower than Go on this hybrid loopback path.
The preceding socket-tuned native run measured 169,037 ns/op and 328.7 µs p99;
the latest native result is lower, but the unmodified Go baseline also improved
and the final change only affects UDP credits. These unpinned runs do not
establish a causal TCP idle-latency improvement. IPC batching has no wait timer,
but the two-runtime scheduling and extra socket crossings remain overheads.
These measurements do not cover WAN/Android latency, loaded percentiles or
CPU-affinity-normalized results.

| Implementation | Idle UDP ns/op | p50 µs | p95 µs | p99 µs | Go bytes/op | Go allocations/op |
|---|---:|---:|---:|---:|---:|---:|
| Current Go mask path | 88,208 | 86.6 | 116.7 | 144.9 | 1,834 | 56 |
| Current native adapter | 139,530 | 130.4 | 187.2 | 253.1 | 935 | 38 |

This is the first adapter-specific UDP latency baseline; no same-harness
before/after UDP latency comparison is established. Native uses fewer Go
allocations, but neither TCP nor UDP idle latency beats Go in this run.

The adapter now uses scatter/gather on Rust, a bounded peer cache, up to eight
outgoing QUIC datagrams per IPC message, and the direct-native network socket
buffer policy. Batching is only IPC; mask `WriteTo` still sees separate packets.
It is disabled by `disableGSO`; generic-path PMTUD stays disabled at 1200 bytes.
UDP receive credits now flush when the Go per-flow queue drains, retaining
batching for bursts and leaving TCP batching/windows unchanged. This fixes a
confirmed source of UDP application drops without increasing queue capacity.
See [PACKET_LOSS.md](PACKET_LOSS.md) for saturated loss measurements and limits,
including kernel queue memory that is not included in Go allocation statistics.

Reproduce after rebuilding the release library:

```sh
native_hash=$(sha256sum native/libxhysteria/target/release/libxhysteria.a)
CGO_ENABLED=1 go test -c -tags 'xhysteria_native xhysteria_interop' \
  -ldflags="-buildid=xhysteria-${native_hash%% *}" \
  -o /tmp/xhys-packet-bench.test ./native/libxhysteria/interop
GOMAXPROCS=1 /tmp/xhys-packet-bench.test -test.run '^$' \
  -test.bench '^BenchmarkNativePacket(Transfer|Latency|UDPLatency)$' \
  -test.benchtime=1s -test.count=3 -test.benchmem
```

## Direct header/Salamander mask chains — November 18, 2026

The same Salamander + WireGuard harness now selects a combined direct Rust
mask socket instead of the Go PacketConn/Unix datagram adapter. Chains preserve
mask order, sequence fields and per-datagram salts while keeping a single bounded
send buffer. Six appearance headers and repeated Salamander layers can be mixed;
unknown masks, local hopping and excessive chains still use the adapter.

Before/after runs use `GOMAXPROCS=1 taskset -c 14`, release Rust, shared flow
bridges, pool size one, three one-second repetitions, and no concurrent load
tests. The 64 KiB bidirectional echo counts both directions. CPU includes Rust
threads; allocation metrics include only Go, not Rust/kernel allocations or RSS.
Median samples, not confidence intervals:

| Path | Transfer MB/s | Process CPU ms/op | Go bytes/op | Go allocations/op |
|---|---:|---:|---:|---:|
| Go before | 81.77 | 1.467 | 27,257 | 724 |
| Native adapter before | 40.79 | 2.966 | 13,187 | 428 |
| Go after | 79.23 | 1.519 | 27,262 | 724 |
| Native direct chain after | 95.71 | 1.116 | 4,318 | 115 |

Native throughput is about **2.35× its previous adapter** and **1.21× the paired
Go path**, not 2.5× Go. Process CPU per exchange falls about 62% from the native
adapter and is about 27% below the paired Go value. Go allocation bytes are
about 67% below the adapter / 84% below Go. These data do not prove a total-RSS
advantage, WAN throughput, or a universal multiplier.

| Path | TCP ns/op | TCP p50 µs | TCP p99 µs | UDP ns/op | UDP p50 µs | UDP p99 µs |
|---|---:|---:|---:|---:|---:|---:|
| Go before | 97,404 | 90.25 | 172.8 | 92,636 | 89.55 | 181.4 |
| Native adapter before | 222,651 | 215.9 | 334.4 | 219,868 | 212.4 | 333.4 |
| Go after | 103,619 | 97.54 | 172.2 | 97,419 | 94.18 | 175.2 |
| Native direct chain after | 193,999 | 187.3 | 294.0 | 172,739 | 166.7 | 246.2 |

Native TCP p50/p99 decrease about 13%/12%, UDP about 22%/26%. Idle native latency
is **still slower than Go**. CPU affinity places both runtimes on one CPU; do
not combine these results with earlier unpinned tables. Go timing also shifts
across runs. There is no busy-spin loop or arbitrary delay reduction. Next
latency work should measure shared-flow notification/task scheduling separately.

Validation: 71 Rust unit tests, engine/probe all-target Clippy with warnings
denied, 140 mixed Go/native mask cases, 12 local-hopping cases, and two direct
header remote-hopping cases with TCP/fragmented UDP surviving two hops. Four
native saturated loss scenarios deliver 2000/2000 replies each with zero sampled
kernel or credit drops; see `PACKET_LOSS.md`. Raw logs/binaries are retained in
ignored `build_assets/direct-masks-20261118/`.
The final race rerun also passes traffic/concurrency, Brutal, HTTP string/file/
proxy/lifecycle and pooling tests (about 107 seconds). Go-only transport tests
pass. Linux and Android arm64 masking binaries are rebuilt, not deployed.
