# Native Hysteria performance checks

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
