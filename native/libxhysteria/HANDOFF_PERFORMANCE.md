# Managed Handoff Load Assessment — 2026-11-20

The new handoff code has little measured impact on ordinary Hysteria bulk
throughput, but the **managed UDP ingress is a substantial native performance
and loss regression under this load**. Functional cutover/rollback tests pass;
they do not establish zero-loss or acceptable tail latency under load.
Neither backend passes the stricter managed-ingress zero-drop load gate.

This is an assessment, not a production optimization patch. No transport,
buffer, scheduler, admission or handoff implementation was changed to make a
test pass. The new fixture and this report preserve the failed results.

## Versions and Method

- Before: Android branch `e909e2a8`, before the eight handoff commits.
- After: Android branch `9cd30b4b`, through **Pass Hysteria datagrams between
  managed Xray processes**. The corresponding node-main tip is `918c73e4`;
  the numerical matrix below measures the Android-branch server build with
  `xhysteria_native xray_server`, not a separate node-main executable.
- The same newly compiled load generator drives both server revisions. Native
  server rows use a native client with four pooled sessions; Go rows use the
  ordinary Go client/connection reuse. Thus before/after comparisons keep the
  load generator fixed within a backend. Native-versus-Go comparisons include
  their different connection topology and are not language-only comparisons.
- Every server is a separate real `xray run` process. Managed mode uses an
  additional process owning the actual `handoff.UDPIngress` and Unix-datagram
  descriptors. The helper also owns the TCP/UDP echo targets in both direct
  and managed cases. Direct mode does not create the ingress.
- Client CPUs 8–15 are eight physical cores of a Xeon E5-2698 v3. Server plus
  supervisor/echo use CPUs 0–3, four different physical cores with no SMT-sibling
  overlap; both processes have `GOMAXPROCS=4`, and the native server has four
  workers. There is no cgroup CPU quota. The host has approximately 62.6 GiB
  RAM and unrelated workloads; it is not an isolated host or 1 GiB VPS test.
- All cases use Salamander plus the WireGuard appearance header. Direct native
  mode applies supported masks in Rust. Managed mode applies them once at the
  shared Go ingress before generation routing, as required by the implementation.
  The managed native path also disables packet batching and PMTU discovery;
  it is not just a pure extra-call experiment with identical packetization.
- Thirty-two TCP flows are opened and warmed before timing. Each bulk sample
  completes 8192 echoes of 64 KiB each way: exactly 1 GiB duplex application
  payload. Each small-message sample completes 200,000 echoes of 64 B each way.
  Tables are medians of three fresh-process samples; the second repetition
  reverses configuration order. MB/s is decimal and sums both directions.
- Server CPU comes from `/proc/PID/stat` deltas, with `getconf CLK_TCK=100`.
  Supervisor/echo CPU uses its own `RUSAGE_SELF`. The combined CPU columns below
  include both processes and exclude the client. Snapshot/control overhead is
  small but not subtracted. CPU is normalized by completed application echoes.
- RSS is each process's `/proc/PID/status` `VmHWM`, including startup. The
  helper is a test executable, not production NodeX: its footprint is not an
  estimate of NodeX memory. Adding separate process high-water marks is not
  the same as measuring a simultaneous node peak. Kernel socket memory is
  excluded. Go benchmark allocation counters also exclude Rust and children.

Artifacts and exact matrix runner are under `build_assets/handoff-load-20261120/`.
The first matrix below matches the pilot's raw public UDP socket setup, with
system-default buffers (212,992 bytes on this host). Direct QUIC configures its
own socket buffers. That is not an equal-buffer comparison. The separately
labelled **configured public-socket control** below calls the existing native
server tuner before wrapping the managed public socket and confirms whether
the result survives the caller-side setup expected by the ingress API.

## Four Server CPUs: Bulk Traffic

| Server version / mode | Duplex MB/s | Echo RTT p50, ms | Echo RTT p99, ms | Server + supervisor/echo CPU, µs/echo | Ingress dropped QUIC datagrams / sample |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native before, direct | 620.32 | 5.980 | 20.151 | 660.80 | 0 |
| Native after, direct | 612.95 | 5.974 | 20.318 | 662.70 | 0 |
| Native after, managed | 264.69 | 7.165 | 129.500 | 1355.91 | 46,578 |
| Go before, direct | 253.84 | 16.434 | 20.156 | 1307.81 | 0 |
| Go after, direct | 255.51 | 16.293 | 19.518 | 1302.55 | 0 |
| Go after, managed | 255.21 | 16.204 | 21.101 | 1466.04 | 4587 |

Without managed ingress, native bulk throughput changes by −1.2%, CPU per
echo by +0.3% and p99 by +0.8%. Go direct throughput changes by +0.7%. These
small shared-host differences do not establish a material bulk regression
from merely compiling in handoff support, nor prove equivalence on every path.

Enabling managed mode reduces native delivered speed by **56.8%**, raises bulk
p99 **6.37×** and doubles combined server-side CPU per echo relative to the
new direct mode. All TCP echoes still complete: QUIC retransmission and
congestion control conceal datagram drops from the TCP application's byte
count, at significant CPU/tail cost. The drop counter is **not** an application
UDP loss percentage, and no such percentage is calculated without packet totals.

Native direct is 2.40× Go direct in this bulk topology, with about 49% less
combined server-side CPU per echo. Managed native is only 1.04× managed Go,
with roughly 7.5% less combined CPU per echo and a much higher tail. The
previous native throughput advantage is almost entirely lost at the ingress.

| New server / mode, bulk sample | Xray-only peak RSS, MiB | Supervisor/echo peak RSS, MiB |
| --- | ---: | ---: |
| Native direct | 52.26 | 29.99 |
| Native managed | 52.66 | 31.93 |
| Go direct | 45.13 | 27.97 |
| Go managed | 45.68 | 32.49 |

Raw results: `matrix-r*-*-transfer.log`; `matrix-summary.txt` includes separate
Xray and supervisor/echo CPU measurements rather than only their sum.

## Four Server CPUs: Short Messages

| Server version / mode | Echo RTT p50, µs | Echo RTT p99, µs | Server + supervisor/echo CPU, µs/echo | Ingress drops |
| --- | ---: | ---: | ---: | ---: |
| Native before, direct | 551.9 | 1415 | 56.76 | 0 |
| Native after, direct | 555.7 | 1363 | 57.10 | 0 |
| Native after, managed | 646.9 | 1358 | 67.02 | 0 |
| Go before, direct | 326.9 | 797.0 | 29.23 | 0 |
| Go after, direct | 323.7 | 795.1 | 29.02 | 0 |
| Go after, managed | 415.2 | 838.6 | 37.52 | 0 |

The managed hop increases median small-message RTT about 16% for native and
28% for Go, while native p99 is effectively unchanged in this small-payload
case. No ingress drops occur in these samples. Native still uses substantially
more CPU and has a higher small-message tail than Go. The bulk result must
not be generalized into a small-message latency advantage.

## One Physical Server CPU

The client remains on eight separate physical cores; Xray, supervisor and
echo share CPU 0, with one Go/native server worker. Three rotating repetitions
each transfer 4096 × 64 KiB each way, exactly 512 MiB duplex payload.

| Server version / mode | Duplex MB/s | Echo RTT p99, ms | Ingress dropped QUIC datagrams / sample |
| --- | ---: | ---: | ---: |
| Native before, direct | 226.75 | 52.584 | 0 |
| Native after, direct | 229.67 | 59.777 | 0 |
| Native after, managed | 113.46 | 296.874 | 66,283 |
| Go after, direct | 116.98 | 46.196 | 0 |
| Go after, managed | 109.81 | 45.305 | 17,617 |

Managed native loses 50.6% of new direct throughput and has about 4.97× its
bulk p99 on this weak-server control. Native's throughput lead over Go falls
from about 1.96× direct to 1.03× managed. New direct native p99 is higher than
before in this three-sample control; unchanged throughput alone is not evidence
that every tail is unchanged. Raw results are `cpu1-r*-*.log`.

## Application UDP During Load and Generation Overlap

`TestHandoffUnderLoad` sends 2000 sequence-checked 4 KiB UDP packets at nominal
5 ms intervals while four existing TCP flows offer 128 MiB/s duplex background.
Managed cases start B and change admission/ingress selection after sequence
599, open and verify a new TCP flow, and retain old TCP/UDP work on A. Both
generation counters must remain nonzero. The UDP sending loop pauses while
starting/probing B; the already running TCP background does not. Thus this
does not continuously probe UDP during the full launch interval or measure
zero cutover interruption. Existing process tests separately cover probe-failure
rollback and generation-specific replies.

| Case | Application UDP received | Delivered TCP background, MiB/s | Ingress dropped datagrams |
| --- | ---: | ---: | ---: |
| Native before, direct, control | 2000/2000 | 127.40 | 0 |
| Native after, direct, control | 2000/2000 | 127.94 | 0 |
| Go after, direct, control | 2000/2000 | 127.95 | 0 |
| Native managed, repeat 1 | 1987/2000 | 80.18 | 891 |
| Native managed, repeat 2 | 1982/2000 | 83.92 | 799 |
| Native managed, repeat 3 | 1965/2000 | 83.54 | 674 |
| Go managed, repeat 1 | 1999/2000 | 127.27 | 59 |
| Go managed, repeat 2 | 1999/2000 | 127.95 | 58 |
| Go managed, repeat 3 | 2000/2000 | 127.81 | 7 |

Native managed loses **66/6000 application packets (1.10%)**, while delivering
only 80–84 MiB/s background rather than the offered 128. Go managed loses
**2/6000 (0.033%)** at approximately 127–128 MiB/s. All managed repetitions
fail the strict gate, including Go repeat 3: its application UDP is complete
but its ingress still discarded QUIC datagrams. These are not zero-loss
results. Successful byte echoes and functional cutovers cannot replace this
check. Loss in this scenario is not by itself proof that the selection instant
caused every missing packet; the fixture now records missing sequences before
and after the sequence-600 boundary to help distinguish phases.

After cutover the observed live connection counts are `[4 1]` for native and
`[1 1]` for Go. Both old and new generations continue running. Native's summed
per-generation Xray high-water marks are 82–85 MiB, plus about 32–34 MiB for
the supervisor/echo helper; Go uses about 79–81 MiB plus 32 MiB. These are
overlap observations, not proof of safe retirement. The current implementation
deliberately refuses UDP `drain` completion and does not automatically retire
the old Hysteria process. That persistent overlap must be budgeted separately.

Raw logs are `load-*.log`, including the explicit failed test results.

An additional phase-diagnostic run (`load-phases-native.log`) receives
1982/2000 native replies: three missing sequences are below 600 and fifteen
are at or above 600, distributed as far out as sequence 1888. The corresponding
Go diagnostic receives all 2000 replies but still counts twenty ingress drops.
These extra diagnostics are not included in the three-run totals above and
show that observed native loss is not confined to the selector transaction.

## Configured Public-Socket Control

`NewUDPIngress` expects an already configured public socket. To avoid blaming
that API for missing caller-side buffer setup, the fixture also supports
`XHYS_HANDOFF_TUNE_PUBLIC=1`, which calls the same `TunePacketSocket(..., true,
false)` used for native server packet sockets before applying masks. It requests
4 MiB receive and 1 MiB send buffers; Linux accounts double those values.
The system limits permit those sizes. Only this public socket changes; the
bounded per-generation Unix queues, forwarding code, packet wire and process
counts stay unchanged. Kernel buffer memory is outside the RSS columns.

Three additional samples per backend use the same 32-flow, 1 GiB duplex bulk
workload and four server CPUs:

| New managed server with tuned public socket | Duplex MB/s | Bulk RTT p99, ms | Combined server-side CPU, µs/echo | Ingress drops / sample |
| --- | ---: | ---: | ---: | ---: |
| Native | 271.53 | 118.093 | 1331.52 | 82,831 |
| Go | 255.57 | 20.970 | 1459.73 | 4504 |

This improves native speed slightly compared with the unconfigured pilot
socket, but still leaves it 55.7% below new direct native throughput, with
5.81× the direct bulk p99. Native is only 1.06× managed Go. The regression and
ingress discards therefore remain after applying the expected public-socket
setup. A higher ingress-drop counter after tuning does not imply that total
end-to-end packet loss increased: the previous counter could not see packets
lost before the ingress read. No total packet-loss percentage is inferred from
these counters.

For 64-byte exchanges with this tuned public socket, native p50/p99 is
646.8/1349 µs and combined CPU is 66.93 µs/echo; Go is 420.5/860.7 µs and
37.69 µs/echo. Neither backend counts ingress drops in those small-message
benchmarks. Xray-only bulk peak RSS is 51.11 MiB native and 45.75 MiB Go;
the supervisor/echo helper is 32.06/32.04 MiB. Raw results are
`tuned-r*-*.log`; the direct references come from the earlier matrix, not an
additional alternating direct/tuned pair.

The tuned public-socket generation-overlap load checks also run three times:

| Backend | UDP replies by run | Delivered TCP background, MiB/s | Missing below / at-or-above sequence 600 | Ingress drops by run |
| --- | --- | --- | --- | --- |
| Native | 1990, 1993, 1997 out of 2000 each | 92.11, 93.38, 95.63 | 1/9, 1/6, 3/0 | 5250, 5639, 4374 |
| Go | 2000, 2000, 2000 out of 2000 each | 127.64, 127.92, 127.79 | 0/0 in every run | 68, 8, 14 |

That is **20/6000 missing native application UDP replies (0.333%)** and
**0/6000 missing Go replies** in this configured-public-socket control.
Native still delivers substantially less TCP background than offered. All six
strict tests report failure because the ingress counter rises, including Go
with no missing application replies. The public tuning helps, but neither
the strict ingress-drop condition nor native application zero-loss is met.
The raw failed logs and missing sequence lists are `tuned-load-r*-*.log`.

The configured-public-socket control was also repeated three times with the
server/supervisor/echo restricted to one physical CPU, using the same 512 MiB
duplex workload as the weak-server table:

| Tuned public socket, one CPU | Duplex MB/s | Bulk RTT p99, ms | Combined CPU, µs/echo | Ingress drops / sample |
| --- | ---: | ---: | ---: | ---: |
| Native managed | 104.70 | 332.971 | 1115.89 | 145,169 |
| Go managed | 109.28 | 48.436 | 1079.91 | 30,785 |

In this control, managed native is actually about 4.2% slower than managed Go,
uses about 3.3% more combined CPU per echo and has 6.87× its bulk p99. Against
new direct native on one CPU, its throughput is 54.4% lower. Public buffering
alone does not resolve the weak-CPU case. Logs are `tuned-cpu1-r*-*.log`.

## Where the Extra Cost Is

The managed ingress has one public receive/routing loop and one egress loop
per generation. It forwards through bounded Unix datagram socket queues;
`packet_linux.go` uses `SendmsgN(... MSG_DONTWAIT ...)` and increments `Dropped`
on failure rather than blocking another generation behind a stalled worker.
The packet path therefore has an intentional loss point under backpressure.
This is compatible with the isolation unit test, but not a zero-drop guarantee.
The aggregate counter does not distinguish every error cause, so these load
results alone do not identify the syscall error for each discarded packet.

Managed mode also moves native-supported masking to the shared Go receiver,
uses a single-datagram worker wire and sets native `PacketBatch=false` and
`DisablePMTUD=true`. Native already avoids a redundant second Go packet pump
inside the worker, but the remaining supervisor stage still limits the tested
configuration. The measurements assess the whole managed path, not separate
causal percentages for masking, packetization, sockets and scheduling.

Three component benchmark medians on the same host are:

- Established-CID routing: 24.30 ns, zero Go allocations per operation.
- Unmasked raw UDP round trip: 5.525 µs direct versus 11.209 µs via ingress,
  zero per-operation Go allocations in both cases.
- XHTTP managed request lifetime enter/exit accounting: 16.85 ns, zero Go
  allocations per operation.

These are component timings, not loaded QUIC throughput or XHTTP/gRPC traffic
benchmarks. The small CID classification cost does not establish a cheap
end-to-end managed packet path. `component-bench.log` retains all samples.

## Correctness and Race Results

- Rust engine suite: 98 passed.
- Go race run passes `handoff`, `hysteria`, `xhysteria` and `grpc` packages.
- Real independent-process tests pass for all six Go/native IPv4/IPv6 Hysteria
  combinations, gRPC and gRPC/REALITY, and XHTTP Unix/TCP and Unix/REALITY.
  They exercise established sessions, new admission, rollback, live connection
  reporting and refusal to falsely declare UDP retirement safe.
- The new direct load fixture also passes a race-instrumented test with
  2000/2000 application UDP replies. That race run is not used for performance.
- The broader XHTTP package race run fails in TLS certificate reload at
  `transport/internet/tls/config.go:101` / `:264`, in
  `Test_ListenXHAndDial_TLS` and `Test_ListenXHAndDial_QUIC`. The same tests
  reproduce the same race on the isolated **before `e909e2a8` checkout**, and
  that TLS file is unchanged by the new commits. It is a pre-existing failure,
  not a passing full race suite or an established new handoff regression.
- No production network namespace/iptables rules were changed. The optional
  isolated REDIRECT path, Android device behavior, WAN conditions, mixed
  production Nginx roll and full NodeX supervisor rollout were not measured.
  gRPC/XHTTP numerical load throughput before/after is not claimed here;
  their process correctness tests and the small XHTTP accounting benchmark
  cover different questions.

Logs: `rust-after.log`, `go-race-after.log`, `process-integration.log`,
`tls-race-{before,current}.log`, `fixture-race.log`.

## Reproduction

Build separate before/after native server executables, preserving their exact
commit identities. Build the tagged interop test driver from this revision:

```sh
cargo build --locked --release --features engine --lib \
  --manifest-path native/libxhysteria/Cargo.toml
bash native/libxhysteria/link-go.sh native/libxhysteria/target/release objcopy \
  env CGO_ENABLED=1 go test -c -tags 'xhysteria_native xhysteria_interop' \
  -o /tmp/handoff-load.test ./native/libxhysteria/interop

XHYS_HANDOFF_SERVER_BINARY=/absolute/path/to/xray-after \
  XHYS_HANDOFF_SERVER_CPUS=0-3 XHYS_HANDOFF_SERVER_PROCS=4 \
  XHYS_HANDOFF_BACKEND=native XHYS_HANDOFF_MODE=managed XHYS_HANDOFF_TUNE_PUBLIC=1 \
  XRAY_HYSTERIA_NATIVE=1 XRAY_HYSTERIA_CLIENT_WORKERS=4 XRAY_HYSTERIA_POOL_SIZE=4 \
  taskset -c 8-15 /tmp/handoff-load.test -test.run '^$' \
  -test.bench '^BenchmarkHandoffServerTransfer$' -test.benchtime 8192x \
  -test.cpu 8 -test.count 1 -test.timeout 120s
```

Use `XHYS_HANDOFF_MODE=direct` to measure the unmanaged listener, and both
`XHYS_HANDOFF_BACKEND=go` and `XRAY_HYSTERIA_NATIVE=0` for a Go/Go sample.
Unset `XHYS_HANDOFF_TUNE_PUBLIC` to reproduce the initial raw pilot socket
matrix rather than the configured-public-socket control.
For small messages choose `BenchmarkHandoffServerLatency` with `200000x`.
For a weak-server sample use server CPUs `0`, server procs `1` and `4096x`.
Use CPU sets matched to the actual physical-core/SMT topology of the machine.

To run the stricter generation-overlap test, replace the benchmark arguments
with `-test.run '^TestHandoffUnderLoad$' -test.v -test.timeout 65s`; keep
`-test.cpu 8`. A nonzero exit is expected when either application UDP replies
are missing or the ingress drop counter rises. Do not suppress those failures
or present retransmitted QUIC packet drops as an application UDP percentage.

Before production activation, the managed forwarding/packetization stage needs
additional work and zero-loss validation at the intended CPU/traffic budget.
Simply increasing a bounded queue can trade drops for latency/RSS and does
not resolve the measured single-stage bottleneck or UDP retirement problem.
