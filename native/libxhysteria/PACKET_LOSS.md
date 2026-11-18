# Hysteria packet-loss diagnosis

## Payload-lease regression — November 18, 2026

With payload leases, release Rust, direct Salamander + WireGuard masking and
shared bridges, a twelve-second-per-case saturated run produces:

| Cascade | Payload bytes | Sent | Received | Application loss | Sampled kernel UDP drops |
|---|---:|---:|---:|---:|---:|
| No | 256 | 2000 | 2000 | 0% | 0 |
| No | 4096 | 2000 | 1999 | 0.05% | 0 |
| Yes | 256 | 2000 | 1995 | 0.25% | 0 |
| Yes | 4096 | 2000 | 2000 | 0% | 0 |

CPU placement is unrestricted in this run, unlike the previous CPU-14-pinned
check. Background throughput is about 276–369 MiB/s. Missing application replies
are not attributed yet. The harness fails only when no application packets
arrive, so its PASS does **not** establish zero loss. There is no matched Go
loss measurement in this run, and no production/WAN sampling. Raw log:
`build_assets/payload-leases-20261118/xhys-payload-loss.log`.

## Direct mask regression — November 18, 2026

Salamander + WireGuard now uses direct Rust UDP masking instead of the packet
adapter. With release Rust, `GOMAXPROCS=1 taskset -c 14`, shared flow bridges and
pool size one, saturated TCP background plus UDP replies gives:

| Native path | UDP payload | Delivered | Missing | Background MiB/s | Sampled kernel UDP drops |
|---|---:|---:|---:|---:|---:|
| One Hysteria leg | 256 B | 2000/2000 | 0 | 123.82 | 0 |
| One Hysteria leg | 4096 B | 2000/2000 | 0 | 119.71 | 0 |
| Two Hysteria legs | 256 B | 2000/2000 | 0 | 55.58 | 0 |
| Two Hysteria legs | 4096 B | 2000/2000 | 0 | 55.32 | 0 |

Sampled `credit_dropped_delta` and QUIC UDP receive/send/MTU-drop counters remain
zero. This is local regression coverage, **not evidence of zero loss on VPS
paths**. The process/socket sampler has the exclusions documented below; QUIC
loss detection and application reply loss are distinct metrics. Raw diagnostics
are in ignored `build_assets/direct-masks-20261118/`.

Run with `XRAY_NET_DIAG=1 XHYS_LOSS_PACKET_MASK=header GOMAXPROCS=1 taskset -c 14`
and `-test.run '^TestHysteriaPacketLossUnderLoad/native/'` on a rebuilt interop
test binary. Engine startup now labels direct versus Go FinalMask packet I/O,
making the selected network path inspectable without mistaking the flow bridge
(`shared`/`socket`) for the packet adapter.

## What was measured on 2026-11-17

The running desktop Xray was sampled without restarting the VPN or changing its
configuration. Over 60 seconds its UDP sockets reported zero additional kernel
drops; the network namespace also reported zero `RcvbufErrors`, `SndbufErrors`,
and checksum errors. This is not a measurement of the VPS servers. That process
did not have `XRAY_NET_DIAG` enabled, so its QUIC loss counts were unavailable.

The 60-second namespace sample overlaps the isolated test, unlike the
per-Xray socket counters. A separate 30-second sample after all load tests
observed 2,190 received and 2,415 sent namespace UDP datagrams, zero kernel or
Xray-socket drops, and zero physical-interface errors/drops. Desktop Xray used
about 0.67% of one CPU core and 55.7–55.9 MiB RSS during that light workload.

An isolated loopback test used Salamander, one CPU (14), `GOMAXPROCS=1`, four
saturated TCP echo flows, and 2,000 sequence-numbered UDP messages at a nominal
200 messages/second, followed by two seconds to drain replies. Native selection
is checked after the first exchange so a Go fallback cannot pass unnoticed.

| Backend | Hysteria legs | UDP payload | Sent / received | Missing replies | TCP background MiB/s |
|---|---:|---:|---:|---:|---:|
| Native | 1 | 256 B | 2000 / 2000 | 0% | 137.46 |
| Native | 1 | 4096 B | 2000 / 2000 | 0% | 140.81 |
| Native | 2 | 256 B | 2000 / 2000 | 0% | 67.09 |
| Native | 2 | 4096 B | 2000 / 2000 | 0% | 65.69 |
| Go | 1 | 256 B | 2000 / 1977 | 1.15% | 84.88 |
| Go | 1 | 4096 B | 2000 / 1933 | 3.35% | 82.12 |
| Go | 2 | 256 B | 2000 / 1984 | 0.80% | 43.83 |
| Go | 2 | 4096 B | 2000 / 1950 | 2.50% | 41.18 |

Twelve native ten-second connection samples contained 4,288,480 sent QUIC
packets and zero declared losses, receive/send DATAGRAM queue drops, MTU queue
drops, or black-hole detections. These are samples, not lifetime totals. The Go
application loss was observed; it was not localized to a particular queue.
This deliberately saturated, single-core loopback workload is neither a WAN
benchmark nor proof that production paths are loss-free. Each row is one run.
With the TCP background disabled, both native and Go delivered 2000/2000 replies
for both payload sizes through the two-leg cascade (four additional runs).
Raw local artifacts are in ignored `build_assets/packet-loss-20261117/`.

## Hybrid UDP-mask adapter follow-up

The same load harness also covers the generic Go-mask/native-QUIC path using
`XHYS_LOSS_PACKET_MASK=header` (Salamander plus WireGuard appearance header).
These runs use `GOMAXPROCS=1` without CPU affinity, four saturated TCP echoes,
2,000 UDP exchanges at nominal 200/s and a two-second drain. Do not mix them with
the direct Rust results above. Each matrix row is one local loopback sample.

Scatter/gather and IPC batching alone did not remove missing UDP replies:
native initially missed 0.90–1.60%. Inspection found that the hybrid path skipped
the buffer setup used by direct Rust sockets: mask-wrapped base UDP sockets
retained the OS defaults (212,992 actual bytes on this host). Base sockets now
receive the same bounded buffer requests as direct Rust: server RX/TX 4 MiB/1
MiB; client RX/TX 1 MiB/256 KiB. The callback runs before masks and on replacement
local-hopping sockets. No global sysctl is changed. This host allowed actual
RX buffers of 8 MiB/2 MiB, and TX buffers of 2 MiB/512 KiB. Other hosts can clamp
requests, so diagnostics report both requested and actual sizes.

| Backend | Hysteria legs | UDP payload | Sent / received | Missing replies | TCP MiB/s | Kernel UDP drops |
|---|---:|---:|---:|---:|---:|---:|
| Socket-tuned native, before credit fix | 1 | 256 B | 2000 / 2000 | 0% | 94.37 | 0 |
| Socket-tuned native, before credit fix | 1 | 4096 B | 2000 / 2000 | 0% | 90.48 | 0 |
| Socket-tuned native, before credit fix | 2 | 256 B | 2000 / 1993 | 0.35% | 46.74 | 0 |
| Socket-tuned native, before credit fix | 2 | 4096 B | 2000 / 2000 | 0% | 44.68 | 0 |
| Current Go | 1 | 256 B | 2000 / 1984 | 0.80% | 74.25 | 17,236 |
| Current Go | 1 | 4096 B | 2000 / 1942 | 2.90% | 71.77 | 17,699 |
| Current Go | 2 | 256 B | 2000 / 1981 | 0.95% | 38.87 | 3,464 |
| Current Go | 2 | 4096 B | 2000 / 1949 | 2.55% | 35.45 | 4,837 |

The native two-leg/256-byte case was repeated twice: 1992/2000 (0.40%) and
1997/2000 (0.15%), still zero kernel drops. A later run with additional
flow-credit diagnostics delivered 2000/2000 at 47.24 MiB/s, with zero sampled
QUIC losses, DATAGRAM queue drops, kernel drops and flow-credit drops. These
variations do not establish a zero-loss guarantee. The later investigation below
localizes drops in a separate repeated run, not retrospectively in every earlier
sample. Kernel drops in the Go rows establish socket-queue losses
under that workload, not that every missing application reply has that cause.

### Confirmed UDP flow-credit bottleneck

A fresh socket-tuned matrix with credit diagnostics missed 12/2000 one-leg
256-byte replies and 1/2000 two-leg replies. The respective server engines
reported exactly 12 and one exhausted receive-credit drops, while observed
kernel drops and sampled QUIC/DATAGRAM drops remained zero. This establishes
an application bridge source of drops in those runs, not a network-path loss.

Go batched receive-credit returns just like TCP, retaining up to three credits
on clients and one on servers after sparse UDP traffic. Under a scheduler burst,
Rust could run out of credits and discard a datagram even though Go had already
drained its queue. Go now flushes remaining credits when a UDP queue drains;
queued bursts still batch them, and TCP/window capacities are unchanged. This
does not add queues, a timer, or retries that would hide unread-flow behavior.

| Backend after credit fix | Hysteria legs | UDP payload | Sent / received | Missing replies | TCP MiB/s | Kernel UDP drops |
|---|---:|---:|---:|---:|---:|---:|
| Native | 1 | 256 B | 2000 / 2000 | 0% | 101.21 | 0 |
| Native | 1 | 4096 B | 2000 / 2000 | 0% | 101.18 | 0 |
| Native | 2 | 256 B | 2000 / 2000 | 0% | 49.93 | 0 |
| Native | 2 | 4096 B | 2000 / 2000 | 0% | 49.51 | 0 |

Two additional two-leg/256-byte runs also delivered 2000/2000, at 50.26 and
50.46 MiB/s. All six runs show zero kernel drop deltas and sampled credit drops.
This is a local saturated-workload regression check, not proof of zero loss on
remote servers. The unmodified Go rows above were not artificially buffer-tuned.

`kernel_udp_dropped` sums `/proc/self/net/udp{,6}` drop deltas only for sockets
owned by the test process and present at both observations. The test includes
echo sockets, not just QUIC sockets. Sockets closed/replaced between observations
and other kernel/path queues are not accounted for. Ten-second QUIC samples
are not final connection totals, and no remote VPS was inspected by this test.

`event=udp_bridge role=client/server credit_dropped_total=...` now distinguishes
Rust→Go application payloads discarded when a UDP flow has no receive credit.
This is an engine-level counter separate from per-connection QUIC DATAGRAM
queues, `lost_packets`, and packet-adapter send-error counters. An unread flow
still drops UDP rather than blocking unrelated flows; buffers were not enlarged
to hide that behavior. Packet-adapter `tx_mtu_dropped` counts reported
`EMSGSIZE`, `tx_buffer_dropped` counts `ENOBUFS`/`ENOMEM`, and `tx_mask_dropped`
counts short/suppressed writes. Silent fragmentation/path loss remains invisible
to those send-error counters. Generic-path inner MTU is still 1200 and PMTUD off.

Network socket queue allowances increase under load; kernel queue memory is not
Go heap memory or a whole-process/node cap. Each mask and each client socket can
retain additional memory. This fix does not make a 1 GiB VPS OOM-proof. A memory-
pressure deployment requires measuring cgroup socket usage and the other node
services along with native budgets and RSS.

```sh
XRAY_NET_DIAG=1 XHYS_LOSS_PACKET_MASK=header GOMAXPROCS=1 \
  /tmp/xhys-packet-bench.test -test.run '^TestHysteriaPacketLossUnderLoad$' \
  -test.v -test.timeout=120s
```

Build the test executable with the command in [PERFORMANCE.md](PERFORMANCE.md).
Raw logs and matching binaries are under `build_assets/hybrid-packet-20261117/`.

## Confirmed native BBR bug

The bandwidth estimator updated its ten-round maximum filter only for higher,
non-application-limited samples. Lower samples never advanced the filter, so
a previously observed bandwidth peak could persist indefinitely after a link
slowed down. The regression test demonstrated 1,200,000 B/s retained after
30 rounds at 120,000 B/s. The fix passes all non-application-limited samples
to the filter, and permits higher application-limited samples without expiring
the peak on lower application-limited samples. Both cases have regression tests.

This can affect congestion-window/rate decisions after a path slows down. It
does not establish that this bug caused the losses on every VPS. Stock Quinn
window-derived pacing, ACK aggregation, and real WAN scheduling remain separate
investigation targets; the loopback traces can still show large model windows.
The fix does not change the FFI or shared-memory ABI and is not deployed to a
running process simply by rebuilding this checkout.

## Read-only sampling on a server

Use the PID of the intended Xray service, not an arbitrary matching process:

```sh
pid=$(systemctl show -p MainPID --value xray.service)
sudo python3 native/libxhysteria/diagnose-packet-loss.py --pid "$pid" --interval 10 --samples 6 > packet-loss-kernel.jsonl
sudo journalctl -u xray.service --since '2 minutes ago' -o short-iso > packet-loss-xray.log
sudo journalctl -k --since '2 minutes ago' -o short-iso > packet-loss-kernel.log
```

The sampler reads `/proc` only and does not modify sysctls, sockets, firewall
rules, or the service. It records per-Xray socket drop totals/deltas, receive
queue bytes, RSS, CPU usage (100% means one core), and network-namespace UDP
and interface deltas. Namespace counters include other processes. A newly
observed socket has `dropped_delta: null`, rather than pretending its lifetime
drops occurred within the sampling window. Socket address fields are hexadecimal
as stored in `/proc`; the script never reads the Xray configuration or secrets.
Run it inside the relevant container when host `/proc` permissions prevent
inspection of that process. A socket closed between samples cannot be read at
the end of the window; use frequent sampling for short-lived/hopping sockets.

For QUIC counters, the rebuilt binary must start with `XRAY_NET_DIAG=1`. Setting
an environment variable in another shell does not enable diagnostics in a
running process. Arrange any diagnostic restart separately; this investigation
does not restart or replace the production service.

### Counter interpretation

- `lost_packets` is a cumulative count of locally **sent QUIC packets declared
  lost**, not an end-user UDP loss percentage and not a measure of received
  packet loss. Reordering or delayed ACKs can produce declared/spurious loss.
- Native `sent_packets` counts QUIC packets; `tx_packets`/`rx_packets` count UDP
  datagrams, which are not interchangeable when QUIC packets are coalesced.
- `loss_total_pct` is `100 * lost_packets / sent_packets`. Go uses its QUIC
  `PacketsSent`/`PacketsLost` counters for the same calculation.
- `sent_delta`, `lost_delta`, and `sample_ms` separate current activity from
  history. `loss_window_pct` divides these deltas, not matched packet cohorts:
  late declarations can exceed 100%. It is `n/a` when no packets were sent.
- `udp_rx_dropped`, `udp_tx_dropped`, and `udp_mtu_dropped` count local native
  QUIC DATAGRAM queue evictions, separately from QUIC loss detection. They do
  not cover every later Xray application queue or incomplete fragment assembly.
- `mtu_probe_lost` counts failed PMTU probes, which Quinn excludes from normal
  `lost_packets`; `black_holes` and `mtu` help diagnose a path-MTU problem.
- A process UDP socket's growing `dropped_delta` or namespace `RcvbufErrors`
  indicates local kernel receive pressure. Diagnose CPU starvation, socket
  queues and forwarding before blaming the remote network or increasing RAM.

In the supplied VPS excerpt, `94.77.36.169:4481` gained 446 declared losses while
sending about 19.8 MB over ten seconds. `90.151.236.217:36576` gained one loss;
its large lifetime count mostly predates the sample. Without sent-packet counts
neither establishes a loss percentage. The two `global_oom` events separately
confirm that the kernel killed Xray; the shown connection/flow admission limits
were not full. Kernel UDP drops and a current per-leg QUIC sample are still
needed to distinguish receive pressure, congestion, path loss and retransmits.

Do not solve this by globally enlarging every buffer on the 1 GiB VPS. First
compare both ends of each Hysteria leg in the same time window. For Brutal,
verify that configured rates do not exceed the usable link rate and account
for aggregate concurrent sessions; loss compensation can send above the target.
For BBR, deploy the estimator fix and remeasure rather than assuming zero loss.

Protocol references: [RFC 9002 loss detection](https://www.rfc-editor.org/rfc/rfc9002.html#section-6.1)
and [Hysteria bandwidth/loss compensation](https://v2.hysteria.network/docs/advanced/Full-Client-Config/).

## Reproduce locally

Build the native archive before linking Go; Go does not track external archive
changes in its build cache:

```sh
bash native/libxhysteria/build-linux.sh build_assets/xray-native
archive_hash=$(sha256sum native/libxhysteria/target/release/libxhysteria.a | cut -d ' ' -f 1)
CGO_ENABLED=1 go test -c -tags 'xhysteria_native xhysteria_interop' -ldflags="-buildid=xhysteria-$archive_hash" -o build_assets/xhysteria-loss.test ./native/libxhysteria/interop
XRAY_NET_DIAG=1 GOMAXPROCS=1 taskset -c 14 build_assets/xhysteria-loss.test -test.run='^TestHysteriaPacketLossUnderLoad$' -test.v -test.timeout=180s
```

Choose an allowed CPU instead of 14 on another host. `XHYS_LOSS_LOAD=0` removes
the TCP background for an idle baseline. `-short` skips the long loss test.

Validation: 181 vendored Quinn unit tests, 45 native engine tests, all-target
Clippy with warnings denied, Go race tests for xhysteria/Hysteria/short interop,
Go-only Hysteria tests, all eight loaded scenarios, and four idle cascade
scenarios passed. Linux diagnostic Xray is built in
`build_assets/packet-loss-20261117/xray-diagnostic`. The running desktop binary
and remote servers were not replaced; Android was not rebuilt in this stage.

## TCP batching validation — November 18, 2026

After bounded opportunistic TCP writes and synchronous command dispatch,
the both-native/both-Go loaded comparison was rerun with Salamander + WireGuard,
four saturated TCP flows, `GOMAXPROCS=4 taskset -c 12-15`. All eight
direct/cascade × 256/4096-byte UDP scenarios receive 2000/2000 replies each;
sampled owned kernel UDP socket drops are zero. Native achieved TCP background
is 247–254 MiB/s direct and 142–145 MiB/s cascade; Go is 195–196 and 124–127.
These are local tests with different achieved load, not proof about live VPS
paths. Raw counts and QUIC diagnostics are in ignored
`build_assets/shared-dispatch-20261118/loss-go-native.log`.

The previous unrestricted-CPU run observed 0–0.25% missing application replies;
that result is not erased by this CPU-pinned run. `TestHysteriaPacketLossUnderLoad`
still fails only on complete loss, so PASS alone is not a no-loss assertion.
The new fixed-512MiB upload memory comparison measures native peak RSS at
82.72 MiB versus Go 38.75 MiB. Fewer Go allocations do not mean lower total
memory; this change does not establish that the original 1 GiB VPS OOM is fixed.

## Matched-load UDP diagnostics: November 19, 2026

The loopback test accepts `XHYS_LOSS_PACKETS` (1–12,000, default 2,000)
and `XHYS_LOSS_PACED_MIBPS` (aggregate offered bidirectional TCP rate).
The reported background rate counts completed TCP echo bytes during UDP
sending, excluding the two-second UDP drain. With `XRAY_NET_DIAG=1`, native
`event=udp_reassembly` reports cumulative expired, evicted, replaced and
oversized incomplete assemblies per engine. These are assembly counts, not
QUIC packet loss; an incomplete assembly can expire after an upstream loss.

One direct 4 KiB run per backend, with the header mask, four background
TCP flows and 192 MiB/s offered load, received 8,000/8,000 replies on both.
Achieved rates were 191.27 MiB/s native and 189.66 MiB/s Go. Sampled kernel
UDP drops were zero on both, and native bridge credit, QUIC DATAGRAM and
reassembly drop counters stayed zero in the sampled logs. This does not
explain or rule out the earlier intermittent loss under saturation. Logs:
`build_assets/loaded-latency-20261119/udp-192-*.log`.

## Send MTU and Bridge Credits: November 19, 2026

An MTU reduction during UDP enqueue no longer closes the session when Quinn
returns `TooLarge`. Before the first fragment is queued, the sender retries
once at a smaller limit. If part of the old layout was already queued, it
abandons that datagram without mixing layouts and keeps the session open.
`send_mtu_dropped_total` counts those local enqueue failures independently of
QUIC's counter for datagrams already queued. A deterministic unit test checks
both cases. Server UDP reuses the existing flow credit handle rather than
locking the global flow table for each incoming datagram. Queue limits are
unchanged.

With both endpoints and the load generator pinned to one logical CPU, a
direct 4 KiB UDP test got 4,000/4,000 before and 3,999/4,000 after, at 94.27
and 94.35 MiB/s of completed TCP background traffic. The latter run recorded
one server bridge-credit drop; sampled kernel, QUIC DATAGRAM, reassembly and
send MTU drop counters stayed zero. That run identifies a local bottleneck,
not a reproducible regression or the cause of WAN losses. Measure the delay
from Go consuming a datagram to Rust processing the returned credit before
changing queue sizes. Logs: `build_assets/weak-server-20261119/udp-*.log`.
