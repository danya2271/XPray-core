# Managed Hysteria performance investigation

The original managed Hysteria relay is not suitable for enabling native NodeX
updates by default. The reported 32-flow, 64 KiB duplex regression was reproduced
locally: about 270 MB/s, 102 ms p99, and 78,695 relay drops for 8192 exchanges.
Neither the cheap CID classifier nor normal-start tests demonstrate acceptable
managed throughput or tail latency.

## Experimental paths

The worktree adds explicit, opt-in experiments, not production activation:

- `packet_batch` in the generation manifest permits the native engine to use
  segmented Unix frames, up to eight UDP datagrams per frame. It defaults to
  false for existing manifests. Frames preserve peer address and datagram
  boundaries, including a short final datagram. The Go endpoint can decode
  segmented frames too; unmodified native engines must not receive them.
- `xhysteria.NewNativeIngress` is a Linux-only experimental relay using the
  existing Rust mask implementation and batched public UDP I/O. The Go wrapper
  performs setup and control only. The receiver owns Initial/0-RTT pins and
  acknowledges admission changes between batches. Neighboring datagrams for
  the same peer/generation can share one vectored Unix frame. There is no wait
  to fill a frame. The generation worker uses `recvmmsg` on Linux/Android.
- Optional `transmit_fd` gives a native worker a duplicate of the public UDP
  descriptor for sending replies directly with its configured masks and GSO.
  Only the ingress receives from that descriptor. Its bound address must match
  the ingress; an unsupported backend or mask configuration is rejected. The
  default remains relay egress, and ordinary native startup remains unchanged.

The native relay now retries transient Unix socket backpressure with a separate
bounded queue per generation (512 KiB and 256 frames). The healthy fast path
still borrows receive buffers without allocation; only queued frames are copied.
Each generation gets an independent readiness turn, so a stalled worker does
not block admission changes or another worker. Queue overflow remains packet
loss rather than unbounded memory or blocking the public receive owner.
Directional counters separate queue overflow, send errors and egress failures.
They are approximate telemetry, **not** a drain certificate.

At saturation this still causes loss and QUIC retransmissions. It is an
unresolved performance defect, not a successful zero-overhead implementation.
This ingress has no recovery protocol or QUIC retirement certificate and is
not connected to the node supervisor or TUI activation path.

## Exploratory results, November 20, 2026

These are **single samples**, not medians or release acceptance results. All
use 32 TCP flows exchanging 64 KiB, 8192 completed exchanges, server/supervisor/
echo on physical CPUs 0–3 and client on 8–15, four native client workers and
four sessions, Salamander plus WireGuard header, and a public UDP buffer policy
matching direct native. MB/s sums both directions. Some builds ran concurrently
with exploratory tests; an interleaved repeated matrix is still required.

| Experimental variant | MB/s | Exchange p99, ms | Relay drops |
| --- | ---: | ---: | ---: |
| Original managed relay | 270.23 | 101.592 | 78,695 |
| Segmented native egress through Go relay | 284.94 | 170.660 | 75,400 |
| Rust ingress, individual worker ingress frames | 441.10 | 91.338 | 204,499 |
| Rust ingress, grouped worker ingress frames | 507.12 | 54.131 | 137,235 |
| Grouped Rust ingress, direct native UDP replies | 481.29 | 46.214 | 104,974 |
| Direct replies plus worker `recvmmsg` | 494.10 | 65.001 | 108,990 |

No row meets the requirement of retaining direct native performance with low
tail latency. The direct-native figures in the user's three-sample comparison
are 613 MB/s and 20.3 ms p99. These new single-sample experiments must not be
substituted for that controlled baseline. A user-space Unix relay still costs
CPU, queue capacity and scheduling. The packet data path, including overload
behavior, needs further work before seamless Hysteria activation is enabled.

The local user-space `perf` recording completed, but the stripped Xray binary
does not provide reliable per-function attribution. It does not prove a sole
cause. A symbolized profile, directional drop accounting, and repeatable load
measurements are required before making another performance claim.

## Reproduction

Build the native archive, Xray and benchmark driver from the same checkout.
Use `native/libxhysteria/link-go.sh` when the local Go linker requires normalized
Rust archives. The interop driver is test-only and never creates public node
services. Example after building it as `/tmp/handoff-load.test`:

```sh
XHYS_HANDOFF_SERVER_BINARY=/tmp/xray-native \
XHYS_HANDOFF_SERVER_CPUS=0-3 XHYS_HANDOFF_SERVER_PROCS=4 \
XHYS_HANDOFF_MODE=managed XHYS_HANDOFF_BACKEND=native \
XHYS_HANDOFF_TUNE_PUBLIC=1 XHYS_HANDOFF_PACKET_BATCH=1 \
XHYS_HANDOFF_NATIVE_INGRESS=1 XHYS_HANDOFF_DIRECT_TX=1 \
XRAY_HYSTERIA_NATIVE=1 XRAY_HYSTERIA_CLIENT_WORKERS=4 \
XRAY_HYSTERIA_POOL_SIZE=4 taskset -c 8-15 /tmp/handoff-load.test \
  -test.run '^$' -test.bench '^BenchmarkHandoffServerTransfer$' \
  -test.benchtime 8192x -test.cpu 8 -test.count 1 -test.timeout 120s
```

Choose disjoint physical CPU sets appropriate to the host. Omit the native
ingress, batch and direct-TX flags for the original managed path; set mode to
`direct` for ordinary startup. The same fixture has `TestHandoffUnderLoad` to
check live switching with background TCP and paced UDP, including missing
sequence counts. A completed TCP benchmark alone does not certify zero UDP
loss, safe Hysteria drain, wildcard binding, or supervisor recovery.

The first `TestHandoffUnderLoad` run of the direct-TX/`recvmmsg` experiment
**failed**: 1985/2000 paced UDP replies arrived, with 7463 relay drops.
Three replies were missing before the switch and twelve afterwards; both
generations still reported live QUIC connections. Background duplex TCP was
98.91 MiB/s, UDP p99 1.631 ms and maximum 23.810 ms. Those latency numbers
describe received replies only, not a loss-free result. This failure is retained
as evidence that the experimental path cannot be enabled as a seamless updater.

## Bounded backpressure retry, November 20, 2026

Three new load-switch tests with retry all passed: 2000/2000 paced UDP replies
each, zero relay drops, old/new generations both live, and 127.25–127.52 MiB/s
background duplex TCP. Received UDP p99 was 1.405–1.469 ms. Peak queued data
was 126080–130439 bytes, not the 512 KiB limit. Tests additionally fill one
generation's socket and queue, verify another generation can still activate,
check retry order and segment boundaries, and check bounded overflow.

An interleaved three-round matrix then ran direct, previous native relay, and
retry relay sequentially without builds running. Medians of each metric:

| Variant | MB/s | TCP p99, ms | Relay drops | Total server CPU, µs/exchange |
| --- | ---: | ---: | ---: | ---: |
| Direct native | 624.98 | 20.201 | 0 | 661.04 |
| Previous native relay + direct TX | 494.59 | 53.239 | 105384 | 830.43 |
| Native relay + retry + direct TX | 510.47 | 31.547 | 12546 | 800.80 |

The same 32-flow/64 KiB/8192-exchange, server CPUs 0–3 and client CPUs 8–15
setup was used. Total server CPU is the per-run sum of Xray and supervisor/echo
CPU, then its median. Retry improves tail latency and reduces drops, but the
full-load queue reaches its bound. All remaining counted drops in that matrix
are queue overflow, not Unix send errors. Do not enlarge queues and call this
fixed; managed throughput still trails direct by about 18%.
