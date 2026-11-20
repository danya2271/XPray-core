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

The native relay keeps bounded kernel queues and drops unsent datagrams instead
of blocking the other generation. At saturation this still causes significant
loss and QUIC retransmissions. It is an unresolved performance defect, not a
successful zero-overhead implementation. This ingress has no recovery protocol
or QUIC retirement certificate and is not connected to the node supervisor or
TUI activation path.

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
