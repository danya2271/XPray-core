# Third-party Hysteria2 stream-limit investigation

Date: November 22, 2026. Baseline: `a5dd2330`.

## Report and local reproductions

The reported client error is:

```text
connection: open connection ... using outbound/hysteria2[proxy]: too many open streams
```

It appears after approximately fifteen minutes of use on a client other than
this Xray. The client product/version, deployed server commit, backend and
current concurrency are not known. The error is emitted by a nonblocking
QUIC stream opener when the peer's currently advertised stream allowance is
exhausted. It is not an operating-system file-descriptor error or a claim that
the destination TCP server rejected the connection.

The local regression uses quic-go directly, authenticates via the normal
Hysteria HTTP/3 request, and calls `OpenStream`, not `OpenStreamSync`. It does
not use our native client, its QUIC session pool, or reconnection.

Two relevant defects are reproduced:

1. Native advertises only eight bidirectional streams at handshake and raises
   the allowance to 64 after the second TCP request. A single-session client
   trying to retain about 65 TCP streams receives the exact error above.
   Pooled native clients distribute those streams among multiple sessions and
   had hidden this limit from earlier tests.
2. A peer `STOP_SENDING` can leave an idle TCP relay waiting on Go commands
   rather than releasing its flow. A normal third-party close can send FIN
   and STOP_SENDING together. Increasing the QUIC stream allowance alone
   exposes accumulated active flows: the first always-1024 candidate reaches
   the application flow cap during rapid repeated closes and is reset with
   code `0x107`. This is a second reproduced cleanup defect, not the same
   client error string.

Elapsed time alone does not distinguish live-stream concurrency, lost or
delayed stream-credit updates, retained relay state, and a different server
configuration. These reproductions establish defects in the baseline native
backend, not sole causality for every production occurrence.

## Fix and preserved limits

* Race client/server raw TCP relays against Quinn's send-half stopped future.
  Peer STOP_SENDING cancels the relay and releases its application flow.
  A successful local FIN does **not** cancel the remaining receive direction.
* Start with 64 bidirectional stream handles rather than eight. Expand to
  the Go server's 1024-handle allowance once the session reaches sixteen
  simultaneously active TCP flows, leaving room for the MAX_STREAMS update.
* An RAII counter tracks active TCP relays and decrements on every exit path.
  Serial short flows alone do not expand all low-concurrency sessions.
* Replace the old prototype's fixed 128-task acceptance guard with the
  configured listener flow cap plus bounded HTTP-auth/diagnostic/UDP room.
* Listener-wide application-flow admission, per-flow credits and queues,
  process send/RX budgets, receive windows, UDP buffers and ring wakeup
  ordering are unchanged. Excess active flows still fail and recover.

The patch does not make QUIC streams unlimited. A burst that exhausts the
advertised allowance before the peer receives a MAX_STREAMS update can still
make a nonblocking `OpenStream` fail. Proper client-side stream backpressure
remains necessary; increasing all server buffers is not a substitute.

## Memory qualification

The first attempt advertised 1024 streams for **every** connection. Quinn
preallocates stream-ID tables, so this regressed the 1000-session fixture:

| One native server worker, 1000 sessions | Baseline | Always 1024, rejected | Adaptive |
| --- | ---: | ---: | ---: |
| Idle server RSS, MiB | 127.9 | 215.8 | 138.3 |
| Peak server RSS, MiB | 213.2 | 299.9 | 221.0 |

Baseline/adaptive values are medians of three fresh-process samples per
variant in alternating order. The rejected always-1024 value is one sample.
The adaptive policy avoids about 79 MiB of the rejected prototype's peak
overhead in this fixture. The remaining difference is 7.8 MiB across
1000 low-concurrency sessions, with idle overhead of 10.4 MiB.
Median duplex throughput is155.62→154.19MiB/s, heartbeat p99
2553.9→2638.3µs, and generation process CPU4.266→4.337s.
This qualifies the memory cost of compatibility; it is not a memory speedup
or production OOM guarantee.

The fixture authenticates 500 identities in 1000 QUIC sessions with one live
TCP flow per session, then transfers 500 MiB duplex with delayed readers and
checks heartbeats. Server is pinned to logical CPU 8 with GOMAXPROCS=1 and
GOMEMLIMIT=128MiB; the parent uses CPUs8–15/GOMAXPROCS4. No 1GiB cgroup was
applied. All six baseline/adaptive samples and the rejected prototype sample
pass the fixture's 512MiB peak guard.
Rejected prototype executables/logs are retained, not shipped or cited as the
corrected policy's results.

## Local tests and reproduction

The new `stream_limit_test.go` tests cover:

* 192 concurrently live TCP echo streams on one authenticated QUIC session.
* 1200 alternating FIN+STOP_SENDING/reset closes while one permanent stream
  remains alive, crossing the advertised stream-window count.
* The finite low-concurrency advertised handle limit, without opening
  application flows.
* Opt-in fifteen-minute real-time soak: one permanent TCP echo stream,
  a new stream about every 50 ms, no connection replacement or pooling.

Three quick plain repetitions pass. Shared/socket bridges, each plain and
Salamander, pass the 192-live and 1200-churn checks. Unit tests verify that
peer STOP cancels idle work but local FIN preserves half-close. 128 release
engine tests also pass with allocation profiling enabled. Production-library
Clippy and fresh-linked Go race tests for xhysteria, Hysteria, handoff and
supervisor pass. Short-flow credit recovery, configured-cap rejection and
recovery, 140 packet-mask cases and synthetic 100ms RTT bulk transfer pass.
Linux and Android arm64/API26 build successfully; no device run was done.

For the opt-in soak, run the fresh-linked interop test binary:

```sh
XHYS_STREAM_SOAK=1 XRAY_HYSTERIA_BRIDGE=shared \
  interop.test -test.v \
  -test.run '^TestNativeSingleQUICSessionFifteenMinuteSoak$' \
  -test.timeout 17m
```

Add `XHYS_STREAM_MASKED=1` and use `XRAY_HYSTERIA_BRIDGE=socket` to test the
masked socket path. Binary build tags are `xhysteria_native xhysteria_interop`.

Raw results, prototype failures and executables are under ignored
`build_assets/stream-interop-20261122/`. Preserve `reproduce-*.log`, the failed
always-1024 churn run, and `soak-15m.log`: the latter fails at sixty seconds
because the initial fixture echo deadline was sixty seconds. The corrected
fixture allows twenty minutes. An additional always-1024 soak was intentionally
terminated when the memory regression was confirmed. Neither is a fifteen-minute
pass. Only completed `soak-adaptive*.log` runs qualify.

Both corrected real-time soaks **passed** for the full fifteen minutes without
reconnection or a pool: plain/shared recycled17591 streams, and Salamander/
socket recycled17580. Each kept one permanent TCP echo stream alive through
the final check. These are local cross-implementation tests, not a test of an
unknown deployed client binary or production network.

## Server performance check

The repeated A/B uses the same fresh client/echo harness, three fresh server
processes per version and workload, alternating variant order. Server,
supervisor and echo use logical CPUs4 or4–7; client uses8–15. The two
low-rate time-soak tests use distinct physical cores0–3. Both variants use
managed native shared RX/direct TX and measure 16 old plus16 new-generation
TCP flows. Bulk samples complete32768 duplex64KiB exchanges; small samples
complete200000 64B exchanges. Medians:

| Server CPUs | Workload | Metric | Baseline | Adaptive fix |
| ---: | --- | --- | ---: | ---: |
| 1 | 64KiB duplex | MB/s | 229.68 | 228.15 |
| 1 | 64KiB duplex | Total server CPU, µs/exchange | 520.61 | 523.28 |
| 1 | 64KiB duplex | p99, ms | 35.758 | 36.824 |
| 1 | 64B | Total server CPU, µs/exchange | 40.46 | 41.22 |
| 1 | 64B | p99, ms | 2.511 | 2.544 |
| 4 | 64KiB duplex | MB/s | 713.86 | 706.61 |
| 4 | 64KiB duplex | Total server CPU, µs/exchange | 632.29 | 635.58 |
| 4 | 64KiB duplex | p99, ms | 12.911 | 11.855 |
| 4 | 64B | Total server CPU, µs/exchange | 73.53 | 75.15 |
| 4 | 64B | p99, ms | 2.117 | 2.196 |

All24 samples report zero ingress/queue/send/public UDP drops. This is a
compatibility/cleanup fix, not a throughput optimization: median bulk rate
decreases0.67%/1.02%, small-exchange CPU increases1.88%/2.20%, and p99 is
mixed. Loopback timing on a shared host and three samples do not establish
a universal latency effect. Generation bulk peak RSS medians103.10→103.90
and111.20→112.30MiB; no ordinary-stream memory saving is claimed.

Additional saturated application UDP checks accept6000/6000 each on one/four
server CPUs, zero relay/public UDP drops. Loaded drain completes43683/63804
exchanges and returns the surviving generation to direct receive. These
functional runs overlapped other load checks and are not performance samples.
`isolated-cpu*.log` is the controlled table; the first `compare-cpu*.log`
matrix is retained but overlapped build/validation activity. `results.json`
stores individual samples and medians.

## Deployment and production uncertainty

No remote deployment/restart has been performed. Updating the repository or
rebuilding an artifact does not update a running VPS. Verify which native/Go
backend and server version is deployed before treating the patch as a
production resolution. The client product/version and reconnect behavior
would help distinguish its exact limit handling from this direct quic-go
reproduction.
