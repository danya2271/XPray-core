# Shared payload and send-budget ownership

Date: November 24, 2026. Baseline: `d1856927`.
Host: Intel Xeon E5-2698 v3, 16 physical cores / 32 SMT threads.

## Outcome

Accepted as a bounded ownership-metadata optimization. The normal shared DATA
send path retains one reference-counted owner instead of two, without growing
the Rust frame or changing payload/queue bounds. Seven-pair micro medians save
4.3–8.1% for the tested frame sizes. The lifetime/backpressure checks and
transport acceptance below pass.

This is not a universal throughput, latency or RSS factor. Five-pair combined
one/two-core upload/download medians improve, but the independent one-core
managed-server throughput changes **252.86→248.78 MB/s** (−1.6%), with server-side
CPU **477.039→485.453 µs/exchange** (+1.8%). One-core combined bulk-duplex p99
changes **59.319→65.428 ms** (+10.3%). Two-core paced p99 improves while CPU per
probe increases. All observations remain recorded.

At 1000 sessions, peak-before-close server RSS is **222.4→222.7 MiB** by median.
There is no established process-RSS reduction. Eliminating the extra owner
allocation is the deterministic resource saving; small transport differences
remain subject to uncontrolled host variation and scheduler behavior.

## Change

The previous shared bridge materialized each leased command payload as
`Bytes::from_owner(Payload)`. When a TCP/HTTP send takes its per-engine and
process send-budget permits, it wrapped those Bytes again in
`Bytes::from_owner(BufferedChunk)`. Both reference-counted owner allocations
remain live until Quinn releases the last payload reference.

The change moves the arena lease through the internal command queue
without materializing its Bytes owner. The send-budget wrapper then owns
the lease and permits directly, producing one reference-counted owner instead
of two. Ordinary socket-backed Bytes remain supported. Consumers without a
send budget materialize the lease normally when they require Bytes.

The shared-memory wire/storage ABI remains version 6. Descriptor/payload slot
counts, publication ordering, SeqCst wakeup handshakes, packet masks, crypto,
Tokio scheduling intervals, worker defaults, flow credits and queue bounds
have no changes. The existing payload copy into the shared arena remains.
This candidate removes ownership metadata, not a wire encryption step or a
payload copy.

The final candidate keeps the internal amd64 Rust `Frame` at 48 bytes and
the budget wrapper at 64 bytes. A validated 0–15 slot index uses `u8`, so
Rust can encode the enum tag without increasing its size. The old separate leased-payload
owner is removed. The earlier 56/72-byte variant was superseded after mixed
multicore/server results; all 152 of its transport observations remain in
`hysteria-shared-owner-20261124/uncompacted-rejected.json`.
The instrumented fixture observes **two→one live owner allocations** for
64, 4096 and 16384-byte frames and returns to the original live-allocation
count after the last reference drops.

## Lifetime and backpressure checks

Regression tests verify that:

- A budgeted shared lease keeps the exact payload address and bytes.
- The arena and both budget reservations outlive the original frame and bus,
  including a retained Quinn-style retransmission slice.
- Dropping the final slice frees the arena and returns both permit counts.
- Cancelling an async wait for either local or process budget drops the lease
  and any partial budget reservation.
- Existing shared-slot reuse, payload-pool fallback, concurrent release,
  FIN/error ordering and finite batch/budget tests still pass.

The new moveable lease implements `AsRef<[u8]>`; its final Bytes owner uses
the existing Bytes ownership API. No new raw-pointer lifetime extension is
introduced. The old slot release logic remains the lease's destructor.

## Microbenchmark

Seven alternating paired runs use one release test executable pinned to
physical CPU 4. Each sample performs 262,144 shared-ring publish/pop,
send-budget acquisition, owner construction and release cycles. Payload
sizes are 64, 4096 and 16384 bytes. The old-policy reference materializes
the payload Bytes and uses a separate old-sized budget wrapper.

| Payload bytes | Reference median ns/frame | Candidate median ns/frame | Reduction |
| ---: | ---: | ---: | ---: |
| 64 | 225.919 | 214.940 | 4.86% |
| 4096 | 273.789 | 251.760 | 8.05% |
| 16384 | 627.468 | 600.553 | 4.29% |

The allocation-profile feature is disabled for these timing observations.
Separate instrumented runs check the live owner allocation count and cleanup;
their timings are not substituted for the normal-allocator measurements.
Both policies share the candidate's frame parser/enum and test loop, so the
reference isolates the ownership change rather than rebuilding the complete
old transport. It also uses the compact slot representation; it does not time
the old, larger payload-lease struct.

Reproduce one normal-allocator policy at a time, after completing the build:

```sh
taskset -c 4 env -u XHYS_SHARED_OWNER_REFERENCE cargo test --locked --release \
  --features engine --lib --manifest-path native/libxhysteria/Cargo.toml \
  -- --ignored --exact engine::tests::benchmark_shared_send_ownership \
  --nocapture --test-threads 1
```

Set `XHYS_SHARED_OWNER_REFERENCE=1` instead of unsetting it for the old-policy
reference. Alternate policies over seven repeats. For the separate live-owner
allocation assertion, use `--features allocation-profile` and run only this
ignored test; counters cover the entire Rust process. Its timings have
instrumentation overhead and are not part of the normal-allocator table.

## Transport method

The primary A/B compares the complete client/native-server/TCP-target process
on CPU 4 or CPUs 4,5. GOMAXPROCS and native workers per role equal the core
budget. Both versions use pool=2, shared bridges, 32 TCP flows, Salamander
and WireGuard masks. Five paired fresh-process repeats alternate version
and core-budget order. Task builds, microbenchmarks and functional tests do
not overlap timed matrices; unrelated host load remains uncontrolled.

Duplex is 8192 checked exchanges of 64 KiB per direction, 1 GiB aggregate.
Upload/download each transfer 512 MiB and wait for the receiver to finish.
Paced latency uses 10,000 64-byte probes alongside an offered 64 MiB/s duplex
background. The attained background is recorded. Timed CPU is the whole
process per operation; peak RSS includes setup and both native roles.
Calibration engines finish before each setup; socket identities are pinned
for UDP-drop observations. Nonzero-drop samples remain in summaries.

MB/s is decimal throughput, MiB/s is binary background load. Duplex speed
sums both directions. Neither a duplex result nor a combined process budget
is an isolated server's one-direction performance claim.


## Final measurements

### Combined client/server/target on one and two cores

Medians of five paired fresh-process repetitions per workload:

| Cores | Workload | MB/s before → candidate | Process CPU µs/op | p99 µs | Peak RSS MiB | Drop samples before/candidate |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 1 | duplex | 179.26 → 179.33 | 691.34 → 690.84 | 59319.00 → 65428.00 | 63.47 → 65.82 | 0/0 |
| 1 | upload | 178.42 → 182.45 | 346.74 → 338.93 | — | 149.46 → 155.94 | 0/0 |
| 1 | download | 186.15 → 190.53 | 336.82 → 328.91 | — | 136.17 → 132.11 | 0/0 |
| 1 | paced | — | 238.37 → 236.51 | 4695.00 → 4603.00 | 54.45 → 53.11 | 0/0 |
| 2 | duplex | 312.50 → 319.78 | 755.24 → 735.28 | 50279.00 → 45193.00 | 66.49 → 66.79 | 0/0 |
| 2 | upload | 292.03 → 293.73 | 389.56 → 388.72 | — | 169.33 → 170.73 | 0/0 |
| 2 | download | 328.65 → 337.27 | 361.46 → 353.28 | — | 94.20 → 75.67 | 2/0 |
| 2 | paced | — | 299.81 → 306.60 | 2207.00 → 1845.00 | 53.98 → 53.25 | 0/0 |

Attained paced background medians:

- 1 core(s): 64.24 → 64.24 MiB/s.
- 2 core(s): 64.01 → 63.94 MiB/s.

### Four/eight-core controls

Three paired repeats, four native workers per role; pool2/4 respectively.

| Cores | Workload | MB/s before → candidate | Process CPU µs/op | p99 µs | Peak RSS MiB | Drop samples before/candidate |
| ---: | --- | ---: | ---: | ---: | ---: | :---: |
| 4 | duplex | 476.79 → 485.73 | 884.80 → 878.19 | 34406.00 → 35115.00 | 69.90 → 70.02 | 0/0 |
| 4 | upload | 453.60 → 459.10 | 454.92 → 451.17 | — | 181.59 → 183.00 | 0/0 |
| 4 | download | 486.03 → 485.97 | 422.58 → 424.46 | — | 93.88 → 89.63 | 0/0 |
| 4 | paced | — | 272.29 → 271.45 | 1420.00 → 1339.00 | 55.35 → 56.92 | 0/0 |
| 8 | duplex | 697.00 → 673.56 | 1076.44 → 1138.74 | 23017.00 → 25290.00 | 70.67 → 68.02 | 0/0 |
| 8 | upload | 623.42 → 632.49 | 573.89 → 565.95 | — | 217.79 → 208.94 | 3/3 |
| 8 | download | 711.77 → 730.36 | 509.95 → 492.53 | — | 101.79 → 105.57 | 0/0 |
| 8 | paced | — | 915.29 → 829.04 | 1929.00 → 2637.00 | 56.88 → 55.72 | 0/0 |

### Managed server with separate unchanged baseline load generator

Three paired repeats per configuration. Server generations, ingress and TCP
echo share CPU4 or CPUs4,5. The client stays on CPUs8–15, GOMAXPROCS8,
four native workers and pool4. There are 32 flows across two live generations.
Transfer is 32768 duplex64KiB exchanges, latency200000×64B. All samples pass
strict zero ingress/public UDP drop gates. CPU below sums server generations
and supervisor/echo, excluding the client. Server RSS excludes supervisor/echo.

| Server cores | Workload | MB/s before → candidate | Total server-side CPU µs/op | p99 µs | Server peak RSS MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | transfer | 252.86 → 248.78 | 477.04 → 485.45 | 37122.00 → 37197.00 | 103.50 → 103.70 |
| 1 | latency | — | 40.61 → 40.79 | 2487.00 → 2517.00 | 92.44 → 92.48 |
| 2 | transfer | 451.25 → 451.54 | 522.09 → 522.05 | 19976.00 → 19456.00 | 106.40 → 108.10 |
| 2 | latency | — | 62.94 → 63.24 | 3457.00 → 3524.00 | 95.25 → 94.90 |

### One-core server: 500 users and 1000 QUIC sessions/TCP flows

Three alternating pairs, serverCPU4/GOMAXPROCS1, GOMEMLIMIT128MiB; the fixture
and generator occupy CPUs4–7. All six runs must pass the512MiB server RSS guard.
The CPU metric covers the server process lifetime, not only the bulk interval.

| Metric | Baseline median | Candidate median |
| --- | ---: | ---: |
| server_idle_rss_mib | 137.00 | 136.90 |
| server_buffered_rss_mib | 214.10 | 214.60 |
| server_peak_rss_mib | 222.40 | 222.70 |
| server_after_close_rss_mib | 219.60 | 221.10 |
| duplex_transfer_mib_per_second | 167.37 | 167.52 |
| heartbeat_p99_us | 4408.40 | 4204.70 |
| server_process_cpu_seconds | 4.33 | 4.29 |

All observations, minima/maxima, exact bulk commands and binary hashes:
[results.json](hysteria-shared-owner-20261124/results.json).
Raw logs and large binaries remain ignored in `build_assets/shared-owner-20261124/`.

## Extended eight-core control

The initial three-pair control showed worse duplex CPU and paced p99, so four
additional pairs repeat only those workloads at exactly the same CPU/worker/
pool settings and with the same binaries. They run after all other task builds
and tests finish. The first matrix remains above; the table below includes all
seven pairs, without removing any early or nonzero-drop observation.

| Workload | MB/s before → candidate | CPU µs/op | p99 µs | Attained background MiB/s | Drop samples before/candidate |
| --- | ---: | ---: | ---: | ---: | :---: |
| duplex | 665.16 → 676.79 | 1157.70 → 1138.74 | 27044.00 → 25920.00 | — | 0/0 |
| paced | — | 1062.02 → 1032.79 | 3224.00 → 3229.00 | 63.99 → 64.00 | 0/0 |

## Validation

- 133 release and 133 debug engine/library tests pass; two manual benchmarks
  are ignored by the ordinary suites.
- Release engine library Clippy passes with `-D warnings`.
- Fresh native Go race tests pass for xhysteria, hysteria, handoff, supervisor
  and proxy packages.
- One/two-core checks pass packet masks, concurrent transfers, HTTP file/proxy/
  trailers/cancellation/early-response lifecycle, stream credit, capacity
  rejection/recovery and 100 ms RTT bulk transfers.
- Masked shared/socket third-party clients pass 192 concurrent stream,
  1200 close/reopen and advertised finite-window checks on both budgets.
- Strict direct/cascade UDP under TCP load passes **8000/8000 per budget**,
  with 256/4096-byte payloads, zero sampled kernel drops and duplicates.
- One/two-core managed saturated UDP passes **6000/6000 per budget** with
  zero ingress/public-UDP drops; retained-connection and under-load drain
  tests also pass.
- All six 1000-session capacity observations pass the 512 MiB server RSS guard.
- Android arm64/API26 builds with NDK30.0.14904198. No device execution or
  Android performance result follows.

No full-repository green, new 15-minute soak, WAN performance, production
deployment or universal metric improvement is claimed. All 168 final transport
observations, six capacity runs, 42 normal micro observations, six allocation
checks and validation records are retained. The superseded variant has its
own separate 152-observation dataset and historical source patch.
