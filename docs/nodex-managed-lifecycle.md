# NodeX managed generation lifecycle

`cmd/nodex-xray-supervisor` manages independent Linux Xray generations.
It is opt-in: installing these binaries does not import an unmanaged process,
enable the managed systemd unit, or make an ordinary restart seamless.
**Production acceptance remains incomplete**, particularly broker failure during
relay/overlap and the full failure/network matrix. The November 20 lossless
backpressure checks passed saturated TCP/response-window UDP workloads, not
arbitrary flood or WAN qualification. This document supersedes lifecycle
restrictions in the historical TCP pilot and relay experiment notes.

## Implemented lifecycle

- Public TCP/Unix listeners are bound once. Generations inherit duplicates;
  there is no permanent TCP data proxy. gRPC retains accepted RPCs. XHTTP pins
  session IDs and forwards only requests received by the wrong generation.
- Native Hysteria begins with exclusive direct UDP RX/TX. A separate persistent
  broker retains descriptors, CID keys, generation slots and Initial pins.
  During overlap it receives UDP and forwards to fixed, bounded shared rings.
  After verified retirement the active native generation reacquires direct RX;
  the dormant broker unregisters its public receive reactor. Go Hysteria uses
  the compatible Unix-datagram relay and does not reacquire direct RX.
- Each binary and exact config is snapshotted privately in its generation
  directory. Launch is gated by a pipe until the PID identity is journaled.
  Readiness is followed by a real, candidate-only public tunnel probe.
- Admission changes only after peer linking. A failed probe rolls admission
  back while preserving work already accepted on either process. Unknown
  admission is retained, not reported as a successful rollback.
- Hysteria drain fences Initial pins, relay/worker receive queues, engine
  handshakes and connections, application work and Go relay egress. Vendored
  quic-go/Quinn patches retire only an idle endpoint without closing existing
  handshakes. A connection-count snapshot alone cannot authorize retirement.
- The old generation has **no forced session timeout**. Retirement follows
  verified transport completion, final counter persistence, UDP route
  retirement/direct-RX transfer and XHTTP unlinking. A long session can keep
  drain pending indefinitely; further updates are rejected during overlap.
- Upgrade work belongs to the supervisor context, not the SSH connection.
  Losing the requesting client does not cancel the admitted update.

The private control API is `GET /status`, `POST /upgrade` on a mode-0600 Unix
socket in a mode-0700 directory. `202` means accepted, not complete. Poll until
`active` with the candidate hash and no retained generation, or confirmed
`draining`. During `switching`/`admission-unknown`, `last_confirmed_active`
is historical, not proof of current admission.

## Private transport fences

`drain-udp TAG:PORT TOKEN_HEX` consumes a private FIFO marker and returns
`udp-waiting` until the endpoint can retire, then `udp-drained`. Ordinary
`drain` refuses completion until every UDP binding is drained. Resume after
UDP retirement is prohibited. `quic-status` remains diagnostic only.

The 24-byte private marker (`NXDFENC` version 1 plus a nonzero 16-byte token)
travels through the same receive FIFO as packets. Patched quic-go orders
retirement on its received-packet queue and fences its stateless send owner.
Patched Quinn checks endpoint/protocol state under the endpoint mutex,
including queued Initials, then disables admission without closing live work.
Go uses a further worker-to-broker egress fence before removing its route.
Native direct TX has no relay egress to flush.

Native `release-receive TAG:PORT` returns a monotonic lease with the CID key,
local address, slot and pending Initial pins. The broker validates/adopts it
before switching admission. Once old work drains, the broker queues an acquire
marker, unregisters public receive and acknowledges release;
`acquire-receive TAG:PORT TOKEN_HEX` lets the active generation reclaim RX.
Stale leases/tokens are rejected. Keys/snapshots are private; do not log them.
Never let two generations receive from the public socket simultaneously.
On repeated release, adoption merges still-live broker Initial pins with the
worker snapshot instead of discarding the broker table. Existing expiry is
not extended; conflicting owners or a merged table above the fixed limit
reject adoption without changing routes or lease state.

The fixed sealed SPSC ring is ~2 MiB per live generation. ABI 2 stores a
17-byte local destination in existing slot padding. It copies into the ring
and Quinn buffers, not zero-copy. Bounded overflow is still packet loss;
current saturated overlap measurements have losses.

## Recovery and process ownership

SIGKILL of the supervisor does not kill workers or the separate UDP broker.
Recovery validates boot ID, start ticks, executable/config hashes and pidfds,
then reconnects to the existing listeners and broker without rebinding.
The journal supports preparing, switching, draining and retiring states as
well as a single active generation. Unprovable transitions remain blocked;
never delete a runtime marker to bypass this protection.
The generation's saved `config.json` must exist and match its hash: a missing
snapshot is not accepted merely because the caller supplied another copy.
Rejected recovery retains the live generation. If recovery finds B already
active but its new public probe fails, it verifies rollback to A and drains B
without terminating work previously accepted by B. A failed rollback/probe
retains both generations in `admission-unknown`, never claims success.

The companion NodeX service uses `KillMode=process`,
`RuntimeDirectoryPreserve=yes` and `RestartPreventExitStatus=23`. Rejected
recovery exits 23 and retains live processes instead of repeatedly retrying an
uncertain transition. An **explicit SIGTERM/service stop intentionally stops
generations**: do not restart this service to update Xray or the supervisor.
### Broker failure in stable native direct mode

The supervisor retains a checkpoint of the public UDP socket and existing
producer-side Unix socket/ring/eventfd descriptors while exactly one native
generation owns direct public reception. `broker_recovery_ready=true` in status
means those descriptors are held. No extra reader, packet copies or per-packet
monitoring is introduced. The monitor waits for process exit through pidfd,
without a periodic timer or repeated `/proc` reads while the broker lives.

After confirmed broker death, a replacement reattaches the **same** empty, open,
sealed packet ring. It never reinitializes indices or consumer ownership,
creates a competing UDP socket, or replaces the live Xray process. The
replacement keeps the old CID key/current slot and reserves all previous
attempted slots. The supervisor checkpoints it again for subsequent failure.
The startup gate is released only after its process identity is journaled.

The checkpoint is invalidated before a new generation starts. Go relay,
native relay/overlap, mixed Go/native bindings, or simultaneous loss of broker
and supervisor are **not** recovered by this direct-only path. A dead broker
without a valid checkpoint blocks further updates without signalling live
Xray; it does not repair the failed relay's data path. A closed/nonempty ring
is rejected rather than declared losslessly recovered.

Interrupted `broker-recovering` can reconnect to an already-launched healthy
replacement after supervisor recovery; the journal still must match its
PID/start ticks, binary/config hash, binding and direct slot. If the replacement
never crossed its startup gate, recovery refuses to invent missing ownership.

Validation on November 21: the native lifecycle test injected three broker
SIGKILLs, retained existing TCP tunnels, recovered the same Xray PIDs, and
completed two subsequent generation updates plus supervisor SIGKILL recovery.
The separate application UDP test ran three times; each delivered 4000/4000
1200-byte replies across three broker deaths without retransmission or Xray
replacement. This is local response-window/paced UDP, not saturated WAN proof.
The one-core overlap/direct-reacquire load regression delivered 6000/6000 UDP
replies with zero relay/kernel drops and drained under 32 continuous TCP flows.
122 native Rust tests, engine Clippy and native/non-native Go race suites passed.

```sh
XRAY_TEST_GENERATION_BINARY=/path/to/xray-native XRAY_TEST_MANAGED_NATIVE=1 \
  NODEX_TEST_PUBLIC_PROBE=/path/to/nodex-xray-probe \
  /path/to/native-supervisor.test \
  -test.run '^TestSupervisorHysteriaBrokerDirectRecovery$' -test.v -test.timeout 160s
XRAY_TEST_GENERATION_BINARY=/path/to/xray-native /path/to/native-supervisor.test \
  -test.run '^TestNativeDirectBrokerRecoveryPreservesApplicationUDP$' \
  -test.v -test.count 3 -test.timeout 60s
```

## Management API and metrics

The supervisor keeps the original loopback HandlerService/StatsService/
LoggerService endpoint; each generation gets a private Unix API. Stable
user email identities, additions and removal tombstones are journaled before
fan-out to every live generation. Partial failure is reported as unavailable
and retained for replay, never falsely acknowledged as synchronized.
Candidate bootstrap reconciles active users against stale candidate config.
Individual mutations use targeted email lookups instead of fetching N users
for every one of N changes.

Traffic counters sum retained final snapshots and nonreset live worker values.
Gateway resets change a durable virtual baseline, not worker counters. Final
accounting is persisted before stopping a generation; finalized sources cannot
be reattached. The HTTP metrics endpoint proxies to the current generation;
it is **not** an aggregate of every generation. Unsupported management methods
(including online-IP/stat variants) remain unimplemented.

## Supported configs and restrictions

gRPC/REALITY (`vg`), TCP XHTTP/REALITY (`vxr`), Unix/TCP XHTTP behind Nginx
(`vxt`, `vxt_white`), and Go/native Hysteria are supported building blocks.
Every generation must retain the same public listeners, API/metrics bindings,
socket policy, backend and UDP masks. Wildcard UDP preserves local destination
metadata; IPv4 source-address preservation has a focused test.

Not supported: XHTTP/3, socket wrappers such as PROXY protocol/TPROXY/original
destination, masks that own network listeners, arbitrary listener/redirect/
Nginx changes, or an unmanaged-to-managed live migration. Existing external
port redirects must remain unchanged. Full IPv6 dual-stack, multihoming,
port-hopping and WAN mixed-mode acceptance is still required.

UDP slot IDs are never reused within a broker/key lifetime. The current bound
is **256 attempted generations**, including failed attempts; capacity exhaustion
rejects an update rather than misrouting old CIDs. There is no seamless slot/key
rotation yet. Binary/config snapshots do not snapshot certificate, geodata or
other referenced assets: use stable absolute paths and do not replace them
under a live generation.

## Build and run

```sh
go build -o /tmp/nodex-xray-supervisor ./cmd/nodex-xray-supervisor
CGO_ENABLED=0 go build -o /tmp/nodex-xray-probe ./cmd/nodex-xray-probe
CGO_ENABLED=0 go build -tags xray_server -o /tmp/xray-managed ./main

# Dedicated unprivileged account, fresh private runtime, unused listeners.
/tmp/nodex-xray-supervisor -runtime /absolute/private/runtime \
  -binary /tmp/xray-managed -config /absolute/private/node.json \
  -probe /tmp/nodex-xray-probe serve

/tmp/nodex-xray-supervisor -runtime /absolute/private/runtime status
/tmp/nodex-xray-supervisor -runtime /absolute/private/runtime \
  -binary /absolute/releases/new-xray -config /absolute/private/node.json upgrade

# After abrupt supervisor loss: exact saved active identity/config.
/tmp/nodex-xray-supervisor -runtime /absolute/private/runtime \
  -binary /absolute/private/runtime/g1/xray \
  -config /absolute/private/runtime/g1/config.json \
  -probe /tmp/nodex-xray-probe recover
```

Native builds require `xhysteria_native`, CGO and the matching C ABI 6 Rust
archive; the managed ring ABI is 2. Build Xray and supervisor from the same
source/archive. The Go-only probe explicitly selects Go Hysteria even if it
inherits a native-server backend override.

### Public probe contract

The trusted executable receives the generation ID and
`NODEX_CANDIDATE_PID`, `NODEX_CANDIDATE_SHA256`,
`NODEX_CANDIDATE_CONFIG`, `NODEX_CANDIDATE_API`. The built-in probe creates a
temporary user **only on the candidate**, opens a fresh real public tunnel
through each inbound, and requires a 204 response from
`http://proxvless.test/generate_204`. The node must route this to its tunnel
probe service. Credential cleanup failure also fails verification.

VXT metadata (`_vxt`) or `NODEX_MANAGED_PROBE_ENDPOINTS` (private JSON file
mapping inbound tags to address/port/host/sni) supplies the public Nginx path.
An optional `ca` path selects a private trust root for a controlled Nginx
deployment/test; certificate verification is never disabled.
A Unix origin without a public endpoint fails rather than bypassing Nginx.
Passing `/bin/true`, checking an open port or `run -test` does not satisfy the
probe contract. Actual built-in gRPC/REALITY and XHTTP/REALITY production
config probes have a local two-generation integration test with real VLESS
credentials and cleanup checks. This does not qualify every deployment config.

## Validation and outstanding acceptance

Local tests cover actual gRPC/XHTTP generations, rollback, TCP SIGKILL recovery;
Go/native Hysteria A→B→C, supervisor SIGKILL with the same broker/worker PIDs,
wildcard IPv4, Salamander, counters, metrics, public probe and natural drain.
Companion Nginx tests cover stream-up/white packet-up with HTTP/1 and HTTP/2
origins, including the built-in candidate-only probe when
`NODEX_TEST_PUBLIC_PROBE` is set. Focused Go lifecycle race tests, native
engine tests and Clippy pass.
These are not a complete production qualification.
Explicit live tests also interrupt `switching` after real candidate traffic,
recover the same generations/broker, and inject a failed recovery probe to
verify fresh work returns to A while old A/B sessions remain live. TCP recovery
tests reject missing/tampered generation snapshots without disrupting a held
session. Focused Rust tests check GRO metadata and a long-header Initial/0-RTT
behind a short first segment; these are regressions, not a new fast-path.

```sh
XRAY_TEST_GENERATION_BINARY=/tmp/xray-managed \
  go test -race -count=1 ./cmd/nodex-xray-supervisor

# Native test executable exercises both Hysteria backends.
XRAY_TEST_GENERATION_BINARY=/tmp/xray-native XRAY_TEST_MANAGED_NATIVE=1 \
  NODEX_TEST_REPEAT_UPDATE=1 NODEX_TEST_PUBLIC_PROBE=/tmp/nodex-xray-probe \
  /tmp/nodex-supervisor.test -test.run '^TestSupervisorHysteriaGenerations$' \
  -test.v -test.timeout 200s

python3 cmd/nodex-xray-supervisor/nginx_canary_test.py \
  /tmp/xray-managed /tmp/nodex-xray-supervisor \
  /absolute/DanVlessBot/build/vless_xhttp_tls_test
# Repeat with --http2-upstream.
# Set NODEX_TEST_PUBLIC_PROBE=/tmp/nodex-xray-probe to verify the packaged probe too.
```

Before enabling by default: extend loss-free overlap checks to sustained UDP/WAN,
reduce the remaining one-core overhead, qualify public probes and systemd
privileges/assets, exercise every ownership failure boundary and UDP broker
failure during relay/overlap, extend the network/mask matrix, and provide a deliberate initial
migration/slot-rotation procedure. See
`nodex-managed-hysteria-performance.md` for measurements.
