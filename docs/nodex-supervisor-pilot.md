# NodeX TCP supervisor canary

`cmd/nodex-xray-supervisor` owns listening sockets across independent managed
Xray processes. It is an opt-in Linux canary, **not a production NodeX updater**.
It does not alter `xray.service`, stop an existing unmanaged Xray, or activate a
staged release on existing nodes.

## Implemented lifecycle

- The supervisor binds public TCP and Unix listeners once and passes duplicate
  descriptors to each generation. No extra TCP proxy is introduced.
- A fresh candidate starts with admission paused. Its binary is copied into a
  private generation directory, hashed, and executed there. JSON config is
  supplied through stdin, not stored with the release or in state.
- It reuses `handoff.LinkXHTTP` and `handoff.Switch`; a failed candidate probe
  restores admission to the previous process without killing candidate work.
- Before issuing irreversible drain, it atomically saves the confirmed active
  identity and retained generation to `state.json`, including file and directory
  fsync. If persistence fails, neither generation is retired.
- The retiring process stays alive without a drain timeout. The next update is
  rejected while starting, switching, draining, or blocked. Completion requires
  `drained`; XHTTP peers are unlinked before stopping the retiring process.
- Completed generation binaries are removed. Uncertain or partially linked
  generations stay available for operator inspection instead of being stopped.
- Upgrade work belongs to the supervisor context, not to the requesting SSH/HTTP
  connection. Losing a client does not cancel cutover or drain.

The private API supports `GET /status` and `POST /upgrade` over a Unix socket
with mode `0600` inside a directory of mode `0700`. A response of `202` means
**accepted**, not activated. Poll status through completion. During `switching`
or `admission-unknown`, the reported `last_confirmed_active` is historical, not
a claim that current ownership has been proven.

## Canary restrictions

Run only as a dedicated unprivileged account, initially on nonprivileged ports.
Every runtime directory must be fresh and owned by this account. Public ports
and Unix origins must not already be used; no existing socket is removed.

Supported: single numeric TCP ports, gRPC, XHTTP over TCP/Unix, and the same
listener set for every generation. REALITY/TLS is handled by the Xray process.
The pilot rejects Hysteria, XHTTP/3, API inbounds, port ranges, and socket options
it does not preserve. It does not accept an ordinary NodeX config with its
management API: user updates and traffic accounting have not been integrated.

The runtime marker intentionally survives shutdown and prevents starting over
live or uncertain state. There is no automated listener/child recovery yet.
Children are not tied to the requesting client or forcibly killed when the
supervisor is abruptly lost, but a crash can leave admission and peer state
uncertain. Do not delete the marker and rebind to "recover" a live node.
An orderly supervisor termination stops both children and therefore is **not**
a seamless update operation.

Only the binary and config bytes are snapshotted. Use absolute paths for
certificates and other dependencies and do not change them during the canary.
Supply `XRAY_LOCATION_ASSET` pointing at a release-specific read-only asset
directory when assets are not in the standard Xray locations. Complete asset
snapshotting and production privilege separation remain prerequisites.

## Build and exercise

```sh
go build -o /tmp/nodex-xray-supervisor ./cmd/nodex-xray-supervisor
CGO_ENABLED=0 go build -tags xray_server -o /tmp/xray-managed ./main

# Run under the dedicated account, with unused listeners in this config.
/tmp/nodex-xray-supervisor \
  -runtime /run/user/1000/nodex-canary \
  -binary /tmp/xray-managed \
  -config /absolute/private/canary-config.json \
  -probe /absolute/private/verify-new-tunnels serve

# From another terminal (flags precede the action):
/tmp/nodex-xray-supervisor -runtime /run/user/1000/nodex-canary status
/tmp/nodex-xray-supervisor -runtime /run/user/1000/nodex-canary \
  -binary /absolute/releases/new-xray \
  -config /absolute/private/new-config.json upgrade
```

### Probe contract

The trusted executable receives the generation ID as its single argument and
`NODEX_CANDIDATE_PID` / `NODEX_CANDIDATE_SHA256` in the environment. It must
create **new** real tunnel work through every configured public inbound,
exchange data, and independently prove that the candidate owns the accepted
work. A local control response, an open port, or `run -test` is insufficient.
It runs after the first generation starts and after each admission switch.
A nonzero exit causes rollback. The operation has a bounded startup/probe
deadline, but there is no forced expiration of established user sessions.

There is no production built-in probe yet. Passing `/bin/true` defeats this
contract and must not be used to claim a verified upgrade.

## Regression test

```sh
XRAY_TEST_GENERATION_BINARY=/tmp/xray-managed \
  go test -race -count=1 ./cmd/nodex-xray-supervisor
```

The test holds gRPC and Unix XHTTP sessions on A; admits B and independently
verifies both transports; injects a probe failure; proves old A and admitted B
work remain live after rollback; rejects a concurrent upgrade during drain;
then rolls to C and verifies A's work survives until client completion. It also
checks detached request lifetime, persisted state, release cleanup, unsupported
config rejection, and refusal to replace an existing Unix listener.

## Required before production enablement

1. Recover listening descriptors, generation ownership, and pending jobs after
   supervisor loss without rebinding or terminating active work.
2. Add managed NodeX API endpoints, fan-out user mutations, and aggregate/final
   statistics from every live generation without duplication.
3. Fence and settle Hysteria Initial/handshake work and ingress queues before
   allowing QUIC generation retirement; preserve wildcard destination metadata,
   routing keys, slots, port masks and redirects across recovery.
4. Integrate immutable assets, service ownership, watchdog coordination, update
   locks and capability-preserving binds, then provide a NodeX protocol-aware
   probe and explicit TUI activation/status commands.
5. Validate all production gRPC/REALITY, XHTTP/REALITY, VXT/Nginx and Go/Rust
   Hysteria combinations under load, including update failure and recovery.

Do not change the ordinary restart-based NodeX update path to use this canary
until these requirements are met.
