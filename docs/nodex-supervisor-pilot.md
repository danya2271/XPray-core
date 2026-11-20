# NodeX TCP supervisor canary

> Historical TCP-only pilot notes. The current integrated lifecycle, including
> Go/native Hysteria drain, persistent broker recovery, API/counters and the
> opt-in NodeX/TUI path, is documented in
> [nodex-managed-lifecycle.md](nodex-managed-lifecycle.md). Restrictions below
> describe the original pilot; production acceptance is still incomplete.

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
live or uncertain state. The explicit `recover` command supports a confirmed
single active TCP generation: it acquires the supervisor lock, obtains copies
of the same listening sockets from the surviving Xray, and validates the boot
ID, process start time, executable hash, and exact config snapshot. Signals
and exit observation use a pidfd, never a possibly reused numeric PID.
Recovery neither rebinds public sockets nor changes admission. A rejected
recovery leaves the surviving Xray alone. Configurations without a matching
config snapshot, and states with a pending switch, drain or UDP generation,
remain blocked. Do not delete the marker and rebind to "recover" a live node.
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

# After abrupt supervisor loss, only from a confirmed single-active state:
/tmp/nodex-xray-supervisor -runtime /run/user/1000/nodex-canary \
  -binary /absolute/releases/active-xray \
  -config /absolute/private/exact-active-config.json \
  -probe /absolute/private/verify-new-tunnels recover
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
It also kills the supervisor with SIGKILL, checks a surviving gRPC session,
rejects recovery with the wrong config, recovers the original process without
rebinding, and completes a later upgrade while preserving the old session.

`nginx_canary_test.py` additionally accepts a DanVlessBot
`vless_xhttp_tls_test` executable with the `--streaming-fixture` entrypoint. It
uses the NodeX-generated Nginx configuration and real VLESS clients, proves new
connections reached B using distinct backend responses, and keeps old VXT
`stream-up` and white `packet-up` tunnels exchanging data on A until closure:

```sh
python3 cmd/nodex-xray-supervisor/nginx_canary_test.py \
  /tmp/xray-managed /tmp/nodex-xray-supervisor \
  /absolute/DanVlessBot/build/vless_xhttp_tls_test
# Repeat with --http2-upstream to exercise the generated HTTP/2 origin route.
```

This optional integration fixture is provided by the companion repository;
older builds without that entrypoint cannot run the test. The supervisor code
does not modify or install the generated Nginx configuration on real nodes.

## Required before production enablement

1. Extend recovery to pending switches/drains and UDP ingress ownership without
   rebinding or terminating active work; single-active TCP recovery exists.
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
