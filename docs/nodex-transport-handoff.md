# NodeX transport handoff

This branch provides transport building blocks and a managed-process pilot
for gRPC. Normal `xray run` does not enable managed mode. Do not install this
with the expectation that restarting the service will preserve a session.

| Transport | Building block | Coverage |
| --- | --- | --- |
| gRPC | A paused `handoff.TCPListener` imports a duplicate of the same listen socket. `grpc.ServeListener` serves an existing listener and `Drain` waits for its accepted RPCs. | Two actual `xray run` processes are tested over gRPC and gRPC/REALITY on the same TCP port. Existing RPCs survive cutover and a failed-probe rollback on both processes. |
| XHTTP | `SetHandoffPeer` seals the previous generation against new session IDs. Unknown IDs are resolved through `HandoffHandler`; known local IDs stay on the fast path. Only requests received by the wrong generation go over its private Unix socket. | Path and header IDs, `packet-up` and `stream-up`, uplink-before-downlink, and loss of the owner lookup socket. The transport tests do not exercise a complete REALITY handshake or Nginx cutover. |
| Hysteria | `ListenGeneration` accepts a demultiplexed packet socket. Go and Rust issue opaque generation-aware QUIC IDs. `QUICRoutes` pins retransmitted Initials and routes established packets by server CID, independent of the peer address. | A live Hysteria stream stays on A while a new connection reaches B on the same UDP port. Tested with Go, Rust, and Salamander. The test demultiplexer is not the production ingress; tests do not exercise port hopping or a rolling process restart. |

## Managed-process pilot

On Linux, `xray run -handoff-manifest /path/to/manifest.json` starts with public
admission paused. The supervisor inherits/passes the listening descriptors;
Xray never rebinds a managed port in a new process:

```json
{
  "control": "/run/xray-pilot/generation-a/control.sock",
  "tcp": [{"tag": "inbound-vg-443", "port": 443, "fd": 3}]
}
```

The supervisor must create the parent directory (mode `0700`) and configure a
separate private API port for each generation. The pilot accepts managed gRPC
TCP inbounds and a regular `api` on loopback. Any other transport or an
unclaimed binding prevents readiness; Unix, XHTTP and Hysteria cannot be
activated in managed mode yet.

The private socket takes one newline-delimited `status`, `pause`, `resume` or
`drain` command per connection and sends versioned JSON. `handoff.Switch`
checks both generations, switches admission, and runs a caller-supplied probe
that must establish a new session and distinguish B's result from A's.
On a failed probe the gates switch back without terminating either process.
If gate ownership cannot be established, both processes are retained and the
result is `unknown`; it is not reported as a successful rollback. Only after
a successful switch has been verified and persisted should the caller issue
`drain`. It is irreversible for gRPC. A process reporting `drained` reached
gRPC's graceful shutdown completion; `paused` alone is not safe to stop.

Test with real processes from the checkout:

```sh
CGO_ENABLED=0 go build -tags xray_server -o /tmp/xray-generation ./main
XRAY_TEST_GENERATION_BINARY=/tmp/xray-generation \
  go test -race -run '^TestManagedXrayProcesses$' ./transport/internet/grpc
```

## Before enabling NodeX activation

- A persistent node supervisor must own shared listening sockets and their
  lifetime. It must start both independent Xray processes with separate local
  API endpoints, immutable config and assets, and inherited listener
  descriptors (rather than creating competing listeners on the same port).
- The supervisor must start private peer sockets for every XHTTP inbound,
  publish an admission change for all inbounds together, and track generation
  dependencies including forwarded requests on both sides. A public HTTP
  server must not expose `HandoffHandler`.
- The UDP ingress must process any masks once, preserve addressing information,
  and provide per-generation socket views without closing unrelated generations.
  CID keys and slot identity must survive supervisor reconnects; slots cannot be
  reused until their old CIDs cannot be received. Retiring a generation requires
  accounting for live QUIC connections, not just currently open proxy streams.
- Running-binary readiness, user mutation, traffic accounting, update locking,
  job recovery and verified rollback must be coordinated with NodeX. A health
  check or watchdog must not restart the other generation during a drain.
- Run a complete mixed-node upgrade with live REALITY, VXT origin and Hysteria
  traffic, port hopping, and both implementations of Hysteria. Measure
  end-to-end throughput, CPU, drops and tail latency; `BenchmarkQUICEstablishedRoute`
  measures classification alone, not packet transport or deobfuscation.

The initial migration from a running version without transport support has to
be managed separately. Its existing sockets, XHTTP IDs and QUIC state are not
automatically imported by these building blocks.
