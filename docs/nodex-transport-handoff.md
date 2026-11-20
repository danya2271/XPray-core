# NodeX transport handoff

This branch provides a managed-process pilot for gRPC, XHTTP and Hysteria. It
does not enable managed mode for a normal `xray run` or make a service restart
seamless. Automatic retirement of a generation with Hysteria is not enabled.

The [opt-in TCP supervisor canary](nodex-supervisor-pilot.md) drives this API
for isolated gRPC/XHTTP configs without installing or replacing the NodeX
production service.

| Transport | Building block | Coverage |
| --- | --- | --- |
| gRPC | A paused `handoff.TCPListener` imports a duplicate of the same listen socket. `grpc.ServeListener` serves an existing listener and `Drain` waits for its accepted RPCs. | Two actual `xray run` processes are tested over gRPC and gRPC/REALITY on the same TCP port. Existing RPCs survive cutover and a failed-probe rollback on both processes. |
| XHTTP | Managed processes share TCP or Unix listeners without rebinding. `SetHandoffPeer` seals the previous generation against new IDs. Only requests received by the wrong generation go over its private Unix socket. The retiring process counts both unfinished sessions and active or forwarded requests. | Two processes over XHTTP/REALITY and a shared Unix origin. A response forwarded through A keeps it in drain even if B owns that session. Transport tests also cover header IDs, `stream-up`, uplink-before-downlink, and loss of the lookup socket. A mixed roll through production Nginx is not tested here. |
| Hysteria | A shared `UDPIngress` sends datagrams to independently launched Xray processes over connected Unix datagram sockets. It applies the public socket's mask once and keeps established QUIC CIDs with their owner. Rust reads the existing native packet wire without a second Go relay. | Go and Rust on IPv4 and IPv6; Salamander and Linux IPv4 `REDIRECT` port hops. An existing TCP stream and an application UDP flow keep exchanging data on A after cutover. New connections after a failed probe go back to A without closing sessions on B. |

## Managed-process pilot

On Linux, `xray run -handoff-manifest /path/to/manifest.json` starts with public
admission paused. The supervisor inherits/passes the listening descriptors;
Xray never rebinds a managed port in a new process:

```json
{
  "control": "/run/xray-pilot/generation-a/control.sock",
  "tcp": [{"tag": "inbound-vg-443", "port": 443, "fd": 3}],
  "unix": [{"tag": "inbound-vxt-8443", "path": "/dev/shm/xrxh-8443.socket", "fd": 4}]
}
```

The supervisor must create the parent directory (mode `0700`) and configure a
separate private API port for each generation. The pilot accepts managed gRPC
TCP inbounds, XHTTP over TCP or Unix stream sockets, and a regular `api` on
loopback, plus inherited Hysteria datagram channels (`udp` bindings with tag,
numeric address, port, FD, 16-byte routing key and generation slot). XHTTP/3,
unsupported transports, and unclaimed bindings prevent readiness. The supervisor owns both the socket file and its
permissions; disposing of one process cannot unlink the public Unix origin.

The private socket takes one newline-delimited command per connection and
sends versioned JSON. Use `handoff.LinkXHTTP` (`link <peer directory>` on
each) to validate XHTTP peer configuration before changing admission. Then
`handoff.Switch` (`status`, `pause`, `resume`)
checks both generations, switches admission, and runs a caller-supplied probe
that must establish a new session and distinguish B's result from A's.
On a failed probe the gates switch back without terminating either process.
If gate ownership cannot be established, both processes are retained and the
result is `unknown`; it is not reported as a successful rollback. Only after
a successful switch has been verified and persisted should the caller issue
`drain`. It is irreversible. A process reporting `drained` has received
completion from every managed transport; `paused` alone is not safe to stop.
For XHTTP, issue `unlink` on the active generation before stopping the old
one. The API refuses to unlink a peer that hasn't settled. During a failed
cutover keep both generations while established work survives.

For Hysteria verify `quic-status` on each generation and drive the external
selector with `handoff.SwitchWithIngress`. A process's `paused` state does not
change UDP routing by itself. The relay currently requires an explicitly
bound address, not a wildcard (per-packet local destination metadata for
multihomed nodes is not wired yet). The relay adds one Unix datagram hop per
direction. `BenchmarkManagedUDPIngressRoundTrip` measures that overhead on an
unmasked loopback path, not throughput or latency on a VPS. Retain the old process: `drain` refuses to
declare completion when UDP bindings are present. `quic-status` counts accepted
QUIC connections, not application streams. `work_known` / `transport_work`
also expose a diagnostic snapshot of Go connection contexts before acceptance,
or Rust endpoint connections plus application permits. Rust may count a
connection twice and its snapshot omits Initials queued in Quinn before accept.
Neither value proves that the worker or engine packet queues are empty.

`UDPIngress.FenceInitials` waits for the inactive slot's client-chosen
Initial/0-RTT pins to expire and synchronizes with the ingress receive loop.
A concurrent admission change invalidates the result. Established CID routing
stays available; no synchronization was added to its fast path. This is one
prerequisite, **not** permission to stop a generation: server-issued CIDs, the
worker queue, and the engine's own pending handshakes still need coordination.

Test with real processes from the checkout:

```sh
CGO_ENABLED=0 go build -tags xray_server -o /tmp/xray-generation ./main
XRAY_TEST_GENERATION_BINARY=/tmp/xray-generation \
  go test -race -run '^TestManaged(Xray|XHTTP)Processes$' ./transport/internet/{grpc,splithttp}
```

To run the Hysteria process test with real IPv4 port redirects, use a private
network namespace, and point `XRAY_TEST_GENERATION_BINARY` at a native-enabled
Xray from this branch:

```sh
export XRAY_TEST_UDP_NAT_HOST_NS=$(readlink /proc/self/ns/net)
export XRAY_TEST_ISOLATED_UDP_NAT=1 XRAY_TEST_MANAGED_NATIVE=1
unshare -Urn sh -c 'ip link set lo up && go test -race -run "^TestManagedHysteriaProcesses$" ./transport/internet/hysteria'
```

## Before enabling NodeX activation

- A persistent node supervisor must own shared listening sockets and their
  lifetime. It must start both independent Xray processes with separate local
  API endpoints, immutable config and assets, and inherited listener
  descriptors (rather than creating competing listeners on the same port).
- The supervisor must link, switch, and unlink XHTTP peers as described above.
  A public HTTP server must not expose `HandoffHandler`.
- The supervisor must configure the shared UDP ingress from the validated node
  config, preserve local destination metadata for wildcard binds, and own all
  associated port redirects without changing the live rule set during a switch.
  CID keys and slot identity must survive supervisor reconnects; slots cannot be
  reused until their old CIDs cannot be received. Retiring a generation requires
  accounting for live QUIC connections, not just currently open proxy streams.
  `GenerationConnections` is necessary but not sufficient: the supervisor
  must also fence new work and account for encrypted packets still queued
  for the generation and Initial/handshake work not accepted by the engine yet.
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
