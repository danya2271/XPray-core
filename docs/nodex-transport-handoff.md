# NodeX transport handoff

This branch provides transport building blocks, not an executable upgrade
workflow. `xray run` does not enable them by default. Do not install it with
the expectation that restarting the service will preserve a session.

| Transport | Building block | Coverage |
| --- | --- | --- |
| gRPC | A paused `handoff.TCPListener` imports a duplicate of the same listen socket. `grpc.ServeListener` serves an existing listener and `Drain` waits for its accepted RPCs. | Existing gRPC streams continue after a switch on the same port. The TCP descriptor is also exercised across independent processes. |
| XHTTP | `SetHandoffPeer` seals the previous generation against new session IDs. Unknown IDs are resolved through `HandoffHandler`; known local IDs stay on the fast path. Only requests received by the wrong generation go over its private Unix socket. | Path and header IDs, `packet-up` and `stream-up`, uplink-before-downlink, and loss of the owner lookup socket. The transport tests do not exercise a complete REALITY handshake or Nginx cutover. |
| Hysteria | `ListenGeneration` accepts a demultiplexed packet socket. Go and Rust issue opaque generation-aware QUIC IDs. `QUICRoutes` pins retransmitted Initials and routes established packets by server CID, independent of the peer address. | A live Hysteria stream stays on A while a new connection reaches B on the same UDP port. Tested with Go, Rust, and Salamander. The test demultiplexer is not the production ingress; tests do not exercise port hopping or a rolling process restart. |

## Before enabling activation

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
