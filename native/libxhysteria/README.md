# Embedded native Hysteria prototype

`libxhysteria.a` runs QUIC, TLS, HTTP/3 authentication, Salamander, TCP framing,
UDP fragmentation/reassembly, and congestion control on Rust-owned threads.
The Go adapter connects these flows to Xray routing, policy, and byte counters.
The normal Go backend remains available.

## Build and run on Linux

Requires Rust 1.88+, Cargo, Go matching `go.mod`, and a C compiler. From the
repository root:

```sh
bash native/libxhysteria/build-linux.sh
```

The output is `build_assets/xray-native`, with the Rust archive linked into the
executable. No separate Hysteria process or `libxhysteria.so` is used. System
libraries such as libc are still dynamically linked.

Enable the prototype with `XRAY_HYSTERIA_NATIVE=1`. Existing Hysteria JSON stays
valid, but `streamSettings.finalmask.quicParams.disableChromeParrot` must be true.
The switch applies to both Hysteria inbounds and outbounds in that process.
Without the environment variable, the executable uses the existing Go backend.

For a local client/server test, create a one-day localhost certificate in the
current directory:

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout key.pem -out cert.pem -days 1 -subj /CN=localhost \
  -addext subjectAltName=DNS:localhost -addext basicConstraints=critical,CA:FALSE
XRAY_LOCATION_CERT="$PWD" XRAY_HYSTERIA_NATIVE=1 build_assets/xray-native run -c native/libxhysteria/examples/server.json
```

Run the client in another terminal from the same directory:

```sh
XRAY_LOCATION_CERT="$PWD" XRAY_HYSTERIA_NATIVE=1 build_assets/xray-native run -c native/libxhysteria/examples/client.json
```

The client exposes SOCKS5 at `127.0.0.1:1080`, with TCP and UDP enabled. The
examples listen on loopback and contain test passwords. The commands set Xray's
certificate directory to the current directory. Certificate files require
`oneTimeLoading: true`; inline PEM certificates are also supported.

## Verify

```sh
bash native/libxhysteria/test-linux.sh
```

Tests exercise the embedded backend through Xray's real inbound/outbound
registration and routing. They cover native-to-Go, Go-to-native, native-to-native,
Salamander, TCP, fragmented 4096-byte UDP, authentication/TLS rejection, concurrent
TCP, and negotiated Brutal. Standalone `xhys-probe` and `xhys-echo` fixtures remain
available with `cargo build --locked --features probe --bins` in this directory.
The echo fixture is an echo endpoint, not a routing proxy.

## Prototype boundaries

This implementation is a copying, bounded socket-IPC baseline. Go uses netpoll
on a Unix socket and CGO only for start/version/join; traffic does not call CGO.
The shared-memory ring experiment in `src/ffi.rs` is not the active traffic path.
No zero-copy or performance improvement is claimed yet.

One native worker/QUIC connection is created for each outbound flow; the server
uses one native worker per inbound listener. Connection pooling is still pending.
Per-flow credits bound queued TCP data and apply backpressure; unread UDP
datagrams are dropped when receive credits run out. Flow limits are currently
128 per engine. QUIC's own buffers also consume memory.

The prototype supports Linux; Android has a CGO/Unix code path but has not yet
been cross-built or tested. Windows notification/socket integration is pending.
Go creates and configures tunnel UDP sockets with Xray's system dialer/listener,
then Rust duplicates the descriptor and owns packet I/O. This preserves socket
controllers during setup, including Android protection where the embedding app
provides it; network handover and replacement sockets still need validation.

Only Salamander is supported as a UDP mask, and only 404 HTTP masquerading is
implemented. Unsupported masks, advanced TLS options, certificate reload,
custom QUIC windows/timers, and user mutation are rejected explicitly. Restart
the inbound to change its users. The native engine uses rustls/ring certificate
verification with bundled public roots and configured verification certificates;
it does not implement Xray's ECH or QUIC/TLS fingerprint behavior.

Brutal uses packet-count loss compensation and its own pacer. Rate negotiation
matches this fork's zero-rate/BBR fallback policy. `force-brutal` requires a
positive send rate. Adaptive BBR uses Quinn's experimental BBR controller; its
behavior does not reproduce Xray's BBR profiles. `reno` selects Quinn NewReno.

## Dependency patch

`vendor/quinn-proto` is the crates.io `quinn-proto 0.11.19` source at upstream
commit `8192ed399a26a6e8f0aba43d10ec8badaa5b905e`. Its MIT/Apache licenses are
retained. `Cargo.lock` pins the complete dependency graph.

Local changes add:

- An opt-in fallback for peers that omit `max_datagram_frame_size`, needed by
  this fork's Go client. Standard Quinn behavior remains the default.
- Defaulted controller hooks for exact lost-packet counts, pacing deadlines,
  and send-window admission. Existing controllers retain their default behavior.

The native tools enable the omitted-parameter fallback only for Hysteria. The
engine disables GSO while its custom pacer accounts individual transmissions.
