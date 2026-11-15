//! Loopback-only protocol interoperability fixture, not a routing proxy.
//! Echoes authenticated TCP/UDP data while preserving Hysteria addresses.

use bytes::{Buf, Bytes};
use h3::quic::{self, RecvStream as _, SendStream as _};
use quinn::Runtime;
use std::{
    collections::VecDeque,
    env,
    error::Error,
    fs::File,
    future::poll_fn,
    io::{BufReader, Write},
    net::{SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::task::JoinSet;
use xhysteria::{
    protocol,
    socket::MaskedSocket,
    udp::{Defragger, Message},
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Raw = h3_quinn::BidiStream<Bytes>;

struct Replay {
    raw: Raw,
    prefix: VecDeque<Bytes>,
}

impl quic::RecvStream for Replay {
    type Buf = Bytes;
    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Option<Bytes>, quic::StreamErrorIncoming>> {
        if let Some(data) = self.prefix.pop_front() {
            return Poll::Ready(Ok(Some(data)));
        }
        self.raw.poll_data(cx)
    }
    fn stop_sending(&mut self, code: u64) {
        self.raw.stop_sending(code)
    }
    fn recv_id(&self) -> quic::StreamId {
        self.raw.recv_id()
    }
}

impl quic::SendStream<Bytes> for Replay {
    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), quic::StreamErrorIncoming>> {
        self.raw.poll_ready(cx)
    }
    fn poll_finish(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), quic::StreamErrorIncoming>> {
        self.raw.poll_finish(cx)
    }
    fn send_data<T: Into<quic::WriteBuf<Bytes>>>(
        &mut self,
        data: T,
    ) -> std::result::Result<(), quic::StreamErrorIncoming> {
        self.raw.send_data(data)
    }
    fn reset(&mut self, code: u64) {
        self.raw.reset(code)
    }
    fn send_id(&self) -> quic::StreamId {
        self.raw.send_id()
    }
}

impl quic::SendStreamUnframed<Bytes> for Replay {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut D,
    ) -> Poll<std::result::Result<usize, quic::StreamErrorIncoming>> {
        self.raw.poll_send(cx, buf)
    }
}

struct Dispatch(h3_quinn::Connection);
struct Opener(h3_quinn::OpenStreams);

macro_rules! open_streams {
    ($ty:ty) => {
        impl quic::OpenStreams<Bytes> for $ty {
            type BidiStream = Replay;
            type SendStream = h3_quinn::SendStream<Bytes>;
            fn poll_open_bidi(
                &mut self,
                cx: &mut Context<'_>,
            ) -> Poll<std::result::Result<Replay, quic::StreamErrorIncoming>> {
                self.0.poll_open_bidi(cx).map_ok(|raw| Replay {
                    raw,
                    prefix: VecDeque::new(),
                })
            }
            fn poll_open_send(
                &mut self,
                cx: &mut Context<'_>,
            ) -> Poll<std::result::Result<Self::SendStream, quic::StreamErrorIncoming>> {
                self.0.poll_open_send(cx)
            }
            fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
                <_ as quic::OpenStreams<Bytes>>::close(&mut self.0, code, reason)
            }
        }
    };
}
open_streams!(Dispatch);
open_streams!(Opener);

impl quic::Connection<Bytes> for Dispatch {
    type RecvStream = h3_quinn::RecvStream;
    type OpenStreams = Opener;
    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Self::RecvStream, quic::ConnectionErrorIncoming>> {
        <h3_quinn::Connection as quic::Connection<Bytes>>::poll_accept_recv(&mut self.0, cx)
    }
    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Replay, quic::ConnectionErrorIncoming>> {
        <h3_quinn::Connection as quic::Connection<Bytes>>::poll_accept_bidi(&mut self.0, cx).map_ok(
            |raw| Replay {
                raw,
                prefix: VecDeque::new(),
            },
        )
    }
    fn opener(&self) -> Opener {
        Opener(<h3_quinn::Connection as quic::Connection<Bytes>>::opener(
            &self.0,
        ))
    }
}

async fn classify(mut stream: Replay) -> Result<(Replay, bool)> {
    let mut prefix = [0u8; 8];
    let mut length = 0;
    loop {
        let data = poll_fn(|cx| stream.raw.poll_data(cx))
            .await?
            .ok_or("empty request stream")?;
        let copied = data.len().min(8 - length);
        prefix[length..length + copied].copy_from_slice(&data[..copied]);
        length += copied;
        stream.prefix.push_back(data);
        match protocol::read_varint(&prefix[..length]) {
            Ok((kind, _)) => return Ok((stream, kind == protocol::TCP_REQUEST)),
            Err(protocol::Error::Incomplete) => (),
            Err(error) => return Err(error.into()),
        }
    }
}

async fn write_raw<S: quic::SendStreamUnframed<Bytes>>(
    stream: &mut S,
    mut data: Bytes,
) -> Result<()> {
    while data.has_remaining() {
        let written = poll_fn(|cx| stream.poll_send(cx, &mut data)).await?;
        if written == 0 {
            return Err("zero-byte stream write".into());
        }
    }
    Ok(())
}

async fn echo_tcp(mut stream: Replay, authenticated: Arc<AtomicBool>) -> Result<()> {
    if !authenticated.load(Ordering::Acquire) {
        stream.stop_sending(0x101);
        stream.reset(0x101);
        return Ok(());
    }
    let mut header = Vec::new();
    loop {
        let data = poll_fn(|cx| stream.poll_data(cx))
            .await?
            .ok_or("incomplete TCP request")?;
        header.extend_from_slice(&data);
        match protocol::parse_tcp_request(&header) {
            Ok((_, consumed)) => {
                write_raw(
                    &mut stream,
                    Bytes::from(protocol::encode_tcp_response(true, b"", b"native-echo")?),
                )
                .await?;
                write_raw(&mut stream, Bytes::copy_from_slice(&header[consumed..])).await?;
                break;
            }
            Err(protocol::Error::Incomplete)
                if header.len() <= protocol::MAX_ADDRESS + protocol::MAX_PADDING + 32 => {}
            Err(error) => return Err(error.into()),
        }
    }
    while let Some(data) = poll_fn(|cx| stream.poll_data(cx)).await? {
        write_raw(&mut stream, data).await?;
    }
    poll_fn(|cx| stream.poll_finish(cx)).await?;
    Ok(())
}

async fn echo_udp(connection: quinn::Connection, authenticated: Arc<AtomicBool>) -> Result<()> {
    let start = Instant::now();
    let mut defrag = Defragger::new(8)?;
    loop {
        let data = connection.read_datagram().await?;
        if !authenticated.load(Ordering::Acquire) {
            continue;
        }
        let Ok(message) = Message::decode(&data) else {
            continue;
        };
        let Ok(Some(datagram)) = defrag.feed(start.elapsed().as_nanos() as u64, &message) else {
            continue;
        };
        let response = Message {
            session: datagram.session,
            packet: message.packet,
            fragment: 0,
            count: 1,
            address: &datagram.address,
            payload: &datagram.payload,
        };
        let limit = connection
            .max_datagram_size()
            .ok_or("no DATAGRAM support")?;
        for part in response.fragment(limit)? {
            let mut wire = Vec::new();
            part.encode(&mut wire)?;
            connection.send_datagram_wait(Bytes::from(wire)).await?;
        }
    }
}

async fn serve(connection: quinn::Connection, password: String) -> Result<()> {
    let authenticated = Arc::new(AtomicBool::new(false));
    let mut handlers = JoinSet::<Result<()>>::new();
    handlers.spawn(echo_udp(connection.clone(), authenticated.clone()));
    let mut classifiers = JoinSet::new();
    let mut h3 =
        h3::server::Connection::new(Dispatch(h3_quinn::Connection::new(connection.clone())))
            .await?;
    loop {
        tokio::select! {
            accepted = poll_fn(|cx| h3.poll_accept_request_stream(cx)), if classifiers.len() < 32 && handlers.len() < 32 => {
                match accepted {
                    Ok(Some(stream)) => { classifiers.spawn(async { tokio::time::timeout(Duration::from_secs(10), classify(stream)).await? }); }
                    _ => break,
                }
            }
            Some(classified) = classifiers.join_next(), if !classifiers.is_empty() => {
                let Ok(Ok((stream, tcp))) = classified else { continue };
                if tcp {
                    let authenticated = authenticated.clone();
                    handlers.spawn(async move { tokio::time::timeout(Duration::from_secs(15), echo_tcp(stream, authenticated)).await? });
                } else {
                    let resolver = h3.create_resolver(h3::frame::FrameStream::new(h3::stream::BufRecvStream::new(stream)));
                    let authenticated = authenticated.clone();
                    let password = password.clone();
                    handlers.spawn(async move {
                        tokio::time::timeout(Duration::from_secs(10), async {
                            let (request, mut stream) = resolver.resolve_request().await?;
                            let valid = request.method() == "POST" && request.uri().host() == Some("hysteria")
                                && request.uri().path() == "/auth"
                                && request.headers().get("Hysteria-CC-RX").and_then(|h| h.to_str().ok()).and_then(|h| h.parse::<u64>().ok()).is_some()
                                && request.headers().get("Hysteria-Auth").is_some_and(|v| bool::from(v.as_bytes().ct_eq(password.as_bytes())));
                            let response = if valid {
                                authenticated.store(true, Ordering::Release);
                                http::Response::builder().status(233).header("Hysteria-UDP", "true").header("Hysteria-CC-RX", "0")
                                    .header("Hysteria-Padding", "e".repeat(256)).body(())?
                            } else {
                                http::Response::builder().status(404).body(())?
                            };
                            stream.send_response(response).await?;
                            stream.finish().await?;
                            Ok(())
                        }).await?
                    });
                }
            }
            Some(completed) = handlers.join_next(), if !handlers.is_empty() => {
                if let Ok(Err(error)) = completed { eprintln!("xhys-echo stream: {error}"); }
            }
            _ = connection.closed() => break,
        }
    }
    classifiers.abort_all();
    handlers.abort_all();
    while classifiers.join_next().await.is_some() {}
    while handlers.join_next().await.is_some() {}
    Ok(())
}

async fn run() -> Result<()> {
    let mut args = env::args().skip(1);
    let mut options = std::collections::HashMap::new();
    while let Some(key) = args.next() {
        if ![
            "--listen",
            "--cert",
            "--key",
            "--auth-env",
            "--salamander-env",
        ]
        .contains(&key.as_str())
        {
            return Err("unknown option".into());
        }
        let value = args.next().ok_or("missing option value")?;
        if options.insert(key, value).is_some() {
            return Err("duplicate option".into());
        }
    }
    let get = |key: &str| -> Result<&str> {
        options
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| format!("missing {key}").into())
    };
    let address: SocketAddr = get("--listen")?.parse()?;
    if !address.ip().is_loopback() {
        return Err("echo fixture requires a loopback listen address".into());
    }
    let password =
        env::var(get("--auth-env")?).map_err(|_| "authentication environment variable missing")?;
    if password.is_empty() {
        return Err("empty authentication password".into());
    }
    let certificates = rustls_pemfile::certs(&mut BufReader::new(File::open(get("--cert")?)?))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(get("--key")?)?))?
        .ok_or("missing private key")?;
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certificates, key)?;
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls)?,
    ));
    let mut transport = quinn::TransportConfig::default();
    transport
        .max_concurrent_bidi_streams(64u32.into())
        .max_idle_timeout(Some(Duration::from_secs(15).try_into()?));
    transport.datagram_receive_buffer_size(Some(256 * 1024));
    transport.datagram_send_buffer_size(256 * 1024);
    transport.assume_peer_max_datagram_frame_size(Some(1200));
    if options.contains_key("--salamander-env") {
        transport
            .initial_mtu(1200)
            .min_mtu(1200)
            .mtu_discovery_config(None);
    }
    server.transport_config(Arc::new(transport));
    let runtime = Arc::new(quinn::TokioRuntime);
    let raw = UdpSocket::bind(address)?;
    raw.set_nonblocking(true)?;
    let socket: Arc<dyn quinn::AsyncUdpSocket> =
        if let Some(variable) = options.get("--salamander-env") {
            let psk = env::var(variable).map_err(|_| "Salamander environment variable missing")?;
            Arc::new(MaskedSocket::new(raw, psk.as_bytes())?)
        } else {
            runtime.wrap_udp_socket(raw)?
        };
    let endpoint = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        Some(server),
        socket,
        runtime,
    )?;
    println!("listen={}", endpoint.local_addr()?);
    std::io::stdout().flush()?;
    // The fixture serves one connection and exits; no unattended proxy listener.
    let connection = endpoint.accept().await.ok_or("endpoint closed")?.await?;
    let result = serve(connection, password).await;
    endpoint.close(0x100u32.into(), b"fixture complete");
    endpoint.wait_idle().await;
    result
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match tokio::time::timeout(Duration::from_secs(60), run()).await {
        Ok(Ok(())) => (),
        result => {
            match result {
                Ok(Err(error)) => eprintln!("xhys-echo: {error}"),
                Err(_) => eprintln!("xhys-echo: fixture timed out"),
                _ => unreachable!(),
            }
            std::process::exit(1);
        }
    }
}
