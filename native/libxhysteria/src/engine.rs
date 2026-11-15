//! First embedded engine: native QUIC workers and bounded framed socket IPC.
//! The socket bridge is a copying correctness baseline for a future arena path.

use crate::{
    congestion,
    h3_transport::{self, Dispatch, Replay, Result},
    protocol,
    socket::MaskedSocket,
    udp::{Defragger, Message},
};
use bytes::Bytes;
use h3::quic::{RecvStream as _, SendStream as _};
use quinn::{AsyncUdpSocket, Runtime};
use serde::Deserialize;
use std::{
    collections::HashMap,
    ffi::c_void,
    future::poll_fn,
    io::{self, BufReader},
    net::UdpSocket,
    os::fd::{FromRawFd, IntoRawFd},
    os::unix::net::UnixStream,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, Semaphore, mpsc},
    task::JoinSet,
};

const OPEN_TCP: u8 = 1;
const OPEN_UDP: u8 = 2;
const DATA: u8 = 3;
const DATAGRAM: u8 = 4;
const FIN: u8 = 5;
const CLOSE: u8 = 6;
const OPENED: u8 = 7;
const INCOMING_TCP: u8 = 8;
const INCOMING_UDP: u8 = 9;
const ERROR: u8 = 10;
const READY: u8 = 11;
const CREDIT: u8 = 13;
const WRITABLE: u8 = 14;
const MAX_FRAME: usize = 65536 + 4096;
const MAX_FLOWS: usize = 128;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: bool,
    pub peer: String,
    pub server_name: String,
    pub auth: String,
    pub users: Vec<String>,
    pub ca_pem: String,
    pub certificate_pem: String,
    pub key_pem: String,
    pub salamander: String,
    pub system_roots: bool,
    pub tx_bps: u64,
    pub rx_bps: u64,
    pub loss_compensation: bool,
    pub congestion: String,
    pub disable_pmtud: bool,
    #[serde(default)]
    pub max_idle_timeout: u64,
    #[serde(default)]
    pub keep_alive_period: u64,
    pub diagnostic: bool,
}

#[derive(Debug)]
struct Frame {
    kind: u8,
    id: u64,
    data: Bytes,
}

impl Frame {
    fn new(kind: u8, id: u64, data: impl Into<Bytes>) -> Self {
        Self {
            kind,
            id,
            data: data.into(),
        }
    }
}

async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Frame> {
    let mut header = [0u8; 16];
    reader.read_exact(&mut header).await?;
    let len = u32::from_be_bytes(header[12..].try_into()?) as usize;
    if len > MAX_FRAME || header[1..4] != [0; 3] {
        return Err("invalid bridge frame".into());
    }
    let mut data = vec![0; len];
    reader.read_exact(&mut data).await?;
    Ok(Frame::new(
        header[0],
        u64::from_be_bytes(header[4..12].try_into()?),
        data,
    ))
}

async fn write_frame<W: AsyncWriteExt + Unpin>(writer: &mut W, frame: Frame) -> Result<()> {
    if frame.data.len() > MAX_FRAME {
        return Err("oversized bridge frame".into());
    }
    let mut header = [0u8; 16];
    header[0] = frame.kind;
    header[4..12].copy_from_slice(&frame.id.to_be_bytes());
    header[12..].copy_from_slice(&(frame.data.len() as u32).to_be_bytes());
    writer.write_all(&header).await?;
    writer.write_all(&frame.data).await?;
    Ok(())
}

fn address_data(address: &[u8], payload: &[u8]) -> Result<Bytes> {
    if address.is_empty() || address.len() > protocol::MAX_ADDRESS {
        return Err("invalid bridge address".into());
    }
    let mut data = Vec::with_capacity(2 + address.len() + payload.len());
    data.extend_from_slice(&(address.len() as u16).to_be_bytes());
    data.extend_from_slice(address);
    data.extend_from_slice(payload);
    Ok(Bytes::from(data))
}

fn parse_address(data: &[u8]) -> Result<(&[u8], &[u8])> {
    if data.len() < 2 {
        return Err("short bridge address".into());
    }
    let n = u16::from_be_bytes(data[..2].try_into()?) as usize;
    if n == 0 || n > protocol::MAX_ADDRESS || 2 + n > data.len() {
        return Err("invalid bridge address".into());
    }
    Ok((&data[2..2 + n], &data[2 + n..]))
}

struct Bus {
    events: mpsc::Sender<Frame>,
    flows: Mutex<HashMap<u64, Flow>>,
    next: AtomicU64,
}

struct Flow {
    owner: u64,
    sender: mpsc::Sender<Frame>,
    receive_credit: Arc<Semaphore>,
}

impl Drop for Flow {
    fn drop(&mut self) {
        self.receive_credit.close();
    }
}

impl Bus {
    async fn register(&self, id: u64, owner: u64) -> Result<mpsc::Receiver<Frame>> {
        let mut flows = self.flows.lock().await;
        if flows.len() >= MAX_FLOWS || flows.contains_key(&id) {
            return Err("native flow capacity reached".into());
        }
        let (tx, rx) = mpsc::channel(16);
        flows.insert(
            id,
            Flow {
                owner,
                sender: tx,
                receive_credit: Arc::new(Semaphore::new(16)),
            },
        );
        Ok(rx)
    }
    async fn send(&self, frame: Frame) -> Result<()> {
        self.events
            .send(frame)
            .await
            .map_err(|_| "Go bridge closed".into())
    }
    async fn payload(&self, frame: Frame) -> Result<()> {
        let credit = self
            .flows
            .lock()
            .await
            .get(&frame.id)
            .map(|flow| flow.receive_credit.clone())
            .ok_or("closed native flow")?;
        credit
            .acquire()
            .await
            .map_err(|_| "closed native flow")?
            .forget();
        self.send(frame).await
    }
    async fn udp_payload(&self, frame: Frame) -> Result<()> {
        let credit = self
            .flows
            .lock()
            .await
            .get(&frame.id)
            .map(|flow| flow.receive_credit.clone());
        let Some(credit) = credit else { return Ok(()) };
        // An unread UDP session drops payloads instead of blocking other sessions.
        let Ok(permit) = credit.try_acquire() else {
            return Ok(());
        };
        permit.forget();
        self.send(frame).await
    }
    async fn done(&self, id: u64, result: Result<()>) {
        self.flows.lock().await.remove(&id);
        let frame = match result {
            Ok(()) => Frame::new(CLOSE, id, Bytes::new()),
            Err(_) => Frame::new(ERROR, id, "native flow failed"),
        };
        let _ = self.send(frame).await;
    }
    async fn close_owner(&self, owner: u64) {
        let ids: Vec<u64> = self
            .flows
            .lock()
            .await
            .iter()
            .filter(|(_, flow)| flow.owner == owner)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.done(id, Err("native connection closed".into())).await;
        }
    }
    async fn route(&self, frame: Frame) {
        let id = frame.id;
        let mut flows = self.flows.lock().await;
        if let Some(flow) = flows.get(&id) {
            if frame.kind == CREDIT {
                if flow.receive_credit.available_permits() < 16 {
                    flow.receive_credit.add_permits(1);
                }
                return;
            }
            if frame.kind == CLOSE {
                flows.remove(&id);
                return;
            }
            if flow.sender.try_send(frame).is_err() {
                // Terminate a slow flow explicitly, never discard TCP silently.
                // Dropping the sender wakes its task; its completion closes Go.
                flows.remove(&id);
            }
        }
    }
}

fn roots(config: &Config) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    if config.system_roots {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    for cert in rustls_pemfile::certs(&mut BufReader::new(config.ca_pem.as_bytes())) {
        roots.add(cert?)?;
    }
    if roots.is_empty() {
        return Err("native client has no trusted certificate roots".into());
    }
    Ok(roots)
}

fn quic_timers(config: &Config) -> Result<(Duration, Duration)> {
    const MAX_TIMER_SECONDS: u64 = i64::MAX as u64 / 1_000_000_000;
    if config.max_idle_timeout > MAX_TIMER_SECONDS || config.keep_alive_period > MAX_TIMER_SECONDS {
        return Err("native QUIC timers exceed the Go duration range".into());
    }
    let idle = if config.max_idle_timeout == 0 {
        30
    } else {
        config.max_idle_timeout
    };
    let alive = if config.keep_alive_period == 0 {
        5
    } else {
        config.keep_alive_period
    };
    Ok((Duration::from_secs(idle), Duration::from_secs(alive)))
}

fn transport(config: &Config) -> Result<quinn::TransportConfig> {
    let (idle, alive) = quic_timers(config)?;
    let mut t = quinn::TransportConfig::default();
    t.max_concurrent_bidi_streams(64u32.into())
        .max_idle_timeout(Some(idle.try_into()?))
        .keep_alive_interval(Some(alive));
    t.datagram_receive_buffer_size(Some(256 * 1024));
    t.datagram_send_buffer_size(256 * 1024);
    t.assume_peer_max_datagram_frame_size(Some(1200));
    let fallback: Arc<dyn quinn::congestion::ControllerFactory + Send + Sync> =
        if config.congestion == "reno" {
            Arc::new(quinn::congestion::NewRenoConfig::default())
        } else {
            Arc::new(quinn::congestion::BbrConfig::default())
        };
    t.congestion_controller_factory(Arc::new(congestion::Factory {
        compensate: config.loss_compensation,
        fallback,
    }));
    // The custom pacer accounts one UDP transmission per callback.
    t.enable_segmentation_offload(false);
    if config.disable_pmtud || !config.salamander.is_empty() {
        t.initial_mtu(1200).min_mtu(1200).mtu_discovery_config(None);
    }
    Ok(t)
}

fn endpoint(config: &Config, socket: UdpSocket) -> Result<quinn::Endpoint> {
    let runtime = Arc::new(quinn::TokioRuntime);
    let udp: Arc<dyn AsyncUdpSocket> = if config.salamander.is_empty() {
        runtime.wrap_udp_socket(socket)?
    } else {
        Arc::new(MaskedSocket::new(socket, config.salamander.as_bytes())?)
    };
    let server = if config.server {
        let certs = rustls_pemfile::certs(&mut BufReader::new(config.certificate_pem.as_bytes()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(config.key_pem.as_bytes()))?
            .ok_or("missing native server key")?;
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let mut s = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls)?,
        ));
        s.transport_config(Arc::new(transport(config)?));
        Some(s)
    } else {
        None
    };
    let mut ep = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        server,
        udp,
        runtime,
    )?;
    if !config.server {
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots(config)?)
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let mut client = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls)?,
        ));
        client.transport_config(Arc::new(transport(config)?));
        ep.set_default_client_config(client);
    }
    Ok(ep)
}

async fn client_auth(
    connection: &quinn::Connection,
    config: &Config,
) -> Result<(
    bool,
    h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    tokio::task::JoinHandle<()>,
)> {
    let (mut driver, mut requests) =
        h3::client::new(h3_quinn::Connection::new(connection.clone())).await?;
    // This handle is stopped by connection closure; the runtime owns its lifetime.
    let task = tokio::spawn(async move {
        let _ = poll_fn(|cx| driver.poll_close(cx)).await;
    });
    let request = http::Request::builder()
        .method("POST")
        .uri("https://hysteria/auth")
        .header("Hysteria-Auth", &config.auth)
        .header("Hysteria-CC-RX", config.rx_bps.to_string())
        .header("Hysteria-Padding", "n".repeat(256))
        .body(())?;
    let mut stream = requests.send_request(request).await?;
    stream.finish().await?;
    let response = stream.recv_response().await?;
    if response.status().as_u16() != 233 {
        task.abort();
        return Err("native authentication failed".into());
    }
    let udp = response
        .headers()
        .get("Hysteria-UDP")
        .is_some_and(|v| v == "true");
    let peer_rate = response
        .headers()
        .get("Hysteria-CC-RX")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("0");
    congestion::negotiate(connection, &config.congestion, config.tx_bps, peer_rate);
    while stream.recv_data().await?.is_some() {}
    Ok((udp, requests, task))
}

async fn relay_client_tcp(
    connection: quinn::Connection,
    frame: Frame,
    mut commands: mpsc::Receiver<Frame>,
    bus: Arc<Bus>,
) -> Result<()> {
    let target = std::str::from_utf8(&frame.data)?;
    let (mut tx, mut rx) = connection.open_bi().await?;
    tx.write_all(&protocol::encode_tcp_request(
        target.as_bytes(),
        b"native-xray",
    )?)
    .await?;
    let id = frame.id;
    let mut response = Vec::new();
    loop {
        let chunk = rx
            .read_chunk(65536, true)
            .await?
            .ok_or("truncated TCP response")?;
        response.extend_from_slice(&chunk.bytes);
        match protocol::parse_tcp_response(&response) {
            Ok(header) => {
                if !header.accepted {
                    return Err("TCP target rejected".into());
                }
                bus.send(Frame::new(OPENED, id, Bytes::new())).await?;
                if header.consumed < response.len() {
                    bus.payload(Frame::new(
                        DATA,
                        id,
                        Bytes::copy_from_slice(&response[header.consumed..]),
                    ))
                    .await?;
                }
                break;
            }
            Err(protocol::Error::Incomplete) if response.len() < 8192 => (),
            Err(error) => return Err(error.into()),
        }
    }
    let up = async {
        while let Some(command) = commands.recv().await {
            match command.kind {
                DATA => {
                    bus.send(Frame::new(WRITABLE, id, Bytes::new())).await?;
                    tx.write_all(&command.data).await?
                }
                FIN => {
                    tx.finish()?;
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
                }
                _ => {
                    let _ = tx.reset(0u32.into());
                    return Ok(());
                }
            }
        }
        Err("Go flow closed".into())
    };
    let down = async {
        while let Some(chunk) = rx.read_chunk(65536, true).await? {
            bus.payload(Frame::new(DATA, id, chunk.bytes)).await?;
        }
        bus.send(Frame::new(FIN, id, Bytes::new())).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    tokio::try_join!(up, down)?;
    Ok(())
}

async fn relay_server_tcp(
    mut stream: Replay,
    user: u32,
    peer: &str,
    bus: Arc<Bus>,
    owner: u64,
) -> Result<()> {
    let mut header = Vec::new();
    let (target, consumed) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let chunk = poll_fn(|cx| stream.poll_data(cx))
                .await?
                .ok_or("truncated TCP request")?;
            header.extend_from_slice(&chunk);
            match protocol::parse_tcp_request(&header) {
                Ok((target, consumed)) => {
                    break Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                        target.to_vec(),
                        consumed,
                    ));
                }
                Err(protocol::Error::Incomplete) if header.len() < 8192 => (),
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await??;
    let id = bus.next.fetch_add(1, Ordering::Relaxed);
    let mut commands = bus.register(id, owner).await?;
    let result: Result<()> = async {
        let mut metadata = user.to_be_bytes().to_vec();
        metadata.extend_from_slice(&address_data(peer.as_bytes(), &target)?);
        bus.send(Frame::new(INCOMING_TCP, id, metadata)).await?;
        h3_transport::write_raw(
            &mut stream,
            Bytes::from(protocol::encode_tcp_response(true, b"", b"native-xray")?),
        )
        .await?;
        if consumed < header.len() {
            bus.payload(Frame::new(
                DATA,
                id,
                Bytes::copy_from_slice(&header[consumed..]),
            ))
            .await?;
        }
        let (mut tx, mut rx) = stream.split();
        let up = async {
            while let Some(command) = commands.recv().await {
                match command.kind {
                    DATA => {
                        bus.send(Frame::new(WRITABLE, id, Bytes::new())).await?;
                        h3_transport::write_raw(&mut tx, command.data).await?
                    }
                    FIN => {
                        poll_fn(|cx| tx.poll_finish(cx)).await?;
                        return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
                    }
                    _ => {
                        tx.reset(0);
                        return Ok(());
                    }
                }
            }
            Err("Go flow closed".into())
        };
        let down = async {
            while let Some(data) = poll_fn(|cx| rx.poll_data(cx)).await? {
                bus.payload(Frame::new(DATA, id, data)).await?;
            }
            bus.send(Frame::new(FIN, id, Bytes::new())).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        tokio::try_join!(up, down)?;
        Ok(())
    }
    .await;
    bus.done(id, result).await;
    Ok(())
}

async fn send_udp(
    connection: &quinn::Connection,
    session: u32,
    packet: u16,
    data: &[u8],
) -> Result<()> {
    let (address, payload) = parse_address(data)?;
    let message = Message {
        session,
        packet,
        fragment: 0,
        count: 1,
        address,
        payload,
    };
    let limit = connection.max_datagram_size().ok_or("peer disabled UDP")?;
    for part in message.fragment(limit)? {
        let mut wire = Vec::new();
        part.encode(&mut wire)?;
        connection.send_datagram_wait(Bytes::from(wire)).await?;
    }
    Ok(())
}

async fn client_udp(
    connection: quinn::Connection,
    bus: Arc<Bus>,
    sessions: Arc<Mutex<HashMap<u32, u64>>>,
) -> Result<()> {
    let start = Instant::now();
    let mut defrag = Defragger::new(128)?;
    loop {
        let data = connection.read_datagram().await?;
        let Ok(message) = Message::decode(&data) else {
            continue;
        };
        let Ok(Some(datagram)) = defrag.feed(start.elapsed().as_nanos() as u64, &message) else {
            continue;
        };
        let id = sessions.lock().await.get(&datagram.session).copied();
        if let Some(id) = id {
            bus.udp_payload(Frame::new(
                DATAGRAM,
                id,
                address_data(&datagram.address, &datagram.payload)?,
            ))
            .await?;
        }
    }
}

async fn server_udp(
    connection: quinn::Connection,
    user: Arc<AtomicU32>,
    bus: Arc<Bus>,
    peer: String,
    owner: u64,
) -> Result<()> {
    let start = Instant::now();
    let mut defrag = Defragger::new(128)?;
    let mut sessions: HashMap<u32, (u64, Arc<AtomicU64>)> = HashMap::new();
    let mut tasks = JoinSet::new();
    let mut cleanup = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            data = connection.read_datagram() => {
                let data = data?;
                let who = user.load(Ordering::Acquire);
                if who == u32::MAX { continue; }
                let Ok(message) = Message::decode(&data) else { continue };
                let Ok(Some(datagram)) = defrag.feed(start.elapsed().as_nanos() as u64, &message) else { continue };
                let now_ns = start.elapsed().as_nanos() as u64;
                let id = if let Some((id, activity)) = sessions.get(&datagram.session) {
                    activity.store(now_ns, Ordering::Relaxed);
                    *id
                } else {
                    if sessions.len() >= MAX_FLOWS { continue; }
                    let id = bus.next.fetch_add(1, Ordering::Relaxed);
                    let Ok(mut commands) = bus.register(id, owner).await else { continue };
                    let mut metadata = who.to_be_bytes().to_vec();
                    metadata.extend_from_slice(&address_data(peer.as_bytes(), &datagram.address)?);
                    bus.send(Frame::new(INCOMING_UDP, id, metadata)).await?;
                    let connection = connection.clone();
                    let bus = bus.clone();
                    let session = datagram.session;
                    let activity = Arc::new(AtomicU64::new(now_ns));
                    let session_activity = activity.clone();
                    tasks.spawn(async move {
                        let mut packet = 1u16;
                        let mut expiry = tokio::time::interval(Duration::from_secs(1));
                        loop {
                            tokio::select! {
                                command = commands.recv() => {
                                    let Some(frame) = command else { break };
                                    if frame.kind != DATAGRAM { break; }
                                    activity.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                    let _ = bus.send(Frame::new(WRITABLE, id, Bytes::new())).await;
                                    if send_udp(&connection, session, packet, &frame.data).await.is_err() { break; }
                                    packet = packet.wrapping_add(1);
                                }
                                _ = expiry.tick() => {
                                    if (start.elapsed().as_nanos() as u64).saturating_sub(activity.load(Ordering::Relaxed)) >= 60_000_000_000 { break; }
                                }
                            }
                        }
                        bus.done(id, Ok(())).await;
                        session
                    });
                    sessions.insert(datagram.session, (id, session_activity));
                    id
                };
                bus.udp_payload(Frame::new(DATAGRAM, id, address_data(&datagram.address, &datagram.payload)?)).await?;
            }
            _ = cleanup.tick() => { defrag.expire(start.elapsed().as_nanos() as u64); }
            Some(done) = tasks.join_next(), if !tasks.is_empty() => {
                if let Ok(session) = done { sessions.remove(&session); }
            }
        }
    }
}

async fn serve(connection: quinn::Connection, config: Arc<Config>, bus: Arc<Bus>) -> Result<()> {
    let owner = bus.next.fetch_add(1, Ordering::Relaxed);
    let user = Arc::new(AtomicU32::new(u32::MAX));
    let mut tasks = JoinSet::<Result<()>>::new();
    let peer = connection.remote_address().to_string();
    tasks.spawn(server_udp(
        connection.clone(),
        user.clone(),
        bus.clone(),
        peer.clone(),
        owner,
    ));
    let mut classifiers = JoinSet::new();
    let mut h3 =
        h3::server::Connection::new(Dispatch(h3_quinn::Connection::new(connection.clone())))
            .await?;
    loop {
        tokio::select! {
            accepted = poll_fn(|cx| h3.poll_accept_request_stream(cx)), if classifiers.len() < 32 && tasks.len() < 64 => {
                match accepted {
                    Ok(Some(stream)) => { classifiers.spawn(async { tokio::time::timeout(Duration::from_secs(10), h3_transport::classify(stream)).await? }); }
                    _ => break,
                }
            }
            Some(done) = classifiers.join_next(), if !classifiers.is_empty() => {
                let Ok(Ok((mut stream, tcp))) = done else { continue };
                if tcp {
                    let who = user.load(Ordering::Acquire);
                    if who == u32::MAX { stream.reset(0x101); stream.stop_sending(0x101); continue; }
                    let bus = bus.clone();
                    let peer = peer.clone();
                    tasks.spawn(async move { relay_server_tcp(stream, who, &peer, bus, owner).await });
                } else {
                    let resolver = h3.create_resolver(h3::frame::FrameStream::new(h3::stream::BufRecvStream::new(stream)));
                    let user = user.clone();
                    let config = config.clone();
                    let connection = connection.clone();
                    tasks.spawn(async move {
                        tokio::time::timeout(Duration::from_secs(10), async {
                            let (request, mut stream) = resolver.resolve_request().await?;
                            let mut matched = None;
                            if request.method() == "POST" && request.uri().host() == Some("hysteria") && request.uri().path() == "/auth"
                                && request.headers().get("Hysteria-CC-RX").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).is_some()
                                && let Some(auth) = request.headers().get("Hysteria-Auth") {
                                for (index, password) in config.users.iter().enumerate() {
                                    if bool::from(auth.as_bytes().ct_eq(password.as_bytes())) { matched = Some(index as u32); }
                                }
                            }
                            let response = if let Some(index) = matched {
                                let existing = user.load(Ordering::Acquire);
                                if existing != u32::MAX && existing != index { return Err("connection auth identity changed".into()); }
                                user.store(index, Ordering::Release);
                                let peer_rate = request.headers().get("Hysteria-CC-RX").and_then(|v| v.to_str().ok()).unwrap_or("0");
                                congestion::negotiate(&connection, &config.congestion, config.tx_bps, peer_rate);
                                http::Response::builder().status(233).header("Hysteria-UDP", "true")
                                    .header("Hysteria-CC-RX", config.rx_bps.to_string()).header("Hysteria-Padding", "n".repeat(256)).body(())?
                            } else { http::Response::builder().status(404).body(())? };
                            stream.send_response(response).await?;
                            stream.finish().await?;
                            Ok(())
                        }).await?
                    });
                }
            }
            Some(_) = tasks.join_next(), if !tasks.is_empty() => (),
            _ = connection.closed() => break,
        }
    }
    connection.close(0x100u32.into(), b"native session closed");
    tasks.abort_all();
    classifiers.abort_all();
    bus.close_owner(owner).await;
    Ok(())
}

async fn run(config: Config, socket: UdpSocket, bridge: tokio::net::UnixStream) -> Result<()> {
    let ep = endpoint(&config, socket)?;
    let (mut reader, mut writer) = bridge.into_split();
    let (events, mut outgoing) = mpsc::channel(128);
    let bus = Arc::new(Bus {
        events,
        flows: Mutex::new(HashMap::new()),
        next: AtomicU64::new(1),
    });
    let config = Arc::new(config);
    let mut tasks = JoinSet::<Result<()>>::new();
    tasks.spawn(async move {
        while let Some(frame) = outgoing.recv().await {
            write_frame(&mut writer, frame).await?;
        }
        Ok(())
    });
    let (incoming_tx, mut incoming_rx) = mpsc::channel(128);
    tasks.spawn(async move {
        loop {
            let frame = match read_frame(&mut reader).await {
                Ok(frame) => frame,
                Err(_) => break,
            };
            if incoming_tx.send(frame).await.is_err() {
                break;
            }
        }
        Ok(())
    });
    let mut client = None;
    let sessions = Arc::new(Mutex::new(HashMap::new()));
    let mut udp_enabled = false;
    let mut http_guard = None;
    let mut http_driver = None;
    if !config.server {
        let startup = async {
            let connection = ep
                .connect(config.peer.parse()?, &config.server_name)?
                .await?;
            let auth = client_auth(&connection, &config).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((connection, auth))
        };
        let (connection, (udp, guard, driver)) = tokio::select! {
            ready = tokio::time::timeout(Duration::from_secs(10), startup) => ready??,
            _ = incoming_rx.recv() => return Err("Go closed native startup".into()),
        };
        udp_enabled = udp;
        http_guard = Some(guard);
        http_driver = Some(driver);
        tasks.spawn(client_udp(
            connection.clone(),
            bus.clone(),
            sessions.clone(),
        ));
        client = Some(connection);
    }
    bus.send(Frame::new(READY, 0, Bytes::new())).await?;
    let mut next_session = 1u32;
    loop {
        tokio::select! {
            frame = incoming_rx.recv() => {
                let Some(frame) = frame else { break };
                if let Some(connection) = &client {
                    if frame.kind == OPEN_TCP {
                        let id = frame.id;
                        match bus.register(id, 0).await {
                            Ok(commands) => {
                                let bus = bus.clone(); let connection = connection.clone();
                                tasks.spawn(async move {
                                    let result = relay_client_tcp(connection, frame, commands, bus.clone()).await;
                                    bus.done(id, result).await;
                                    Ok(())
                                });
                            }
                            Err(_) => bus.send(Frame::new(ERROR, id, "native flow capacity reached")).await?,
                        }
                        continue;
                    }
                    if frame.kind == OPEN_UDP {
                        let id = frame.id;
                        if !udp_enabled { bus.send(Frame::new(ERROR, id, "server disabled UDP")).await?; continue; }
                        let session = next_session;
                        next_session = next_session.checked_add(1).ok_or("UDP session IDs exhausted")?;
                        let mut commands = bus.register(id, 0).await?;
                        sessions.lock().await.insert(session, id);
                        bus.send(Frame::new(OPENED, id, Bytes::new())).await?;
                        let connection = connection.clone(); let bus = bus.clone(); let sessions = sessions.clone();
                        tasks.spawn(async move {
                            let mut packet = 1u16;
                            while let Some(command) = commands.recv().await {
                                if command.kind != DATAGRAM { break; }
                                bus.send(Frame::new(WRITABLE, id, Bytes::new())).await?;
                                if send_udp(&connection, session, packet, &command.data).await.is_err() { break; }
                                packet = packet.wrapping_add(1);
                            }
                            sessions.lock().await.remove(&session);
                            bus.done(id, Ok(())).await;
                            Ok(())
                        });
                        continue;
                    }
                }
                bus.route(frame).await;
            }
            incoming = ep.accept(), if config.server && tasks.len() < 33 => {
                if let Some(incoming) = incoming {
                    let config = config.clone(); let bus = bus.clone();
                    tasks.spawn(async move {
                        if let Ok(connection) = incoming.await { let _ = serve(connection, config, bus).await; }
                        Ok(())
                    });
                }
            }
            _ = async { if let Some(c) = &client { c.closed().await; } else { std::future::pending().await } } => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if !matches!(result, Ok(Ok(()))) { break; }
            },
        }
    }
    ep.close(0x100u32.into(), b"Go engine stopped");
    drop(http_guard);
    if let Some(driver) = http_driver {
        driver.abort();
        let _ = driver.await;
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
}

pub struct Engine {
    worker: Option<JoinHandle<()>>,
}

/// Starts one native worker. Input and output pointers must be valid for the call.
/// The returned bridge descriptor belongs to the caller. UDP is duplicated.
///
/// # Safety
/// `config` must point to `len` readable bytes and `out_fd` to writable storage.
/// `udp_fd` must remain a valid UDP descriptor until the call returns.
#[allow(clippy::missing_safety_doc)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_engine_start(
    config: *const u8,
    len: usize,
    udp_fd: i32,
    out_fd: *mut i32,
) -> *mut c_void {
    if config.is_null() || out_fd.is_null() || len > 1024 * 1024 {
        return std::ptr::null_mut();
    }
    let result = std::panic::catch_unwind(|| -> Result<Engine> {
        let config: Config =
            serde_json::from_slice(unsafe { std::slice::from_raw_parts(config, len) })?;
        if config.users.len() > 1024 || (config.server && config.users.is_empty()) {
            return Err("invalid native user table".into());
        }
        if config.tx_bps > i64::MAX as u64 / 2
            || !["", "brutal", "force-brutal", "bbr", "reno"].contains(&config.congestion.as_str())
            || (config.congestion == "force-brutal" && config.tx_bps == 0)
        {
            return Err("invalid native congestion configuration".into());
        }
        let fd = unsafe { libc::fcntl(udp_fd, libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let udp = unsafe { UdpSocket::from_raw_fd(fd) };
        udp.set_nonblocking(true)?;
        let (go, native) = UnixStream::pair()?;
        native.set_nonblocking(true)?;
        let worker = std::thread::Builder::new()
            .name("xhysteria".into())
            .spawn(move || {
                let _ = std::panic::catch_unwind(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("native runtime");
                    runtime.block_on(async {
                        let bridge =
                            tokio::net::UnixStream::from_std(native).expect("native bridge");
                        // Keep an error writer available when setup fails before the event pump starts.
                        let duplicate = unsafe { libc::dup(bridge.as_raw_fd()) };
                        let diagnostic = config.diagnostic;
                        let result = run(config, udp, bridge).await;
                        if diagnostic && let Err(error) = &result {
                            eprintln!("[XRAY_NET_DIAG][XHYSTERIA] worker: {error}");
                        }
                        if duplicate >= 0 {
                            let mut error_pipe = tokio::net::UnixStream::from_std(unsafe {
                                UnixStream::from_raw_fd(duplicate)
                            })
                            .ok();
                            if let Err(error) = result
                                && let Some(writer) = error_pipe.as_mut()
                            {
                                let _ = write_frame(
                                    writer,
                                    Frame::new(ERROR, 0, format!("native engine: {error}")),
                                )
                                .await;
                            }
                        }
                    });
                });
            })?;
        unsafe {
            *out_fd = go.into_raw_fd();
        }
        Ok(Engine {
            worker: Some(worker),
        })
    });
    match result {
        Ok(Ok(engine)) => Box::into_raw(Box::new(engine)).cast(),
        _ => std::ptr::null_mut(),
    }
}

use std::os::fd::AsRawFd;

/// The caller must close the Go bridge endpoint before joining.
///
/// # Safety
/// `engine` must be null or a live handle returned by `xhys_engine_start`,
/// used exactly once with this function, after the bridge endpoint is closed.
#[allow(clippy::missing_safety_doc)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_engine_join(engine: *mut c_void) {
    if engine.is_null() {
        return;
    }
    let mut engine = unsafe { Box::from_raw(engine.cast::<Engine>()) };
    if let Some(worker) = engine.worker.take() {
        let _ = worker.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quic_timers_preserve_defaults_and_profile_values() {
        let mut config = Config::default();
        assert_eq!(
            quic_timers(&config).unwrap(),
            (Duration::from_secs(30), Duration::from_secs(5))
        );
        config.max_idle_timeout = 45;
        config.keep_alive_period = 9;
        assert_eq!(
            quic_timers(&config).unwrap(),
            (Duration::from_secs(45), Duration::from_secs(9))
        );
        assert!(transport(&config).is_ok());
        config.max_idle_timeout = u64::MAX;
        assert!(transport(&config).is_err());
        config.max_idle_timeout = 45;
        config.keep_alive_period = u64::MAX;
        assert!(transport(&config).is_err());
    }
}
