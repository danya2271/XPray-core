//! Embedded QUIC workers with bounded shared-memory or socket traffic bridges.

use crate::{
    congestion,
    h3_transport::{self, Dispatch, Replay, Result},
    hopping::{self, HoppingSocket},
    protocol, shared,
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
    sync::{Mutex, Semaphore, mpsc, oneshot},
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
const AUTH_REQUEST: u8 = 15;
const AUTH_RESPONSE: u8 = 16;
const AUTH_CLOSED: u8 = 17;
const UPDATE_TLS: u8 = 18;
const CONTROL_RESULT: u8 = 19;
const MAX_FRAME: usize = 65536 + 4096;
const MAX_FLOWS: usize = 128;

#[derive(Clone, Default, Deserialize)]
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
    pub disable_gso: bool,
    #[serde(default)]
    pub max_idle_timeout: u64,
    #[serde(default)]
    pub keep_alive_period: u64,
    #[serde(default)]
    pub port_hopping: Option<hopping::Config>,
    pub diagnostic: bool,
    #[serde(default)]
    pub external_auth: bool,
    #[serde(default)]
    pub udp_idle_timeout: u64,
    #[serde(default)]
    pub max_flows: u32,
    #[serde(default)]
    pub max_connections: u32,
    #[serde(default)]
    pub server_workers: u8,
}

impl Config {
    fn flow_limit(&self) -> usize {
        if self.max_flows != 0 {
            self.max_flows as usize
        } else if self.server {
            1024
        } else {
            MAX_FLOWS
        }
    }

    fn connection_limit(&self) -> usize {
        if self.max_connections == 0 {
            256
        } else {
            self.max_connections as usize
        }
    }
}

fn runtime(config: &Config) -> io::Result<tokio::runtime::Runtime> {
    if config.server_workers > 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "server_workers must be 0..16",
        ));
    }
    let workers = if config.server {
        if config.server_workers != 0 {
            config.server_workers as usize
        } else {
            std::thread::available_parallelism().map_or(1, |n| n.get().min(4))
        }
    } else {
        1
    };
    let mut builder = if workers > 1 {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(workers)
            .thread_name("xhysteria-quic");
        builder
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    builder.enable_all().build()
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
    shared: Option<Arc<shared::Bridge>>,
    flows: std::sync::Mutex<HashMap<u64, Flow>>,
    next: AtomicU64,
    max_flows: usize,
    pending_auth: std::sync::Mutex<HashMap<u64, oneshot::Sender<bool>>>,
}

struct AuthWaiter<'a> {
    bus: &'a Bus,
    id: u64,
}

impl Drop for AuthWaiter<'_> {
    fn drop(&mut self) {
        self.bus.pending_auth.lock().unwrap().remove(&self.id);
    }
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
        let mut flows = self.flows.lock().unwrap();
        if flows.len() >= self.max_flows || flows.contains_key(&id) {
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
    async fn authenticate(&self, owner: u64, auth: &[u8]) -> Result<bool> {
        if auth.len() > MAX_FRAME - 8 {
            return Ok(false);
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending_auth.lock().unwrap().insert(id, sender);
        let _waiter = AuthWaiter { bus: self, id };
        let mut request = owner.to_be_bytes().to_vec();
        request.extend_from_slice(auth);
        self.send(Frame::new(AUTH_REQUEST, id, request)).await?;
        Ok(receiver.await.unwrap_or(false))
    }
    async fn send(&self, frame: Frame) -> Result<()> {
        if let Some(shared) = &self.shared {
            return shared.send(frame.kind, frame.id, &frame.data).await;
        }
        self.events
            .send(frame)
            .await
            .map_err(|_| "Go bridge closed".into())
    }
    async fn payload(&self, frame: Frame) -> Result<()> {
        self.payload_bytes(frame.id, &frame.data).await
    }
    async fn payload_bytes(&self, id: u64, data: &[u8]) -> Result<()> {
        let credit = self
            .flows
            .lock()
            .unwrap()
            .get(&id)
            .map(|flow| flow.receive_credit.clone())
            .ok_or("closed native flow")?;
        credit
            .acquire()
            .await
            .map_err(|_| "closed native flow")?
            .forget();
        if let Some(shared) = &self.shared {
            shared.send(DATA, id, data).await
        } else {
            self.send(Frame::new(DATA, id, Bytes::copy_from_slice(data)))
                .await
        }
    }
    async fn udp_payload(&self, frame: Frame) -> Result<()> {
        let credit = self
            .flows
            .lock()
            .unwrap()
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
    async fn writable(&self, id: u64, pending: &mut u8) -> Result<()> {
        *pending += 1;
        if *pending >= 8 {
            let count = *pending;
            *pending = 0;
            self.send(Frame::new(WRITABLE, id, Bytes::copy_from_slice(&[count])))
                .await?;
        }
        Ok(())
    }
    async fn done(&self, id: u64, result: Result<()>) {
        self.flows.lock().unwrap().remove(&id);
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
            .unwrap()
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
        let mut flows = self.flows.lock().unwrap();
        if let Some(flow) = flows.get(&id) {
            if frame.kind == CREDIT {
                let count = frame.data.first().copied().unwrap_or(1) as usize;
                let available = flow.receive_credit.available_permits();
                if count != 0 && count <= 16 && available < 16 {
                    flow.receive_credit.add_permits(count.min(16 - available));
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

    async fn command(&self, incoming: &mpsc::Sender<Frame>, frame: Frame) -> Result<()> {
        match frame.kind {
            OPEN_TCP | OPEN_UDP | CLOSE | AUTH_RESPONSE | UPDATE_TLS => incoming
                .send(frame)
                .await
                .map_err(|_| "Go bridge closed".into()),
            _ => {
                self.route(frame).await;
                Ok(())
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
        // Match Xray's Go Hysteria windows on WAN paths, while bounding memory
        // across streams instead of retaining Quinn's effectively unlimited cap.
        .stream_receive_window((8u32 * 1024 * 1024).into())
        .receive_window((20u32 * 1024 * 1024).into())
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
    // Hybrid restricts Brutal to individual sends after rate negotiation.
    t.enable_segmentation_offload(!config.disable_gso);
    if config.disable_pmtud {
        t.initial_mtu(1200).min_mtu(1200).mtu_discovery_config(None);
    } else if !config.salamander.is_empty() {
        let mut discovery = quinn::MtuDiscoveryConfig::default();
        // Reserve Salamander's salt even on an IPv6 1500-byte path.
        discovery.upper_bound(1500 - 40 - 8 - crate::salamander::SALT_LEN as u16);
        t.mtu_discovery_config(Some(discovery));
    }
    Ok(t)
}

fn udp_buffers(socket: &UdpSocket, diagnostic: bool) {
    for (option, name) in [(libc::SO_RCVBUF, "recv"), (libc::SO_SNDBUF, "send")] {
        let get = || {
            let mut bytes = 0i32;
            let mut len = std::mem::size_of_val(&bytes) as libc::socklen_t;
            let result = unsafe {
                libc::getsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    option,
                    (&mut bytes as *mut i32).cast(),
                    &mut len,
                )
            };
            (result == 0).then_some(bytes)
        };
        let set = |bytes: i32| unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                option,
                (&bytes as *const i32).cast(),
                std::mem::size_of_val(&bytes) as libc::socklen_t,
            )
        };
        if let Some(before) = get() {
            if before < 8 * 1024 * 1024 {
                set(8 * 1024 * 1024);
                // Linux reports twice the requested value. A capped request
                // must not shrink a socket configured by the system dialer.
                if get().is_some_and(|after| after < before) {
                    set(before / 2);
                }
            }
            if diagnostic {
                eprintln!(
                    "[XRAY_NET_DIAG][XHYSTERIA] UDP {name}_buffer={}",
                    get().unwrap_or(before)
                );
            }
        }
    }
}

fn server_config(config: &Config) -> Result<quinn::ServerConfig> {
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
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls)?,
    ));
    server.transport_config(Arc::new(transport(config)?));
    Ok(server)
}

fn endpoint(config: &Config, socket: UdpSocket) -> Result<quinn::Endpoint> {
    udp_buffers(&socket, config.diagnostic);
    if config.port_hopping.is_some() {
        if config.server {
            return Err("native UDP hopping is client-only".into());
        }
        if socket.peer_addr().is_ok() {
            return Err("native UDP hopping requires an unconnected UDP socket".into());
        }
    }
    let runtime = Arc::new(quinn::TokioRuntime);
    let udp: Arc<dyn AsyncUdpSocket> = if config.salamander.is_empty() {
        runtime.wrap_udp_socket(socket)?
    } else {
        Arc::new(MaskedSocket::new(socket, config.salamander.as_bytes())?)
    };
    let udp: Arc<dyn AsyncUdpSocket> = if let Some(hop) = &config.port_hopping {
        Arc::new(HoppingSocket::new(
            udp,
            config.peer.parse()?,
            hop.clone(),
            config.diagnostic,
        )?)
    } else {
        udp
    };
    let server = if config.server {
        Some(server_config(config)?)
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

async fn connection_diagnostics(connection: quinn::Connection, role: &'static str) -> Result<()> {
    let peer = connection.remote_address();
    let mut timer = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            reason = connection.closed() => {
                eprintln!("[XRAY_NET_DIAG][XHYSTERIA] connection closed role={role} peer={peer} reason={reason}");
                return Ok(());
            }
            _ = timer.tick() => {
                let s = connection.stats();
                eprintln!("[XRAY_NET_DIAG][XHYSTERIA] connection role={role} peer={peer} rtt_ms={} tx_bytes={} rx_bytes={} lost_packets={} cwnd={}",
                    s.path.rtt.as_millis(), s.udp_tx.bytes, s.udp_rx.bytes, s.path.lost_packets, s.path.cwnd);
            }
        }
    }
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
        let mut credit = 0;
        let mut budget = 0;
        while let Some(command) = commands.recv().await {
            match command.kind {
                DATA => {
                    bus.writable(id, &mut credit).await?;
                    budget += command.data.len();
                    tx.write_chunk(command.data).await?;
                    if budget >= 256 * 1024 {
                        budget = 0;
                        tokio::task::yield_now().await;
                    }
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
        // Quinn fills only the currently readable bytes, without waiting for a
        // full 64 KiB. Coalescing ready QUIC fragments reduces bridge events.
        let mut buffer = vec![0; 65536];
        let mut budget = 0;
        while let Some(n) = rx.read(&mut buffer).await? {
            bus.payload_bytes(id, &buffer[..n]).await?;
            budget += n;
            if budget >= 256 * 1024 {
                budget = 0;
                tokio::task::yield_now().await;
            }
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
        metadata.extend_from_slice(&owner.to_be_bytes());
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
        let (mut tx, mut rx) = stream.split()?;
        let up = async {
            let mut credit = 0;
            let mut budget = 0;
            while let Some(command) = commands.recv().await {
                match command.kind {
                    DATA => {
                        bus.writable(id, &mut credit).await?;
                        budget += command.data.len();
                        tx.write_chunk(command.data).await?;
                        if budget >= 256 * 1024 {
                            budget = 0;
                            tokio::task::yield_now().await;
                        }
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
            // Aggregate ready fragments on the server as on the client. Do not
            // spend a bridge credit and wake Go for each individual QUIC chunk.
            let mut buffer = vec![0; 65536];
            let mut budget = 0;
            loop {
                let n = rx.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                bus.payload_bytes(id, &buffer[..n]).await?;
                budget += n;
                if budget >= 256 * 1024 {
                    budget = 0;
                    tokio::task::yield_now().await;
                }
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
    if message.header_len()? + payload.len() <= limit {
        let mut wire = Vec::with_capacity(message.header_len()? + payload.len());
        message.encode(&mut wire)?;
        connection.send_datagram_wait(Bytes::from(wire)).await?;
        return Ok(());
    }
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
        let assembled;
        let (session, address, payload) = if message.count == 1 {
            (message.session, message.address, message.payload)
        } else {
            let Ok(Some(datagram)) = defrag.feed(start.elapsed().as_nanos() as u64, &message)
            else {
                continue;
            };
            assembled = datagram;
            (
                assembled.session,
                assembled.address.as_slice(),
                assembled.payload.as_slice(),
            )
        };
        let id = sessions.lock().await.get(&session).copied();
        if let Some(id) = id {
            bus.udp_payload(Frame::new(DATAGRAM, id, address_data(address, payload)?))
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
    idle: Duration,
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
                let assembled;
                let (session_id, address, payload) = if message.count == 1 {
                    (message.session, message.address, message.payload)
                } else {
                    let Ok(Some(datagram)) = defrag.feed(start.elapsed().as_nanos() as u64, &message) else { continue };
                    assembled = datagram;
                    (assembled.session, assembled.address.as_slice(), assembled.payload.as_slice())
                };
                let now_ns = start.elapsed().as_nanos() as u64;
                let id = if let Some((id, activity)) = sessions.get(&session_id) {
                    activity.store(now_ns, Ordering::Relaxed);
                    *id
                } else {
                    if sessions.len() >= MAX_FLOWS { continue; }
                    let id = bus.next.fetch_add(1, Ordering::Relaxed);
                    let Ok(mut commands) = bus.register(id, owner).await else { continue };
                    let mut metadata = who.to_be_bytes().to_vec();
                    metadata.extend_from_slice(&owner.to_be_bytes());
                    metadata.extend_from_slice(&address_data(peer.as_bytes(), address)?);
                    bus.send(Frame::new(INCOMING_UDP, id, metadata)).await?;
                    let connection = connection.clone();
                    let bus = bus.clone();
                    let session = session_id;
                    let activity = Arc::new(AtomicU64::new(now_ns));
                    let session_activity = activity.clone();
                    tasks.spawn(async move {
                        let mut packet = 1u16;
                        let mut credit = 0;
                        let mut expiry = tokio::time::interval(Duration::from_secs(1));
                        loop {
                            tokio::select! {
                                command = commands.recv() => {
                                    let Some(frame) = command else { break };
                                    if frame.kind != DATAGRAM { break; }
                                    activity.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                    let _ = bus.writable(id, &mut credit).await;
                                    if send_udp(&connection, session, packet, &frame.data).await.is_err() { break; }
                                    packet = packet.wrapping_add(1);
                                }
                                _ = expiry.tick() => {
                                    if (start.elapsed().as_nanos() as u64).saturating_sub(activity.load(Ordering::Relaxed)) >= idle.as_nanos() as u64 { break; }
                                }
                            }
                        }
                        bus.done(id, Ok(())).await;
                        session
                    });
                    sessions.insert(session_id, (id, session_activity));
                    id
                };
                bus.udp_payload(Frame::new(DATAGRAM, id, address_data(address, payload)?)).await?;
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
    if config.diagnostic {
        tasks.spawn(connection_diagnostics(connection.clone(), "server"));
    }
    let peer = connection.remote_address().to_string();
    tasks.spawn(server_udp(
        connection.clone(),
        user.clone(),
        bus.clone(),
        peer.clone(),
        owner,
        Duration::from_secs(if config.udp_idle_timeout == 0 {
            60
        } else {
            config.udp_idle_timeout
        }),
    ));
    let mut classifiers = JoinSet::new();
    let mut h3 =
        h3::server::Connection::new(Dispatch(h3_quinn::Connection::new(connection.clone())))
            .await?;
    loop {
        tokio::select! {
            accepted = poll_fn(|cx| h3.poll_accept_request_stream(cx)), if classifiers.len() < 32 && tasks.len() < MAX_FLOWS + 1 => {
                match accepted {
                    Ok(Some(stream)) => { classifiers.spawn(async { tokio::time::timeout(Duration::from_secs(10), h3_transport::classify(stream)).await? }); }
                    Ok(None) => break,
                    Err(error) => {
                        if config.diagnostic {
                            eprintln!("[XRAY_NET_DIAG][XHYSTERIA] HTTP/3 closed role=server peer={peer} reason={error}");
                        }
                        break;
                    },
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
                    let bus = bus.clone();
                    tasks.spawn(async move {
                        tokio::time::timeout(Duration::from_secs(10), async {
                            let (request, mut stream) = resolver.resolve_request().await?;
                            let mut matched = None;
                            if request.method() == "POST" && request.uri().host() == Some("hysteria") && request.uri().path() == "/auth"
                                && request.headers().get("Hysteria-CC-RX").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).is_some()
                                && let Some(auth) = request.headers().get("Hysteria-Auth") {
                                if config.external_auth {
                                    if bus.authenticate(owner, auth.as_bytes()).await? { matched = Some(0); }
                                } else {
                                    for (index, password) in config.users.iter().enumerate() {
                                        if bool::from(auth.as_bytes().ct_eq(password.as_bytes())) { matched = Some(index as u32); }
                                    }
                                }
                            }
                            let response = if let Some(index) = matched {
                                let existing = user.load(Ordering::Acquire);
                                if existing != u32::MAX && existing != index { return Err("connection auth identity changed".into()); }
                                user.store(index, Ordering::Release);
                                if config.diagnostic {
                                    eprintln!("[XRAY_NET_DIAG][XHYSTERIA] authenticated role=server peer={}", connection.remote_address());
                                }
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
            reason = connection.closed() => {
                if config.diagnostic {
                    eprintln!("[XRAY_NET_DIAG][XHYSTERIA] session closed role=server peer={peer} reason={reason}");
                }
                break;
            },
        }
    }
    connection.close(0x100u32.into(), b"native session closed");
    tasks.abort_all();
    classifiers.abort_all();
    bus.close_owner(owner).await;
    if config.external_auth {
        bus.send(Frame::new(AUTH_CLOSED, owner, Bytes::new()))
            .await?;
    }
    Ok(())
}

enum Bridge {
    Socket(tokio::net::UnixStream),
    Shared(Arc<shared::Bridge>),
}

fn bridge_tasks(
    bridge: Bridge,
    mut outgoing: mpsc::Receiver<Frame>,
    incoming: mpsc::Sender<Frame>,
    bus: Arc<Bus>,
    tasks: &mut JoinSet<Result<()>>,
) {
    match bridge {
        Bridge::Socket(bridge) => {
            // OwnedWriteHalf shuts down the entire socket on drop, including
            // the duplicate used to report startup errors after pump teardown.
            let (mut reader, mut writer) = tokio::io::split(bridge);
            tasks.spawn(async move {
                while let Some(frame) = outgoing.recv().await {
                    write_frame(&mut writer, frame).await?;
                }
                Ok(())
            });
            tasks.spawn(async move {
                while let Ok(frame) = read_frame(&mut reader).await {
                    if bus.command(&incoming, frame).await.is_err() {
                        break;
                    }
                }
                Ok(())
            });
        }
        Bridge::Shared(bridge) => {
            drop(outgoing);
            tasks.spawn(bridge.clone().notifications());
            tasks.spawn(async move {
                loop {
                    let changed = bridge.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if bridge.closed() {
                        break;
                    }
                    let mut drained = 0;
                    let mut bytes = 0;
                    while drained < 32 && bytes < 256 * 1024 {
                        let Some((kind, id, data)) = bridge.pop(true)? else {
                            break;
                        };
                        drained += 1;
                        bytes += data.len();
                        if incoming.capacity() == 0 {
                            bridge.notify_space();
                        }
                        if bus
                            .command(&incoming, Frame::new(kind, id, data))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    if drained != 0 {
                        bridge.notify_space();
                    }
                    if drained == 32 || bytes >= 256 * 1024 {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    changed.await;
                }
                Ok(())
            });
        }
    }
}

async fn run(config: Config, socket: UdpSocket, bridge: Bridge) -> Result<()> {
    let ep = endpoint(&config, socket)?;
    let (events, outgoing) = mpsc::channel(128);
    let bus = Arc::new(Bus {
        events,
        shared: match &bridge {
            Bridge::Shared(shared) => Some(shared.clone()),
            Bridge::Socket(_) => None,
        },
        flows: std::sync::Mutex::new(HashMap::new()),
        next: AtomicU64::new(1),
        max_flows: config.flow_limit(),
        pending_auth: std::sync::Mutex::new(HashMap::new()),
    });
    let config = Arc::new(config);
    let connection_slots = Arc::new(Semaphore::new(config.connection_limit()));
    let mut tasks = JoinSet::<Result<()>>::new();
    let (incoming_tx, mut incoming_rx) = mpsc::channel(128);
    bridge_tasks(bridge, outgoing, incoming_tx, bus.clone(), &mut tasks);
    let mut client = None;
    let sessions = Arc::new(Mutex::new(HashMap::new()));
    let mut udp_enabled = false;
    let mut http_guard = None;
    let mut http_driver = None;
    if !config.server {
        let startup = async {
            let connection = ep
                .connect(config.peer.parse()?, &config.server_name)?
                .await
                .map_err(|error| format!("QUIC handshake: {error}"))?;
            let auth = client_auth(&connection, &config)
                .await
                .map_err(|error| format!("HTTP/3 auth: {error}"))?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((connection, auth))
        };
        let (connection, (udp, guard, driver)) = tokio::select! {
            ready = tokio::time::timeout(Duration::from_secs(10), startup) =>
                ready.map_err(|_| "QUIC/auth startup timed out after 10s")??,
            _ = incoming_rx.recv() => return Err("Go closed native startup".into()),
        };
        udp_enabled = udp;
        if config.diagnostic {
            tasks.spawn(connection_diagnostics(connection.clone(), "client"));
        }
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
    let result = loop {
        tokio::select! {
            frame = incoming_rx.recv() => {
                let Some(frame) = frame else { break Ok(()) };
                if frame.kind == AUTH_RESPONSE {
                    if let Some(sender) = bus.pending_auth.lock().unwrap().remove(&frame.id) {
                        let _ = sender.send(frame.data.as_ref() == [1]);
                    }
                    continue;
                }
                if frame.kind == UPDATE_TLS {
                    #[derive(Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Certificate { certificate_pem: String, key_pem: String }
                    let result = (|| -> Result<()> {
                        if !config.server { return Err("not a native server".into()); }
                        let certificate: Certificate = serde_json::from_slice(&frame.data)?;
                        let mut updated = (*config).clone();
                        updated.certificate_pem = certificate.certificate_pem;
                        updated.key_pem = certificate.key_pem;
                        ep.set_server_config(Some(server_config(&updated)?));
                        Ok(())
                    })();
                    let data = result.err().map(|e| e.to_string()).unwrap_or_default();
                    bus.send(Frame::new(CONTROL_RESULT, frame.id, data)).await?;
                    continue;
                }
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
                        let Ok(mut commands) = bus.register(id, 0).await else {
                            bus.send(Frame::new(ERROR, id, "native flow capacity reached")).await?;
                            continue;
                        };
                        sessions.lock().await.insert(session, id);
                        bus.send(Frame::new(OPENED, id, Bytes::new())).await?;
                        let connection = connection.clone(); let bus = bus.clone(); let sessions = sessions.clone();
                        tasks.spawn(async move {
                            let mut packet = 1u16;
                            let mut credit = 0;
                            while let Some(command) = commands.recv().await {
                                if command.kind != DATAGRAM { break; }
                                bus.writable(id, &mut credit).await?;
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
            incoming = ep.accept(), if config.server && connection_slots.available_permits() != 0 => {
                if let Some(incoming) = incoming {
                    let config = config.clone(); let bus = bus.clone();
                    let permit = connection_slots.clone().acquire_owned().await?;
                    tasks.spawn(async move {
                        let _permit = permit;
                        if let Ok(connection) = incoming.await { let _ = serve(connection, config, bus).await; }
                        Ok(())
                    });
                }
            }
            reason = async { if let Some(c) = &client { c.closed().await } else { std::future::pending().await } } => {
                break Err(format!("QUIC connection closed: {reason}").into());
            },
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                match result {
                    Ok(Ok(())) => (),
                    Ok(Err(error)) => break Err(error),
                    Err(error) => break Err(error.into()),
                }
            },
        }
    };
    ep.close(0x100u32.into(), b"Go engine stopped");
    drop(http_guard);
    if let Some(driver) = http_driver {
        driver.abort();
        let _ = driver.await;
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result
}

pub struct Engine {
    worker: Option<JoinHandle<()>>,
    shared: Option<Arc<shared::Bridge>>,
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
        if config.users.len() > 1024
            || config.max_flows > 65536
            || config.max_connections > 4096
            || config.udp_idle_timeout > 600
            || (config.server && config.users.is_empty() && !config.external_auth)
        {
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
                    let runtime = runtime(&config).expect("native runtime");
                    runtime.block_on(async {
                        let bridge =
                            tokio::net::UnixStream::from_std(native).expect("native bridge");
                        // Keep an error writer available when setup fails before the event pump starts.
                        let duplicate = unsafe { libc::dup(bridge.as_raw_fd()) };
                        let diagnostic = config.diagnostic;
                        let result = run(config, udp, Bridge::Socket(bridge)).await;
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
            shared: None,
        })
    });
    match result {
        Ok(Ok(engine)) => Box::into_raw(Box::new(engine)).cast(),
        _ => std::ptr::null_mut(),
    }
}

/// Starts the active shared-memory bridge. Three duplicated notification fds are
/// transferred to Go; arena storage remains owned by the Engine until join.
///
/// # Safety
/// Input pointers must be valid for the call. Go must cease every arena access
/// before calling join, including accesses from pending read/write operations.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_engine_start_shared(
    config: *const u8,
    len: usize,
    udp_fd: i32,
    out: *mut shared::View,
) -> *mut c_void {
    if config.is_null() || out.is_null() || len > 1024 * 1024 {
        return std::ptr::null_mut();
    }
    let result = std::panic::catch_unwind(|| -> Result<Engine> {
        let config: Config =
            serde_json::from_slice(unsafe { std::slice::from_raw_parts(config, len) })?;
        if config.users.len() > 1024
            || config.max_flows > 65536
            || config.max_connections > 4096
            || config.udp_idle_timeout > 600
            || (config.server && config.users.is_empty() && !config.external_auth)
            || config.tx_bps > i64::MAX as u64 / 2
            || !["", "brutal", "force-brutal", "bbr", "reno"].contains(&config.congestion.as_str())
            || (config.congestion == "force-brutal" && config.tx_bps == 0)
        {
            return Err("invalid native shared engine configuration".into());
        }
        let fd = unsafe { libc::fcntl(udp_fd, libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let udp = unsafe { UdpSocket::from_raw_fd(fd) };
        udp.set_nonblocking(true)?;
        let bridge = shared::Bridge::new()?;
        let owner = bridge.clone();
        let worker = std::thread::Builder::new()
            .name("xhysteria".into())
            .spawn(move || {
                let diagnostic = config.diagnostic;
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
                        let runtime = runtime(&config)?;
                        runtime.block_on(run(config, udp, Bridge::Shared(bridge.clone())))
                    }));
                if diagnostic && !matches!(result, Ok(Ok(()))) {
                    eprintln!("[XRAY_NET_DIAG][XHYSTERIA] shared worker: {result:?}");
                }
                // The runtime (including the ring writer) is gone. This thread
                // is now the sole producer, so publish the real failure before
                // closing; Go drains published events even after close.
                let error = match result {
                    Ok(Err(error)) => Some(format!("native engine: {error}")),
                    Err(_) => Some("native engine panicked".to_string()),
                    Ok(Ok(())) => None,
                };
                if !bridge.closed()
                    && let Some(error) = error
                {
                    let _ = bridge.push(false, ERROR, 0, error.as_bytes());
                }
                bridge.close();
            })?;
        let view = match owner.view() {
            Ok(view) => view,
            Err(error) => {
                owner.close();
                let _ = worker.join();
                return Err(error.into());
            }
        };
        unsafe { *out = view };
        Ok(Engine {
            worker: Some(worker),
            shared: Some(owner),
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
    if let Some(bridge) = &engine.shared {
        bridge.close();
    }
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
