//! Embedded QUIC workers with bounded shared-memory or socket traffic bridges.

use crate::{
    congestion,
    h3_transport::{self, Dispatch, Replay, Result},
    hopping::{self, HoppingSocket},
    mask_socket::{self, MaskSocket},
    masquerade,
    packet_socket::PacketSocket,
    protocol, shared,
    socket::{MaskedSocket, ReceiveSocket},
    udp::{Defragger, Message},
};
use bytes::{Buf, Bytes};
use h3::quic::{RecvStream as _, SendStream as _};
use quinn::{AsyncUdpSocket, Runtime};
use serde::Deserialize;
use std::{
    collections::HashMap,
    ffi::c_void,
    future::poll_fn,
    io::{self, BufReader},
    net::UdpSocket,
    os::fd::{FromRawFd, IntoRawFd, OwnedFd},
    os::unix::net::{UnixDatagram, UnixStream},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
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
const INCOMING_HTTP: u8 = 20;
const HTTP_HEAD: u8 = 21;
const HTTP_TRAILERS: u8 = 22;
const MAX_FRAME: usize = 65536 + 4096;
const MAX_FLOWS: usize = 128;
const SERVER_CHUNK: usize = 16384;
const SERVER_RECEIVE_WINDOW: u32 = 64 * 1024;
const SERVER_BULK_WINDOW: u32 = 8 * 1024 * 1024;

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
    #[serde(default)]
    pub udp_masks: Vec<mask_socket::Config>,
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
    #[serde(default)]
    pub masquerade: Option<masquerade::Config>,
    #[serde(default)]
    pub external_masquerade: bool,
    #[serde(default)]
    pub packet_bridge: bool,
    #[serde(default)]
    pub packet_batch: bool,
    #[serde(default)]
    pub packet_local: String,
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
    #[serde(default)]
    pub process_workers: u8,
    #[serde(default)]
    pub send_buffer_bytes: u32,
    #[serde(default)]
    pub receive_buffer_bytes: u32,
    #[serde(default)]
    pub process_send_buffer_bytes: u32,
    #[serde(default)]
    pub process_receive_buffer_bytes: u32,
}

impl Config {
    fn flow_window(&self) -> usize {
        if self.server { 4 } else { 8 }
    }

    fn flow_limit(&self) -> usize {
        if self.max_flows != 0 {
            self.max_flows as usize
        } else if self.server {
            64
        } else {
            MAX_FLOWS
        }
    }

    fn connection_limit(&self) -> usize {
        if self.max_connections == 0 {
            32
        } else {
            self.max_connections as usize
        }
    }
}

fn worker_count(config: &Config) -> io::Result<usize> {
    if config.server_workers > 16 || config.process_workers > 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker counts must be 0..16",
        ));
    }
    Ok(if config.server {
        if config.server_workers != 0 {
            config.server_workers as usize
        } else {
            std::thread::available_parallelism().map_or(1, |n| n.get().min(4))
        }
    } else {
        1
    })
}

fn runtime(config: &Config) -> io::Result<tokio::runtime::Runtime> {
    let workers = worker_count(config)?;
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

static SHARED_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

enum Worker {
    Dedicated(JoinHandle<()>),
    Shared(tokio::task::JoinHandle<()>),
}

fn start_worker(
    config: &Config,
    task: impl std::future::Future<Output = ()> + Send + 'static,
) -> io::Result<Worker> {
    worker_count(config)?;
    if config.server {
        let runtime = runtime(config)?;
        return std::thread::Builder::new()
            .name("xhysteria".into())
            .spawn(move || runtime.block_on(task))
            .map(Worker::Dedicated);
    }
    let runtime = SHARED_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(usize::from(config.process_workers.max(1)))
            .thread_name("xhysteria-shared")
            .enable_all()
            .build()
            .expect("native client runtime")
    });
    Ok(Worker::Shared(runtime.spawn(task)))
}

impl Worker {
    fn join(self) {
        match self {
            Self::Dedicated(worker) => {
                let _ = worker.join();
            }
            Self::Shared(worker) => {
                let _ = SHARED_RUNTIME.get().unwrap().block_on(worker);
            }
        }
    }
}

struct BackgroundTask(tokio::task::JoinHandle<()>);

impl Drop for BackgroundTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct EndpointOwner(quinn::Endpoint);

impl Drop for EndpointOwner {
    fn drop(&mut self) {
        self.0.close(0x100u32.into(), b"Go engine stopped");
    }
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
    flow_window: usize,
    udp_credit_dropped: AtomicU64,
    capacity: Option<ServerCapacity>,
    send_budget: Option<Arc<Semaphore>>,
    process_send_budget: Option<Arc<Semaphore>>,
    pending_auth: std::sync::Mutex<HashMap<u64, oneshot::Sender<bool>>>,
}

struct BufferedChunk {
    data: Bytes,
    _permit: OwnedSemaphorePermit,
    _process_permit: Option<OwnedSemaphorePermit>,
}

static PROCESS_SEND_BUDGET: OnceLock<(usize, Arc<Semaphore>)> = OnceLock::new();
static PROCESS_RECEIVE_BUDGET: OnceLock<(usize, Arc<Semaphore>)> = OnceLock::new();

fn process_send_budget(config: &Config) -> &'static (usize, Arc<Semaphore>) {
    PROCESS_SEND_BUDGET.get_or_init(|| {
        let bytes = if config.process_send_buffer_bytes == 0 {
            64 * 1024 * 1024
        } else {
            config.process_send_buffer_bytes as usize
        };
        (bytes, Arc::new(Semaphore::new(bytes)))
    })
}

fn process_receive_budget(config: &Config) -> &'static (usize, Arc<Semaphore>) {
    PROCESS_RECEIVE_BUDGET.get_or_init(|| {
        let bytes = if config.process_receive_buffer_bytes == 0 {
            64 * 1024 * 1024
        } else {
            config.process_receive_buffer_bytes as usize
        };
        (bytes, Arc::new(Semaphore::new(bytes)))
    })
}

impl AsRef<[u8]> for BufferedChunk {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

struct ServerCapacity {
    connections: Arc<Semaphore>,
    max_connections: usize,
    max_flows: usize,
    diagnostic: bool,
    rejected_connections: AtomicU64,
    rejected_flows: AtomicU64,
    last_rejection: std::sync::Mutex<Option<Instant>>,
    receive_budget: Arc<Semaphore>,
    process_receive_budget: Arc<Semaphore>,
    buffer_bytes: usize,
    process_buffer_bytes: usize,
}

impl ServerCapacity {
    fn new(config: &Config) -> Self {
        let buffer_bytes = if config.receive_buffer_bytes == 0 {
            64 * 1024 * 1024
        } else {
            config.receive_buffer_bytes as usize
        };
        let (process_buffer_bytes, process_receive_budget) = process_receive_budget(config);
        Self {
            connections: Arc::new(Semaphore::new(config.connection_limit())),
            max_connections: config.connection_limit(),
            max_flows: config.flow_limit(),
            diagnostic: config.diagnostic,
            rejected_connections: AtomicU64::new(0),
            rejected_flows: AtomicU64::new(0),
            last_rejection: std::sync::Mutex::new(None),
            receive_budget: Arc::new(Semaphore::new(buffer_bytes)),
            process_receive_budget: process_receive_budget.clone(),
            buffer_bytes,
            process_buffer_bytes: *process_buffer_bytes,
        }
    }

    fn report(&self, event: &str, active_flows: usize) {
        if self.diagnostic {
            #[cfg(feature = "allocation-profile")]
            crate::allocation_profile::report();
            #[cfg(all(
                feature = "allocation-profile",
                target_os = "linux",
                target_env = "gnu"
            ))]
            {
                let memory = unsafe { libc::mallinfo2() };
                eprintln!(
                    "[XRAY_NET_DIAG][XHYSTERIA] native_allocator allocated_bytes={} mapped_bytes={} retained_free_bytes={}",
                    memory.uordblks, memory.hblkhd, memory.fordblks
                );
            }
            let (send_bytes, send_limit) = PROCESS_SEND_BUDGET
                .get()
                .map(|(limit, budget)| (limit - budget.available_permits(), *limit))
                .unwrap_or_default();
            eprintln!(
                "[XRAY_NET_DIAG][XHYSTERIA] event={event} role=server active_connections={} max_connections={} active_flows={active_flows} max_flows={} rejected_connections={} rejected_flows={} receive_boost_bytes={} receive_boost_budget_bytes={} process_receive_boost_bytes={} process_receive_budget_bytes={} process_send_bytes={send_bytes} process_send_budget_bytes={send_limit}",
                self.max_connections - self.connections.available_permits(),
                self.max_connections,
                self.max_flows,
                self.rejected_connections.load(Ordering::Relaxed),
                self.rejected_flows.load(Ordering::Relaxed),
                self.buffer_bytes - self.receive_budget.available_permits(),
                self.buffer_bytes,
                self.process_buffer_bytes - self.process_receive_budget.available_permits(),
                self.process_buffer_bytes,
            );
        }
    }

    fn rejection(&self, event: &str, active_flows: usize) {
        if !self.diagnostic {
            return;
        }
        let mut last = self.last_rejection.lock().unwrap();
        if last.is_none_or(|last| last.elapsed() >= Duration::from_secs(5)) {
            *last = Some(Instant::now());
            self.report(event, active_flows);
        }
    }
}

struct ReceiveWindow {
    connection: quinn::Connection,
    budget: Arc<Semaphore>,
    process_budget: Arc<Semaphore>,
    received: AtomicU64,
    streams: AtomicU32,
    reservation: std::sync::Mutex<Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>>,
}

impl ReceiveWindow {
    fn observe(&self, bytes: usize) {
        let previous = self.received.fetch_add(bytes as u64, Ordering::Relaxed);
        if previous / u64::from(SERVER_RECEIVE_WINDOW)
            == (previous + bytes as u64) / u64::from(SERVER_RECEIVE_WINDOW)
        {
            return;
        }
        let mut reservation = self.reservation.lock().unwrap();
        if reservation.is_none()
            && let Ok(listener_permit) = self
                .budget
                .clone()
                .try_acquire_many_owned(SERVER_BULK_WINDOW - SERVER_RECEIVE_WINDOW)
            && let Ok(process_permit) = self
                .process_budget
                .clone()
                .try_acquire_many_owned(SERVER_BULK_WINDOW - SERVER_RECEIVE_WINDOW)
        {
            *reservation = Some((listener_permit, process_permit));
            self.connection
                .set_receive_window(SERVER_BULK_WINDOW.into());
        }
    }
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
    async fn register(
        &self,
        id: u64,
        owner: u64,
    ) -> Result<(mpsc::Receiver<Frame>, Arc<Semaphore>)> {
        let mut flows = self.flows.lock().unwrap();
        if flows.len() >= self.max_flows {
            if let Some(capacity) = &self.capacity {
                capacity.rejected_flows.fetch_add(1, Ordering::Relaxed);
                capacity.rejection("flow_rejected", flows.len());
            }
            return Err("native flow capacity reached".into());
        }
        if flows.contains_key(&id) {
            return Err("duplicate native flow".into());
        }
        let (tx, rx) = mpsc::channel(self.flow_window);
        let receive_credit = Arc::new(Semaphore::new(self.flow_window));
        flows.insert(
            id,
            Flow {
                owner,
                sender: tx,
                receive_credit: receive_credit.clone(),
            },
        );
        Ok((rx, receive_credit))
    }
    async fn buffered_chunk(&self, data: Bytes) -> Result<Bytes> {
        let Some(budget) = &self.send_budget else {
            return Ok(data);
        };
        let permit = budget.clone().acquire_many_owned(data.len() as u32).await?;
        let process_permit = match &self.process_send_budget {
            Some(budget) => Some(budget.clone().acquire_many_owned(data.len() as u32).await?),
            None => None,
        };
        Ok(Bytes::from_owner(BufferedChunk {
            data,
            _permit: permit,
            _process_permit: process_permit,
        }))
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
    async fn payload_bytes(&self, id: u64, credit: &Semaphore, data: &[u8]) -> Result<()> {
        self.payload_parts(id, credit, &[data]).await
    }
    async fn payload_parts(&self, id: u64, credit: &Semaphore, parts: &[&[u8]]) -> Result<()> {
        credit
            .acquire()
            .await
            .map_err(|_| "closed native flow")?
            .forget();
        if let Some(shared) = &self.shared {
            shared.send_vectored(DATA, id, parts).await
        } else {
            let mut data = Vec::with_capacity(parts.iter().map(|part| part.len()).sum());
            for part in parts {
                data.extend_from_slice(part);
            }
            self.send(Frame::new(DATA, id, data)).await
        }
    }
    async fn payload_chunks(
        &self,
        id: u64,
        credit: &Semaphore,
        chunks: &[Bytes],
        frame_size: usize,
    ) -> Result<usize> {
        let mut parts = [b"".as_slice(); 16];
        let mut count = 0;
        let mut size = 0;
        let mut total = 0;
        for chunk in chunks {
            let mut data = chunk.as_ref();
            while !data.is_empty() {
                let len = data.len().min(frame_size - size);
                parts[count] = &data[..len];
                count += 1;
                size += len;
                data = &data[len..];
                if size == frame_size || count == parts.len() {
                    self.payload_parts(id, credit, &parts[..count]).await?;
                    total += size;
                    count = 0;
                    size = 0;
                }
            }
        }
        if count != 0 {
            self.payload_parts(id, credit, &parts[..count]).await?;
            total += size;
        }
        Ok(total)
    }
    async fn udp_payload(&self, id: u64, address: &[u8], payload: &[u8]) -> Result<()> {
        if address.is_empty() || address.len() > protocol::MAX_ADDRESS {
            return Err("invalid bridge address".into());
        }
        let credit = self
            .flows
            .lock()
            .unwrap()
            .get(&id)
            .map(|flow| flow.receive_credit.clone());
        let Some(credit) = credit else { return Ok(()) };
        // An unread UDP session drops payloads instead of blocking other sessions.
        let Ok(permit) = credit.try_acquire() else {
            self.udp_credit_dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };
        permit.forget();
        if let Some(shared) = &self.shared {
            let length = (address.len() as u16).to_be_bytes();
            shared
                .send_vectored(DATAGRAM, id, &[&length, address, payload])
                .await
        } else {
            self.send(Frame::new(DATAGRAM, id, address_data(address, payload)?))
                .await
        }
    }
    async fn writable(&self, id: u64, pending: &mut u8) -> Result<()> {
        *pending += 1;
        if *pending as usize >= self.flow_window / 2 {
            let count = *pending;
            *pending = 0;
            static CREDITS: [u8; 9] = [0, 1, 2, 3, 4, 5, 6, 7, 8];
            self.send(Frame::new(
                WRITABLE,
                id,
                Bytes::from_static(&CREDITS[count as usize..count as usize + 1]),
            ))
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
                if count != 0 && count <= self.flow_window && available < self.flow_window {
                    flow.receive_credit
                        .add_permits(count.min(self.flow_window - available));
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
    let receive_window = if config.server {
        SERVER_RECEIVE_WINDOW
    } else if config.receive_buffer_bytes != 0 {
        config.receive_buffer_bytes
    } else {
        8u32 * 1024 * 1024
    };
    t.max_concurrent_bidi_streams((if config.server { 8u32 } else { 0 }).into())
        .max_concurrent_uni_streams(8u32.into())
        // Match Xray's Go Hysteria windows on WAN paths, while bounding memory
        // across streams instead of retaining Quinn's effectively unlimited cap.
        .stream_receive_window((8u32 * 1024 * 1024).into())
        .receive_window(receive_window.into())
        .max_idle_timeout(Some(idle.try_into()?))
        .keep_alive_interval(Some(alive));
    let datagram_buffer = if config.server { 32 * 1024 } else { 256 * 1024 };
    t.datagram_receive_buffer_size(Some(datagram_buffer));
    t.datagram_send_buffer_size(datagram_buffer);
    t.send_window(8 * 1024 * 1024);
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
    let mask_overhead = mask_socket::overhead(&config.udp_masks)?;
    if mask_overhead != 0 && !config.salamander.is_empty() {
        return Err("UDP mask chain and legacy Salamander cannot be combined".into());
    }
    if config.disable_pmtud {
        t.initial_mtu(1200).min_mtu(1200).mtu_discovery_config(None);
    } else if !config.salamander.is_empty() || mask_overhead != 0 {
        let mut discovery = quinn::MtuDiscoveryConfig::default();
        let overhead = if mask_overhead == 0 {
            crate::salamander::SALT_LEN
        } else {
            mask_overhead
        };
        discovery.upper_bound(1500 - 40 - 8 - overhead as u16);
        t.mtu_discovery_config(Some(discovery));
    }
    Ok(t)
}

fn udp_buffers(socket: &UdpSocket, server: bool, diagnostic: bool) {
    for (option, name) in [(libc::SO_RCVBUF, "recv"), (libc::SO_SNDBUF, "send")] {
        let target = match (server, option) {
            (true, libc::SO_RCVBUF) => 4 * 1024 * 1024,
            (true, _) => 1024 * 1024,
            (false, libc::SO_RCVBUF) => 1024 * 1024,
            (false, _) => 256 * 1024,
        };
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
            if before != target * 2 {
                set(target);
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
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
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

fn crypto_provider() -> rustls::crypto::CryptoProvider {
    let mut provider = rustls::crypto::ring::default_provider();
    provider
        .cipher_suites
        .sort_by_key(|suite| suite.suite() != rustls::CipherSuite::TLS13_AES_128_GCM_SHA256);
    provider
}

enum NetworkSocket {
    Udp(UdpSocket),
    Packet(UnixDatagram),
}

fn network_socket(config: &Config, descriptor: i32) -> Result<NetworkSocket> {
    let descriptor = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 0) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let descriptor = unsafe { OwnedFd::from_raw_fd(descriptor) };
    let mut kind: libc::c_int = 0;
    let mut kind_length = std::mem::size_of_val(&kind) as libc::socklen_t;
    let mut address: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut address_length = std::mem::size_of_val(&address) as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            descriptor.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut kind as *mut libc::c_int).cast(),
            &mut kind_length,
        )
    } != 0
        || unsafe {
            libc::getsockname(
                descriptor.as_raw_fd(),
                (&mut address as *mut libc::sockaddr_storage).cast(),
                &mut address_length,
            )
        } != 0
    {
        return Err(io::Error::last_os_error().into());
    }
    let family = address.ss_family as libc::c_int;
    if kind != libc::SOCK_DGRAM
        || if config.packet_bridge {
            family != libc::AF_UNIX
        } else {
            family != libc::AF_INET && family != libc::AF_INET6
        }
    {
        return Err("native network socket has an incompatible family or type".into());
    }
    if config.packet_bridge {
        let socket = unsafe { UnixDatagram::from_raw_fd(descriptor.into_raw_fd()) };
        socket.set_nonblocking(true)?;
        Ok(NetworkSocket::Packet(socket))
    } else {
        let socket = unsafe { UdpSocket::from_raw_fd(descriptor.into_raw_fd()) };
        socket.set_nonblocking(true)?;
        Ok(NetworkSocket::Udp(socket))
    }
}

fn endpoint(config: &Config, socket: NetworkSocket) -> Result<quinn::Endpoint> {
    let runtime = Arc::new(quinn::TokioRuntime);
    let udp: Arc<dyn AsyncUdpSocket> = match socket {
        NetworkSocket::Packet(socket) => {
            if !config.salamander.is_empty()
                || !config.udp_masks.is_empty()
                || config.port_hopping.is_some()
            {
                return Err("packet bridge masks must be applied by Go".into());
            }
            Arc::new(PacketSocket::new(
                socket,
                config.packet_local.parse()?,
                config.packet_batch,
            )?)
        }
        NetworkSocket::Udp(socket) => {
            udp_buffers(&socket, config.server, config.diagnostic);
            if config.port_hopping.is_some() {
                if config.server {
                    return Err("native UDP hopping is client-only".into());
                }
                if socket.peer_addr().is_ok() {
                    return Err("native UDP hopping requires an unconnected UDP socket".into());
                }
            }
            let descriptor = socket.as_raw_fd();
            let udp: Arc<dyn AsyncUdpSocket> = if !config.udp_masks.is_empty() {
                Arc::new(MaskSocket::new(socket, &config.udp_masks)?)
            } else if config.salamander.is_empty() {
                runtime.wrap_udp_socket(socket)?
            } else {
                Arc::new(MaskedSocket::new(socket, config.salamander.as_bytes())?)
            };
            let udp: Arc<dyn AsyncUdpSocket> = Arc::new(ReceiveSocket::new(
                udp,
                descriptor,
                !config.server
                    && config.receive_buffer_bytes != 0
                    && config.receive_buffer_bytes <= 2 * 1024 * 1024,
            ));
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
            udp
        }
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
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
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
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut previous = quinn::ConnectionStats::default();
    let mut sampled = Instant::now();
    loop {
        tokio::select! {
            reason = connection.closed() => {
                report_connection_diagnostics(&connection, role, &mut previous, &mut sampled);
                eprintln!("[XRAY_NET_DIAG][XHYSTERIA] connection closed role={role} peer={peer} reason={reason}");
                return Ok(());
            }
            _ = timer.tick() => {
                report_connection_diagnostics(&connection, role, &mut previous, &mut sampled);
            }
        }
    }
}

fn report_connection_diagnostics(
    connection: &quinn::Connection,
    role: &str,
    previous: &mut quinn::ConnectionStats,
    sampled: &mut Instant,
) {
    let stats = connection.stats();
    let elapsed = sampled.elapsed().as_millis();
    let sent = stats
        .path
        .sent_packets
        .saturating_sub(previous.path.sent_packets);
    let lost = stats
        .path
        .lost_packets
        .saturating_sub(previous.path.lost_packets);
    let total_percent =
        100.0 * stats.path.lost_packets as f64 / stats.path.sent_packets.max(1) as f64;
    let window_percent = if sent == 0 {
        "n/a".to_owned()
    } else {
        format!("{:.3}", 100.0 * lost as f64 / sent as f64)
    };
    eprintln!(
        "[XRAY_NET_DIAG][XHYSTERIA] connection role={role} peer={} rtt_ms={} min_rtt_ms={} tx_bytes={} rx_bytes={} lost_packets={} cwnd={} tx_packets={} rx_packets={} sent_packets={} lost_bytes={} loss_total_pct={total_percent:.3} sample_ms={elapsed} sent_delta={sent} lost_delta={lost} loss_window_pct={window_percent} mtu={} mtu_probe_lost={} black_holes={} udp_received={} udp_rx_dropped={} udp_tx_dropped={} udp_mtu_dropped={}",
        connection.remote_address(),
        stats.path.rtt.as_millis(),
        stats.path.min_rtt.as_millis(),
        stats.udp_tx.bytes,
        stats.udp_rx.bytes,
        stats.path.lost_packets,
        stats.path.cwnd,
        stats.udp_tx.datagrams,
        stats.udp_rx.datagrams,
        stats.path.sent_packets,
        stats.path.lost_bytes,
        stats.path.current_mtu,
        stats.path.lost_plpmtud_probes,
        stats.path.black_holes_detected,
        stats.datagrams.received,
        stats.datagrams.receive_dropped,
        stats.datagrams.send_dropped,
        stats.datagrams.send_dropped_mtu,
    );
    *previous = stats;
    *sampled = Instant::now();
}

async fn client_auth(
    connection: &quinn::Connection,
    config: &Config,
) -> Result<(
    bool,
    h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    BackgroundTask,
)> {
    let (mut driver, mut requests) =
        h3::client::new(h3_quinn::Connection::new(connection.clone())).await?;
    let task = BackgroundTask(tokio::spawn(async move {
        let _ = poll_fn(|cx| driver.poll_close(cx)).await;
    }));
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
    receive_credit: Arc<Semaphore>,
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
                    for chunk in response[header.consumed..].chunks(SERVER_CHUNK) {
                        bus.payload_bytes(id, &receive_credit, chunk).await?;
                    }
                }
                break;
            }
            Err(protocol::Error::Incomplete) if response.len() < 8192 => (),
            Err(error) => return Err(error.into()),
        }
    }
    drop(response);
    let up = async {
        let mut credit = 0;
        let mut budget = 0;
        while let Some(command) = commands.recv().await {
            match command.kind {
                DATA => {
                    bus.writable(id, &mut credit).await?;
                    budget += command.data.len();
                    tx.write_chunk(bus.buffered_chunk(command.data).await?)
                        .await?;
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
        let _ = tx.reset(0u32.into());
        Err("Go flow closed".into())
    };
    let down = async {
        let mut chunks = std::array::from_fn::<_, 8, _>(|_| Bytes::new());
        let mut budget = 0;
        while let Some(count) = rx.read_chunks(&mut chunks).await? {
            budget += bus
                .payload_chunks(id, &receive_credit, &chunks[..count], SERVER_CHUNK)
                .await?;
            for chunk in &mut chunks[..count] {
                *chunk = Bytes::new();
            }
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
    receive_window: Arc<ReceiveWindow>,
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
    let (mut commands, receive_credit) = match bus.register(id, owner).await {
        Ok(commands) => commands,
        Err(error) => {
            stream.reset(0x107);
            stream.stop_sending(0x107);
            return Err(error);
        }
    };
    let streams = receive_window.streams.fetch_add(1, Ordering::Relaxed);
    if streams == 1 {
        receive_window
            .connection
            .set_max_concurrent_bi_streams(64u32.into());
    }
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
        for chunk in header[consumed..].chunks(SERVER_CHUNK) {
            bus.payload_bytes(id, &receive_credit, chunk).await?;
        }
        drop(header);
        let (mut tx, mut rx) = stream.split()?;
        let up = async {
            let mut credit = 0;
            let mut budget = 0;
            while let Some(command) = commands.recv().await {
                match command.kind {
                    DATA => {
                        bus.writable(id, &mut credit).await?;
                        budget += command.data.len();
                        tx.write_chunk(bus.buffered_chunk(command.data).await?)
                            .await?;
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
            let _ = tx.reset(0u32.into());
            Err("Go flow closed".into())
        };
        let down = async {
            let mut chunks = std::array::from_fn::<_, 8, _>(|_| Bytes::new());
            let mut budget = 0;
            while let Some(count) = rx.read_chunks(&mut chunks).await? {
                let received = bus
                    .payload_chunks(id, &receive_credit, &chunks[..count], SERVER_CHUNK)
                    .await?;
                receive_window.observe(received);
                budget += received;
                for chunk in &mut chunks[..count] {
                    *chunk = Bytes::new();
                }
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

async fn relay_http(
    request: http::Request<()>,
    stream: h3::server::RequestStream<Replay, Bytes>,
    bus: Arc<Bus>,
    owner: u64,
    id: u64,
    peer: &str,
) -> Result<()> {
    let request_length = masquerade::content_length(request.headers())?;
    let metadata = serde_json::to_vec(&masquerade::RequestHead {
        method: request.method().as_str(),
        uri: request.uri().to_string(),
        headers: masquerade::encode_headers(request.headers())?,
        remote: peer,
        content_length: request_length.map_or(-1, |length| length as i64),
    })?;
    if metadata.len() > MAX_FRAME {
        return Err("native HTTP request headers exceed bridge limit".into());
    }
    let (mut commands, receive_credit) = bus.register(id, owner).await?;
    bus.send(Frame::new(INCOMING_HTTP, id, metadata)).await?;
    let (mut response_stream, mut request_stream) = stream.split();
    let upload = async {
        let mut received = 0u64;
        while let Some(mut data) = request_stream.recv_data().await? {
            received = received
                .checked_add(data.remaining() as u64)
                .ok_or("HTTP request body too large")?;
            if request_length.is_some_and(|length| received > length) {
                return Err("HTTP request exceeds Content-Length".into());
            }
            while data.has_remaining() {
                let chunk = data.chunk();
                let count = chunk.len().min(SERVER_CHUNK);
                bus.payload_bytes(id, &receive_credit, &chunk[..count])
                    .await?;
                data.advance(count);
            }
        }
        if request_length.is_some_and(|length| received != length) {
            return Err("HTTP request shorter than Content-Length".into());
        }
        if let Some(trailers) = request_stream.recv_trailers().await? {
            let data = serde_json::to_vec(&masquerade::encode_headers(&trailers)?)?;
            if data.len() > MAX_FRAME {
                return Err("native HTTP request trailers exceed bridge limit".into());
            }
            receive_credit.acquire().await?.forget();
            bus.send(Frame::new(HTTP_TRAILERS, id, data)).await?;
        }
        bus.send(Frame::new(FIN, id, Bytes::new())).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let download = async {
        let mut headers_sent = false;
        let mut trailers_sent = false;
        let mut credit = 0;
        let mut body_allowed = true;
        let mut response_length = None;
        let mut sent = 0u64;
        while let Some(command) = commands.recv().await {
            match command.kind {
                HTTP_HEAD if !headers_sent => {
                    let response = masquerade::decode_response(&command.data)?;
                    let final_response = response.status().as_u16() >= 200;
                    body_allowed = request.method() != http::Method::HEAD
                        && !matches!(
                            response.status(),
                            http::StatusCode::NO_CONTENT | http::StatusCode::NOT_MODIFIED
                        );
                    if final_response {
                        response_length = masquerade::content_length(response.headers())?;
                    }
                    response_stream.send_response(response).await?;
                    headers_sent = final_response;
                    bus.writable(id, &mut credit).await?;
                }
                DATA if headers_sent && !trailers_sent => {
                    if body_allowed {
                        sent = sent
                            .checked_add(command.data.len() as u64)
                            .ok_or("HTTP response body too large")?;
                        if response_length.is_some_and(|length| sent > length) {
                            return Err("HTTP response exceeds Content-Length".into());
                        }
                        response_stream
                            .send_data(bus.buffered_chunk(command.data).await?)
                            .await?;
                    }
                    bus.writable(id, &mut credit).await?;
                }
                HTTP_TRAILERS if headers_sent && !trailers_sent => {
                    let trailers: HashMap<String, Vec<String>> =
                        serde_json::from_slice(&command.data)?;
                    response_stream
                        .send_trailers(masquerade::decode_headers(trailers)?)
                        .await?;
                    trailers_sent = true;
                    bus.writable(id, &mut credit).await?;
                }
                FIN if headers_sent => {
                    if body_allowed && response_length.is_some_and(|length| sent != length) {
                        return Err("HTTP response shorter than Content-Length".into());
                    }
                    response_stream.finish().await?;
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
                }
                _ => return Err("invalid native HTTP response frame".into()),
            }
        }
        Err("Go HTTP handler closed".into())
    };
    tokio::pin!(upload);
    tokio::pin!(download);
    tokio::select! {
        result = &mut download => result,
        result = &mut upload => { result?; download.await }
    }
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
            bus.udp_payload(id, address, payload).await?;
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
    first: Bytes,
) -> Result<()> {
    let start = Instant::now();
    let mut defrag = Defragger::new(8)?;
    let mut sessions: HashMap<u32, (u64, Arc<AtomicU64>)> = HashMap::new();
    let mut tasks = JoinSet::new();
    let mut cleanup = tokio::time::interval(Duration::from_secs(1));
    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut first = Some(first);
    loop {
        tokio::select! {
            data = async {
                match first.take() {
                    Some(data) => Ok(data),
                    None => connection.read_datagram().await,
                }
            } => {
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
                    let Ok((mut commands, _)) = bus.register(id, owner).await else { continue };
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
                        let expiry = tokio::time::sleep(idle);
                        tokio::pin!(expiry);
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
                                _ = &mut expiry => {
                                    let inactive = Duration::from_nanos(
                                        (start.elapsed().as_nanos() as u64).saturating_sub(activity.load(Ordering::Relaxed))
                                    );
                                    if inactive >= idle { break; }
                                    expiry.as_mut().reset(tokio::time::Instant::now() + idle.saturating_sub(inactive));
                                }
                            }
                        }
                        bus.done(id, Ok(())).await;
                        session
                    });
                    sessions.insert(session_id, (id, session_activity));
                    id
                };
                bus.udp_payload(id, address, payload).await?;
            }
            _ = cleanup.tick(), if defrag.pending() != 0 => { defrag.expire(start.elapsed().as_nanos() as u64); }
            Some(done) = tasks.join_next(), if !tasks.is_empty() => {
                if let Ok(session) = done { sessions.remove(&session); }
            }
        }
    }
}

async fn serve(
    connection: quinn::Connection,
    config: Arc<Config>,
    bus: Arc<Bus>,
    masquerade: Arc<masquerade::Prepared>,
) -> Result<()> {
    let owner = bus.next.fetch_add(1, Ordering::Relaxed);
    let user = Arc::new(AtomicU32::new(u32::MAX));
    let receive_window = Arc::new(ReceiveWindow {
        connection: connection.clone(),
        budget: bus.capacity.as_ref().unwrap().receive_budget.clone(),
        process_budget: bus
            .capacity
            .as_ref()
            .unwrap()
            .process_receive_budget
            .clone(),
        received: AtomicU64::new(0),
        streams: AtomicU32::new(0),
        reservation: std::sync::Mutex::new(None),
    });
    let mut tasks = JoinSet::<Result<()>>::new();
    if config.diagnostic {
        tasks.spawn(connection_diagnostics(connection.clone(), "server"));
    }
    let peer = connection.remote_address().to_string();
    let mut udp_started = false;
    let mut classifiers = JoinSet::new();
    let auth_deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(auth_deadline);
    let mut incoming = h3_quinn::Connection::new(connection.clone());
    let (http_streams, http_receiver) = mpsc::channel(32);
    let mut h3 = h3::server::builder()
        .max_field_section_size(32 * 1024)
        .build(Dispatch(
            h3_quinn::Connection::new(connection.clone()),
            http_receiver,
        ))
        .await?;
    loop {
        tokio::select! {
            _ = &mut auth_deadline, if user.load(Ordering::Acquire) == u32::MAX => break,
            data = connection.read_datagram(), if !udp_started => {
                let Ok(data) = data else { break };
                if user.load(Ordering::Acquire) == u32::MAX { continue; }
                udp_started = true;
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
                    data,
                ));
            }
            accepted = h3_transport::accept(&mut incoming), if classifiers.len() < 32 && tasks.len() < MAX_FLOWS + 1 => {
                match accepted {
                    Ok(stream) => { classifiers.spawn(async { tokio::time::timeout(Duration::from_secs(10), h3_transport::classify(stream)).await? }); }
                    Err(_) => break,
                }
            }
            accepted = poll_fn(|cx| h3.poll_accept_request_stream(cx)), if tasks.len() < MAX_FLOWS + 1 => {
                match accepted {
                    Ok(Some(stream)) => {
                        let stopped = config.external_masquerade.then(|| stream.stopped());
                        let resolver = h3.create_resolver(h3::frame::FrameStream::new(h3::stream::BufRecvStream::new(stream)));
                        let user = user.clone();
                        let config = config.clone();
                        let connection = connection.clone();
                        let bus = bus.clone();
                        let masquerade = masquerade.clone();
                        let http_id = config.external_masquerade.then(|| bus.next.fetch_add(1, Ordering::Relaxed));
                        tasks.spawn(async move {
                            let response = async { tokio::time::timeout(Duration::from_secs(10), async {
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
                                let (response, body) = if let Some(index) = matched {
                                    let existing = user.load(Ordering::Acquire);
                                    if existing != u32::MAX && existing != index { return Err("connection auth identity changed".into()); }
                                    user.store(index, Ordering::Release);
                                    if config.diagnostic {
                                        eprintln!("[XRAY_NET_DIAG][XHYSTERIA] authenticated role=server peer={}", connection.remote_address());
                                    }
                                    let peer_rate = request.headers().get("Hysteria-CC-RX").and_then(|v| v.to_str().ok()).unwrap_or("0");
                                    congestion::negotiate(&connection, &config.congestion, config.tx_bps, peer_rate);
                                    (http::Response::builder().status(233).header("Hysteria-UDP", "true")
                                        .header("Hysteria-CC-RX", config.rx_bps.to_string()).header("Hysteria-Padding", "n".repeat(256)).body(())?, Bytes::new())
                                } else if let Some(id) = http_id {
                                    return relay_http(request, stream, bus.clone(), owner, id, &connection.remote_address().to_string()).await;
                                } else { masquerade.response(request.method()) };
                                stream.send_response(response).await?;
                                if !body.is_empty() { stream.send_data(body).await?; }
                                stream.finish().await?;
                                Ok(())
                            }).await? };
                            let result: Result<()> = tokio::select! {
                                biased;
                                result = response => result,
                                _ = async { stopped.unwrap().await }, if config.external_masquerade => Err("HTTP request cancelled".into()),
                            };
                            if let Some(id) = http_id {
                                bus.done(id, result).await;
                                return Ok(());
                            }
                            result
                        });
                    }
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
                    let receive_window = receive_window.clone();
                    tasks.spawn(async move { relay_server_tcp(stream, who, &peer, bus, owner, receive_window).await });
                } else {
                    if let Err(error) = http_streams.try_send(stream) {
                        let mut stream = error.into_inner();
                        stream.reset(0x107);
                        stream.stop_sending(0x107);
                    }
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
            tasks.spawn(async move {
                let notification_fd = bridge.notification_fd()?;
                loop {
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
                        let command = bus.command(&incoming, Frame::new(kind, id, data));
                        tokio::pin!(command);
                        loop {
                            tokio::select! {
                                result = &mut command => {
                                    if result.is_err() { return Ok(()); }
                                    break;
                                }
                                result = bridge.notification(&notification_fd) => {
                                    result?;
                                    if bridge.closed() { return Ok(()); }
                                }
                            }
                        }
                    }
                    if drained != 0 {
                        bridge.notify_space();
                    }
                    if drained == 32 || bytes >= 256 * 1024 {
                        // Even a continuously full command ring must service
                        // event-space notifications before the next batch.
                        tokio::select! {
                            biased;
                            result = bridge.notification(&notification_fd) => result?,
                            _ = std::future::ready(()) => (),
                        }
                        tokio::task::yield_now().await;
                        continue;
                    }
                    bridge.notification(&notification_fd).await?;
                }
                Ok(())
            });
        }
    }
}

async fn run(mut config: Config, socket: NetworkSocket, bridge: Bridge) -> Result<()> {
    let masquerade = if config.server {
        Some(Arc::new(masquerade::Prepared::new(
            config.masquerade.take(),
        )?))
    } else {
        None
    };
    let ep = endpoint(&config, socket)?;
    let _endpoint_owner = EndpointOwner(ep.clone());
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
        flow_window: config.flow_window(),
        udp_credit_dropped: AtomicU64::new(0),
        capacity: config.server.then(|| ServerCapacity::new(&config)),
        send_budget: Some({
            Arc::new(Semaphore::new(if config.send_buffer_bytes == 0 {
                if config.server {
                    64 * 1024 * 1024
                } else {
                    8 * 1024 * 1024
                }
            } else {
                config.send_buffer_bytes as usize
            }))
        }),
        process_send_budget: Some(process_send_budget(&config).1.clone()),
        pending_auth: std::sync::Mutex::new(HashMap::new()),
    });
    let config = Arc::new(config);
    let mut capacity_timer = tokio::time::interval(Duration::from_secs(10));
    let mut previous_udp_credit_dropped = 0;
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
                            Ok((commands, receive_credit)) => {
                                let bus = bus.clone(); let connection = connection.clone();
                                tasks.spawn(async move {
                                    let result = relay_client_tcp(connection, frame, commands, receive_credit, bus.clone()).await;
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
                        let Ok((mut commands, _)) = bus.register(id, 0).await else {
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
            incoming = ep.accept(), if config.server => {
                let Some(incoming) = incoming else { break Ok(()) };
                let capacity = bus.capacity.as_ref().unwrap();
                let Ok(permit) = capacity.connections.clone().try_acquire_owned() else {
                    incoming.refuse();
                    capacity.rejected_connections.fetch_add(1, Ordering::Relaxed);
                    capacity.rejection("connection_rejected", bus.flows.lock().unwrap().len());
                    continue;
                };
                let config = config.clone(); let bus = bus.clone();
                let masquerade = masquerade.as_ref().unwrap().clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    if let Ok(Ok(connection)) = tokio::time::timeout(Duration::from_secs(10), incoming).await {
                        let _ = serve(connection, config, bus, masquerade).await;
                    }
                    Ok(())
                });
            }
            _ = capacity_timer.tick(), if config.diagnostic => {
                if let Some(capacity) = &bus.capacity {
                    capacity.report("capacity", bus.flows.lock().unwrap().len());
                }
                let dropped = bus.udp_credit_dropped.load(Ordering::Relaxed);
                eprintln!(
                    "[XRAY_NET_DIAG][XHYSTERIA] event=udp_bridge role={} credit_dropped_total={dropped} credit_dropped_delta={}",
                    if config.server { "server" } else { "client" },
                    dropped.saturating_sub(previous_udp_credit_dropped),
                );
                previous_udp_credit_dropped = dropped;
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
    if let Some(mut driver) = http_driver {
        driver.0.abort();
        let _ = (&mut driver.0).await;
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result
}

pub struct Engine {
    worker: Option<Worker>,
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
            || config.server_workers > 16
            || config.process_workers > 16
            || (config.send_buffer_bytes != 0
                && !(MAX_FRAME as u32..=1 << 30).contains(&config.send_buffer_bytes))
            || (config.receive_buffer_bytes != 0
                && !(MAX_FRAME as u32..=1 << 30).contains(&config.receive_buffer_bytes))
            || (config.process_send_buffer_bytes != 0
                && !(MAX_FRAME as u32..=1 << 30).contains(&config.process_send_buffer_bytes))
            || (config.process_receive_buffer_bytes != 0
                && !(MAX_FRAME as u32..=1 << 30).contains(&config.process_receive_buffer_bytes))
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
        let udp = network_socket(&config, udp_fd)?;
        let (go, native) = UnixStream::pair()?;
        native.set_nonblocking(true)?;
        let worker = start_worker(&config.clone(), async move {
            let bridge = tokio::net::UnixStream::from_std(native).expect("native bridge");
            let duplicate = unsafe { libc::dup(bridge.as_raw_fd()) };
            let diagnostic = config.diagnostic;
            let result = match tokio::spawn(run(config, udp, Bridge::Socket(bridge))).await {
                Ok(result) => result,
                Err(_) => Err("native engine panicked".into()),
            };
            if diagnostic && let Err(error) = &result {
                eprintln!("[XRAY_NET_DIAG][XHYSTERIA] worker: {error}");
            }
            if duplicate >= 0 {
                let mut error_pipe =
                    tokio::net::UnixStream::from_std(unsafe { UnixStream::from_raw_fd(duplicate) })
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
/// transferred to Go together with an arena owner that can outlive engine join.
///
/// # Safety
/// Input pointers must be valid for the call. Go must cease every arena access
/// before calling join, except accesses protected by the returned arena owner.
/// Release that owner exactly once after the last outstanding payload lease.
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
            || config.server_workers > 16
            || config.process_workers > 16
            || (config.send_buffer_bytes != 0
                && !(MAX_FRAME as u32..=1 << 30).contains(&config.send_buffer_bytes))
            || (config.receive_buffer_bytes != 0
                && !(MAX_FRAME as u32..=1 << 30).contains(&config.receive_buffer_bytes))
            || (config.process_send_buffer_bytes != 0
                && !(MAX_FRAME as u32..=1 << 30).contains(&config.process_send_buffer_bytes))
            || (config.process_receive_buffer_bytes != 0
                && !(MAX_FRAME as u32..=1 << 30).contains(&config.process_receive_buffer_bytes))
            || config.udp_idle_timeout > 600
            || (config.server && config.users.is_empty() && !config.external_auth)
            || config.tx_bps > i64::MAX as u64 / 2
            || !["", "brutal", "force-brutal", "bbr", "reno"].contains(&config.congestion.as_str())
            || (config.congestion == "force-brutal" && config.tx_bps == 0)
        {
            return Err("invalid native shared engine configuration".into());
        }
        let udp = network_socket(&config, udp_fd)?;
        let bridge = shared::Bridge::new()?;
        let owner = bridge.clone();
        let worker = start_worker(&config.clone(), async move {
            let diagnostic = config.diagnostic;
            let result = tokio::spawn(run(config, udp, Bridge::Shared(bridge.clone()))).await;
            if diagnostic && !matches!(result, Ok(Ok(()))) {
                eprintln!("[XRAY_NET_DIAG][XHYSTERIA] shared worker: {result:?}");
            }
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
                worker.join();
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
        worker.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udp_mask_chain_mtu_validation_and_legacy_exclusion() {
        let mask = mask_socket::Config {
            kind: "salamander".into(),
            password: "test-password".into(),
            prefix: String::new(),
        };
        for disable_pmtud in [false, true] {
            let mut config = Config {
                disable_pmtud,
                udp_masks: vec![mask.clone(); 16],
                ..Config::default()
            };
            assert!(transport(&config).is_ok());
            config.salamander = "legacy-password".into();
            assert!(transport(&config).is_err());
            config.salamander.clear();
            config.udp_masks.push(mask.clone());
            assert!(transport(&config).is_err());
        }
    }

    #[test]
    fn network_socket_validates_packet_bridge_descriptor_type_and_family() {
        let udp = UdpSocket::bind("127.0.0.1:0").unwrap();
        let (packet, _) = UnixDatagram::pair().unwrap();
        let (stream, _) = UnixStream::pair().unwrap();
        let direct = Config::default();
        let bridge = Config {
            packet_bridge: true,
            ..Config::default()
        };
        assert!(network_socket(&direct, udp.as_raw_fd()).is_ok());
        assert!(network_socket(&bridge, packet.as_raw_fd()).is_ok());
        assert!(network_socket(&bridge, udp.as_raw_fd()).is_err());
        assert!(network_socket(&direct, packet.as_raw_fd()).is_err());
        assert!(network_socket(&bridge, stream.as_raw_fd()).is_err());
        assert!(network_socket(&bridge, -1).is_err());
    }

    fn capacity_bus() -> Bus {
        let config = Config {
            server: true,
            max_connections: 2,
            max_flows: 2,
            ..Config::default()
        };
        let (events, _) = mpsc::channel(1);
        Bus {
            events,
            shared: None,
            flows: std::sync::Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            max_flows: config.flow_limit(),
            flow_window: config.flow_window(),
            udp_credit_dropped: AtomicU64::new(0),
            capacity: Some(ServerCapacity::new(&config)),
            send_budget: Some(Arc::new(Semaphore::new(8))),
            process_send_budget: None,
            pending_auth: std::sync::Mutex::new(HashMap::new()),
        }
    }

    #[tokio::test]
    async fn unread_udp_flow_counts_credit_drops_without_blocking_other_flows() {
        let mut bus = capacity_bus();
        let (events, mut receiver) = mpsc::channel(1);
        bus.events = events;
        let (_, blocked) = bus.register(1, 1).await.unwrap();
        let (_, readable) = bus.register(2, 1).await.unwrap();
        let window = blocked.available_permits();
        blocked.acquire_many(window as u32).await.unwrap().forget();
        bus.udp_payload(1, b"target", b"dropped").await.unwrap();
        assert_eq!(bus.udp_credit_dropped.load(Ordering::Relaxed), 1);
        assert!(receiver.try_recv().is_err());
        bus.udp_payload(2, b"target", b"delivered").await.unwrap();
        let frame = receiver.try_recv().unwrap();
        assert_eq!(frame.id, 2);
        assert_eq!(
            parse_address(&frame.data).unwrap(),
            (b"target".as_slice(), b"delivered".as_slice())
        );
        assert_eq!(readable.available_permits(), window - 1);
        assert_eq!(blocked.available_permits(), 0);
        assert_eq!(bus.udp_credit_dropped.load(Ordering::Relaxed), 1);
        bus.route(Frame::new(CREDIT, 1, Bytes::from_static(&[1])))
            .await;
        bus.udp_payload(1, b"target", b"resumed").await.unwrap();
        assert_eq!(
            parse_address(&receiver.try_recv().unwrap().data).unwrap().1,
            b"resumed"
        );
        assert_eq!(bus.udp_credit_dropped.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn flow_capacity_recovers_after_close() {
        let bus = capacity_bus();
        let _first = bus.register(1, 1).await.unwrap();
        let _second = bus.register(2, 2).await.unwrap();
        assert!(bus.register(3, 2).await.is_err());
        assert_eq!(
            bus.capacity
                .as_ref()
                .unwrap()
                .rejected_flows
                .load(Ordering::Relaxed),
            1
        );
        bus.route(Frame::new(CLOSE, 1, Bytes::new())).await;
        assert!(bus.register(3, 2).await.is_ok());
        assert_eq!(bus.flows.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn send_budget_follows_the_last_quic_chunk_reference() {
        let bus = capacity_bus();
        let budget = bus.send_budget.as_ref().unwrap();
        let data = bus
            .buffered_chunk(Bytes::from_static(b"12345678"))
            .await
            .unwrap();
        let retained = data.slice(4..);
        drop(data);
        assert_eq!(budget.available_permits(), 0);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                bus.buffered_chunk(Bytes::from_static(b"x"))
            )
            .await
            .is_err()
        );
        drop(retained);
        assert_eq!(budget.available_permits(), 8);
        let data = bus
            .buffered_chunk(Bytes::from_static(b"reused"))
            .await
            .unwrap();
        assert_eq!(data.as_ref(), b"reused");
        drop(data);
        assert_eq!(budget.available_permits(), 8);
    }

    #[tokio::test]
    async fn send_budget_is_shared_between_inbound_and_outbound_engines() {
        let budget = Arc::new(Semaphore::new(8));
        let mut inbound = capacity_bus();
        let mut outbound = capacity_bus();
        inbound.process_send_budget = Some(budget.clone());
        outbound.process_send_budget = Some(budget.clone());
        let data = inbound
            .buffered_chunk(Bytes::from_static(b"12345678"))
            .await
            .unwrap();
        let retained = data.slice(4..);
        drop(data);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                outbound.buffered_chunk(Bytes::from_static(b"x"))
            )
            .await
            .is_err()
        );
        assert_eq!(
            outbound.send_budget.as_ref().unwrap().available_permits(),
            8
        );
        drop(retained);
        let data = outbound
            .buffered_chunk(Bytes::from_static(b"12345678"))
            .await
            .unwrap();
        assert_eq!(budget.available_permits(), 0);
        drop(data);
        assert_eq!(budget.available_permits(), 8);
    }

    #[tokio::test]
    async fn chunk_batches_preserve_order_and_server_frame_limits() {
        let mut bus = capacity_bus();
        let bridge = shared::Bridge::new().unwrap();
        bus.shared = Some(bridge.clone());
        let (_commands, receive_credit) = bus.register(1, 1).await.unwrap();
        let chunks = [
            Bytes::from(vec![0xa7; 10000]),
            Bytes::from(vec![0x71; 40000]),
        ];
        assert_eq!(
            bus.payload_chunks(1, &receive_credit, &chunks, SERVER_CHUNK)
                .await
                .unwrap(),
            50000
        );
        let mut received = Vec::new();
        for _ in 0..4 {
            let (kind, id, data) = bridge.pop(false).unwrap().unwrap();
            assert_eq!((kind, id), (DATA, 1));
            assert!(data.len() <= SERVER_CHUNK);
            received.extend_from_slice(&data);
        }
        assert_eq!(received, [chunks[0].as_ref(), chunks[1].as_ref()].concat());
        assert!(bridge.pop(false).unwrap().is_none());
    }

    #[tokio::test]
    async fn retained_flow_credit_wakes_on_close_and_is_not_reused() {
        let mut bus = capacity_bus();
        let bridge = shared::Bridge::new().unwrap();
        bus.shared = Some(bridge.clone());
        let (_commands, credit) = bus.register(1, 1).await.unwrap();
        for _ in 0..bus.flow_window {
            bus.payload_bytes(1, &credit, b"data").await.unwrap();
            assert_eq!(bridge.pop(false).unwrap().unwrap().2.as_ref(), b"data");
        }
        let pending = bus.payload_bytes(1, &credit, b"blocked");
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut pending)
                .await
                .is_err()
        );
        bus.route(Frame::new(CLOSE, 1, Bytes::new())).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut pending)
                .await
                .unwrap()
                .is_err()
        );
        let (_replacement_commands, replacement_credit) = bus.register(1, 2).await.unwrap();
        bus.route(Frame::new(CREDIT, 1, Bytes::from_static(&[1])))
            .await;
        assert!(bus.payload_bytes(1, &credit, b"stale").await.is_err());
        assert!(bridge.pop(false).unwrap().is_none());
        bus.payload_bytes(1, &replacement_credit, b"replacement")
            .await
            .unwrap();
        assert_eq!(
            bridge.pop(false).unwrap().unwrap().2.as_ref(),
            b"replacement"
        );
    }

    #[test]
    fn default_server_limits_fit_small_vps() {
        let config = Config {
            server: true,
            ..Config::default()
        };
        assert_eq!(config.connection_limit(), 32);
        assert_eq!(config.flow_limit(), 64);
    }

    #[test]
    fn client_engines_share_the_runtime_and_join_independently() {
        let config = Config::default();
        let (sender, receiver) = std::sync::mpsc::channel();
        let first_sender = sender.clone();
        let first = start_worker(&config, async move {
            first_sender.send(std::thread::current().id()).unwrap();
        })
        .unwrap();
        let first_thread = receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        first.join();
        let second = start_worker(&config, async move {
            sender.send(std::thread::current().id()).unwrap();
        })
        .unwrap();
        assert_eq!(
            first_thread,
            receiver.recv_timeout(Duration::from_secs(2)).unwrap()
        );
        second.join();
        let (sender, receiver) = std::sync::mpsc::channel();
        let server = start_worker(
            &Config {
                server: true,
                server_workers: 1,
                ..Config::default()
            },
            async move {
                sender.send(std::thread::current().id()).unwrap();
            },
        )
        .unwrap();
        assert_ne!(
            first_thread,
            receiver.recv_timeout(Duration::from_secs(2)).unwrap()
        );
        server.join();
        assert!(
            worker_count(&Config {
                server_workers: 17,
                ..Config::default()
            })
            .is_err()
        );
    }

    #[tokio::test]
    async fn dropping_auth_driver_aborts_the_task() {
        let (started, ready) = oneshot::channel();
        let (released, stopped) = oneshot::channel::<()>();
        let task = BackgroundTask(tokio::spawn(async move {
            let _released = released;
            let _ = started.send(());
            std::future::pending::<()>().await;
        }));
        ready.await.unwrap();
        drop(task);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), stopped)
                .await
                .unwrap()
                .is_err()
        );
    }

    #[test]
    fn receive_budget_reports_the_first_process_limit() {
        let capacity = ServerCapacity::new(&Config::default());
        let smaller = ServerCapacity::new(&Config {
            process_receive_buffer_bytes: MAX_FRAME as u32,
            diagnostic: true,
            ..Config::default()
        });
        assert_eq!(smaller.process_buffer_bytes, capacity.process_buffer_bytes);
        assert!(Arc::ptr_eq(
            &smaller.process_receive_budget,
            &capacity.process_receive_budget
        ));
        smaller.report("capacity", 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocked_control_queue_still_delivers_event_space_notifications() {
        use std::future::Future;
        use std::os::fd::OwnedFd;
        let bridge = shared::Bridge::new().unwrap();
        let view = bridge.view().unwrap();
        unsafe {
            shared::xhys_shared_release(view.owner);
        }
        let (_notify, wake, _space) = unsafe {
            (
                OwnedFd::from_raw_fd(view.notify_fd),
                OwnedFd::from_raw_fd(view.wake_fd),
                OwnedFd::from_raw_fd(view.space_fd),
            )
        };
        for id in 0..shared::CAPACITY {
            assert!(bridge.push(false, DATA, id as u64, b"full").unwrap());
        }
        let (events, outgoing) = mpsc::channel(1);
        let bus = Arc::new(Bus {
            events,
            shared: Some(bridge.clone()),
            flows: std::sync::Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            max_flows: 128,
            flow_window: 16,
            udp_credit_dropped: AtomicU64::new(0),
            capacity: None,
            send_budget: None,
            process_send_budget: None,
            pending_auth: std::sync::Mutex::new(HashMap::new()),
        });
        let (incoming, _receiver) = mpsc::channel(1);
        // The first control fills the queue, the second waits for space.
        incoming
            .send(Frame::new(OPEN_UDP, 1, Bytes::new()))
            .await
            .unwrap();
        assert!(bridge.push(true, OPEN_UDP, 2, b"target:53").unwrap());
        let mut tasks = JoinSet::new();
        bridge_tasks(
            Bridge::Shared(bridge.clone()),
            outgoing,
            incoming,
            bus.clone(),
            &mut tasks,
        );
        let (started, registered) = oneshot::channel();
        let waiting = tokio::spawn(async move {
            let publish = bus.send(Frame::new(READY, 0, Bytes::new()));
            tokio::pin!(publish);
            poll_fn(|cx| {
                assert!(publish.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            started.send(()).unwrap();
            publish.await
        });
        registered.await.unwrap();
        assert!(!waiting.is_finished());
        bridge.pop(false).unwrap().unwrap();
        let value = 1u64.to_ne_bytes();
        assert_eq!(
            unsafe { libc::write(wake.as_raw_fd(), value.as_ptr().cast(), value.len()) },
            8
        );
        tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        bridge.close();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

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
