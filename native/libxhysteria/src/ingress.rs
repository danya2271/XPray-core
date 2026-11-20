//! Opt-in managed ingress. Masks and batched public UDP I/O stay in Rust.
//! Control changes are acknowledged by the sole receive owner, between batches.
//! This module does not certify QUIC retirement or implement crash recovery.

use crate::{generation_cid, mask_socket, packet_ring, packet_socket};
use quinn::{AsyncUdpSocket, Runtime, udp};
use serde::Deserialize;
use std::{
    collections::{HashMap, VecDeque},
    ffi::c_void,
    future::poll_fn,
    io::{self, IoSliceMut},
    net::{SocketAddr, UdpSocket},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    os::unix::net::UnixDatagram,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    task::{Context, Poll},
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::{io::Interest, sync::mpsc as control, task::JoinSet};

const HEADER: usize = 24;
const BATCH: usize = 32;
const PIN_LIMIT: usize = 4096;
// A bounded slow path for transient backpressure, not a larger public socket
// buffer. Healthy workers keep the borrowed-buffer, zero-allocation fast path.
const QUEUE_BYTES: usize = 512 * 1024;
const QUEUE_FRAMES: usize = 256;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    key: [u8; 16],
    slot: u8,
    #[serde(default)]
    masks: Vec<mask_socket::Config>,
}

#[derive(Default)]
struct Health {
    drops: AtomicU64,
    received: AtomicU64,
    forwarded: AtomicU64,
    backpressure: AtomicU64,
    queue_drops: AtomicU64,
    send_drops: AtomicU64,
    egress_drops: AtomicU64,
    queued_bytes: AtomicU64,
    peak_queued_bytes: AtomicU64,
    running: AtomicBool,
}

impl Health {
    fn drop_send(&self, packets: usize) {
        self.send_drops.fetch_add(packets as u64, Ordering::Relaxed);
        self.drops.fetch_add(packets as u64, Ordering::Relaxed);
    }

    fn drop_egress(&self) {
        self.egress_drops.fetch_add(1, Ordering::Relaxed);
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[repr(C)]
#[derive(Default)]
pub struct IngressStats {
    received: u64,
    forwarded: u64,
    backpressure: u64,
    queue_drops: u64,
    send_drops: u64,
    egress_drops: u64,
    queued_bytes: u64,
    peak_queued_bytes: u64,
    running: u64,
}

enum Command {
    Add(u8, UnixDatagram, mpsc::SyncSender<bool>),
    AddRing(u8, UnixDatagram, mpsc::SyncSender<Option<[OwnedFd; 3]>>),
    Activate(u8, mpsc::SyncSender<bool>),
    Stop,
}

struct Ingress {
    commands: control::UnboundedSender<Command>,
    worker: Option<JoinHandle<()>>,
    health: Arc<Health>,
}

// Pins are touched only by the receive owner. Established routes require no
// map lookup, mutex, address hashing, or per-packet allocation.
struct Routes {
    codec: generation_cid::Generator,
    current: u8,
    pins: HashMap<(u32, u8, [u8; 20]), (u8, Instant)>,
    next_gc: Instant,
}

impl Routes {
    fn route(&mut self, packet: &[u8], live: &[bool; 256]) -> Option<u8> {
        let first = *packet.first()?;
        if first & 0x80 == 0 {
            if packet.len() < 18 {
                return None;
            }
            let slot = self.codec.decode(&packet[1..17])?;
            return live[slot as usize].then_some(slot);
        }
        let version = u32::from_be_bytes(packet.get(1..5)?.try_into().ok()?);
        let length = *packet.get(5)? as usize;
        if length > 20 {
            return None;
        }
        let dcid = packet.get(6..6 + length)?;
        let scid = *packet.get(6 + length)? as usize;
        if scid > 20 || packet.len() < 7 + length + scid {
            return None;
        }
        if let Some(slot) = self.codec.decode(dcid) {
            return live[slot as usize].then_some(slot);
        }
        let kind = (first >> 4) & 3;
        let initial = (version == 1 && kind == 0) || (version == 0x6b3343cf && kind == 1);
        let early = (version == 1 && kind == 1) || (version == 0x6b3343cf && kind == 2);
        if length < 8 || (!initial && !early) {
            return None;
        }
        let now = Instant::now();
        let mut cid = [0; 20];
        cid[..length].copy_from_slice(dcid);
        let key = (version, length as u8, cid);
        if let Some(&(slot, until)) = self.pins.get(&key)
            && now < until
        {
            return live[slot as usize].then_some(slot);
        }
        if !initial || packet.len() < 1200 {
            return None;
        }
        if now >= self.next_gc {
            self.pins.retain(|_, owner| now < owner.1);
            self.next_gc = now + Duration::from_secs(1);
        }
        if !self.pins.contains_key(&key) && self.pins.len() >= PIN_LIMIT {
            return None;
        }
        if !live[self.current as usize] {
            return None;
        }
        self.pins
            .insert(key, (self.current, now + Duration::from_secs(30)));
        Some(self.current)
    }
}

fn frame_shape(
    packets: &[(SocketAddr, &[u8])],
    max_segments: usize,
) -> io::Result<(usize, [u8; HEADER])> {
    let (address, payload) = packets[0];
    let stride = payload.len();
    let mut count = 1;
    let mut length = stride;
    while count < max_segments && count < packets.len() {
        let (peer, next) = packets[count];
        if peer != address || next.len() > stride || length + next.len() > 65535 {
            break;
        }
        count += 1;
        length += next.len();
        if next.len() < stride {
            break;
        }
    }
    let mut header = packet_socket::encode_header(address, length)?;
    if count > 1 {
        header[0] = 2;
        header[20..22].copy_from_slice(&(stride as u16).to_be_bytes());
        header[22..24].copy_from_slice(&(length as u16).to_be_bytes());
    }
    Ok((count, header))
}

fn send_messages(
    socket: &tokio::net::UnixDatagram,
    messages: &mut [libc::mmsghdr],
) -> io::Result<usize> {
    socket.try_io(Interest::WRITABLE, || {
        loop {
            let result = unsafe {
                libc::sendmmsg(
                    socket.as_raw_fd(),
                    messages.as_mut_ptr(),
                    messages.len() as u32,
                    libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                )
            };
            if result >= 0 {
                return Ok(result as usize);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    })
}

// A batch borrows the receive buffers only for this synchronous syscall.
// Pointers never survive an await or escape into another thread.
fn forward(
    socket: &tokio::net::UnixDatagram,
    packets: &[(SocketAddr, &[u8])],
) -> io::Result<usize> {
    let mut headers = [[0u8; HEADER]; BATCH];
    let mut vectors: [[libc::iovec; 9]; BATCH] = unsafe { std::mem::zeroed() };
    let mut messages: [libc::mmsghdr; BATCH] = unsafe { std::mem::zeroed() };
    let mut index = 0;
    let mut frames = 0;
    let mut segments = [0; BATCH];
    while index < packets.len() {
        let (count, header) = frame_shape(&packets[index..], 8)?;
        headers[frames] = header;
        vectors[frames][0] = libc::iovec {
            iov_base: headers[frames].as_mut_ptr().cast(),
            iov_len: HEADER,
        };
        for offset in 0..count {
            let data = packets[index + offset].1;
            vectors[frames][offset + 1] = libc::iovec {
                iov_base: data.as_ptr().cast_mut().cast(),
                iov_len: data.len(),
            };
        }
        messages[frames].msg_hdr.msg_iov = vectors[frames].as_mut_ptr();
        messages[frames].msg_hdr.msg_iovlen = count + 1;
        segments[frames] = count;
        frames += 1;
        index += count;
    }
    let sent = send_messages(socket, &mut messages[..frames])?;
    Ok(segments[..sent].iter().sum())
}

struct QueuedFrame {
    wire: Vec<u8>,
    packets: usize,
}

struct Peer {
    socket: Arc<tokio::net::UnixDatagram>,
    ring: Option<packet_ring::Sender>,
    queue: VecDeque<QueuedFrame>,
    bytes: usize,
}

impl Peer {
    fn try_forward(&mut self, packets: &[(SocketAddr, &[u8])]) -> io::Result<usize> {
        let Some(ring) = self.ring.as_mut() else {
            return forward(&self.socket, packets);
        };
        let mut batch = ring.batch();
        let mut sent = 0;
        while sent < packets.len() {
            let (count, header) = frame_shape(&packets[sent..], 32)?;
            match batch.push(&header, &packets[sent..sent + count]) {
                Ok(()) => sent += count,
                Err(_) if sent != 0 => break,
                Err(error) => return Err(error),
            }
        }
        Ok(sent)
    }

    fn forward(&mut self, packets: &[(SocketAddr, &[u8])], health: &Health) {
        let sent = if self.queue.is_empty() {
            match self.try_forward(packets) {
                Ok(sent) => sent,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => 0,
                Err(_) => {
                    health.drop_send(packets.len());
                    return;
                }
            }
        } else {
            0
        };
        health.forwarded.fetch_add(sent as u64, Ordering::Relaxed);
        let pending = &packets[sent..];
        health
            .backpressure
            .fetch_add(pending.len() as u64, Ordering::Relaxed);
        let mut index = 0;
        while index < pending.len() {
            let Ok((count, header)) =
                frame_shape(&pending[index..], if self.ring.is_some() { 32 } else { 8 })
            else {
                health.drop_send(pending.len() - index);
                break;
            };
            let length = HEADER
                + pending[index..index + count]
                    .iter()
                    .map(|p| p.1.len())
                    .sum::<usize>();
            if self.queue.len() == QUEUE_FRAMES || self.bytes + length > QUEUE_BYTES {
                health
                    .queue_drops
                    .fetch_add(count as u64, Ordering::Relaxed);
                health.drops.fetch_add(count as u64, Ordering::Relaxed);
            } else {
                let mut wire = Vec::with_capacity(length);
                wire.extend_from_slice(&header);
                for (_, payload) in &pending[index..index + count] {
                    wire.extend_from_slice(payload);
                }
                self.queue.push_back(QueuedFrame {
                    wire,
                    packets: count,
                });
                self.bytes += length;
                let total = health
                    .queued_bytes
                    .fetch_add(length as u64, Ordering::Relaxed)
                    + length as u64;
                health.peak_queued_bytes.fetch_max(total, Ordering::Relaxed);
            }
            index += count;
        }
    }

    fn flush(&mut self, health: &Health) -> io::Result<()> {
        if let Some(ring) = self.ring.as_mut() {
            let mut sent = 0;
            let mut batch = ring.batch();
            for frame in self.queue.iter().take(BATCH) {
                match batch.push_wire(&frame.wire) {
                    Ok(()) => sent += 1,
                    Err(_) if sent != 0 => break,
                    Err(error) => return Err(error),
                }
            }
            drop(batch);
            self.release_frames(sent, health);
            return Ok(());
        }
        let mut vectors: [libc::iovec; BATCH] = unsafe { std::mem::zeroed() };
        let mut messages: [libc::mmsghdr; BATCH] = unsafe { std::mem::zeroed() };
        let count = self.queue.len().min(BATCH);
        for (index, frame) in self.queue.iter().take(count).enumerate() {
            vectors[index] = libc::iovec {
                iov_base: frame.wire.as_ptr().cast_mut().cast(),
                iov_len: frame.wire.len(),
            };
            messages[index].msg_hdr.msg_iov = &mut vectors[index];
            messages[index].msg_hdr.msg_iovlen = 1;
        }
        // No queue mutation occurs until the borrowed frame pointers are dead.
        let sent = send_messages(&self.socket, &mut messages[..count])?;
        self.release_frames(sent, health);
        Ok(())
    }

    fn release_frames(&mut self, sent: usize, health: &Health) {
        let mut packets = 0;
        let mut bytes = 0;
        for _ in 0..sent {
            let frame = self.queue.pop_front().unwrap();
            packets += frame.packets;
            bytes += frame.wire.len();
        }
        self.bytes -= bytes;
        health
            .queued_bytes
            .fetch_sub(bytes as u64, Ordering::Relaxed);
        health
            .forwarded
            .fetch_add(packets as u64, Ordering::Relaxed);
    }

    fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(ring) = &self.ring {
            ring.poll_writable(cx)
        } else {
            self.socket.poll_send_ready(cx)
        }
    }

    fn discard(&mut self, health: &Health) {
        let packets = self.queue.iter().map(|f| f.packets).sum();
        health.drop_send(packets);
        health
            .queued_bytes
            .fetch_sub(self.bytes as u64, Ordering::Relaxed);
        self.queue.clear();
        self.bytes = 0;
    }
}

// One readiness turn per generation, never wait for one worker in the public
// receive branch. Admission commands and healthy workers remain independent.
fn poll_pending(
    peers: &mut [Option<Peer>; 256],
    slots: &[u8],
    health: &Health,
    cx: &mut Context<'_>,
) -> Poll<()> {
    let mut progress = false;
    for &slot in slots {
        let peer = peers[slot as usize].as_mut().unwrap();
        if peer.queue.is_empty() {
            continue;
        }
        match peer.poll_writable(cx) {
            Poll::Ready(Ok(())) => match peer.flush(health) {
                Ok(()) => progress = true,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    // try_io cleared readiness. Register the waiter again.
                    if peer.poll_writable(cx).is_ready() {
                        cx.waker().wake_by_ref();
                    }
                }
                Err(_) => {
                    peer.discard(health);
                    progress = true;
                }
            },
            Poll::Ready(Err(_)) => {
                peer.discard(health);
                progress = true;
            }
            Poll::Pending => {}
        }
    }
    if progress {
        Poll::Ready(())
    } else {
        Poll::Pending
    }
}

async fn egress(
    socket: Arc<tokio::net::UnixDatagram>,
    public: Arc<dyn AsyncUdpSocket>,
    health: Arc<Health>,
) {
    let mut wire = vec![0u8; 65535 + HEADER + 1];
    let mut writable = public.clone().create_io_poller();
    loop {
        let Ok(count) = socket.recv(&mut wire).await else {
            return;
        };
        let frame = &mut wire[..count];
        if frame.len() <= HEADER || frame.len() > 65535 + HEADER {
            health.drop_egress();
            continue;
        }
        let length = frame.len() - HEADER;
        let stride = if frame[0] == 2 {
            let stride = u16::from_be_bytes(frame[20..22].try_into().unwrap()) as usize;
            let size = u16::from_be_bytes(frame[22..24].try_into().unwrap()) as usize;
            if size != length || stride == 0 || stride > length || length.div_ceil(stride) > 8 {
                health.drop_egress();
                continue;
            }
            frame[0] = 1;
            frame[20..24].copy_from_slice(&(length as u32).to_be_bytes());
            Some(stride)
        } else {
            None
        };
        let Ok(address) = packet_socket::decode_header(frame[..HEADER].try_into().unwrap(), length)
        else {
            health.drop_egress();
            continue;
        };
        let transmit = udp::Transmit {
            destination: address,
            contents: &frame[HEADER..],
            segment_size: stride,
            ecn: None,
            src_ip: None,
        };
        loop {
            if poll_fn(|cx| writable.as_mut().poll_writable(cx))
                .await
                .is_err()
            {
                return;
            }
            match public.try_send(&transmit) {
                Ok(()) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(_) => {
                    health.drop_egress();
                    break;
                }
            }
        }
    }
}

async fn run(
    config: Config,
    socket: UdpSocket,
    mut commands: control::UnboundedReceiver<Command>,
    health: Arc<Health>,
    ready: mpsc::SyncSender<bool>,
) -> io::Result<()> {
    let local = socket.local_addr()?;
    let public: Arc<dyn AsyncUdpSocket> = if config.masks.is_empty() {
        quinn::TokioRuntime.wrap_udp_socket(socket)?
    } else {
        Arc::new(mask_socket::MaskSocket::new(socket, &config.masks)?)
    };
    let mut routes = Routes {
        codec: generation_cid::Generator::new(&generation_cid::Config {
            key: config.key,
            slot: config.slot,
        }),
        current: config.slot,
        pins: HashMap::new(),
        next_gc: Instant::now(),
    };
    let mut peers: [Option<Peer>; 256] = std::array::from_fn(|_| None);
    let mut slots = Vec::new();
    let mut live = [false; 256];
    let mut outputs = JoinSet::new();
    // Quinn's batched I/O may return GRO data. Preserve each segment's CID
    // ownership rather than forwarding an aggregate to one generation.
    let mut storage: Vec<Vec<u8>> = (0..BATCH).map(|_| vec![0; 65536]).collect();
    let mut buffers: Vec<IoSliceMut<'_>> = storage.iter_mut().map(|p| IoSliceMut::new(p)).collect();
    let mut metadata = [udp::RecvMeta::default(); BATCH];
    health.running.store(true, Ordering::Release);
    let _ = ready.send(true);
    loop {
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(Command::Add(slot, socket, ack)) => {
                    let index = slot as usize;
                    if peers[index].is_some() { let _ = ack.send(false); continue; }
                    match tokio::net::UnixDatagram::from_std(socket) {
                        Ok(socket) => {
                            let socket = Arc::new(socket);
                            peers[index] = Some(Peer { socket: socket.clone(), ring: None, queue: VecDeque::new(), bytes: 0 });
                            slots.push(slot); live[index] = true;
                            outputs.spawn(egress(socket, public.clone(), health.clone()));
                            let _ = ack.send(true);
                        }
                        Err(_) => { let _ = ack.send(false); }
                    }
                }
                Some(Command::AddRing(slot, socket, ack)) => {
                    let index = slot as usize;
                    if peers[index].is_some() { let _ = ack.send(None); continue; }
                    let result = (|| -> io::Result<_> {
                        let socket = Arc::new(tokio::net::UnixDatagram::from_std(socket)?);
                        let ring = packet_ring::Sender::new()?;
                        let descriptors = ring.export()?;
                        peers[index] = Some(Peer { socket: socket.clone(), ring: Some(ring), queue: VecDeque::new(), bytes: 0 });
                        slots.push(slot); live[index] = true;
                        outputs.spawn(egress(socket, public.clone(), health.clone()));
                        Ok(descriptors)
                    })();
                    let _ = ack.send(result.ok());
                }
                Some(Command::Activate(slot, ack)) => {
                    let healthy = peers[slot as usize].is_some();
                    if healthy { routes.current = slot; }
                    let _ = ack.send(healthy);
                }
                _ => break,
            },
            _ = outputs.join_next(), if !outputs.is_empty() => break,
            _ = poll_fn(|cx| poll_pending(&mut peers, &slots, &health, cx)) => {}
            received = poll_fn(|cx| public.poll_recv(cx, &mut buffers, &mut metadata)) => {
                let count = received?;
                let mut pending: [(SocketAddr, &[u8]); BATCH] = [(local, &[]); BATCH];
                let mut pending_count = 0;
                let mut pending_slot = 0;
                for index in 0..count {
                    let meta = metadata[index];
                    if meta.stride == 0 { continue; }
                    health.received.fetch_add(meta.len.div_ceil(meta.stride) as u64, Ordering::Relaxed);
                    for payload in buffers[index][..meta.len].chunks(meta.stride) {
                        let Some(slot) = routes.route(payload, &live) else { continue; };
                        if pending_count != 0 && (pending_slot != slot || pending_count == BATCH) {
                            peers[pending_slot as usize].as_mut().unwrap().forward(&pending[..pending_count], &health);
                            pending_count = 0;
                        }
                        pending_slot = slot;
                        let address = if local.is_ipv4() {
                            match meta.addr { SocketAddr::V6(a) if a.ip().to_ipv4_mapped().is_some() => SocketAddr::new(a.ip().to_ipv4_mapped().unwrap().into(), a.port()), other => other }
                        } else { meta.addr };
                        pending[pending_count] = (address, payload);
                        pending_count += 1;
                    }
                }
                if pending_count != 0 {
                    peers[pending_slot as usize].as_mut().unwrap().forward(&pending[..pending_count], &health);
                }
            }
        }
    }
    outputs.abort_all();
    while outputs.join_next().await.is_some() {}
    Ok(())
}

/// # Safety
/// `data` must point to `length` readable bytes. Descriptors are duplicated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_start(
    data: *const u8,
    length: usize,
    descriptor: i32,
) -> *mut c_void {
    if data.is_null() || length > 32768 {
        return std::ptr::null_mut();
    }
    let Ok(config) =
        serde_json::from_slice::<Config>(unsafe { std::slice::from_raw_parts(data, length) })
    else {
        return std::ptr::null_mut();
    };
    if config.key == [0; 16] || mask_socket::overhead(&config.masks).is_err() {
        return std::ptr::null_mut();
    }
    let fd = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 0) };
    if fd < 0 {
        return std::ptr::null_mut();
    }
    let socket = unsafe { UdpSocket::from_raw_fd(fd) };
    let Ok(local) = socket.local_addr() else {
        return std::ptr::null_mut();
    };
    if local.ip().is_unspecified() || local.port() == 0 || socket.set_nonblocking(true).is_err() {
        return std::ptr::null_mut();
    }
    let (tx, rx) = control::unbounded_channel();
    let (ready, wait) = mpsc::sync_channel(1);
    let health = Arc::new(Health::default());
    let owner = health.clone();
    let Ok(worker) = std::thread::Builder::new()
        .name("xhys-ingress".into())
        .spawn(move || {
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                let _ = runtime.block_on(run(config, socket, rx, owner.clone(), ready));
            }
            owner.running.store(false, Ordering::Release);
        })
    else {
        return std::ptr::null_mut();
    };
    if wait.recv().ok() != Some(true) {
        let _ = worker.join();
        return std::ptr::null_mut();
    }
    Box::into_raw(Box::new(Ingress {
        commands: tx,
        worker: Some(worker),
        health,
    }))
    .cast()
}

/// # Safety
/// `handle` must identify a live ingress and exclude join.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_add(handle: *mut c_void, slot: u8, descriptor: i32) -> bool {
    let Some(ingress) = (unsafe { handle.cast::<Ingress>().as_ref() }) else {
        return false;
    };
    let fd = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 0) };
    if fd < 0 {
        return false;
    }
    let descriptor = unsafe { OwnedFd::from_raw_fd(fd) };
    let socket = UnixDatagram::from(descriptor);
    if socket.peer_addr().is_err() || socket.set_nonblocking(true).is_err() {
        return false;
    }
    let (tx, rx) = mpsc::sync_channel(1);
    ingress
        .commands
        .send(Command::Add(slot, socket, tx))
        .is_ok()
        && rx.recv_timeout(Duration::from_secs(3)).ok() == Some(true)
}

/// # Safety
/// `handle` must identify a live ingress and exclude join.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_activate(handle: *mut c_void, slot: u8) -> bool {
    let Some(ingress) = (unsafe { handle.cast::<Ingress>().as_ref() }) else {
        return false;
    };
    let (tx, rx) = mpsc::sync_channel(1);
    ingress.commands.send(Command::Activate(slot, tx)).is_ok()
        && rx.recv_timeout(Duration::from_secs(3)).ok() == Some(true)
}

/// # Safety
/// `handle` must identify a live ingress and exclude join.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_dropped(handle: *const c_void) -> u64 {
    unsafe { handle.cast::<Ingress>().as_ref() }
        .map_or(0, |i| i.health.drops.load(Ordering::Relaxed))
}

#[repr(C)]
pub struct RingDescriptors {
    file: i32,
    wake: i32,
    space: i32,
}

/// # Safety
/// Exclude ingress join. `out` must be writable; returned descriptors belong
/// to the caller. A missing acknowledgement leaves ownership uncertain.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_add_ring(
    handle: *mut c_void,
    slot: u8,
    descriptor: i32,
    out: *mut RingDescriptors,
) -> bool {
    use std::os::fd::IntoRawFd;
    let Some(ingress) = (unsafe { handle.cast::<Ingress>().as_ref() }) else {
        return false;
    };
    let Some(out) = (unsafe { out.as_mut() }) else {
        return false;
    };
    let fd = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 3) };
    if fd < 0 {
        return false;
    }
    let socket = UnixDatagram::from(unsafe { OwnedFd::from_raw_fd(fd) });
    if socket.peer_addr().is_err() || socket.set_nonblocking(true).is_err() {
        return false;
    }
    let (tx, rx) = mpsc::sync_channel(1);
    if ingress
        .commands
        .send(Command::AddRing(slot, socket, tx))
        .is_err()
    {
        return false;
    }
    let Ok(Some([file, wake, space])) = rx.recv_timeout(Duration::from_secs(3)) else {
        return false;
    };
    *out = RingDescriptors {
        file: file.into_raw_fd(),
        wake: wake.into_raw_fd(),
        space: space.into_raw_fd(),
    };
    true
}

/// # Safety
/// `handle` must identify a live ingress and exclude join. `stats` must be
/// writable. This approximate diagnostic snapshot is not a drain certificate.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_stats(
    handle: *const c_void,
    stats: *mut IngressStats,
) -> bool {
    let Some(ingress) = (unsafe { handle.cast::<Ingress>().as_ref() }) else {
        return false;
    };
    let Some(stats) = (unsafe { stats.as_mut() }) else {
        return false;
    };
    let h = &ingress.health;
    *stats = IngressStats {
        received: h.received.load(Ordering::Relaxed),
        forwarded: h.forwarded.load(Ordering::Relaxed),
        backpressure: h.backpressure.load(Ordering::Relaxed),
        queue_drops: h.queue_drops.load(Ordering::Relaxed),
        send_drops: h.send_drops.load(Ordering::Relaxed),
        egress_drops: h.egress_drops.load(Ordering::Relaxed),
        queued_bytes: h.queued_bytes.load(Ordering::Relaxed),
        peak_queued_bytes: h.peak_queued_bytes.load(Ordering::Relaxed),
        running: h.running.load(Ordering::Acquire).into(),
    };
    true
}

/// # Safety
/// `handle` must be a unique live ingress handle; no calls may race join.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_join(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }
    let mut ingress = unsafe { Box::from_raw(handle.cast::<Ingress>()) };
    let _ = ingress.commands.send(Command::Stop);
    if let Some(worker) = ingress.worker.take() {
        let _ = worker.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer() -> (Peer, tokio::net::UnixDatagram) {
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        sender.set_nonblocking(true).unwrap();
        receiver.set_nonblocking(true).unwrap();
        let bytes: libc::c_int = 32768;
        assert_eq!(
            unsafe {
                libc::setsockopt(
                    sender.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    (&bytes as *const libc::c_int).cast(),
                    std::mem::size_of_val(&bytes) as _,
                )
            },
            0
        );
        (
            Peer {
                socket: Arc::new(tokio::net::UnixDatagram::from_std(sender).unwrap()),
                ring: None,
                queue: VecDeque::new(),
                bytes: 0,
            },
            tokio::net::UnixDatagram::from_std(receiver).unwrap(),
        )
    }

    async fn saturate(peer: &Peer) -> usize {
        poll_fn(|cx| peer.socket.poll_send_ready(cx)).await.unwrap();
        let wire = [0xa5u8; 1200];
        let mut count = 0;
        loop {
            match peer.socket.try_send(&wire) {
                Ok(_) => count += 1,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("saturating private test socket: {error}"),
            }
        }
        assert!(count > 0);
        count
    }

    #[tokio::test]
    async fn backpressure_retries_in_order_and_preserves_segment_boundaries() {
        let (mut peer, receiver) = peer();
        let saturated = saturate(&peer).await;
        let health = Health::default();
        let address: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let payloads: Vec<_> = (0..19)
            .map(|sequence| vec![sequence; if sequence == 18 { 77 } else { 1200 }])
            .collect();
        let packets: Vec<_> = payloads.iter().map(|p| (address, p.as_slice())).collect();
        peer.forward(&packets, &health);
        assert_eq!(
            health.backpressure.load(Ordering::Relaxed),
            packets.len() as u64
        );
        assert_eq!(health.drops.load(Ordering::Relaxed), 0);
        assert!(peer.bytes > 0 && peer.bytes <= QUEUE_BYTES);
        let reader = tokio::spawn(async move {
            let mut wire = vec![0u8; 65560];
            for _ in 0..saturated {
                assert_eq!(receiver.recv(&mut wire).await.unwrap(), 1200);
                assert!(wire[..1200].iter().all(|b| *b == 0xa5));
            }
            let mut next = 0;
            while next < 19 {
                let count = receiver.recv(&mut wire).await.unwrap();
                let mut header: [u8; HEADER] = wire[..HEADER].try_into().unwrap();
                let stride = if header[0] == 2 {
                    u16::from_be_bytes(header[20..22].try_into().unwrap()) as usize
                } else {
                    count - HEADER
                };
                header[0] = 1;
                header[20..24].copy_from_slice(&((count - HEADER) as u32).to_be_bytes());
                assert_eq!(
                    packet_socket::decode_header(&header, count - HEADER).unwrap(),
                    address
                );
                for payload in wire[HEADER..count].chunks(stride) {
                    assert_eq!(payload.len(), if next == 18 { 77 } else { 1200 });
                    assert!(payload.iter().all(|b| *b == next));
                    next += 1;
                }
            }
        });
        let mut peers = std::array::from_fn(|_| None);
        peers[0] = Some(peer);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !peers[0].as_ref().unwrap().queue.is_empty() {
                poll_fn(|cx| poll_pending(&mut peers, &[0], &health, cx)).await;
            }
            reader.await.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(health.forwarded.load(Ordering::Relaxed), 19);
        assert_eq!(health.queued_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(health.drops.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn blocked_generation_is_bounded_and_does_not_hold_up_another() {
        let (mut blocked, _receiver) = peer();
        saturate(&blocked).await;
        let (mut healthy, receiver) = peer();
        poll_fn(|cx| healthy.socket.poll_send_ready(cx))
            .await
            .unwrap();
        let health = Health::default();
        let address: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let payload = [0x4bu8; 1200];
        // Different addresses prohibit grouping, exercising the frame limit.
        for port in 1..=QUEUE_FRAMES * 2 {
            let mut peer = address;
            peer.set_port(port as u16);
            blocked.forward(&[(peer, &payload)], &health);
        }
        assert!(blocked.bytes <= QUEUE_BYTES);
        assert_eq!(blocked.queue.len(), QUEUE_FRAMES);
        assert_eq!(
            health.queue_drops.load(Ordering::Relaxed),
            QUEUE_FRAMES as u64
        );
        healthy.forward(&[(address, &payload)], &health);
        let mut peers = std::array::from_fn(|_| None);
        peers[0] = Some(blocked);
        peers[1] = Some(healthy);
        tokio::time::timeout(Duration::from_secs(1), async {
            if !peers[1].as_ref().unwrap().queue.is_empty() {
                poll_fn(|cx| poll_pending(&mut peers, &[0, 1], &health, cx)).await;
            }
            let mut frame = [0u8; 2048];
            let count = receiver.recv(&mut frame).await.unwrap();
            assert_eq!(&frame[HEADER..count], payload);
        })
        .await
        .unwrap();
        assert!(!peers[0].as_ref().unwrap().queue.is_empty());
        assert_eq!(health.forwarded.load(Ordering::Relaxed), 1);
    }
}
