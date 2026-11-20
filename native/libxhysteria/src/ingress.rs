//! Opt-in managed ingress. Masks and batched public UDP I/O stay in Rust.
//! Control changes are acknowledged by the sole receive owner, between batches.
//! This module does not certify QUIC retirement or implement crash recovery.

use crate::{generation_cid, mask_socket, packet_socket};
use quinn::{AsyncUdpSocket, Runtime, udp};
use serde::Deserialize;
use std::{
    collections::HashMap,
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
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc as control, task::JoinSet};

const HEADER: usize = 24;
const BATCH: usize = 32;
const PIN_LIMIT: usize = 4096;

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
    running: AtomicBool,
}

enum Command {
    Add(u8, UnixDatagram, mpsc::SyncSender<bool>),
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

// A batch borrows the receive buffers only for this synchronous syscall.
// Pointers never survive an await or escape into another thread.
fn forward(socket: &tokio::net::UnixDatagram, packets: &[(SocketAddr, &[u8])]) -> usize {
    let mut headers = [[0u8; HEADER]; BATCH];
    let mut vectors: [[libc::iovec; 9]; BATCH] = unsafe { std::mem::zeroed() };
    let mut messages: [libc::mmsghdr; BATCH] = unsafe { std::mem::zeroed() };
    let mut index = 0;
    let mut frames = 0;
    let mut segments = [0; BATCH];
    while index < packets.len() {
        let (address, payload) = packets[index];
        let stride = payload.len();
        let mut count = 1;
        let mut length = stride;
        while count < 8 && index + count < packets.len() {
            let (peer, next) = packets[index + count];
            if peer != address || next.len() > stride || length + next.len() > 65535 {
                break;
            }
            count += 1;
            length += next.len();
            if next.len() < stride {
                break;
            }
        }
        let Ok(mut header) = packet_socket::encode_header(address, length) else {
            return 0;
        };
        if count > 1 {
            header[0] = 2;
            header[20..22].copy_from_slice(&(stride as u16).to_be_bytes());
            header[22..24].copy_from_slice(&(length as u16).to_be_bytes());
        }
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
    loop {
        let result = unsafe {
            libc::sendmmsg(
                socket.as_raw_fd(),
                messages.as_mut_ptr(),
                frames as u32,
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if result >= 0 {
            return segments[..result as usize].iter().sum();
        }
        if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return 0;
        }
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
            health.drops.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let length = frame.len() - HEADER;
        let stride = if frame[0] == 2 {
            let stride = u16::from_be_bytes(frame[20..22].try_into().unwrap()) as usize;
            let size = u16::from_be_bytes(frame[22..24].try_into().unwrap()) as usize;
            if size != length || stride == 0 || stride > length || length.div_ceil(stride) > 8 {
                health.drops.fetch_add(1, Ordering::Relaxed);
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
            health.drops.fetch_add(1, Ordering::Relaxed);
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
                    health.drops.fetch_add(1, Ordering::Relaxed);
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
    let mut peers: [Option<Arc<tokio::net::UnixDatagram>>; 256] = std::array::from_fn(|_| None);
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
            command = commands.recv() => match command {
                Some(Command::Add(slot, socket, ack)) => {
                    let index = slot as usize;
                    if peers[index].is_some() { let _ = ack.send(false); continue; }
                    match tokio::net::UnixDatagram::from_std(socket) {
                        Ok(socket) => {
                            let socket = Arc::new(socket);
                            peers[index] = Some(socket.clone()); live[index] = true;
                            outputs.spawn(egress(socket, public.clone(), health.clone()));
                            let _ = ack.send(true);
                        }
                        Err(_) => { let _ = ack.send(false); }
                    }
                }
                Some(Command::Activate(slot, ack)) => {
                    let healthy = peers[slot as usize].is_some();
                    if healthy { routes.current = slot; }
                    let _ = ack.send(healthy);
                }
                _ => break,
            },
            received = poll_fn(|cx| public.poll_recv(cx, &mut buffers, &mut metadata)) => {
                let count = received?;
                let mut pending: [(SocketAddr, &[u8]); BATCH] = [(local, &[]); BATCH];
                let mut pending_count = 0;
                let mut pending_slot = 0;
                for index in 0..count {
                    let meta = metadata[index];
                    if meta.stride == 0 { continue; }
                    for payload in buffers[index][..meta.len].chunks(meta.stride) {
                        let Some(slot) = routes.route(payload, &live) else { continue; };
                        if pending_count != 0 && (pending_slot != slot || pending_count == BATCH) {
                            let sent = forward(peers[pending_slot as usize].as_ref().unwrap(), &pending[..pending_count]);
                            health.drops.fetch_add((pending_count - sent) as u64, Ordering::Relaxed);
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
                    let sent = forward(peers[pending_slot as usize].as_ref().unwrap(), &pending[..pending_count]);
                    health.drops.fetch_add((pending_count - sent) as u64, Ordering::Relaxed);
                }
            }
            _ = outputs.join_next(), if !outputs.is_empty() => break,
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
            if let Ok(runtime) = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
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
