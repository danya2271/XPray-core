//! Opt-in managed ingress. Masks and batched public UDP I/O stay in Rust.
//! Admission changes are acknowledged by the sole receive owner. Held batches
//! retain their receive-time admission; ownership fences wait for their delivery.
//! This module does not certify QUIC retirement or implement crash recovery.

use crate::{generation_cid, mask_socket, packet_ring, packet_socket};
use quinn::{AsyncUdpSocket, Runtime, udp};
use serde::Deserialize;
use std::{
    collections::{HashMap, VecDeque},
    ffi::c_void,
    future::poll_fn,
    io::{self, IoSliceMut},
    net::{IpAddr, SocketAddr, UdpSocket},
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
    #[serde(default)]
    dormant: bool,
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
    AddRing(
        u8,
        UnixDatagram,
        bool,
        mpsc::SyncSender<Option<[OwnedFd; 3]>>,
    ),
    Activate(u8, mpsc::SyncSender<bool>),
    AdoptReceive(Vec<u8>, mpsc::SyncSender<bool>),
    Fence(u8, [u8; 16], mpsc::SyncSender<bool>),
    Retire(u8, [u8; 16], mpsc::SyncSender<bool>),
    ReleasePublic(u8, [u8; 16], mpsc::SyncSender<bool>),
    Abandon(u8, mpsc::SyncSender<bool>),
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
    leases: [u64; 256],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceivePin {
    version: u32,
    cid: Vec<u8>,
    remaining_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiveSnapshot {
    lease: u64,
    key: [u8; 16],
    local: SocketAddr,
    slot: u8,
    pins: Vec<ReceivePin>,
}

fn adopt_receive(
    routes: &mut Routes,
    live: &[bool; 256],
    key: [u8; 16],
    local: SocketAddr,
    data: &[u8],
) -> io::Result<()> {
    let snapshot: ReceiveSnapshot = serde_json::from_slice(data)?;
    if snapshot.key != key
        || snapshot.local != local
        || snapshot.slot != routes.current
        || !live[snapshot.slot as usize]
        || snapshot.pins.len() > PIN_LIMIT
        || snapshot.lease == 0
        || snapshot.lease <= routes.leases[snapshot.slot as usize]
    {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let now = Instant::now();
    let mut pins = HashMap::new();
    for pin in snapshot.pins {
        if !(8..=20).contains(&pin.cid.len())
            || ![1, 0x6b3343cf].contains(&pin.version)
            || pin.remaining_ms == 0
            || pin.remaining_ms > 30001
            || routes.codec.decode(&pin.cid).is_some()
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut cid = [0; 20];
        cid[..pin.cid.len()].copy_from_slice(&pin.cid);
        let key = (pin.version, pin.cid.len() as u8, cid);
        if pins
            .insert(
                key,
                (snapshot.slot, now + Duration::from_millis(pin.remaining_ms)),
            )
            .is_some()
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
    }
    // A generation reacquiring direct RX did not receive the broker's Initial
    // table. Keep its still-live broker-created pins across the next release:
    // replacing the table would route an in-flight handshake to generation C.
    // Validate the whole merge first so rejection never mutates ownership.
    for (&cid, &(slot, until)) in &routes.pins {
        if now >= until || !live[slot as usize] {
            continue;
        }
        match pins.get_mut(&cid) {
            Some((owner, expiry)) if *owner == slot => *expiry = (*expiry).min(until),
            Some(_) => return Err(io::ErrorKind::InvalidInput.into()),
            None => {
                if pins.len() >= PIN_LIMIT {
                    return Err(io::ErrorKind::OutOfMemory.into());
                }
                pins.insert(cid, (slot, until));
            }
        }
    }
    routes.pins = pins;
    routes.leases[snapshot.slot as usize] = snapshot.lease;
    Ok(())
}

impl Routes {
    #[cfg(test)]
    fn route(&mut self, packet: &[u8], live: &[bool; 256]) -> Option<u8> {
        self.route_with_admission(packet, live, self.current)
    }

    fn route_with_admission(
        &mut self,
        packet: &[u8],
        live: &[bool; 256],
        admission: u8,
    ) -> Option<u8> {
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
        if !live[admission as usize] {
            return None;
        }
        self.pins
            .insert(key, (admission, now + Duration::from_secs(30)));
        Some(admission)
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

// Each public batch is already routed to one generation and local destination.
// Pack interleaved peers into contiguous groups without changing per-peer
// ordering, queue bounds, wire ABI, or waiting for more datagrams.
fn group_ring_packets<'a>(
    packets: &[(SocketAddr, &'a [u8])],
    grouped: &mut [(SocketAddr, &'a [u8]); BATCH],
) -> [u8; BATCH] {
    debug_assert!(packets.len() <= BATCH);
    let mut consumed = 0u32;
    let mut written = 0;
    let mut order = [0u8; BATCH];
    for (index, &(address, _)) in packets.iter().enumerate() {
        if consumed & (1 << index) != 0 {
            continue;
        }
        for (peer_index, &packet) in packets.iter().enumerate().skip(index) {
            if consumed & (1 << peer_index) == 0 && packet.0 == address {
                grouped[written] = packet;
                order[written] = peer_index as u8;
                written += 1;
                consumed |= 1 << peer_index;
            }
        }
    }
    debug_assert_eq!(written, packets.len());
    order
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
    destination: Option<IpAddr>,
}

struct Peer {
    socket: Arc<tokio::net::UnixDatagram>,
    ring: Option<packet_ring::Sender>,
    queue: VecDeque<QueuedFrame>,
    bytes: usize,
    fence: Option<[u8; 16]>,
    direct_transmit: bool,
}

impl Peer {
    fn try_forward(
        &mut self,
        packets: &[(SocketAddr, &[u8])],
        destination: Option<IpAddr>,
    ) -> io::Result<usize> {
        let Some(ring) = self.ring.as_mut() else {
            return forward(&self.socket, packets);
        };
        let mut batch = ring.batch();
        let mut sent = 0;
        while sent < packets.len() {
            let (count, header) = frame_shape(&packets[sent..], 32)?;
            match batch.push_with_destination(&header, &packets[sent..sent + count], destination) {
                Ok(()) => sent += count,
                Err(_) if sent != 0 => break,
                Err(error) => return Err(error),
            }
        }
        Ok(sent)
    }

    #[cfg(test)]
    fn forward(&mut self, packets: &[(SocketAddr, &[u8])], health: &Health) -> u32 {
        self.forward_with_destination(packets, health, None)
    }

    fn forward_with_destination(
        &mut self,
        packets: &[(SocketAddr, &[u8])],
        health: &Health,
        destination: Option<IpAddr>,
    ) -> u32 {
        if packets.is_empty() {
            return 0;
        }
        assert!(packets.len() <= BATCH);
        let mut grouped = [(packets[0].0, &[][..]); BATCH];
        let mut order = std::array::from_fn(|index| index as u8);
        let packets = if self.ring.is_some() {
            order = group_ring_packets(packets, &mut grouped);
            &grouped[..packets.len()]
        } else {
            packets
        };
        let accepted = self.forward_ordered(packets, health, destination);
        order[..accepted]
            .iter()
            .fold(0, |mask, &index| mask | (1u32 << index))
    }

    fn forward_ordered(
        &mut self,
        packets: &[(SocketAddr, &[u8])],
        health: &Health,
        destination: Option<IpAddr>,
    ) -> usize {
        let sent = if self.queue.is_empty() {
            match self.try_forward(packets, destination) {
                Ok(sent) => sent,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => 0,
                Err(_) => {
                    health.drop_send(packets.len());
                    return packets.len();
                }
            }
        } else {
            0
        };
        health.forwarded.fetch_add(sent as u64, Ordering::Relaxed);
        let pending = &packets[sent..];
        let mut index = 0;
        while index < pending.len() {
            let Ok((count, header)) =
                frame_shape(&pending[index..], if self.ring.is_some() { 32 } else { 8 })
            else {
                health.drop_send(pending.len() - index);
                return packets.len();
            };
            let length = HEADER
                + pending[index..index + count]
                    .iter()
                    .map(|p| p.1.len())
                    .sum::<usize>();
            if self.queue.len() == QUEUE_FRAMES || self.bytes + length > QUEUE_BYTES {
                // The receive owner retains the unaccepted tail in its
                // existing RX buffers. Never consume and drop on backpressure.
                break;
            } else {
                let mut wire = Vec::with_capacity(length);
                wire.extend_from_slice(&header);
                for (_, payload) in &pending[index..index + count] {
                    wire.extend_from_slice(payload);
                }
                self.queue.push_back(QueuedFrame {
                    wire,
                    packets: count,
                    destination,
                });
                self.bytes += length;
                let total = health
                    .queued_bytes
                    .fetch_add(length as u64, Ordering::Relaxed)
                    + length as u64;
                health.peak_queued_bytes.fetch_max(total, Ordering::Relaxed);
                health.backpressure.fetch_add(count as u64, Ordering::Relaxed);
            }
            index += count;
        }
        sent + index
    }

    fn flush(&mut self, health: &Health) -> io::Result<()> {
        if let Some(ring) = self.ring.as_mut() {
            let mut sent = 0;
            let mut batch = ring.batch();
            for frame in self.queue.iter().take(BATCH) {
                match batch.push_wire_with_destination(&frame.wire, frame.destination) {
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

// One readiness turn per generation. Admission commands and healthy workers
// within a held batch remain independent; a full batch pauses public reception.
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

#[derive(Clone, Copy)]
struct HeldPacket {
    buffer: usize,
    start: usize,
    end: usize,
    slot: u8,
    address: SocketAddr,
    destination: Option<IpAddr>,
}

struct ReceivedBatch {
    count: usize,
    index: usize,
    offset: usize,
    admission: u8,
    held: [Option<HeldPacket>; BATCH],
}

impl ReceivedBatch {
    fn new() -> Self {
        Self {
            count: 0,
            index: 0,
            offset: 0,
            admission: 0,
            held: [None; BATCH],
        }
    }

    fn pending(&self) -> bool {
        self.index < self.count || self.held.iter().any(Option::is_some)
    }

    fn received(&mut self, count: usize, admission: u8) {
        assert!(!self.pending());
        self.count = count;
        self.index = 0;
        self.offset = 0;
        self.admission = admission;
    }

    fn prepare(
        &mut self,
        buffers: &[IoSliceMut<'_>],
        metadata: &[udp::RecvMeta],
        routes: &mut Routes,
        live: &[bool; 256],
        local: SocketAddr,
        health: &Health,
    ) {
        if self.held.iter().any(Option::is_some) {
            return;
        }
        let mut written = 0;
        while self.index < self.count && written < BATCH {
            let meta = metadata[self.index];
            if meta.stride == 0 || self.offset >= meta.len {
                self.index += 1;
                self.offset = 0;
                continue;
            }
            let start = self.offset;
            let end = start.saturating_add(meta.stride).min(meta.len);
            self.offset = end;
            let payload = &buffers[self.index][start..end];
            let Some(slot) = routes.route_with_admission(payload, live, self.admission) else {
                continue;
            };
            let destination = local.ip().is_unspecified().then_some(meta.dst_ip).flatten();
            if local.ip().is_unspecified() && destination.is_none() {
                health.drop_send(1);
                continue;
            }
            let address = if local.is_ipv4() {
                match meta.addr {
                    SocketAddr::V6(address) if address.ip().to_ipv4_mapped().is_some() => {
                        SocketAddr::new(address.ip().to_ipv4_mapped().unwrap().into(), address.port())
                    }
                    other => other,
                }
            } else {
                meta.addr
            };
            self.held[written] = Some(HeldPacket {
                buffer: self.index,
                start,
                end,
                slot,
                address,
                destination,
            });
            written += 1;
        }
    }

    fn poll_forward(
        &mut self,
        buffers: &[IoSliceMut<'_>],
        peers: &mut [Option<Peer>; 256],
        health: &Health,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        let mut grouped = 0u32;
        let mut progressed = false;
        for index in 0..BATCH {
            let Some(first) = self.held[index] else {
                continue;
            };
            if grouped & (1 << index) != 0 {
                continue;
            }
            let mut packets = [(first.address, &[][..]); BATCH];
            let mut positions = [0usize; BATCH];
            let mut count = 0;
            for (position, held) in self.held.iter().enumerate().skip(index) {
                let Some(packet) = held else {
                    continue;
                };
                if packet.slot == first.slot && packet.destination == first.destination {
                    grouped |= 1 << position;
                    positions[count] = position;
                    packets[count] = (
                        packet.address,
                        &buffers[packet.buffer][packet.start..packet.end],
                    );
                    count += 1;
                }
            }
            let Some(peer) = peers[first.slot as usize].as_mut() else {
                // An admitted receiver disappearing is a real send failure,
                // never successful delivery or a reason to change ownership.
                health.drop_send(count);
                for &position in &positions[..count] {
                    self.held[position] = None;
                }
                progressed = true;
                continue;
            };
            let accepted =
                peer.forward_with_destination(&packets[..count], health, first.destination);
            for (packet, &position) in positions[..count].iter().enumerate() {
                if accepted & (1 << packet) != 0 {
                    self.held[position] = None;
                    progressed = true;
                }
            }
            if accepted != u32::MAX >> (BATCH - count) {
                // Register space readiness even if the bounded queue is full.
                // poll_pending flushes that queue before this branch is polled.
                if peer.poll_writable(cx).is_ready() {
                    cx.waker().wake_by_ref();
                }
            }
        }
        if progressed {
            Poll::Ready(())
        } else {
            Poll::Pending
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
    // Dormant must mean *unregistered*, not just "do not call recv". Registering
    // the shared UDP descriptor in another reactor still wakes on every burst
    // received by the direct worker, adding CPU in stable operation.
    let wrap = |socket| -> io::Result<Arc<dyn AsyncUdpSocket>> {
        if config.masks.is_empty() {
            quinn::TokioRuntime.wrap_udp_socket(socket)
        } else {
            Ok(Arc::new(mask_socket::MaskSocket::new(
                socket,
                &config.masks,
            )?))
        }
    };
    let mut public_descriptor = socket.as_raw_fd();
    let mut raw = Some(socket);
    let mut public = if config.dormant {
        None
    } else {
        Some(wrap(raw.take().unwrap())?)
    };
    let mut routes = Routes {
        codec: generation_cid::Generator::new(&generation_cid::Config {
            key: config.key,
            slot: config.slot,
        }),
        current: config.slot,
        pins: HashMap::new(),
        next_gc: Instant::now(),
        leases: [0; 256],
    };
    let mut peers: [Option<Peer>; 256] = std::array::from_fn(|_| None);
    let mut slots = Vec::new();
    let mut live = [false; 256];
    let mut used = [false; 256];
    let mut retired = [None; 256];
    let mut outputs = JoinSet::new();
    let mut adopted: Option<Vec<u8>> = None;
    let mut released: Option<(u8, [u8; 16])> = None;
    // Quinn's batched I/O may return GRO data. Preserve each segment's CID
    // ownership rather than forwarding an aggregate to one generation.
    let mut storage: Vec<Vec<u8>> = (0..BATCH).map(|_| vec![0; 65536]).collect();
    let mut buffers: Vec<IoSliceMut<'_>> = storage.iter_mut().map(|p| IoSliceMut::new(p)).collect();
    let mut metadata = [udp::RecvMeta::default(); BATCH];
    let mut received_batch = ReceivedBatch::new();
    health.running.store(true, Ordering::Release);
    let _ = ready.send(true);
    loop {
        received_batch.prepare(&buffers, &metadata, &mut routes, &live, local, &health);
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(Command::Add(slot, socket, ack)) => {
                    let index = slot as usize;
                    if used[index] { let _ = ack.send(false); continue; }
                    used[index] = true;
                    match tokio::net::UnixDatagram::from_std(socket) {
                        Ok(socket) => {
                            let socket = Arc::new(socket);
                            peers[index] = Some(Peer { socket: socket.clone(), ring: None, queue: VecDeque::new(), bytes: 0, fence: None, direct_transmit: false });
                            slots.push(slot); live[index] = true;
                            if let Some(public) = &public {
                                outputs.spawn(egress(socket, public.clone(), health.clone()));
                            }
                            let _ = ack.send(true);
                        }
                        Err(_) => { let _ = ack.send(false); }
                    }
                }
                Some(Command::AddRing(slot, socket, direct_transmit, ack)) => {
                    let index = slot as usize;
                    if used[index] { let _ = ack.send(None); continue; }
                    used[index] = true;
                    let result = (|| -> io::Result<_> {
                        let socket = Arc::new(tokio::net::UnixDatagram::from_std(socket)?);
                        let ring = packet_ring::Sender::new()?;
                        let descriptors = ring.export()?;
                        peers[index] = Some(Peer { socket: socket.clone(), ring: Some(ring), queue: VecDeque::new(), bytes: 0, fence: None, direct_transmit });
                        slots.push(slot); live[index] = true;
                        if let Some(public) = &public && !direct_transmit {
                            outputs.spawn(egress(socket, public.clone(), health.clone()));
                        }
                        Ok(descriptors)
                    })();
                    let _ = ack.send(result.ok());
                }
                Some(Command::Activate(slot, ack)) => {
                    let healthy = public.is_some() && peers[slot as usize].as_ref().is_some_and(|peer| peer.fence.is_none());
                    if healthy { routes.current = slot; }
                    let _ = ack.send(healthy);
                }
                Some(Command::AdoptReceive(snapshot, ack)) => {
                    let valid = if public.is_some() {
                        adopted.as_ref() == Some(&snapshot)
                    } else {
                        adopted.as_ref() != Some(&snapshot) && adopt_receive(&mut routes, &live, config.key, local, &snapshot).is_ok()
                    };
                    if valid && public.is_none() {
                        public_descriptor = raw.as_ref().unwrap().as_raw_fd();
                        let socket = match wrap(raw.take().unwrap()) {
                            Ok(socket) => socket,
                            Err(_) => { let _ = ack.send(false); break; }
                        };
                        for &slot in &slots {
                            let peer = peers[slot as usize].as_ref().unwrap();
                            if !peer.direct_transmit {
                                outputs.spawn(egress(peer.socket.clone(), socket.clone(), health.clone()));
                            }
                        }
                        public = Some(socket); adopted = Some(snapshot); released = None;
                    }
                    let _ = ack.send(valid);
                }
                Some(Command::Fence(slot, token, ack)) => {
                    let now = Instant::now();
                    let pinned = routes.pins.values().any(|&(owner, until)| owner == slot && now < until);
                    let Some(peer) = peers[slot as usize].as_mut() else { let _ = ack.send(false); continue; };
                    if received_batch.pending() || public.is_none() || routes.current == slot || pinned || token == [0;16] {
                        let _ = ack.send(false); continue;
                    }
                    if let Some(previous) = peer.fence {
                        let _ = ack.send(previous == token); continue;
                    }
                    let mut wire = b"NXDFENC\x01".to_vec(); wire.extend_from_slice(&token);
                    if peer.queue.len() >= QUEUE_FRAMES || peer.bytes + wire.len() > QUEUE_BYTES {
                        let _ = ack.send(false); continue;
                    }
                    peer.bytes += wire.len();
                    health.queued_bytes.fetch_add(wire.len() as u64, Ordering::Relaxed);
                    peer.queue.push_back(QueuedFrame { wire, packets: 0, destination:None });
                    peer.fence = Some(token);
                    let _ = ack.send(true);
                }
                Some(Command::Retire(slot, token, ack)) => {
                    if let Some(previous) = retired[slot as usize] { let _ = ack.send(previous == token); continue; }
                    let valid = !received_batch.pending() && routes.current != slot && peers[slot as usize].as_ref().is_some_and(|p| p.fence == Some(token) && p.queue.is_empty());
                    if valid {
                        live[slot as usize] = false;
                        retired[slot as usize] = Some(token);
                        // Direct TX owns no outstanding relay replies. Retain
                        // only the tombstone, not a 2 MiB ring per old release.
                        if peers[slot as usize].as_ref().unwrap().direct_transmit {
                            peers[slot as usize] = None;
                            slots.retain(|&present| present != slot);
                        }
                    }
                    let _ = ack.send(valid);
                }
                Some(Command::Abandon(slot,ack)) => {
                    // Caller proved this slot's process is stopped and it was
                    // never activated. No live QUIC work may be abandoned.
                    if routes.current==slot { let _=ack.send(false);continue; }
                    live[slot as usize]=false;
                    if let Some(mut peer)=peers[slot as usize].take(){peer.discard(&health);}
                    slots.retain(|&present|present!=slot);
                    let _=ack.send(true);
                }
                Some(Command::ReleasePublic(slot, token, ack)) => {
                    if let Some(previous) = released {
                        let _ = ack.send(previous == (slot,token)); continue;
                    }
                    let only_current = live.iter().enumerate().all(|(index, alive)| !alive || index == slot as usize);
                    // A receive-ring fence says nothing about worker->public
                    // egress. Only direct-TX peers have no relay replies to lose.
                    let direct_transmit = slots.iter().all(|&slot| peers[slot as usize].as_ref().unwrap().direct_transmit);
                    let Some(peer) = peers[slot as usize].as_mut() else { let _ = ack.send(false); continue; };
                    if received_batch.pending() || public.is_none() || routes.current != slot || !only_current || !direct_transmit || peer.ring.is_none() || !peer.queue.is_empty() || token == [0;16] {
                        let _ = ack.send(false); continue;
                    }
                    let duplicate = unsafe { libc::fcntl(public_descriptor, libc::F_DUPFD_CLOEXEC, 3) };
                    if duplicate < 0 { let _ = ack.send(false); continue; }
                    let mut marker = b"NXDFENC\x01".to_vec(); marker.extend_from_slice(&token);
                    if peer.ring.as_mut().unwrap().batch().push_wire(&marker).is_err() {
                        unsafe { libc::close(duplicate); }
                        let _ = ack.send(false); continue;
                    }
                    outputs.abort_all();
                    while outputs.join_next().await.is_some() {}
                    // Drop the last registered public socket before ACK.
                    public = None;
                    raw = Some(unsafe { UdpSocket::from_raw_fd(duplicate) });
                    released = Some((slot,token));
                    let _ = ack.send(true);
                }
                _ => break,
            },
            _ = outputs.join_next(), if !outputs.is_empty() => break,
            received = poll_fn(|cx| {
                if poll_pending(&mut peers, &slots, &health, cx).is_ready() {
                    return Poll::Ready(Ok(None));
                }
                if received_batch.pending() {
                    return received_batch.poll_forward(&buffers, &mut peers, &health, cx)
                        .map(|()| Ok(None));
                }
                match &public {
                    Some(socket) => socket.poll_recv(cx, &mut buffers, &mut metadata).map(|result| result.map(Some)),
                    None => Poll::Pending,
                }
            }) => {
                if let Some(count) = received? {
                    for meta in &metadata[..count] {
                        if meta.stride == 0 { continue; }
                        health.received.fetch_add(meta.len.div_ceil(meta.stride) as u64, Ordering::Relaxed);
                    }
                    received_batch.received(count, routes.current);
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
    if local.port() == 0 || socket.set_nonblocking(true).is_err() {
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
/// Caller must prove the slot's process has exited before first admission.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_abandon(handle: *mut c_void, slot: u8) -> bool {
    let Some(ingress) = (unsafe { handle.cast::<Ingress>().as_ref() }) else {
        return false;
    };
    let (tx, rx) = mpsc::sync_channel(1);
    ingress.commands.send(Command::Abandon(slot, tx)).is_ok()
        && rx.recv_timeout(Duration::from_secs(3)).ok() == Some(true)
}

/// # Safety
/// Exclude ingress join. `data` must point to `length` readable snapshot bytes.
/// Caller must prove the old native process released public RX first.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_adopt_receive(
    handle: *mut c_void,
    data: *const u8,
    length: usize,
) -> bool {
    if data.is_null() || length > 512 * 1024 {
        return false;
    }
    let Some(ingress) = (unsafe { handle.cast::<Ingress>().as_ref() }) else {
        return false;
    };
    let snapshot = unsafe { std::slice::from_raw_parts(data, length) }.to_vec();
    let (tx, rx) = mpsc::sync_channel(1);
    ingress
        .commands
        .send(Command::AdoptReceive(snapshot, tx))
        .is_ok()
        && rx.recv_timeout(Duration::from_secs(3)).ok() == Some(true)
}

/// # Safety
/// Exclude join. Token points to 16 readable bytes; retry the identical token.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_fence(
    handle: *mut c_void,
    slot: u8,
    token: *const u8,
) -> bool {
    if token.is_null() {
        return false;
    }
    let Some(ingress) = (unsafe { handle.cast::<Ingress>().as_ref() }) else {
        return false;
    };
    let token = unsafe { std::slice::from_raw_parts(token, 16) }
        .try_into()
        .unwrap();
    let (tx, rx) = mpsc::sync_channel(1);
    ingress
        .commands
        .send(Command::Fence(slot, token, tx))
        .is_ok()
        && rx.recv_timeout(Duration::from_secs(3)).ok() == Some(true)
}

unsafe fn ownership_command(
    handle: *mut c_void,
    slot: u8,
    token: *const u8,
    release: bool,
) -> bool {
    if token.is_null() {
        return false;
    }
    let Some(ingress) = (unsafe { handle.cast::<Ingress>().as_ref() }) else {
        return false;
    };
    let token = unsafe { std::slice::from_raw_parts(token, 16) }
        .try_into()
        .unwrap();
    let (tx, rx) = mpsc::sync_channel(1);
    let command = if release {
        Command::ReleasePublic(slot, token, tx)
    } else {
        Command::Retire(slot, token, tx)
    };
    ingress.commands.send(command).is_ok()
        && rx.recv_timeout(Duration::from_secs(3)).ok() == Some(true)
}

/// # Safety
/// Caller must first confirm worker retirement with the same fence token.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_retire(
    handle: *mut c_void,
    slot: u8,
    token: *const u8,
) -> bool {
    unsafe { ownership_command(handle, slot, token, false) }
}

/// # Safety
/// Exclude join. Token must point to 16 readable bytes, reused on retry.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xhys_ingress_release_public(
    handle: *mut c_void,
    slot: u8,
    token: *const u8,
) -> bool {
    unsafe { ownership_command(handle, slot, token, true) }
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
    direct_transmit: bool,
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
        .send(Command::AddRing(slot, socket, direct_transmit, tx))
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

    #[test]
    fn ring_grouping_preserves_interleaved_peer_order() {
        let a = "127.0.0.1:123".parse().unwrap();
        let b = "[::1]:456".parse().unwrap();
        let payloads: Vec<_> = (0..BATCH).map(|index| vec![index as u8; 1200]).collect();
        let packets: Vec<_> = payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| (if index % 2 == 0 { a } else { b }, payload.as_slice()))
            .collect();
        let mut grouped = [(a, &[][..]); BATCH];
        group_ring_packets(&packets, &mut grouped);
        for (index, &(address, payload)) in grouped.iter().enumerate() {
            assert_eq!(address, if index < BATCH / 2 { a } else { b });
            let expected = if index < BATCH / 2 {
                index * 2
            } else {
                (index - BATCH / 2) * 2 + 1
            };
            assert_eq!(payload[0] as usize, expected);
        }
        assert_eq!(frame_shape(&grouped, BATCH).unwrap().0, BATCH / 2);
        assert_eq!(frame_shape(&grouped[BATCH / 2..], BATCH).unwrap().0, BATCH / 2);
        group_ring_packets(&[], &mut grouped);
    }

    #[tokio::test]
    async fn ring_forward_groups_peers_without_reordering_or_extra_slots() {
        let (mut peer, _) = peer();
        let sender = packet_ring::Sender::new().unwrap();
        let descriptors = sender.export().unwrap();
        let mut receiver = packet_ring::Receiver::import(
            descriptors[0].as_raw_fd(),
            descriptors[1].as_raw_fd(),
            descriptors[2].as_raw_fd(),
        )
        .unwrap();
        peer.ring = Some(sender);
        let a: SocketAddr = "127.0.0.1:123".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:456".parse().unwrap();
        let payloads: Vec<_> = (0..BATCH).map(|index| vec![index as u8; 1200]).collect();
        let packets: Vec<_> = payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| (if index % 2 == 0 { a } else { b }, payload.as_slice()))
            .collect();
        let health = Health::default();
        let destination = "127.0.0.2".parse().unwrap();
        peer.forward_with_destination(&[], &health, Some(destination));
        peer.forward_with_destination(&packets, &health, Some(destination));
        assert!(peer.queue.is_empty());
        assert_eq!(health.forwarded.load(Ordering::Relaxed), BATCH as u64);
        let mut storage = [vec![0; 65535], vec![0; 65535]];
        let mut buffers: Vec<_> = storage
            .iter_mut()
            .map(|buffer| IoSliceMut::new(buffer))
            .collect();
        let mut metadata = [udp::RecvMeta::default(); 2];
        let count = poll_fn(|cx| receiver.poll_recv(cx, &mut buffers, &mut metadata, a))
            .await
            .unwrap();
        assert_eq!(count, 2); // Alternation used to consume 32 ring slots.
        for index in 0..2 {
            assert_eq!(metadata[index].addr, if index == 0 { a } else { b });
            assert_eq!(metadata[index].dst_ip, Some(destination));
            assert_eq!(metadata[index].stride, 1200);
            assert_eq!(metadata[index].len, BATCH / 2 * 1200);
            for (sequence, segment) in buffers[index][..metadata[index].len]
                .chunks(1200)
                .enumerate()
            {
                assert!(segment.iter().all(|&byte| byte as usize == sequence * 2 + index));
            }
        }
    }

    #[tokio::test]
    async fn ring_partial_acceptance_preserves_original_packet_positions() {
        let (mut peer, _) = peer();
        let mut sender = packet_ring::Sender::new().unwrap();
        let descriptors = sender.export().unwrap();
        let mut receiver = packet_ring::Receiver::import(
            descriptors[0].as_raw_fd(),
            descriptors[1].as_raw_fd(),
            descriptors[2].as_raw_fd(),
        )
        .unwrap();
        let a: SocketAddr = "127.0.0.1:123".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:456".parse().unwrap();
        let payload = [0x71u8; 1200];
        let header = packet_socket::encode_header(a, payload.len()).unwrap();
        while sender.batch().push(&header, &[(a, &payload)]).is_ok() {}
        peer.ring = Some(sender);
        let health = Health::default();
        let queued = [(a, payload.as_slice()); BATCH];
        while peer.forward(&queued, &health) != 0 {}
        // Leave room for exactly A's grouped frame, but not B's frame.
        let remaining = QUEUE_BYTES - peer.bytes;
        let reserved = HEADER + payload.len() * 2;
        let filler = vec![0x72u8; remaining - reserved - HEADER];
        assert_eq!(peer.forward(&[(a, &filler)], &health), 1);
        assert_eq!(QUEUE_BYTES - peer.bytes, reserved);
        let packets = [
            (a, payload.as_slice()),
            (b, &payload),
            (a, &payload),
            (b, &payload),
        ];
        assert_eq!(peer.forward(&packets, &health), 0b0101);
        assert_eq!(peer.queue.back().unwrap().packets, 2);
        let mut buffer = [0u8; 65535];
        let mut buffers = [IoSliceMut::new(&mut buffer)];
        let mut metadata = [udp::RecvMeta::default()];
        poll_fn(|cx| receiver.poll_recv(cx, &mut buffers, &mut metadata, a))
            .await
            .unwrap();
        peer.flush(&health).unwrap();
        assert_eq!(peer.forward(&[packets[1], packets[3]], &health), 0b11);
        assert_eq!(health.drops.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn receive_adoption_preserves_pending_initial_routes_after_activation() {
        let mut routes = Routes {
            codec: generation_cid::Generator::new(&generation_cid::Config {
                key: [1; 16],
                slot: 0,
            }),
            current: 0,
            pins: HashMap::new(),
            next_gc: Instant::now(),
            leases: [0; 256],
        };
        let mut live = [false; 256];
        live[0] = true;
        live[1] = true;
        let mut initial = vec![0; 1200];
        initial[0] = 0xc0;
        initial[4] = 1;
        initial[5] = 8;
        initial[6..14].copy_from_slice(b"original");
        let snapshot = serde_json::to_vec(&serde_json::json!({"lease":1,"key":vec![1u8;16], "local":"127.0.0.1:443", "slot":0, "pins":[{"version":1,"cid":[111,114,105,103,105,110,97,108],"remaining_ms":1000}]})).unwrap();
        adopt_receive(
            &mut routes,
            &live,
            [1; 16],
            "127.0.0.1:443".parse().unwrap(),
            &snapshot,
        )
        .unwrap();
        assert!(
            adopt_receive(
                &mut routes,
                &live,
                [1; 16],
                "127.0.0.1:443".parse().unwrap(),
                &snapshot
            )
            .is_err()
        );
        routes.current = 1;
        assert_eq!(routes.route(&initial, &live), Some(0));
        initial[6..14].copy_from_slice(b"new-work");
        assert_eq!(routes.route(&initial, &live), Some(1));
    }

    #[test]
    fn repeated_receive_adoption_preserves_broker_initials_without_extending_expiry() {
        let mut routes = Routes {
            codec: generation_cid::Generator::new(&generation_cid::Config {
                key: [1; 16],
                slot: 1,
            }),
            current: 1,
            pins: HashMap::new(),
            next_gc: Instant::now(),
            leases: [0; 256],
        };
        let live = [true; 256];
        let mut initial = [0u8; 1200];
        initial[0] = 0xc0;
        initial[4] = 1;
        initial[5] = 8;
        initial[6..14].copy_from_slice(b"broker-b");
        assert_eq!(routes.route(&initial, &live), Some(1));
        let mut cid = [0; 20];
        cid[..8].copy_from_slice(b"broker-b");
        let pin_key = (1, 8, cid);
        let before = routes.pins[&pin_key];
        let snapshot = serde_json::to_vec(&serde_json::json!({
            "lease":1,"key":vec![1u8;16],"local":"127.0.0.1:443","slot":1,
            "pins":[{"version":1,"cid":b"direct-b".to_vec(),"remaining_ms":1000}]
        }))
        .unwrap();
        adopt_receive(
            &mut routes,
            &live,
            [1; 16],
            "127.0.0.1:443".parse().unwrap(),
            &snapshot,
        )
        .unwrap();
        assert_eq!(routes.pins[&pin_key], before);
        routes.current = 2;
        assert_eq!(routes.route(&initial, &live), Some(1));
        initial[0] = 0xd0;
        assert_eq!(routes.route(&initial, &live), Some(1));
        initial[6..14].copy_from_slice(b"direct-b");
        assert_eq!(routes.route(&initial, &live), Some(1));
    }

    #[test]
    fn receive_adoption_rejects_conflicting_or_oversized_merge_atomically() {
        let now = Instant::now();
        let mut routes = Routes {
            codec: generation_cid::Generator::new(&generation_cid::Config {
                key: [1; 16],
                slot: 1,
            }),
            current: 1,
            pins: HashMap::new(),
            next_gc: now + Duration::from_secs(1),
            leases: [0; 256],
        };
        let mut live = [true; 256];
        for nonce in 0..PIN_LIMIT {
            let mut cid = [0;20];
            cid[..8].copy_from_slice(&(nonce as u64).to_be_bytes());
            routes.pins.insert((1,8,cid),(1,now+Duration::from_secs(30)));
        }
        let snapshot = |cid: &[u8]| {
            serde_json::to_vec(&serde_json::json!({
                "lease":1,"key":vec![1u8;16],"local":"127.0.0.1:443","slot":1,
                "pins":[{"version":1,"cid":cid,"remaining_ms":30000}]
            }))
            .unwrap()
        };
        let original = routes.pins.clone();
        assert!(adopt_receive(&mut routes,&live,[1;16],"127.0.0.1:443".parse().unwrap(),&snapshot(b"new-work")).is_err());
        assert_eq!(routes.pins,original);
        assert_eq!(routes.leases[1],0);
        let key = (1,8,[0;20]);
        routes.pins.get_mut(&key).unwrap().0 = 0;
        let original = routes.pins.clone();
        assert!(
            adopt_receive(
                &mut routes,
                &live,
                [1; 16],
                "127.0.0.1:443".parse().unwrap(),
                &snapshot(&[0; 8])
            )
            .is_err()
        );
        assert_eq!(routes.pins, original);
        assert_eq!(routes.leases[1], 0);
        live[0] = false;
        routes.pins.get_mut(&key).unwrap().1 = now - Duration::from_secs(1);
        adopt_receive(
            &mut routes,
            &live,
            [1; 16],
            "127.0.0.1:443".parse().unwrap(),
            &snapshot(b"new-work"),
        )
        .unwrap();
        assert_eq!(routes.pins.len(), PIN_LIMIT);
        assert!(!routes.pins.contains_key(&key));
    }

    #[test]
    fn receive_adoption_rejects_incomplete_snapshot_without_mutating_routes() {
        let mut routes = Routes {
            codec: generation_cid::Generator::new(&generation_cid::Config {
                key: [1; 16],
                slot: 0,
            }),
            current: 0,
            pins: HashMap::new(),
            next_gc: Instant::now(),
            leases: [0; 256],
        };
        let mut live = [false; 256];
        live[0] = true;
        for snapshot in [
            r#"{"slot":1,"pins":[]}"#,
            r#"{"slot":0,"pins":[{"version":1,"cid":[1],"remaining_ms":1}]}"#,
            r#"{"slot":0,"pins":[{"version":0,"cid":[1,2,3,4,5,6,7,8],"remaining_ms":1}]}"#,
            r#"{"slot":0,"pins":[{"version":1,"cid":[1,2,3,4,5,6,7,8],"remaining_ms":30002}]}"#,
            r#"{"slot":0,"pins":[{"version":1,"cid":[1,2,3,4,5,6,7,8],"remaining_ms":0}]}"#,
            r#"{"slot":0,"pins":[],"unexpected":true}"#,
        ] {
            let mut value: serde_json::Value = serde_json::from_str(snapshot).unwrap();
            value["key"] = serde_json::json!(vec![1u8; 16]);
            value["lease"] = serde_json::json!(1);
            value["local"] = serde_json::json!("127.0.0.1:443");
            assert!(
                adopt_receive(
                    &mut routes,
                    &live,
                    [1; 16],
                    "127.0.0.1:443".parse().unwrap(),
                    &serde_json::to_vec(&value).unwrap()
                )
                .is_err()
            );
            assert!(routes.pins.is_empty());
        }
        let valid = serde_json::to_vec(
            &serde_json::json!({"lease":1,"key":vec![1u8;16],"local":"127.0.0.1:443","slot":0,"pins":[]}),
        )
        .unwrap();
        assert!(
            adopt_receive(
                &mut routes,
                &live,
                [2; 16],
                "127.0.0.1:443".parse().unwrap(),
                &valid
            )
            .is_err()
        );
        assert!(
            adopt_receive(
                &mut routes,
                &live,
                [1; 16],
                "127.0.0.1:444".parse().unwrap(),
                &valid
            )
            .is_err()
        );
    }

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
                fence: None,
                direct_transmit: false,
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
            let accepted = blocked.forward(&[(peer, &payload)], &health);
            assert_eq!(accepted, u32::from(port <= QUEUE_FRAMES));
        }
        assert!(blocked.bytes <= QUEUE_BYTES);
        assert_eq!(blocked.queue.len(), QUEUE_FRAMES);
        assert_eq!(
            health.queue_drops.load(Ordering::Relaxed),
            0
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

    #[tokio::test]
    async fn held_rx_batch_retries_without_loss_and_serves_another_generation() {
        let (mut blocked, receiver) = peer();
        let saturated = saturate(&blocked).await;
        let (healthy, healthy_receiver) = peer();
        let health = Health::default();
        let address: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let payload = [0x71u8; 1200];
        for port in 1..=QUEUE_FRAMES {
            let peer = SocketAddr::new(address.ip(), port as u16);
            assert_eq!(blocked.forward(&[(peer, &payload)], &health), 1);
        }
        let mut peers = std::array::from_fn(|_| None);
        peers[0] = Some(blocked);
        peers[1] = Some(healthy);
        let mut wire = [0x55u8; 2400];
        wire[1200..].fill(0x66);
        let buffers = [IoSliceMut::new(&mut wire)];
        let mut batch = ReceivedBatch::new();
        batch.held[0] = Some(HeldPacket {
            buffer: 0,
            start: 0,
            end: 1200,
            slot: 0,
            address,
            destination: None,
        });
        batch.held[1] = Some(HeldPacket {
            buffer: 0,
            start: 1200,
            end: 2400,
            slot: 1,
            address,
            destination: None,
        });
        poll_fn(|cx| batch.poll_forward(&buffers, &mut peers, &health, cx)).await;
        assert!(batch.pending());
        assert!(batch.held[0].is_some());
        assert!(batch.held[1].is_none());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !peers[1].as_ref().unwrap().queue.is_empty() {
                poll_fn(|cx| poll_pending(&mut peers, &[0, 1], &health, cx)).await;
            }
        })
        .await
        .unwrap();
        let mut received = [0; 2048];
        let n = tokio::time::timeout(Duration::from_secs(1), healthy_receiver.recv(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert!(received[HEADER..n].iter().all(|&byte| byte == 0x66));
        assert_eq!(health.drops.load(Ordering::Relaxed), 0);
        let reader = tokio::spawn(async move {
            let mut frame = [0; 2048];
            for _ in 0..saturated {
                receiver.recv(&mut frame).await.unwrap();
            }
            for sequence in 0..=QUEUE_FRAMES {
                let n = receiver.recv(&mut frame).await.unwrap();
                let expected = if sequence == QUEUE_FRAMES { 0x55 } else { 0x71 };
                assert!(frame[HEADER..n].iter().all(|&byte| byte == expected));
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while batch.pending() || !peers[0].as_ref().unwrap().queue.is_empty() {
                poll_fn(|cx| {
                    if poll_pending(&mut peers, &[0, 1], &health, cx).is_ready() {
                        return Poll::Ready(());
                    }
                    batch.poll_forward(&buffers, &mut peers, &health, cx)
                })
                .await;
            }
            reader.await.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(health.drops.load(Ordering::Relaxed), 0);
        assert_eq!(
            health.forwarded.load(Ordering::Relaxed),
            (QUEUE_FRAMES + 2) as u64
        );
        assert_eq!(health.queued_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn held_initial_uses_receive_time_admission_after_control_switch() {
        let address: SocketAddr = "127.0.0.1:443".parse().unwrap();
        let mut routes = Routes {
            codec: generation_cid::Generator::new(&generation_cid::Config {
                key: [1; 16],
                slot: 0,
            }),
            current: 1,
            pins: HashMap::new(),
            next_gc: Instant::now(),
            leases: [0; 256],
        };
        let live = [true; 256];
        let mut wire = [0; 1200];
        wire[0] = 0xc0;
        wire[4] = 1;
        wire[5] = 8;
        wire[6..14].copy_from_slice(b"held-cid");
        let buffers = [IoSliceMut::new(&mut wire)];
        let metadata = [udp::RecvMeta {
            addr: address,
            len: 1200,
            stride: 1200,
            ecn: None,
            dst_ip: Some(address.ip()),
        }];
        let mut batch = ReceivedBatch::new();
        batch.received(1, 0);
        batch.prepare(
            &buffers,
            &metadata,
            &mut routes,
            &live,
            address,
            &Health::default(),
        );
        assert_eq!(batch.held[0].unwrap().slot, 0);
        assert!(batch.pending());
    }

    #[test]
    fn held_gro_batch_preserves_every_segment_and_receive_time_admission() {
        let address: SocketAddr = "127.0.0.1:443".parse().unwrap();
        let mut routes = Routes {
            codec: generation_cid::Generator::new(&generation_cid::Config {
                key: [1; 16],
                slot: 0,
            }),
            current: 1,
            pins: HashMap::new(),
            next_gc: Instant::now(),
            leases: [0; 256],
        };
        let live = [true; 256];
        // GRO can contain more than BATCH segments and a shorter final segment.
        let mut wire = vec![0u8; 40 * 1500 - 20];
        for (index, segment) in wire.chunks_mut(1500).enumerate() {
            segment[0] = 0xc0;
            segment[4] = 1;
            segment[5] = 8;
            segment[6..14].copy_from_slice(&(index as u64).to_be_bytes());
        }
        let buffers = [IoSliceMut::new(&mut wire)];
        let metadata = [udp::RecvMeta {
            addr: address,
            len: buffers[0].len(),
            stride: 1500,
            ecn: None,
            dst_ip: Some(address.ip()),
        }];
        let health = Health::default();
        let mut batch = ReceivedBatch::new();
        batch.received(1, 0);
        let mut seen = Vec::new();
        while batch.pending() {
            batch.prepare(&buffers, &metadata, &mut routes, &live, address, &health);
            let retained = batch.held;
            // Preparing again must not overwrite any unaccepted packet.
            batch.prepare(&buffers, &metadata, &mut routes, &live, address, &health);
            for (before, after) in retained.iter().zip(&batch.held) {
                assert_eq!(before.map(|p| p.start), after.map(|p| p.start));
            }
            for packet in batch.held.iter_mut() {
                if let Some(packet) = packet.take() {
                    assert_eq!(packet.slot, 0);
                    let payload = &buffers[packet.buffer][packet.start..packet.end];
                    seen.push(u64::from_be_bytes(payload[6..14].try_into().unwrap()));
                    assert_eq!(payload.len(), if seen.len() == 40 { 1480 } else { 1500 });
                }
            }
        }
        assert_eq!(seen, (0..40).collect::<Vec<u64>>());
        assert_eq!(health.drops.load(Ordering::Relaxed), 0);
    }
}
