//! Experimental one-way public RX lease for a stable native generation.
//! The caller must suspend the ingress before creating this socket. Releasing
//! the lease excludes an in-progress read and exports client-chosen Initial
//! routes before the ingress is resumed. Reacquisition requires safe retirement
//! and is intentionally not implemented here.

use crate::{generation_cid, packet_socket::PacketSocket};
use quinn::{AsyncUdpSocket, UdpPoller, udp};
use serde::Serialize;
use std::{
    collections::HashMap,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

const PIN_LIMIT: usize = 4096;
const PIN_LIFETIME: Duration = Duration::from_secs(30);
const SNAPSHOT_LIMIT: usize = 512 * 1024;

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct InitialKey {
    version: u32,
    length: u8,
    cid: [u8; 20],
}

#[derive(Serialize)]
struct InitialPin {
    version: u32,
    cid: Vec<u8>,
    remaining_ms: u64,
}

#[derive(Serialize)]
struct Snapshot {
    key: [u8; 16],
    local: SocketAddr,
    slot: u8,
    pins: Vec<InitialPin>,
}

struct State {
    direct: bool,
    pins: HashMap<InitialKey, Instant>,
    next_gc: Instant,
    snapshot: Option<Vec<u8>>,
    waker: Option<Waker>,
}

pub struct ReceiveOwner {
    packet: Arc<PacketSocket>,
    public: Arc<dyn AsyncUdpSocket>,
    codec: generation_cid::Generator,
    slot: u8,
    key: [u8; 16],
    state: Mutex<State>,
}

impl std::fmt::Debug for ReceiveOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiveOwner")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

impl ReceiveOwner {
    pub fn new(
        packet: Arc<PacketSocket>,
        public: Arc<dyn AsyncUdpSocket>,
        generation: &generation_cid::Config,
    ) -> Self {
        Self {
            packet,
            public,
            codec: generation_cid::Generator::new(generation),
            slot: generation.slot,
            key: generation.key,
            state: Mutex::new(State {
                direct: true,
                pins: HashMap::new(),
                next_gc: Instant::now(),
                snapshot: None,
                waker: None,
            }),
        }
    }

    // False only for an Initial/0-RTT that cannot be pinned. Never let the
    // engine start an untracked handshake when its export table is full.
    fn observe(&self, state: &mut State, packet: &[u8], now: Instant) -> bool {
        let Some(&first) = packet.first() else {
            return true;
        };
        if first & 0x80 == 0 {
            return true;
        }
        let Some(prefix) = packet.get(..6) else {
            return true;
        };
        let version = u32::from_be_bytes(prefix[1..5].try_into().unwrap());
        let length = prefix[5] as usize;
        if !(8..=20).contains(&length) {
            return true;
        }
        let Some(dcid) = packet.get(6..6 + length) else {
            return true;
        };
        if self.codec.decode(dcid).is_some() {
            return true;
        }
        let Some(&scid) = packet.get(6 + length) else {
            return true;
        };
        if scid > 20 || packet.len() < 7 + length + scid as usize {
            return true;
        }
        let kind = (first >> 4) & 3;
        let initial = (version == 1 && kind == 0) || (version == 0x6b3343cf && kind == 1);
        let early = (version == 1 && kind == 1) || (version == 0x6b3343cf && kind == 2);
        if !initial && !early {
            return true;
        }
        let mut cid = [0; 20];
        cid[..length].copy_from_slice(dcid);
        let key = InitialKey {
            version,
            length: length as u8,
            cid,
        };
        if state.pins.get(&key).is_some_and(|expiry| now < *expiry) {
            return true;
        }
        if !initial || packet.len() < 1200 {
            return false;
        }
        if now >= state.next_gc {
            state.pins.retain(|_, expiry| now < *expiry);
            state.next_gc = now + Duration::from_secs(1);
        }
        if !state.pins.contains_key(&key) && state.pins.len() >= PIN_LIMIT {
            return false;
        }
        state.pins.insert(key, now + PIN_LIFETIME);
        true
    }

    // The mutex excludes poll_recv. Already-returned buffers still belong to
    // this Quinn endpoint and their Initial pins are included in the snapshot.
    pub fn release(&self) -> io::Result<Vec<u8>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("receive lease poisoned"))?;
        if let Some(snapshot) = &state.snapshot {
            return Ok(snapshot.clone());
        }
        let now = Instant::now();
        let pins = state
            .pins
            .iter()
            .filter_map(|(key, expiry)| {
                let remaining = expiry.checked_duration_since(now)?;
                Some(InitialPin {
                    version: key.version,
                    cid: key.cid[..key.length as usize].to_vec(),
                    remaining_ms: remaining.as_millis().min(PIN_LIFETIME.as_millis()) as u64 + 1,
                })
            })
            .collect();
        let snapshot = serde_json::to_vec(&Snapshot {
            key: self.key,
            local: self.public.local_addr()?,
            slot: self.slot,
            pins,
        })?;
        if snapshot.len() > SNAPSHOT_LIMIT {
            return Err(io::Error::other("receive lease snapshot too large"));
        }
        state.direct = false;
        state.snapshot = Some(snapshot.clone());
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
        Ok(snapshot)
    }
}

impl AsyncUdpSocket for ReceiveOwner {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.public.clone().create_io_poller()
    }
    fn try_send(&self, transmit: &udp::Transmit) -> io::Result<()> {
        self.public.try_send(transmit)
    }
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        metadata: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("receive lease poisoned"))?;
        if !state.direct {
            return self.packet.poll_recv(cx, buffers, metadata);
        }
        state.waker = Some(cx.waker().clone());
        for _ in 0..32 {
            let count = match self.public.poll_recv(cx, buffers, metadata) {
                Poll::Ready(Ok(count)) => count,
                result => return result,
            };
            let now = Instant::now();
            let mut kept = 0;
            for index in 0..count {
                let meta = metadata[index];
                if meta.stride == 0 {
                    continue;
                }
                let mut length = 0;
                for start in (0..meta.len).step_by(meta.stride) {
                    let end = (start + meta.stride).min(meta.len);
                    if self.observe(&mut state, &buffers[index][start..end], now) {
                        if length != start {
                            buffers[index].copy_within(start..end, length);
                        }
                        length += end - start;
                    }
                }
                if length == 0 {
                    continue;
                }
                if kept != index {
                    let (previous, current) = buffers.split_at_mut(index);
                    previous[kept][..length].copy_from_slice(&current[0][..length]);
                }
                metadata[kept] = udp::RecvMeta {
                    len: length,
                    ..meta
                };
                kept += 1;
            }
            if kept != 0 {
                return Poll::Ready(Ok(kept));
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.public.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        self.public.max_transmit_segments()
    }
    fn max_receive_segments(&self) -> usize {
        self.public
            .max_receive_segments()
            .max(self.packet.max_receive_segments())
    }
    fn may_fragment(&self) -> bool {
        self.public.may_fragment()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet_socket;
    use quinn::Runtime;
    use std::future::poll_fn;
    use std::{net::UdpSocket, os::unix::net::UnixDatagram};

    #[tokio::test]
    async fn release_fences_reads_and_exports_initials_idempotently() {
        let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
        let local = raw.local_addr().unwrap();
        raw.set_nonblocking(true).unwrap();
        let public = quinn::TokioRuntime.wrap_udp_socket(raw).unwrap();
        let (worker, bridge) = UnixDatagram::pair().unwrap();
        worker.set_nonblocking(true).unwrap();
        let packet = Arc::new(PacketSocket::new(worker, local, true).unwrap());
        let owner = ReceiveOwner::new(
            packet,
            public,
            &generation_cid::Config {
                key: [1; 16],
                slot: 7,
            },
        );
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut initial = [0u8; 1200];
        initial[0] = 0xc0;
        initial[4] = 1;
        initial[5] = 8;
        initial[6..14].copy_from_slice(b"original");
        client.send_to(&initial, local).unwrap();
        let mut storage = [0u8; 2048];
        let mut buffers = [IoSliceMut::new(&mut storage)];
        let mut metadata = [udp::RecvMeta::default()];
        assert_eq!(
            poll_fn(|cx| owner.poll_recv(cx, &mut buffers, &mut metadata))
                .await
                .unwrap(),
            1
        );
        let snapshot = owner.release().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
        assert_eq!(parsed["slot"], 7);
        assert_eq!(parsed["pins"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot, owner.release().unwrap());
        // Public packets remain unread, while private relay packets arrive.
        client.send_to(b"public-unread", local).unwrap();
        let mut frame = packet_socket::encode_header(client.local_addr().unwrap(), 5)
            .unwrap()
            .to_vec();
        frame.extend_from_slice(b"relay");
        bridge.send(&frame).unwrap();
        assert_eq!(
            poll_fn(|cx| owner.poll_recv(cx, &mut buffers, &mut metadata))
                .await
                .unwrap(),
            1
        );
        assert_eq!(&buffers[0][..metadata[0].len], b"relay");
    }

    #[tokio::test]
    async fn direct_reader_never_starts_an_unpinnable_initial() {
        let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
        let local = raw.local_addr().unwrap();
        raw.set_nonblocking(true).unwrap();
        let public = quinn::TokioRuntime.wrap_udp_socket(raw).unwrap();
        let (worker, _bridge) = UnixDatagram::pair().unwrap();
        worker.set_nonblocking(true).unwrap();
        let packet = Arc::new(PacketSocket::new(worker, local, true).unwrap());
        let owner = ReceiveOwner::new(packet, public, &generation_cid::Config { key: [1;16], slot: 7 });
        let mut state = owner.state.lock().unwrap();
        let now = Instant::now();
        state.next_gc = now + Duration::from_secs(1);
        for nonce in 0..PIN_LIMIT {
            let mut cid = [0;20]; cid[..8].copy_from_slice(&(nonce as u64).to_be_bytes());
            state.pins.insert(InitialKey { version: 1, length: 8, cid }, now + PIN_LIFETIME);
        }
        let mut initial = [0;1200];
        initial[0] = 0xc0; initial[4] = 1; initial[5] = 8;
        initial[6..14].copy_from_slice(b"new-work");
        assert!(!owner.observe(&mut state, &initial, now));
        // An existing Initial keeps its original expiry, even under pressure.
        initial[6..14].copy_from_slice(&1u64.to_be_bytes());
        assert!(owner.observe(&mut state, &initial, now));
        assert_eq!(state.pins.len(), PIN_LIMIT);
        drop(state);
        assert!(owner.release().unwrap().len() <= SNAPSHOT_LIMIT);
    }
}
