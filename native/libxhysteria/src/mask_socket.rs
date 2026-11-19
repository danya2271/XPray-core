use crate::salamander::{SALT_LEN, Salamander};
use quinn::{AsyncUdpSocket, Runtime, UdpPoller, udp};
use serde::Deserialize;
use std::{
    cell::RefCell,
    fmt,
    io::{self, IoSliceMut},
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub kind: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub prefix: String,
}

enum Layer {
    Salamander(Salamander),
    Dns(Vec<u8>),
    Dtls,
    Srtp,
    Utp,
    Wechat,
    Wireguard,
}

impl Layer {
    fn size(&self) -> usize {
        match self {
            Self::Salamander(_) => SALT_LEN,
            Self::Dns(prefix) => prefix.len(),
            Self::Dtls | Self::Wechat => 13,
            Self::Srtp | Self::Utp | Self::Wireguard => 4,
        }
    }

    fn random_size(&self) -> usize {
        match self {
            Self::Salamander(_) => SALT_LEN,
            Self::Dns(_) => 2,
            _ => 0,
        }
    }

    fn encode(&self, packet: &mut [u8], counter: &mut Counter, random: &[u8]) {
        let prefix = &mut packet[..self.size()];
        match self {
            Self::Salamander(mask) => mask.seal_in_place(packet, random.try_into().unwrap()),
            Self::Dns(template) => {
                prefix.copy_from_slice(template);
                prefix[..2].copy_from_slice(random);
            }
            Self::Dtls => {
                prefix.copy_from_slice(&[23, 254, 253, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
                prefix[7..11].copy_from_slice(&counter.sequence.to_be_bytes());
                prefix[11..].copy_from_slice(&counter.length.to_be_bytes());
                counter.sequence = counter.sequence.wrapping_add(1);
                counter.length += 17;
                if counter.length > 100 {
                    counter.length -= 50;
                }
            }
            Self::Srtp => {
                counter.sequence = counter.sequence.wrapping_add(1);
                prefix[..2].fill(0);
                prefix[2..].copy_from_slice(&(counter.sequence as u16).to_be_bytes());
            }
            Self::Utp => prefix.fill(0),
            Self::Wechat => {
                counter.sequence = counter.sequence.wrapping_add(1);
                prefix
                    .copy_from_slice(&[0xa1, 8, 0, 0, 0, 0, 0, 0x10, 0x11, 0x18, 0x30, 0x22, 0x30]);
                prefix[2..6].copy_from_slice(&counter.sequence.to_be_bytes());
            }
            Self::Wireguard => prefix.copy_from_slice(&[4, 0, 0, 0]),
        }
    }
}

#[derive(Default)]
struct Counter {
    sequence: u32,
    length: u16,
}

struct SendState {
    wire: Vec<u8>,
    random: [u8; 4096],
    remaining: usize,
    counters: Vec<Counter>,
}

thread_local! {
    static MASK_SEND_STATE: RefCell<SendState> = RefCell::new(SendState::new(0));
}

impl SendState {
    fn new(layers: usize) -> Self {
        Self {
            wire: Vec::new(),
            random: [0; 4096],
            remaining: 0,
            counters: (0..layers).map(|_| Counter::default()).collect(),
        }
    }

    fn ensure_counters(&mut self, layers: usize) {
        self.counters.resize_with(layers, Counter::default);
    }
}

struct Chain {
    layers: Vec<Layer>,
    overhead: usize,
    random_size: usize,
    stateless: bool,
}

impl Chain {
    fn new(config: &[Config]) -> io::Result<Self> {
        if config.len() > 16 {
            return Err(io::Error::other(
                "at most 16 direct UDP masks are supported",
            ));
        }
        let mut layers = Vec::with_capacity(config.len());
        for entry in config {
            if (entry.kind != "salamander" && !entry.password.is_empty())
                || (entry.kind != "dns" && !entry.prefix.is_empty())
            {
                return Err(io::Error::other("unexpected UDP mask fields"));
            }
            let layer = match entry.kind.as_str() {
                "salamander" => Layer::Salamander(
                    Salamander::new(entry.password.as_bytes()).map_err(io::Error::other)?,
                ),
                "dns" => {
                    if entry.prefix.len() < 34
                        || entry.prefix.len() > 544
                        || entry.prefix.len() % 2 != 0
                    {
                        return Err(io::Error::other("invalid DNS mask prefix length"));
                    }
                    let prefix: Vec<_> = entry
                        .prefix
                        .as_bytes()
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .map(|digits| {
                            let text = std::str::from_utf8(digits).map_err(io::Error::other)?;
                            u8::from_str_radix(text, 16).map_err(io::Error::other)
                        })
                        .collect::<io::Result<_>>()?;
                    if prefix[2..12] != [1, 0, 0, 1, 0, 0, 0, 0, 0, 0]
                        || prefix[prefix.len() - 4..] != [0, 1, 0, 1]
                    {
                        return Err(io::Error::other("invalid DNS mask template"));
                    }
                    Layer::Dns(prefix)
                }
                "dtls" => Layer::Dtls,
                "srtp" => Layer::Srtp,
                "utp" => Layer::Utp,
                "wechat" => Layer::Wechat,
                "wireguard" => Layer::Wireguard,
                _ => return Err(io::Error::other("unsupported direct UDP mask")),
            };
            layers.push(layer);
        }
        let overhead = layers.iter().map(Layer::size).sum();
        if overhead > 252 {
            return Err(io::Error::other(
                "direct UDP masks exceed IPv6 1500-byte MTU budget",
            ));
        }
        let random_size = layers.iter().map(Layer::random_size).sum();
        let stateless = layers.iter().all(|layer| {
            matches!(
                layer,
                Layer::Salamander(_) | Layer::Dns(_) | Layer::Utp | Layer::Wireguard
            )
        });
        Ok(Self {
            layers,
            overhead,
            random_size,
            stateless,
        })
    }

    fn encode(&self, contents: &[u8], stride: usize, state: &mut SendState) -> io::Result<bool> {
        if stride == 0
            || stride > u16::MAX as usize - self.overhead
            || contents.len().div_ceil(stride) > 32
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid UDP mask segmentation",
            ));
        }
        let segments = contents.len().div_ceil(stride);
        let wire_len = contents.len() + segments * self.overhead;
        if wire_len > 65536 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP mask batch exceeds 64 KiB",
            ));
        }
        let mut suffix = 0;
        for layer in self.layers.iter().rev() {
            suffix += layer.size();
            if !matches!(layer, Layer::Salamander(_)) && contents.len().min(stride) + suffix > 4096
            {
                return Ok(false);
            }
        }
        let SendState {
            wire,
            random,
            remaining,
            counters,
        } = state;
        let needed = segments * self.random_size;
        if *remaining < needed {
            getrandom::fill(random).map_err(io::Error::other)?;
            *remaining = random.len();
        }
        let mut random_offset = random.len() - *remaining;
        *remaining -= needed;
        if wire.capacity() < wire_len {
            wire.reserve_exact(wire_len - wire.len());
        }
        wire.resize(wire_len, 0);
        let mut wire_offset = 0;
        for payload in contents.chunks(stride) {
            let end = wire_offset + payload.len() + self.overhead;
            let mut start = wire_offset + self.overhead;
            wire[start..end].copy_from_slice(payload);
            for (index, layer) in self.layers.iter().enumerate().rev() {
                start -= layer.size();
                let next_random = random_offset + layer.random_size();
                layer.encode(
                    &mut wire[start..end],
                    &mut counters[index],
                    &random[random_offset..next_random],
                );
                random_offset = next_random;
            }
            wire_offset = end;
        }
        Ok(true)
    }

    fn decode(&self, buffer: &mut [u8], meta: &mut udp::RecvMeta) {
        let wire_len = meta.len;
        let stride = meta.stride;
        meta.len = 0;
        if stride <= self.overhead {
            return;
        }
        meta.stride = stride - self.overhead;
        for offset in (0..wire_len).step_by(stride) {
            let end = (offset + stride).min(wire_len);
            if end - offset <= self.overhead {
                continue;
            }
            let mut start = offset;
            for layer in &self.layers {
                if let Layer::Salamander(mask) = layer {
                    mask.open_in_place(&mut buffer[start..end]).unwrap();
                }
                start += layer.size();
            }
            buffer.copy_within(start..end, meta.len);
            meta.len += end - start;
        }
    }
}

pub fn overhead(config: &[Config]) -> io::Result<usize> {
    Ok(Chain::new(config)?.overhead)
}

pub struct MaskSocket {
    socket: Arc<dyn AsyncUdpSocket>,
    chain: Chain,
    send: Mutex<SendState>,
}

impl MaskSocket {
    pub fn new(socket: UdpSocket, config: &[Config]) -> io::Result<Self> {
        let chain = Chain::new(config)?;
        if chain.layers.is_empty() {
            return Err(io::Error::other("UDP mask chain is empty"));
        }
        let send = Mutex::new(SendState::new(chain.layers.len()));
        Ok(Self {
            socket: quinn::TokioRuntime.wrap_udp_socket(socket)?,
            chain,
            send,
        })
    }
}

impl fmt::Debug for MaskSocket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaskSocket")
            .field("socket", &self.socket)
            .field("layers", &self.chain.layers.len())
            .finish_non_exhaustive()
    }
}

impl MaskSocket {
    fn try_send_with_state(
        &self,
        transmit: &udp::Transmit,
        state: &mut SendState,
    ) -> io::Result<()> {
        if !self.chain.encode(
            transmit.contents,
            transmit.segment_size.unwrap_or(transmit.contents.len()),
            state,
        )? {
            return Ok(());
        }
        self.socket.try_send(&udp::Transmit {
            contents: &state.wire,
            segment_size: transmit.segment_size.map(|size| size + self.chain.overhead),
            ..*transmit
        })
    }
}

impl AsyncUdpSocket for MaskSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.socket.clone().create_io_poller()
    }

    fn try_send(&self, transmit: &udp::Transmit) -> io::Result<()> {
        if self.chain.stateless {
            match self.send.try_lock() {
                Ok(mut state) => return self.try_send_with_state(transmit, &mut state),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(io::Error::other("UDP mask send lock poisoned"));
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let result = MASK_SEND_STATE.with(|state| {
                        let mut state = state.try_borrow_mut().ok()?;
                        state.ensure_counters(self.chain.layers.len());
                        Some(self.try_send_with_state(transmit, &mut state))
                    });
                    if let Some(result) = result {
                        return result;
                    }
                }
            }
        }
        let mut state = self
            .send
            .lock()
            .map_err(|_| io::Error::other("UDP mask send lock poisoned"))?;
        self.try_send_with_state(transmit, &mut state)
    }

    fn poll_recv(
        &self,
        context: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        metadata: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        for _ in 0..32 {
            let count = match self.socket.poll_recv(context, buffers, metadata) {
                Poll::Ready(Ok(count)) => count,
                result => return result,
            };
            if count == 0 {
                return Poll::Ready(Ok(0));
            }
            let mut kept = 0;
            for index in 0..count {
                self.chain.decode(&mut buffers[index], &mut metadata[index]);
                if metadata[index].len != 0 {
                    if kept != index {
                        let length = metadata[index].len;
                        let (prior, current) = buffers.split_at_mut(index);
                        prior[kept][..length].copy_from_slice(&current[0][..length]);
                        metadata[kept] = metadata[index];
                    }
                    kept += 1;
                }
            }
            if kept != 0 {
                return Poll::Ready(Ok(kept));
            }
        }
        context.waker().wake_by_ref();
        Poll::Pending
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        self.socket.max_transmit_segments().min(32)
    }
    fn max_receive_segments(&self) -> usize {
        self.socket.max_receive_segments()
    }
    fn may_fragment(&self) -> bool {
        self.socket.may_fragment()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(kind: &str) -> Config {
        Config {
            kind: kind.into(),
            password: if kind == "salamander" {
                "test-password".into()
            } else {
                String::new()
            },
            prefix: if kind == "dns" {
                "000001000001000000000000076578616d706c6503636f6d0000010001".into()
            } else {
                String::new()
            },
        }
    }

    #[test]
    fn header_sequences_match_go_wire_formats_and_wrap() {
        for (kind, expected) in [
            ("wireguard", vec![4, 0, 0, 0]),
            ("utp", vec![0, 0, 0, 0]),
            ("srtp", vec![0, 0, 0, 1]),
            (
                "wechat",
                vec![0xa1, 8, 0, 0, 0, 1, 0, 0x10, 0x11, 0x18, 0x30, 0x22, 0x30],
            ),
            ("dtls", vec![23, 254, 253, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        ] {
            let chain = Chain::new(&[config(kind)]).unwrap();
            let mut state = SendState::new(1);
            chain.encode(b"packet", 6, &mut state).unwrap();
            assert_eq!(&state.wire[..chain.overhead], expected);
            assert_eq!(&state.wire[chain.overhead..], b"packet");
            if kind == "dtls" {
                for length in [17u16, 34, 51, 68, 85, 52, 69, 86, 53] {
                    chain.encode(b"packet", 6, &mut state).unwrap();
                    assert_eq!(&state.wire[11..13], length.to_be_bytes());
                }
            }
            state.counters[0].sequence = u32::MAX;
            chain.encode(b"packet", 6, &mut state).unwrap();
            match kind {
                "srtp" => assert_eq!(&state.wire[2..4], &[0, 0]),
                "wechat" => assert_eq!(&state.wire[2..6], &[0; 4]),
                "dtls" => assert_eq!(&state.wire[7..11], &u32::MAX.to_be_bytes()),
                _ => {}
            }
        }
    }

    #[test]
    fn stateless_masks_are_safe_for_thread_local_send_buffers() {
        for kind in ["salamander", "dns", "utp", "wireguard"] {
            assert!(Chain::new(&[config(kind)]).unwrap().stateless);
        }
        for kind in ["dtls", "srtp", "wechat"] {
            assert!(!Chain::new(&[config(kind)]).unwrap().stateless);
            assert!(
                !Chain::new(&[config("salamander"), config(kind), config("wireguard")])
                    .unwrap()
                    .stateless
            );
        }
    }

    #[tokio::test]
    async fn stateless_send_uses_thread_local_scratch_only_when_contended() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        for masks in [
            vec![config("wireguard"), config("salamander")],
            vec![config("dns")],
            vec![config("utp"), config("salamander"), config("wireguard")],
        ] {
            let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
            raw.set_nonblocking(true).unwrap();
            let sender = Arc::new(MaskSocket::new(raw, &masks).unwrap());
            let transmit = udp::Transmit {
                destination: receiver.local_addr().unwrap(),
                contents: b"packet",
                segment_size: None,
                ecn: None,
                src_ip: None,
            };
            send_masked(&sender, &transmit).await;
            let state = sender.send.lock().unwrap();
            assert!(!state.wire.is_empty());
            send_masked_blocking(&sender, &transmit);
            drop(state);
            MASK_SEND_STATE.with(|state| {
                let state = state.borrow_mut();
                assert!(!state.wire.is_empty());
                assert!(state.wire.capacity() <= 65536);
                send_masked_blocking(&sender, &transmit);
            });
            for _ in 0..3 {
                let mut packet = [0; 2048];
                let length = receiver.recv(&mut packet).unwrap();
                let mut metadata = udp::RecvMeta {
                    len: length,
                    stride: length,
                    ..Default::default()
                };
                sender.chain.decode(&mut packet, &mut metadata);
                assert_eq!(&packet[..metadata.len], b"packet");
            }
        }
    }

    #[tokio::test]
    async fn concurrent_stateless_send_preserves_payloads_and_unique_salts() {
        let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
        raw.set_nonblocking(true).unwrap();
        let sender =
            Arc::new(MaskSocket::new(raw, &[config("wireguard"), config("salamander")]).unwrap());
        let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let destination = receiver.local_addr().unwrap();
        let mut poller = sender.clone().create_io_poller();
        std::future::poll_fn(|context| poller.as_mut().poll_writable(context))
            .await
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let workers: Vec<_> = (0..4)
            .map(|worker| {
                let sender = sender.clone();
                let barrier = barrier.clone();
                tokio::task::spawn_blocking(move || {
                    barrier.wait();
                    for sequence in 0..16 {
                        let payload = [worker, sequence, 42];
                        send_masked_blocking(
                            &sender,
                            &udp::Transmit {
                                destination,
                                contents: &payload,
                                segment_size: None,
                                ecn: None,
                                src_ip: None,
                            },
                        );
                    }
                })
            })
            .collect();
        let mut salts = std::collections::HashSet::new();
        let mut payloads = std::collections::HashSet::new();
        for _ in 0..64 {
            let mut packet = [0; 128];
            let length = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                receiver.recv(&mut packet),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(salts.insert(packet[4..12].to_vec()));
            let mut metadata = udp::RecvMeta {
                len: length,
                stride: length,
                ..Default::default()
            };
            sender.chain.decode(&mut packet, &mut metadata);
            assert_eq!(metadata.len, 3);
            assert_eq!(packet[2], 42);
            assert!(packet[0] < 4 && packet[1] < 16);
            assert!(payloads.insert(packet[..metadata.len].to_vec()));
        }
        for worker in workers {
            worker.await.unwrap();
        }
        assert!(sender.send.lock().unwrap().wire.capacity() <= 65536);
    }

    async fn send_masked(socket: &Arc<MaskSocket>, transmit: &udp::Transmit<'_>) {
        let mut poller = socket.clone().create_io_poller();
        loop {
            match socket.try_send(transmit) {
                Ok(()) => return,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    std::future::poll_fn(|context| poller.as_mut().poll_writable(context))
                        .await
                        .unwrap();
                }
                Err(error) => panic!("{error}"),
            }
        }
    }

    fn send_masked_blocking(socket: &MaskSocket, transmit: &udp::Transmit<'_>) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match socket.try_send(transmit) {
                Ok(()) => return,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::yield_now();
                }
                Err(error) => panic!("{error}"),
            }
        }
    }

    #[test]
    fn chains_keep_order_salts_gro_metadata_and_short_tail() {
        for kinds in [
            vec!["wireguard", "salamander"],
            vec![
                "salamander",
                "dns",
                "salamander",
                "dtls",
                "wechat",
                "srtp",
                "utp",
            ],
        ] {
            let chain =
                Chain::new(&kinds.iter().map(|kind| config(kind)).collect::<Vec<_>>()).unwrap();
            let mut state = SendState::new(chain.layers.len());
            state.remaining = state.random.len();
            for (index, byte) in state.random.iter_mut().enumerate() {
                *byte = index as u8;
            }
            let contents: Vec<_> = (0..2501).map(|index| (index % 251) as u8).collect();
            chain.encode(&contents, 1200, &mut state).unwrap();
            let wire_stride = 1200 + chain.overhead;
            if kinds[0] == "wireguard" {
                for (index, packet) in state.wire.chunks(wire_stride).enumerate() {
                    assert_eq!(&packet[..4], &[4, 0, 0, 0]);
                    assert_eq!(
                        &packet[4..12],
                        &(index as u8 * 8..index as u8 * 8 + 8).collect::<Vec<_>>()
                    );
                }
            }
            let address = "127.0.0.1:1234".parse().unwrap();
            let mut metadata = udp::RecvMeta {
                addr: address,
                len: state.wire.len(),
                stride: wire_stride,
                ecn: Some(udp::EcnCodepoint::Ect0),
                dst_ip: Some("127.0.0.2".parse().unwrap()),
            };
            chain.decode(&mut state.wire, &mut metadata);
            assert_eq!(&state.wire[..metadata.len], contents);
            assert_eq!(metadata.stride, 1200);
            assert_eq!(metadata.addr, address);
            assert_eq!(metadata.ecn, Some(udp::EcnCodepoint::Ect0));
            assert_eq!(metadata.dst_ip, Some("127.0.0.2".parse().unwrap()));
        }
    }

    #[test]
    fn dns_random_ids_and_header_receive_acceptance_match_go() {
        let chain = Chain::new(&[config("dns")]).unwrap();
        let mut state = SendState::new(1);
        chain.encode(b"packet", 6, &mut state).unwrap();
        let first_id = state.wire[..2].to_vec();
        let template = state.wire[2..chain.overhead].to_vec();
        chain.encode(b"packet", 6, &mut state).unwrap();
        assert_eq!(&state.wire[2..chain.overhead], template);
        let mut any_different = state.wire[..2] != first_id;
        for _ in 0..4 {
            chain.encode(b"packet", 6, &mut state).unwrap();
            any_different |= state.wire[..2] != first_id;
        }
        assert!(any_different);
        state.wire[..chain.overhead].fill(0xff);
        let mut metadata = udp::RecvMeta {
            len: state.wire.len(),
            stride: state.wire.len(),
            ..Default::default()
        };
        chain.decode(&mut state.wire, &mut metadata);
        assert_eq!(&state.wire[..metadata.len], b"packet");
    }

    #[test]
    fn malformed_config_short_packets_and_oversized_batches_are_bounded() {
        for invalid in [
            Config {
                password: "abc".into(),
                ..config("salamander")
            },
            Config {
                prefix: "not hex".into(),
                ..config("dns")
            },
            config("unknown"),
            Config {
                password: "ignored".into(),
                ..config("dtls")
            },
        ] {
            assert!(Chain::new(&[invalid]).is_err());
        }
        assert!(Chain::new(&vec![config("srtp"); 17]).is_err());
        assert!(Chain::new(&vec![config("dns"); 9]).is_err());
        let chain = Chain::new(&[config("salamander"), config("dtls")]).unwrap();
        let mut state = SendState::new(2);
        assert!(chain.encode(b"packet", 0, &mut state).is_err());
        assert!(chain.encode(&[0; 33], 1, &mut state).is_err());
        assert!(!chain.encode(&[0; 4096], 4096, &mut state).unwrap());
        assert!(state.wire.is_empty());
        assert!(chain.encode(&[0; 65536], 65536, &mut state).is_err());
        for length in 0..=chain.overhead {
            let mut packet = vec![0; length];
            let mut metadata = udp::RecvMeta {
                len: length,
                stride: length,
                ..Default::default()
            };
            chain.decode(&mut packet, &mut metadata);
            assert_eq!(metadata.len, 0);
        }
    }

    #[tokio::test]
    async fn segmented_socket_send_preserves_independent_datagrams() {
        use std::future::poll_fn;
        let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
        raw.set_nonblocking(true).unwrap();
        let sender =
            Arc::new(MaskSocket::new(raw, &[config("wireguard"), config("salamander")]).unwrap());
        let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let contents: Vec<_> = (0..3617).map(|index| (index % 251) as u8).collect();
        let transmit = udp::Transmit {
            destination: receiver.local_addr().unwrap(),
            contents: &contents,
            segment_size: Some(1200),
            ecn: None,
            src_ip: None,
        };
        let mut poller = sender.clone().create_io_poller();
        loop {
            poll_fn(|context| poller.as_mut().poll_writable(context))
                .await
                .unwrap();
            match sender.try_send(&transmit) {
                Ok(()) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => (),
                Err(error) => panic!("{error}"),
            }
        }
        let mut received = Vec::new();
        let mut salts = Vec::new();
        for expected in contents.chunks(1200) {
            let mut packet = [0; 2048];
            let length = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                receiver.recv(&mut packet),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(length, expected.len() + sender.chain.overhead);
            assert_eq!(&packet[..4], &[4, 0, 0, 0]);
            assert!(!salts.contains(&packet[4..12].to_vec()));
            salts.push(packet[4..12].to_vec());
            let mut metadata = udp::RecvMeta {
                len: length,
                stride: length,
                ..Default::default()
            };
            sender.chain.decode(&mut packet, &mut metadata);
            assert_eq!(&packet[..metadata.len], expected);
            received.extend_from_slice(&packet[..metadata.len]);
        }
        assert_eq!(received, contents);
        assert!(sender.send.lock().unwrap().wire.capacity() <= 65536);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[tokio::test]
    async fn socket_receive_filters_short_packets_and_compacts_metadata() {
        use std::future::poll_fn;
        let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
        raw.set_nonblocking(true).unwrap();
        let receiver = MaskSocket::new(raw, &[config("wechat"), config("salamander")]).unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut state = SendState::new(2);
        for payload in [b"a".as_slice(), b"truncated", b"b", b"c"] {
            if payload == b"truncated" {
                sender
                    .send_to(payload, receiver.local_addr().unwrap())
                    .unwrap();
            } else {
                receiver
                    .chain
                    .encode(payload, payload.len(), &mut state)
                    .unwrap();
                sender
                    .send_to(&state.wire, receiver.local_addr().unwrap())
                    .unwrap();
            }
        }
        let mut storage = [[0; 2048]; 4];
        let mut buffers: Vec<_> = storage
            .iter_mut()
            .map(|packet| IoSliceMut::new(packet))
            .collect();
        let mut metadata = [udp::RecvMeta::default(); 4];
        let count = poll_fn(|context| receiver.poll_recv(context, &mut buffers, &mut metadata))
            .await
            .unwrap();
        assert_eq!(count, 3);
        for index in 0..count {
            assert_eq!(
                &buffers[index][..metadata[index].len],
                &[b'a' + index as u8]
            );
            assert_eq!(metadata[index].addr, sender.local_addr().unwrap());
            assert_eq!(metadata[index].stride, 1);
        }
    }
}
