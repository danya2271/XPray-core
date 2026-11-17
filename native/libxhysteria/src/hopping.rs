//! Remote address hopping over one protected socket and one logical QUIC peer.

use quinn::{AsyncUdpSocket, UdpPoller, udp};
use serde::Deserialize;
use std::{
    fmt,
    io::{self, IoSliceMut},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub ports: Vec<u16>,
    #[serde(default)]
    pub ips: Vec<String>,
    #[serde(default)]
    pub once: bool,
    pub interval_min: u64,
    pub interval_max: u64,
}

struct State {
    index: usize,
    ip_index: usize,
    destination: SocketAddr,
    next_hop: Instant,
}

pub struct HoppingSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    peer: SocketAddr,
    config: Config,
    prefixes: Vec<Prefix>,
    fixed_destination: Option<SocketAddr>,
    state: Mutex<State>,
    diagnostic: bool,
}

#[derive(Clone, Copy)]
struct Prefix {
    address: IpAddr,
    mask: u128,
}

impl Prefix {
    fn parse(value: &str) -> io::Result<Self> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native UDP hopping IP prefix",
            )
        };
        let (address, bits) = value.split_once('/').ok_or_else(invalid)?;
        let mut address: IpAddr = address.parse().map_err(|_| invalid())?;
        let mut bits: u32 = bits.parse().map_err(|_| invalid())?;
        if let IpAddr::V6(ipv6) = address
            && let Some(ipv4) = ipv6.to_ipv4_mapped()
        {
            bits = bits.checked_sub(96).ok_or_else(invalid)?;
            address = ipv4.into();
        }
        let width = if address.is_ipv4() { 32 } else { 128 };
        if bits > width {
            return Err(invalid());
        }
        let mask = if bits == 0 {
            0
        } else {
            u128::MAX << (width - bits)
        };
        let address = match address {
            IpAddr::V4(address) => Ipv4Addr::from(u32::from(address) & mask as u32).into(),
            IpAddr::V6(address) => Ipv6Addr::from(u128::from(address) & mask).into(),
        };
        Ok(Self { address, mask })
    }

    fn sample(self) -> io::Result<IpAddr> {
        if self.mask == u128::MAX {
            return Ok(self.address);
        }
        let random = getrandom::u64().map_err(io::Error::other)?;
        Ok(match self.address {
            IpAddr::V4(address) => {
                Ipv4Addr::from(u32::from(address) | (random as u32 & !self.mask as u32)).into()
            }
            IpAddr::V6(address) => {
                let random =
                    ((random as u128) << 64) | getrandom::u64().map_err(io::Error::other)? as u128;
                Ipv6Addr::from(u128::from(address) | (random & !self.mask)).into()
            }
        })
    }

    fn contains(self, address: IpAddr) -> bool {
        match (self.address, address.to_canonical()) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                u32::from(address) & self.mask as u32 == u32::from(network)
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                u128::from(address) & self.mask == u128::from(network)
            }
            _ => false,
        }
    }
}

fn next_index(count: usize, previous: Option<usize>) -> io::Result<usize> {
    if count <= 1 {
        return Ok(0);
    }
    match previous {
        Some(previous) => {
            let index = random_below(count as u64 - 1)? as usize;
            Ok(if index >= previous { index + 1 } else { index })
        }
        None => Ok(random_below(count as u64)? as usize),
    }
}

fn random_below(bound: u64) -> io::Result<u64> {
    let threshold = bound.wrapping_neg() % bound;
    loop {
        let value = getrandom::u64().map_err(io::Error::other)?;
        if value >= threshold {
            return Ok(value % bound);
        }
    }
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        const MAX_SECONDS: u64 = i64::MAX as u64 / 1_000_000_000;
        if (self.ports.is_empty() && self.ips.is_empty())
            || self.ports.len() > u16::MAX as usize
            || self.ips.len() > u16::MAX as usize
            || self.ports.contains(&0)
            || self.interval_min < 5
            || self.interval_max < self.interval_min
            || self.interval_max > MAX_SECONDS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native remote address hopping configuration",
            ));
        }
        for prefix in &self.ips {
            Prefix::parse(prefix)?;
        }
        Ok(())
    }

    fn interval(&self) -> io::Result<Duration> {
        if self.interval_min == self.interval_max {
            return Ok(Duration::from_secs(self.interval_min));
        }
        Ok(Duration::from_secs(
            self.interval_min + random_below(self.interval_max - self.interval_min + 1)?,
        ))
    }
}

impl HoppingSocket {
    pub fn new(
        inner: Arc<dyn AsyncUdpSocket>,
        mut peer: SocketAddr,
        mut config: Config,
        diagnostic: bool,
    ) -> io::Result<Self> {
        config.validate()?;
        let local = inner.local_addr()?;
        // Quinn represents IPv4 peers as mapped IPv6 on dual-stack sockets.
        // Use the same address for both outgoing checks and receive metadata.
        if local.is_ipv6()
            && let SocketAddr::V4(address) = peer
        {
            peer = SocketAddr::new(address.ip().to_ipv6_mapped().into(), address.port());
        }
        config.ports.sort_unstable();
        config.ports.dedup();
        let prefixes: Vec<_> = config
            .ips
            .iter()
            .map(|prefix| Prefix::parse(prefix))
            .collect::<io::Result<_>>()?;
        if local.is_ipv4()
            && (peer.is_ipv6() || prefixes.iter().any(|prefix| prefix.address.is_ipv6()))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native IPv6 hopping requires an IPv6 UDP socket",
            ));
        }
        let index = next_index(config.ports.len(), None)?;
        let ip_index = next_index(prefixes.len(), None)?;
        let destination = Self::select_destination(peer, &config, &prefixes, index, ip_index)?;
        let next_hop = Instant::now() + config.interval()?;
        let socket = Self {
            inner,
            peer,
            prefixes,
            fixed_destination: config.once.then_some(destination),
            config,
            state: Mutex::new(State {
                index,
                ip_index,
                destination,
                next_hop,
            }),
            diagnostic,
        };
        socket.report(destination);
        Ok(socket)
    }

    fn select_destination(
        peer: SocketAddr,
        config: &Config,
        prefixes: &[Prefix],
        index: usize,
        ip_index: usize,
    ) -> io::Result<SocketAddr> {
        let mut destination = peer;
        if let Some(port) = config.ports.get(index) {
            destination.set_port(*port);
        }
        if let Some(prefix) = prefixes.get(ip_index) {
            let address = prefix.sample()?;
            destination.set_ip(match address {
                IpAddr::V4(address) if peer.is_ipv6() => address.to_ipv6_mapped().into(),
                address => address,
            });
        }
        Ok(destination)
    }

    fn report(&self, address: SocketAddr) {
        if self.diagnostic {
            eprintln!("[XRAY_NET_DIAG][XHYSTERIA] UDP hopping remote={address}");
        }
    }

    fn destination(&self, now: Instant) -> io::Result<SocketAddr> {
        if let Some(destination) = self.fixed_destination {
            return Ok(destination);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("native UDP hop lock poisoned"))?;
        if now >= state.next_hop {
            let index = next_index(self.config.ports.len(), Some(state.index))?;
            let ip_index = next_index(self.prefixes.len(), Some(state.ip_index))?;
            let destination =
                Self::select_destination(self.peer, &self.config, &self.prefixes, index, ip_index)?;
            if destination != state.destination {
                self.report(destination);
            }
            state.index = index;
            state.ip_index = ip_index;
            state.destination = destination;
            state.next_hop = now + self.config.interval()?;
        }
        Ok(state.destination)
    }

    fn accepts(&self, address: SocketAddr) -> bool {
        (address.ip().to_canonical() == self.peer.ip().to_canonical()
            || self
                .prefixes
                .iter()
                .any(|prefix| prefix.contains(address.ip())))
            && (address.port() == self.peer.port()
                || self.config.ports.binary_search(&address.port()).is_ok())
    }
}

impl fmt::Debug for HoppingSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HoppingSocket")
            .field("peer", &self.peer)
            .field("ports", &self.config.ports.len())
            .finish_non_exhaustive()
    }
}

impl AsyncUdpSocket for HoppingSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.inner.clone().create_io_poller()
    }

    fn try_send(&self, transmit: &udp::Transmit) -> io::Result<()> {
        if transmit.destination != self.peer {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native UDP hopping socket has a single logical peer",
            ));
        }
        let mut transmit = transmit.clone();
        transmit.destination = self.destination(Instant::now())?;
        self.inner.try_send(&transmit)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        for _ in 0..32 {
            match self.inner.poll_recv(cx, bufs, meta) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Ok(0)),
                Poll::Ready(Ok(count)) => {
                    let mut kept = 0;
                    for i in 0..count {
                        if !self.accepts(meta[i].addr) {
                            continue;
                        }
                        if kept != i {
                            let (before, after) = bufs.split_at_mut(i);
                            before[kept][..meta[i].len].copy_from_slice(&after[0][..meta[i].len]);
                        }
                        // A port change is not QUIC migration. Preserve GRO
                        // metadata and compact only after rejected sources.
                        meta[kept] = udp::RecvMeta {
                            addr: self.peer,
                            ..meta[i]
                        };
                        kept += 1;
                    }
                    if kept != 0 {
                        return Poll::Ready(Ok(kept));
                    }
                }
                result => return result,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct Socket {
        sent: Mutex<Vec<SocketAddr>>,
        received: Mutex<std::collections::VecDeque<SocketAddr>>,
        local: Option<SocketAddr>,
    }

    impl AsyncUdpSocket for Socket {
        fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
            unreachable!("mock sends do not block")
        }

        fn try_send(&self, transmit: &udp::Transmit) -> io::Result<()> {
            self.sent.lock().unwrap().push(transmit.destination);
            Ok(())
        }

        fn poll_recv(
            &self,
            _: &mut Context<'_>,
            bufs: &mut [IoSliceMut<'_>],
            meta: &mut [udp::RecvMeta],
        ) -> Poll<io::Result<usize>> {
            let mut received = self.received.lock().unwrap();
            let count = received.len().min(bufs.len());
            for i in 0..count {
                let address = received.pop_front().unwrap();
                bufs[i][..4].copy_from_slice(&address.port().to_be_bytes().repeat(2));
                meta[i] = udp::RecvMeta {
                    addr: address,
                    len: 4,
                    stride: 2,
                    ecn: Some(udp::EcnCodepoint::Ect0),
                    dst_ip: Some("127.0.0.1".parse().unwrap()),
                };
            }
            if count == 0 {
                Poll::Pending
            } else {
                Poll::Ready(Ok(count))
            }
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(self
                .local
                .unwrap_or_else(|| "127.0.0.1:4567".parse().unwrap()))
        }
    }

    fn config(ports: Vec<u16>) -> Config {
        Config {
            ports,
            ips: Vec::new(),
            once: false,
            interval_min: 5,
            interval_max: 10,
        }
    }

    #[test]
    fn hop_preserves_local_socket_and_normalizes_late_replies() {
        let inner = Arc::new(Socket::default());
        let peer = "127.0.0.1:3333".parse().unwrap();
        let socket =
            HoppingSocket::new(inner.clone(), peer, config(vec![1111, 2222, 1111]), false).unwrap();
        let transmit = udp::Transmit {
            destination: peer,
            ecn: None,
            contents: b"test",
            segment_size: None,
            src_ip: None,
        };
        socket.try_send(&transmit).unwrap();
        let next = socket.state.lock().unwrap().next_hop;
        let hopped = socket.destination(next).unwrap();
        let first = inner.sent.lock().unwrap()[0];
        assert_ne!(hopped.port(), first.port());
        assert_eq!(socket.local_addr().unwrap(), inner.local_addr().unwrap());
        inner.received.lock().unwrap().extend([
            "127.0.0.2:1111".parse().unwrap(),
            "127.0.0.1:9999".parse().unwrap(),
            first,
            hopped,
            peer,
        ]);
        let mut bytes = [0; 64];
        let mut buffers = [IoSliceMut::new(&mut bytes)];
        let mut meta = [udp::RecvMeta::default()];
        let mut cx = Context::from_waker(std::task::Waker::noop());
        for _ in 0..3 {
            assert!(matches!(
                socket.poll_recv(&mut cx, &mut buffers, &mut meta),
                Poll::Ready(Ok(1))
            ));
            assert_eq!(meta[0].addr, peer);
            assert_eq!(meta[0].stride, 2);
        }
        assert!(
            socket
                .poll_recv(&mut cx, &mut buffers, &mut meta)
                .is_pending()
        );
    }

    #[test]
    fn receives_batches_without_losing_gro_metadata_or_reordering_payloads() {
        let inner = Arc::new(Socket::default());
        inner.received.lock().unwrap().extend([
            "127.0.0.1:1111".parse::<SocketAddr>().unwrap(),
            "127.0.0.2:1111".parse().unwrap(),
            "127.0.0.1:2222".parse().unwrap(),
            "127.0.0.1:3333".parse().unwrap(),
        ]);
        let peer = "127.0.0.1:3333".parse().unwrap();
        let socket = HoppingSocket::new(inner, peer, config(vec![1111, 2222]), false).unwrap();
        let mut bytes = [[0; 64]; 4];
        let mut buffers: Vec<_> = bytes.iter_mut().map(|b| IoSliceMut::new(b)).collect();
        let mut meta = [udp::RecvMeta::default(); 4];
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            socket.poll_recv(&mut cx, &mut buffers, &mut meta),
            Poll::Ready(Ok(3))
        ));
        for (i, port) in [1111u16, 2222, 3333].into_iter().enumerate() {
            assert_eq!(&buffers[i][..4], &port.to_be_bytes().repeat(2));
            assert_eq!(meta[i].addr, peer);
            assert_eq!(meta[i].stride, 2);
            assert_eq!(meta[i].ecn, Some(udp::EcnCodepoint::Ect0));
            assert_eq!(meta[i].dst_ip, Some("127.0.0.1".parse().unwrap()));
        }
    }

    #[test]
    fn single_port_and_random_interval_bounds() {
        let socket = HoppingSocket::new(
            Arc::new(Socket::default()),
            "127.0.0.1:3333".parse().unwrap(),
            config(vec![1111]),
            false,
        )
        .unwrap();
        let next = socket.state.lock().unwrap().next_hop;
        assert_eq!(socket.destination(next).unwrap().port(), 1111);
        for _ in 0..100 {
            let interval = socket.config.interval().unwrap().as_secs();
            assert!((5..=10).contains(&interval));
        }
    }

    #[test]
    fn dual_stack_sockets_use_quinn_mapped_ipv4_addresses() {
        let inner = Arc::new(Socket {
            local: Some("[::]:4567".parse().unwrap()),
            ..Socket::default()
        });
        let socket = HoppingSocket::new(
            inner.clone(),
            "127.0.0.1:3333".parse().unwrap(),
            config(vec![1111]),
            false,
        )
        .unwrap();
        let mapped = "[::ffff:127.0.0.1]:3333".parse().unwrap();
        assert_eq!(socket.peer, mapped);
        socket
            .try_send(&udp::Transmit {
                destination: mapped,
                ecn: None,
                contents: b"test",
                segment_size: None,
                src_ip: None,
            })
            .unwrap();
        assert_eq!(
            inner.sent.lock().unwrap()[0],
            "[::ffff:127.0.0.1]:1111".parse().unwrap()
        );
        assert!(socket.accepts("127.0.0.1:1111".parse().unwrap()));
    }

    #[test]
    fn rejects_invalid_hop_configuration() {
        assert!(config(vec![]).validate().is_err());
        assert!(config(vec![0]).validate().is_err());
        let mut c = config(vec![65535]);
        c.interval_min = 4;
        assert!(c.validate().is_err());
        c.interval_min = 11;
        assert!(c.validate().is_err());
        c.interval_max = u64::MAX;
        assert!(c.validate().is_err());
    }

    #[test]
    fn once_selects_initial_destination_without_further_hops() {
        let mut configuration = config(vec![1111, 2222]);
        configuration.ips = vec!["127.0.0.2/32".into(), "127.0.0.3/32".into()];
        configuration.once = true;
        let socket = HoppingSocket::new(
            Arc::new(Socket::default()),
            "127.0.0.1:3333".parse().unwrap(),
            configuration,
            false,
        )
        .unwrap();
        let destination = socket.destination(Instant::now()).unwrap();
        assert_ne!(destination.ip(), socket.peer.ip());
        assert_ne!(destination.port(), socket.peer.port());
        let later = socket.state.lock().unwrap().next_hop + Duration::from_secs(3600);
        assert_eq!(socket.destination(later).unwrap(), destination);
    }

    #[test]
    fn ip_only_hopping_preserves_port_and_accepts_delayed_prefix_replies() {
        let mut configuration = config(vec![]);
        configuration.ips = vec!["127.0.0.2/32".into(), "127.0.0.3/32".into()];
        let inner = Arc::new(Socket::default());
        let socket = HoppingSocket::new(
            inner.clone(),
            "127.0.0.1:3333".parse().unwrap(),
            configuration,
            false,
        )
        .unwrap();
        let first = socket.destination(Instant::now()).unwrap();
        let next = socket.state.lock().unwrap().next_hop;
        let second = socket.destination(next).unwrap();
        assert_ne!(first.ip(), second.ip());
        assert_eq!(first.port(), 3333);
        assert_eq!(second.port(), 3333);
        assert!(socket.accepts(first));
        assert!(socket.accepts(second));
        assert!(!socket.accepts("127.0.0.4:3333".parse().unwrap()));
        assert!(!socket.accepts("127.0.0.2:3334".parse().unwrap()));
        inner.received.lock().unwrap().extend([first, second]);
        let mut bytes = [[0; 64]; 2];
        let mut buffers: Vec<_> = bytes
            .iter_mut()
            .map(|buffer| IoSliceMut::new(buffer))
            .collect();
        let mut metadata = [udp::RecvMeta::default(); 2];
        let mut context = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            socket.poll_recv(&mut context, &mut buffers, &mut metadata),
            Poll::Ready(Ok(2))
        ));
        for received in metadata {
            assert_eq!(received.addr, socket.peer);
            assert_eq!(received.stride, 2);
        }
        assert_eq!(socket.local_addr().unwrap(), inner.local_addr().unwrap());
    }

    #[test]
    fn prefixes_preserve_network_bits_and_validate_address_families() {
        for value in [
            "192.0.2.129/24",
            "0.0.0.0/0",
            "192.0.2.1/32",
            "2001:db8::1234/64",
            "::/0",
            "2001:db8::1/128",
            "::ffff:192.0.2.129/120",
        ] {
            let prefix = Prefix::parse(value).unwrap();
            for _ in 0..100 {
                assert!(prefix.contains(prefix.sample().unwrap()));
            }
        }
        assert_eq!(
            Prefix::parse("192.0.2.129/24").unwrap().address,
            "192.0.2.0".parse::<IpAddr>().unwrap()
        );
        for value in [
            "192.0.2.1",
            "192.0.2.1/33",
            "::1/129",
            "::ffff:192.0.2.1/95",
            "invalid/24",
            "::1/-1",
        ] {
            assert!(Prefix::parse(value).is_err(), "accepted {value}");
        }
        let mut configuration = config(vec![]);
        configuration.ips = vec!["2001:db8::1/128".into()];
        assert!(
            HoppingSocket::new(
                Arc::new(Socket::default()),
                "127.0.0.1:3333".parse().unwrap(),
                configuration.clone(),
                false
            )
            .is_err()
        );
        let inner = Arc::new(Socket {
            local: Some("[::]:4567".parse().unwrap()),
            ..Socket::default()
        });
        let socket = HoppingSocket::new(
            inner,
            "127.0.0.1:3333".parse().unwrap(),
            configuration,
            false,
        )
        .unwrap();
        assert_eq!(
            socket.destination(Instant::now()).unwrap(),
            "[2001:db8::1]:3333".parse().unwrap()
        );
    }

    #[test]
    fn ipv4_prefixes_use_mapped_addresses_on_dual_stack_sockets() {
        let inner = Arc::new(Socket {
            local: Some("[::]:4567".parse().unwrap()),
            ..Socket::default()
        });
        let mut configuration = config(vec![]);
        configuration.ips = vec!["::ffff:127.0.0.2/128".into()];
        let socket =
            HoppingSocket::new(inner, "[::1]:3333".parse().unwrap(), configuration, false).unwrap();
        assert_eq!(
            socket.destination(Instant::now()).unwrap(),
            "[::ffff:127.0.0.2]:3333".parse().unwrap()
        );
        assert!(socket.accepts("[::ffff:127.0.0.2]:3333".parse().unwrap()));
    }

    #[test]
    fn legacy_port_hopping_json_keeps_ongoing_hops() {
        let configuration: Config =
            serde_json::from_str(r#"{"ports":[1111],"interval_min":5,"interval_max":5}"#).unwrap();
        assert!(!configuration.once);
        assert!(configuration.ips.is_empty());
        configuration.validate().unwrap();
    }
}
