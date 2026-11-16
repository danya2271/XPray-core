//! Remote port hopping over one protected socket and one logical QUIC peer.

use quinn::{AsyncUdpSocket, UdpPoller, udp};
use serde::Deserialize;
use std::{
    fmt,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub ports: Vec<u16>,
    pub interval_min: u64,
    pub interval_max: u64,
}

struct State {
    index: usize,
    next_hop: Instant,
}

pub struct HoppingSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    peer: SocketAddr,
    config: Config,
    state: Mutex<State>,
    diagnostic: bool,
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
        if self.ports.is_empty()
            || self.ports.len() > u16::MAX as usize
            || self.ports.contains(&0)
            || self.interval_min < 5
            || self.interval_max < self.interval_min
            || self.interval_max > MAX_SECONDS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native remote port hopping configuration",
            ));
        }
        Ok(())
    }

    fn interval(&self) -> io::Result<Duration> {
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
        // Quinn represents IPv4 peers as mapped IPv6 on dual-stack sockets.
        // Use the same address for both outgoing checks and receive metadata.
        if inner.local_addr()?.is_ipv6()
            && let SocketAddr::V4(address) = peer
        {
            peer = SocketAddr::new(address.ip().to_ipv6_mapped().into(), address.port());
        }
        config.ports.sort_unstable();
        config.ports.dedup();
        let index = random_below(config.ports.len() as u64)? as usize;
        let next_hop = Instant::now() + config.interval()?;
        let socket = Self {
            inner,
            peer,
            config,
            state: Mutex::new(State { index, next_hop }),
            diagnostic,
        };
        socket.report(socket.config.ports[index]);
        Ok(socket)
    }

    fn report(&self, port: u16) {
        if self.diagnostic {
            let mut address = self.peer;
            address.set_port(port);
            eprintln!("[XRAY_NET_DIAG][XHYSTERIA] UDP hopping remote={address}");
        }
    }

    fn destination(&self, now: Instant) -> io::Result<SocketAddr> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("native UDP hop lock poisoned"))?;
        if now >= state.next_hop {
            if self.config.ports.len() > 1 {
                let index = random_below(self.config.ports.len() as u64 - 1)? as usize;
                state.index = if index >= state.index {
                    index + 1
                } else {
                    index
                };
                self.report(self.config.ports[state.index]);
            }
            state.next_hop = now + self.config.interval()?;
        }
        let mut destination = self.peer;
        destination.set_port(self.config.ports[state.index]);
        Ok(destination)
    }

    fn accepts(&self, address: SocketAddr) -> bool {
        address.ip().to_canonical() == self.peer.ip().to_canonical()
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
}
