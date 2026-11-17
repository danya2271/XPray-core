use quinn::{AsyncUdpSocket, UdpPoller, udp};
use std::{
    io::{self, IoSliceMut},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::AsRawFd,
    os::unix::net::UnixDatagram,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::Interest;

const HEADER: usize = 24;
const MAX_PACKET: usize = 65535;
const MAX_BATCH: usize = 8;

#[derive(Debug)]
pub struct PacketSocket {
    socket: Arc<tokio::net::UnixDatagram>,
    local: SocketAddr,
    batch: bool,
}

impl PacketSocket {
    pub fn new(socket: UnixDatagram, local: SocketAddr, batch: bool) -> io::Result<Self> {
        Ok(Self {
            socket: Arc::new(tokio::net::UnixDatagram::from_std(socket)?),
            local,
            batch,
        })
    }
}

fn encode_header(address: SocketAddr, length: usize) -> io::Result<[u8; HEADER]> {
    if length > MAX_PACKET {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "packet bridge datagram too large",
        ));
    }
    let mut wire = [0; HEADER];
    wire[0] = 1;
    wire[1] = if address.is_ipv4() { 4 } else { 6 };
    wire[2..4].copy_from_slice(&address.port().to_be_bytes());
    match address.ip() {
        IpAddr::V4(address) => wire[16..20].copy_from_slice(&address.octets()),
        IpAddr::V6(address) => wire[4..20].copy_from_slice(&address.octets()),
    }
    wire[20..24].copy_from_slice(&(length as u32).to_be_bytes());
    Ok(wire)
}

fn decode_header(wire: &[u8; HEADER], length: usize) -> io::Result<SocketAddr> {
    if wire[0] != 1 || length > MAX_PACKET {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid packet bridge frame",
        ));
    }
    if u32::from_be_bytes(wire[20..24].try_into().unwrap()) as usize != length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid packet bridge length",
        ));
    }
    let port = u16::from_be_bytes(wire[2..4].try_into().unwrap());
    let address = match wire[1] {
        4 if wire[4..16].iter().all(|byte| *byte == 0) => {
            IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(&wire[16..20]).unwrap()))
        }
        6 => IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(&wire[4..20]).unwrap())),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid packet bridge address",
            ));
        }
    };
    Ok(SocketAddr::new(address, port))
}

fn send_frame(
    socket: &tokio::net::UnixDatagram,
    header: &[u8; HEADER],
    payload: &[u8],
) -> io::Result<usize> {
    let mut vectors = [
        libc::iovec {
            iov_base: header.as_ptr().cast_mut().cast(),
            iov_len: header.len(),
        },
        libc::iovec {
            iov_base: payload.as_ptr().cast_mut().cast(),
            iov_len: payload.len(),
        },
    ];
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = vectors.as_mut_ptr();
    message.msg_iovlen = vectors.len() as _;
    loop {
        let length = unsafe {
            libc::sendmsg(
                socket.as_raw_fd(),
                &message,
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if length >= 0 {
            return Ok(length as usize);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn receive_frame(
    socket: &tokio::net::UnixDatagram,
    payload: &mut [u8],
) -> io::Result<Option<(SocketAddr, usize)>> {
    let mut header = [0; HEADER];
    let mut vectors = [
        libc::iovec {
            iov_base: header.as_mut_ptr().cast(),
            iov_len: header.len(),
        },
        libc::iovec {
            iov_base: payload.as_mut_ptr().cast(),
            iov_len: payload.len(),
        },
    ];
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = vectors.as_mut_ptr();
    message.msg_iovlen = vectors.len() as _;
    let length = loop {
        let length = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, libc::MSG_DONTWAIT) };
        if length >= 0 {
            break length as usize;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    if message.msg_flags & libc::MSG_TRUNC != 0 || length < HEADER {
        return Ok(None);
    }
    let length = length - HEADER;
    Ok(decode_header(&header, length)
        .ok()
        .map(|address| (address, length)))
}

#[derive(Debug)]
struct PacketPoller(Arc<tokio::net::UnixDatagram>);

impl UdpPoller for PacketPoller {
    fn poll_writable(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_send_ready(context)
    }
}

impl AsyncUdpSocket for PacketSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(PacketPoller(self.socket.clone()))
    }

    fn try_send(&self, transmit: &udp::Transmit) -> io::Result<()> {
        let mut header = encode_header(transmit.destination, transmit.contents.len())?;
        if let Some(stride) = transmit.segment_size {
            if !self.batch
                || stride == 0
                || stride > transmit.contents.len()
                || stride > u16::MAX as usize
                || transmit.contents.len().div_ceil(stride) > MAX_BATCH
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid packet bridge batch",
                ));
            }
            header[0] = 2;
            header[20..22].copy_from_slice(&(stride as u16).to_be_bytes());
            header[22..24].copy_from_slice(&(transmit.contents.len() as u16).to_be_bytes());
        }
        let length = self.socket.try_io(Interest::WRITABLE, || {
            send_frame(&self.socket, &header, transmit.contents)
        })?;
        if length != HEADER + transmit.contents.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short packet bridge write",
            ));
        }
        Ok(())
    }

    fn poll_recv(
        &self,
        context: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        metadata: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let capacity = buffers.len().min(metadata.len());
        if capacity == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty packet receive batch",
            )));
        }
        let mut count = 0;
        for _ in 0..32 {
            let packet = match self.socket.try_io(Interest::READABLE, || {
                receive_frame(&self.socket, &mut buffers[count])
            }) {
                Ok(packet) => packet,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if count != 0 {
                        return Poll::Ready(Ok(count));
                    }
                    match self.socket.poll_recv_ready(context) {
                        Poll::Ready(Ok(())) => continue,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Err(error) => return Poll::Ready(Err(error)),
            };
            let Some((mut address, length)) = packet else {
                continue;
            };
            if self.local.is_ipv6()
                && let IpAddr::V4(ip) = address.ip()
            {
                address = SocketAddr::new(IpAddr::V6(ip.to_ipv6_mapped()), address.port());
            }
            metadata[count] = udp::RecvMeta {
                addr: address,
                len: length,
                stride: length,
                ecn: None,
                dst_ip: (!self.local.ip().is_unspecified()).then_some(self.local.ip()),
            };
            count += 1;
            if count == capacity {
                return Poll::Ready(Ok(count));
            }
        }
        if count != 0 {
            return Poll::Ready(Ok(count));
        }
        context.waker().wake_by_ref();
        Poll::Pending
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }
    fn max_transmit_segments(&self) -> usize {
        if self.batch { MAX_BATCH } else { 1 }
    }
    fn max_receive_segments(&self) -> usize {
        1
    }
    fn may_fragment(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;

    fn encode(address: SocketAddr, payload: &[u8], wire: &mut Vec<u8>) -> io::Result<()> {
        let header = encode_header(address, payload.len())?;
        wire.clear();
        wire.extend_from_slice(&header);
        wire.extend_from_slice(payload);
        Ok(())
    }

    fn decode(wire: &[u8]) -> io::Result<(SocketAddr, &[u8])> {
        let header = wire
            .get(..HEADER)
            .and_then(|header| header.try_into().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short packet header"))?;
        Ok((decode_header(header, wire.len() - HEADER)?, &wire[HEADER..]))
    }

    #[tokio::test]
    async fn packet_socket_batches_sends_without_mutating_payload_and_respects_opt_out() {
        for batch in [false, true] {
            let (raw, peer) = UnixDatagram::pair().unwrap();
            raw.set_nonblocking(true).unwrap();
            peer.set_nonblocking(true).unwrap();
            let peer = tokio::net::UnixDatagram::from_std(peer).unwrap();
            let socket =
                Arc::new(PacketSocket::new(raw, "127.0.0.1:1".parse().unwrap(), batch).unwrap());
            let payload = [0x37; 2401];
            let mut poller = socket.clone().create_io_poller();
            poll_fn(|context| poller.as_mut().poll_writable(context))
                .await
                .unwrap();
            let result = socket.try_send(&udp::Transmit {
                destination: "127.0.0.2:443".parse().unwrap(),
                contents: &payload,
                segment_size: Some(1200),
                ecn: None,
                src_ip: None,
            });
            if !batch {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
                continue;
            }
            result.unwrap();
            let mut received = [0; 4096];
            let length = peer.recv(&mut received).await.unwrap();
            assert_eq!(received[0], 2);
            assert_eq!(
                u16::from_be_bytes(received[20..22].try_into().unwrap()),
                1200
            );
            assert_eq!(
                u16::from_be_bytes(received[22..24].try_into().unwrap()),
                2401
            );
            assert_eq!(&received[HEADER..length], &payload);
            assert_eq!(payload, [0x37; 2401]);
            for stride in [0, 1, 2402] {
                assert_eq!(
                    socket
                        .try_send(&udp::Transmit {
                            destination: "127.0.0.2:443".parse().unwrap(),
                            contents: &payload,
                            segment_size: Some(stride),
                            ecn: None,
                            src_ip: None
                        })
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::InvalidInput
                );
            }
        }
    }

    #[tokio::test]
    async fn packet_socket_drops_truncated_datagrams_without_delivering_partial_payloads() {
        let (raw, peer) = UnixDatagram::pair().unwrap();
        raw.set_nonblocking(true).unwrap();
        let socket = PacketSocket::new(raw, "127.0.0.1:1".parse().unwrap(), true).unwrap();
        let mut wire = Vec::new();
        encode("127.0.0.2:2".parse().unwrap(), &[0x37; 256], &mut wire).unwrap();
        peer.send(&wire).unwrap();
        encode("127.0.0.2:2".parse().unwrap(), b"bad length", &mut wire).unwrap();
        wire[23] = 0;
        peer.send(&wire).unwrap();
        encode("127.0.0.2:2".parse().unwrap(), b"complete", &mut wire).unwrap();
        peer.send(&wire).unwrap();
        let mut storage = [0; 32];
        let mut buffers = [IoSliceMut::new(&mut storage)];
        let mut metadata = [udp::RecvMeta::default()];
        assert_eq!(
            poll_fn(|context| socket.poll_recv(context, &mut buffers, &mut metadata))
                .await
                .unwrap(),
            1
        );
        assert_eq!(&buffers[0][..metadata[0].len], b"complete");
    }

    #[test]
    fn packet_metadata_round_trips_and_rejects_malformed_frames() {
        for address in ["127.0.0.1:443", "[2001:db8::1]:8443"] {
            let address = address.parse().unwrap();
            let mut wire = Vec::new();
            encode(address, b"QUIC packet", &mut wire).unwrap();
            assert_eq!(decode(&wire).unwrap(), (address, b"QUIC packet".as_slice()));
            for length in 0..HEADER {
                assert!(decode(&wire[..length]).is_err());
            }
            wire[20] = 1;
            assert!(decode(&wire).is_err());
        }
        assert!(
            encode(
                "127.0.0.1:1".parse().unwrap(),
                &vec![0; MAX_PACKET + 1],
                &mut Vec::new()
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn packet_socket_matches_dual_stack_peers_and_omits_unknown_destination() {
        for local in ["[::]:1", "[::1]:1", "0.0.0.0:1", "127.0.0.1:1"] {
            let local: SocketAddr = local.parse().unwrap();
            let (raw, peer) = UnixDatagram::pair().unwrap();
            raw.set_nonblocking(true).unwrap();
            let socket = PacketSocket::new(raw, local, true).unwrap();
            let mut wire = Vec::new();
            encode("127.0.0.2:443".parse().unwrap(), b"QUIC", &mut wire).unwrap();
            peer.send(&wire).unwrap();
            let mut storage = [0; 16];
            let mut buffers = [IoSliceMut::new(&mut storage)];
            let mut metadata = [udp::RecvMeta::default()];
            assert_eq!(
                poll_fn(|context| socket.poll_recv(context, &mut buffers, &mut metadata))
                    .await
                    .unwrap(),
                1
            );
            let expected = if local.is_ipv6() {
                "[::ffff:127.0.0.2]:443"
            } else {
                "127.0.0.2:443"
            };
            assert_eq!(metadata[0].addr, expected.parse().unwrap());
            assert_eq!(
                metadata[0].dst_ip,
                (!local.ip().is_unspecified()).then_some(local.ip())
            );
        }
    }

    #[tokio::test]
    async fn packet_socket_preserves_datagram_boundaries_and_skips_invalid_packets() {
        let (raw, peer) = UnixDatagram::pair().unwrap();
        raw.set_nonblocking(true).unwrap();
        let socket =
            Arc::new(PacketSocket::new(raw, "127.0.0.1:1".parse().unwrap(), true).unwrap());
        let peer = tokio::net::UnixDatagram::from_std({
            peer.set_nonblocking(true).unwrap();
            peer
        })
        .unwrap();
        peer.send(b"invalid").await.unwrap();
        let mut wire = Vec::new();
        encode("127.0.0.2:2".parse().unwrap(), b"first", &mut wire).unwrap();
        peer.send(&wire).await.unwrap();
        encode("[::1]:3".parse().unwrap(), b"second", &mut wire).unwrap();
        peer.send(&wire).await.unwrap();
        let mut storage = [[0u8; 128]; 2];
        let mut buffers: Vec<_> = storage
            .iter_mut()
            .map(|data| IoSliceMut::new(data))
            .collect();
        let mut metadata = [udp::RecvMeta::default(); 2];
        let count = poll_fn(|context| socket.poll_recv(context, &mut buffers, &mut metadata))
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(&buffers[0][..metadata[0].len], b"first");
        assert_eq!(metadata[1].addr, "[::1]:3".parse().unwrap());
        let mut poller = socket.clone().create_io_poller();
        poll_fn(|context| poller.as_mut().poll_writable(context))
            .await
            .unwrap();
        socket
            .try_send(&udp::Transmit {
                destination: "127.0.0.3:4".parse().unwrap(),
                contents: b"outgoing",
                ecn: None,
                segment_size: None,
                src_ip: None,
            })
            .unwrap();
        let mut receive = vec![0; 128];
        let length = peer.recv(&mut receive).await.unwrap();
        assert_eq!(
            decode(&receive[..length]).unwrap(),
            ("127.0.0.3:4".parse().unwrap(), b"outgoing".as_slice())
        );
    }
}
