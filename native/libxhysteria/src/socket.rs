//! Salamander over Quinn's batched UDP I/O and nonfatal send-error handling.

use crate::salamander::{SALT_LEN, Salamander};
use quinn::{AsyncUdpSocket, Runtime, UdpPoller, udp};
use std::{
    fmt,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

pub struct MaskedSocket {
    socket: Arc<dyn AsyncUdpSocket>,
    mask: Salamander,
    send_buffer: Mutex<Vec<u8>>,
}

impl MaskedSocket {
    pub fn new(socket: std::net::UdpSocket, psk: &[u8]) -> io::Result<Self> {
        Ok(Self {
            socket: quinn::TokioRuntime.wrap_udp_socket(socket)?,
            mask: Salamander::new(psk).map_err(io::Error::other)?,
            send_buffer: Mutex::new(Vec::with_capacity(65536)),
        })
    }
}

impl fmt::Debug for MaskedSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaskedSocket")
            .field("socket", &self.socket)
            .finish_non_exhaustive()
    }
}

impl AsyncUdpSocket for MaskedSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.socket.clone().create_io_poller()
    }

    fn try_send(&self, transmit: &udp::Transmit) -> io::Result<()> {
        if transmit.segment_size.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Salamander requires individual UDP transmissions",
            ));
        }
        let mut buffer = self
            .send_buffer
            .lock()
            .map_err(|_| io::Error::other("send lock poisoned"))?;
        self.mask
            .seal(transmit.contents, &mut buffer)
            .map_err(io::Error::other)?;
        let masked = udp::Transmit {
            contents: &buffer,
            ..*transmit
        };
        self.socket.try_send(&masked)
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        for _ in 0..32 {
            let count = match self.socket.poll_recv(cx, bufs, meta) {
                Poll::Ready(Ok(count)) => count,
                result => return result,
            };
            if count == 0 {
                return Poll::Ready(Ok(0));
            }
            let mut kept = 0;
            for i in 0..count {
                self.decode(&mut bufs[i], &mut meta[i]);
                if meta[i].len == 0 {
                    continue;
                }
                if kept != i {
                    let (before, after) = bufs.split_at_mut(i);
                    before[kept][..meta[i].len].copy_from_slice(&after[0][..meta[i].len]);
                    meta[kept] = meta[i];
                }
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
        self.socket.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        1
    }
    fn max_receive_segments(&self) -> usize {
        self.socket.max_receive_segments()
    }
    fn may_fragment(&self) -> bool {
        self.socket.may_fragment()
    }
}

impl MaskedSocket {
    fn decode(&self, buffer: &mut [u8], meta: &mut udp::RecvMeta) {
        let wire_len = meta.len;
        let stride = meta.stride;
        meta.len = 0;
        if stride <= SALT_LEN {
            return;
        }
        // Each GRO segment has its own salt. Compact decoded payloads while
        // retaining the segment stride, ECN, destination IP and source address.
        meta.stride = stride - SALT_LEN;
        for offset in (0..wire_len).step_by(stride) {
            let end = (offset + stride).min(wire_len);
            if self.mask.open_in_place(&mut buffer[offset..end]).is_ok() {
                let len = end - offset - SALT_LEN;
                buffer.copy_within(offset + SALT_LEN..end, meta.len);
                meta.len += len;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;

    fn socket() -> MaskedSocket {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        MaskedSocket::new(socket, b"test-password").unwrap()
    }

    #[tokio::test]
    async fn send_errors_are_packet_loss_instead_of_connection_failure() {
        let socket = Arc::new(socket());
        let mut poller = socket.clone().create_io_poller();
        poll_fn(|cx| poller.as_mut().poll_writable(cx))
            .await
            .unwrap();
        // Port zero produces EINVAL without transmitting a packet on Linux.
        socket
            .try_send(&udp::Transmit {
                destination: "127.0.0.1:0".parse().unwrap(),
                contents: b"test",
                ecn: None,
                segment_size: None,
                src_ip: None,
            })
            .unwrap();
    }

    #[tokio::test]
    async fn gro_segments_have_independent_salts_and_preserve_metadata() {
        let socket = socket();
        let mut wire = Vec::new();
        for payload in [b"first packet".as_slice(), b"next packet!", b"last"] {
            let mut packet = Vec::new();
            socket.mask.seal(payload, &mut packet).unwrap();
            wire.extend_from_slice(&packet);
        }
        let mut meta = udp::RecvMeta {
            addr: "127.0.0.1:1234".parse().unwrap(),
            len: wire.len(),
            stride: 12 + SALT_LEN,
            ecn: Some(udp::EcnCodepoint::Ect0),
            dst_ip: Some("127.0.0.2".parse().unwrap()),
        };
        socket.decode(&mut wire, &mut meta);
        assert_eq!(&wire[..meta.len], b"first packetnext packet!last");
        assert_eq!(meta.stride, 12);
        assert_eq!(meta.ecn, Some(udp::EcnCodepoint::Ect0));
        assert_eq!(meta.dst_ip, Some("127.0.0.2".parse().unwrap()));
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[tokio::test]
    async fn receives_a_batch_and_filters_short_packets() {
        let socket = socket();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        for payload in [b"a".as_slice(), b"short", b"b", b"c"] {
            let mut packet = Vec::new();
            if payload == b"short" {
                packet.extend_from_slice(payload);
            } else {
                socket.mask.seal(payload, &mut packet).unwrap();
            }
            sender
                .send_to(&packet, socket.local_addr().unwrap())
                .unwrap();
        }
        let mut storage = [[0; 2048]; 4];
        let mut bufs: Vec<_> = storage.iter_mut().map(|b| IoSliceMut::new(b)).collect();
        let mut meta = [udp::RecvMeta::default(); 4];
        let count = poll_fn(|cx| socket.poll_recv(cx, &mut bufs, &mut meta))
            .await
            .unwrap();
        assert_eq!(count, 3);
        for i in 0..count {
            assert_eq!(&bufs[i][..meta[i].len], &[b'a' + i as u8]);
            assert_eq!(meta[i].addr, sender.local_addr().unwrap());
        }
    }
}
