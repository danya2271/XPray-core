//! Conservative Salamander socket adapter for the interoperability tools.
//! No GSO/GRO, ECN, or source-IP selection; production I/O will replace this.

use crate::salamander::{SALT_LEN, Salamander};
use quinn::{AsyncUdpSocket, UdpPoller, udp};
use std::{
    fmt,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::ReadBuf;

pub struct MaskedSocket {
    socket: Arc<tokio::net::UdpSocket>,
    mask: Salamander,
    send_buffer: Mutex<Vec<u8>>,
}

impl MaskedSocket {
    pub fn new(socket: std::net::UdpSocket, psk: &[u8]) -> io::Result<Self> {
        Ok(Self {
            socket: Arc::new(tokio::net::UdpSocket::from_std(socket)?),
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
        Box::pin(MaskedPoller(self.socket.clone()))
    }

    fn try_send(&self, transmit: &udp::Transmit) -> io::Result<()> {
        if transmit.segment_size.is_some() || transmit.src_ip.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "probe adapter has no GSO or source-IP selection",
            ));
        }
        let mut buffer = self
            .send_buffer
            .lock()
            .map_err(|_| io::Error::other("send lock poisoned"))?;
        self.mask
            .seal(transmit.contents, &mut buffer)
            .map_err(io::Error::other)?;
        if self.socket.try_send_to(&buffer, transmit.destination)? != buffer.len() {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "partial UDP send"));
        }
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        // Plain Tokio sockets never enable UDP GRO. Cap work per poll to stay fair.
        for _ in 0..32 {
            let mut buffer = ReadBuf::new(&mut bufs[0]);
            let address = match self.socket.poll_recv_from(cx, &mut buffer) {
                Poll::Ready(Ok(address)) => address,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            };
            let len = buffer.filled().len();
            if len <= SALT_LEN {
                continue;
            }
            let payload_len = self
                .mask
                .open_in_place(&mut bufs[0][..len])
                .map_err(io::Error::other)?
                .len();
            bufs[0].copy_within(SALT_LEN..len, 0);
            meta[0] = udp::RecvMeta {
                addr: address,
                len: payload_len,
                stride: payload_len,
                ecn: None,
                dst_ip: None,
            };
            return Poll::Ready(Ok(1));
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
        1
    }
    fn may_fragment(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct MaskedPoller(Arc<tokio::net::UdpSocket>);

impl UdpPoller for MaskedPoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_send_ready(cx)
    }
}
