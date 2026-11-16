use crate::protocol;
use bytes::{Buf, Bytes};
use h3::quic::{self, RecvStream as _};
use std::{
    collections::VecDeque,
    future::poll_fn,
    task::{Context, Poll},
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct Replay {
    raw: h3_quinn::BidiStream<Bytes>,
    prefix: VecDeque<Bytes>,
}

pub struct ReplayRecv<S = h3_quinn::RecvStream> {
    raw: S,
    prefix: VecDeque<Bytes>,
    error: Option<quic::StreamErrorIncoming>,
}

impl Replay {
    pub fn split(self) -> (h3_quinn::SendStream<Bytes>, ReplayRecv) {
        let (send, recv) = quic::BidiStream::split(self.raw);
        (
            send,
            ReplayRecv {
                raw: recv,
                prefix: self.prefix,
                error: None,
            },
        )
    }
}

impl<S: quic::RecvStream<Buf = Bytes>> quic::RecvStream for ReplayRecv<S> {
    type Buf = Bytes;
    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Option<Bytes>, quic::StreamErrorIncoming>> {
        if let Some(data) = self.prefix.pop_front() {
            return Poll::Ready(Ok(Some(data)));
        }
        if let Some(error) = self.error.take() {
            return Poll::Ready(Err(error));
        }
        self.raw.poll_data(cx)
    }
    fn stop_sending(&mut self, code: u64) {
        self.raw.stop_sending(code)
    }
    fn recv_id(&self) -> quic::StreamId {
        self.raw.recv_id()
    }
}

impl<S: quic::RecvStream<Buf = Bytes> + Unpin> tokio::io::AsyncRead for ReplayRecv<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let start = buffer.filled().len();
        while buffer.remaining() != 0 {
            match self.poll_data(cx) {
                Poll::Ready(Ok(Some(mut data))) => {
                    let n = buffer.remaining().min(data.len());
                    buffer.put_slice(&data.split_to(n));
                    if !data.is_empty() {
                        self.prefix.push_front(data);
                    }
                }
                Poll::Ready(Ok(None)) => break,
                Poll::Ready(Err(error)) => {
                    if buffer.filled().len() == start {
                        return Poll::Ready(Err(std::io::Error::other(error)));
                    }
                    self.error = Some(error);
                    break;
                }
                Poll::Pending => {
                    if buffer.filled().len() == start {
                        return Poll::Pending;
                    }
                    break;
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl quic::RecvStream for Replay {
    type Buf = Bytes;
    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Option<Bytes>, quic::StreamErrorIncoming>> {
        if let Some(data) = self.prefix.pop_front() {
            return Poll::Ready(Ok(Some(data)));
        }
        self.raw.poll_data(cx)
    }
    fn stop_sending(&mut self, code: u64) {
        self.raw.stop_sending(code)
    }
    fn recv_id(&self) -> quic::StreamId {
        self.raw.recv_id()
    }
}

impl quic::SendStream<Bytes> for Replay {
    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), quic::StreamErrorIncoming>> {
        self.raw.poll_ready(cx)
    }
    fn poll_finish(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), quic::StreamErrorIncoming>> {
        self.raw.poll_finish(cx)
    }
    fn send_data<T: Into<quic::WriteBuf<Bytes>>>(
        &mut self,
        data: T,
    ) -> std::result::Result<(), quic::StreamErrorIncoming> {
        self.raw.send_data(data)
    }
    fn reset(&mut self, code: u64) {
        self.raw.reset(code)
    }
    fn send_id(&self) -> quic::StreamId {
        self.raw.send_id()
    }
}

impl quic::SendStreamUnframed<Bytes> for Replay {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut D,
    ) -> Poll<std::result::Result<usize, quic::StreamErrorIncoming>> {
        self.raw.poll_send(cx, buf)
    }
}

pub struct Dispatch(pub h3_quinn::Connection);
pub struct Opener(h3_quinn::OpenStreams);

macro_rules! open_streams {
    ($ty:ty) => {
        impl quic::OpenStreams<Bytes> for $ty {
            type BidiStream = Replay;
            type SendStream = h3_quinn::SendStream<Bytes>;
            fn poll_open_bidi(
                &mut self,
                cx: &mut Context<'_>,
            ) -> Poll<std::result::Result<Replay, quic::StreamErrorIncoming>> {
                self.0.poll_open_bidi(cx).map_ok(|raw| Replay {
                    raw,
                    prefix: VecDeque::new(),
                })
            }
            fn poll_open_send(
                &mut self,
                cx: &mut Context<'_>,
            ) -> Poll<std::result::Result<Self::SendStream, quic::StreamErrorIncoming>> {
                self.0.poll_open_send(cx)
            }
            fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
                <_ as quic::OpenStreams<Bytes>>::close(&mut self.0, code, reason)
            }
        }
    };
}
open_streams!(Dispatch);
open_streams!(Opener);

impl quic::Connection<Bytes> for Dispatch {
    type RecvStream = h3_quinn::RecvStream;
    type OpenStreams = Opener;
    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Self::RecvStream, quic::ConnectionErrorIncoming>> {
        <h3_quinn::Connection as quic::Connection<Bytes>>::poll_accept_recv(&mut self.0, cx)
    }
    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Replay, quic::ConnectionErrorIncoming>> {
        <h3_quinn::Connection as quic::Connection<Bytes>>::poll_accept_bidi(&mut self.0, cx).map_ok(
            |raw| Replay {
                raw,
                prefix: VecDeque::new(),
            },
        )
    }
    fn opener(&self) -> Opener {
        Opener(<h3_quinn::Connection as quic::Connection<Bytes>>::opener(
            &self.0,
        ))
    }
}

pub async fn classify(mut stream: Replay) -> Result<(Replay, bool)> {
    let mut prefix = [0u8; 8];
    let mut length = 0;
    loop {
        let data = poll_fn(|cx| stream.raw.poll_data(cx))
            .await?
            .ok_or("empty request stream")?;
        let copied = data.len().min(8 - length);
        prefix[length..length + copied].copy_from_slice(&data[..copied]);
        length += copied;
        stream.prefix.push_back(data);
        match protocol::read_varint(&prefix[..length]) {
            Ok((kind, _)) => return Ok((stream, kind == protocol::TCP_REQUEST)),
            Err(protocol::Error::Incomplete) => (),
            Err(error) => return Err(error.into()),
        }
    }
}

pub async fn write_raw<S: quic::SendStreamUnframed<Bytes>>(
    stream: &mut S,
    mut data: Bytes,
) -> Result<()> {
    while data.has_remaining() {
        let written = poll_fn(|cx| stream.poll_send(cx, &mut data)).await?;
        if written == 0 {
            return Err("zero-byte stream write".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    struct Chunks(VecDeque<Poll<std::result::Result<Option<Bytes>, quic::StreamErrorIncoming>>>);

    impl quic::RecvStream for Chunks {
        type Buf = Bytes;
        fn poll_data(
            &mut self,
            _: &mut Context<'_>,
        ) -> Poll<std::result::Result<Option<Bytes>, quic::StreamErrorIncoming>> {
            self.0.pop_front().unwrap_or(Poll::Ready(Ok(None)))
        }
        fn stop_sending(&mut self, _: u64) {
            unreachable!()
        }
        fn recv_id(&self) -> quic::StreamId {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn coalesces_ready_chunks_without_waiting_and_retains_partial_chunks() {
        let mut reader = ReplayRecv {
            raw: Chunks(VecDeque::from([
                Poll::Ready(Ok(Some(Bytes::from_static(b"abcdef")))),
                Poll::Ready(Ok(Some(Bytes::from_static(b"ghij")))),
                Poll::Pending,
                Poll::Ready(Ok(Some(Bytes::from_static(b"kl")))),
            ])),
            prefix: VecDeque::from([Bytes::from_static(b"header-tail")]),
            error: None,
        };
        let mut short = [0; 4];
        assert_eq!(reader.read(&mut short).await.unwrap(), 4);
        assert_eq!(&short, b"head");
        let mut buffer = [0; 64];
        let n = reader.read(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], b"er-tailabcdefghij");
        let n = reader.read(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], b"kl");
        assert_eq!(reader.read(&mut buffer).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn delivers_buffered_payload_before_reset() {
        let mut reader = ReplayRecv {
            raw: Chunks(VecDeque::from([
                Poll::Ready(Ok(Some(Bytes::from_static(b"payload")))),
                Poll::Ready(Err(quic::StreamErrorIncoming::StreamTerminated {
                    error_code: 42,
                })),
            ])),
            prefix: VecDeque::new(),
            error: None,
        };
        let mut buffer = [0; 64];
        let n = reader.read(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], b"payload");
        assert!(reader.read(&mut buffer).await.is_err());
    }
}
