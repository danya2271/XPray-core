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

pub struct ReplayRecv<S = quinn::RecvStream> {
    raw: S,
    prefix: VecDeque<Bytes>,
}

impl Replay {
    pub fn stopped(
        &self,
    ) -> impl std::future::Future<
        Output = std::result::Result<Option<quinn::VarInt>, quinn::StoppedError>,
    > + Send
    + Sync
    + 'static {
        self.raw.stopped()
    }

    pub fn split(self) -> Result<(quinn::SendStream, ReplayRecv)> {
        let (send, recv) = quic::BidiStream::split(self.raw);
        Ok((
            send.into_inner()
                .ok_or("pending HTTP/3 write during raw stream handoff")?,
            ReplayRecv {
                raw: recv
                    .into_inner()
                    .ok_or("pending HTTP/3 read during raw stream handoff")?,
                prefix: self.prefix,
            },
        ))
    }
}

impl ReplayRecv {
    pub async fn read_chunks(
        &mut self,
        chunks: &mut [Bytes],
    ) -> std::result::Result<Option<usize>, quinn::ReadError> {
        let mut count = 0;
        for chunk in chunks.iter_mut() {
            let Some(data) = self.prefix.pop_front() else {
                break;
            };
            *chunk = data;
            count += 1;
        }
        if count != 0 {
            Ok(Some(count))
        } else {
            self.raw.read_chunks(chunks).await
        }
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for ReplayRecv<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let start = buffer.filled().len();
        while buffer.remaining() != 0 {
            let Some(mut data) = self.prefix.pop_front() else {
                break;
            };
            let n = buffer.remaining().min(data.len());
            buffer.put_slice(&data.split_to(n));
            if !data.is_empty() {
                self.prefix.push_front(data);
            }
        }
        // Publish already buffered bytes before polling the underlying stream:
        // a reset must not discard them, and a pending read adds no latency.
        if buffer.filled().len() != start || buffer.remaining() == 0 {
            Poll::Ready(Ok(()))
        } else {
            std::pin::Pin::new(&mut self.raw).poll_read(cx, buffer)
        }
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

impl quic::BidiStream<Bytes> for Replay {
    type SendStream = h3_quinn::SendStream<Bytes>;
    type RecvStream = HttpRecv;

    fn split(self) -> (Self::SendStream, Self::RecvStream) {
        let (send, recv) = quic::BidiStream::split(self.raw);
        (
            send,
            HttpRecv {
                raw: recv,
                prefix: self.prefix,
            },
        )
    }
}

pub struct HttpRecv {
    raw: h3_quinn::RecvStream,
    prefix: VecDeque<Bytes>,
}

impl quic::RecvStream for HttpRecv {
    type Buf = Bytes;

    fn poll_data(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<std::result::Result<Option<Bytes>, quic::StreamErrorIncoming>> {
        if let Some(data) = self.prefix.pop_front() {
            return Poll::Ready(Ok(Some(data)));
        }
        self.raw.poll_data(context)
    }

    fn stop_sending(&mut self, code: u64) {
        self.raw.stop_sending(code);
    }

    fn recv_id(&self) -> quic::StreamId {
        self.raw.recv_id()
    }
}

pub struct Dispatch(
    pub h3_quinn::Connection,
    pub tokio::sync::mpsc::Receiver<Replay>,
);
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
        self.1.poll_recv(cx).map(|stream| {
            stream.ok_or_else(|| {
                quic::ConnectionErrorIncoming::InternalError("stream dispatcher closed".into())
            })
        })
    }
    fn opener(&self) -> Opener {
        Opener(<h3_quinn::Connection as quic::Connection<Bytes>>::opener(
            &self.0,
        ))
    }
}

pub async fn accept(connection: &mut h3_quinn::Connection) -> Result<Replay> {
    let raw = poll_fn(|cx| {
        <h3_quinn::Connection as quic::Connection<Bytes>>::poll_accept_bidi(connection, cx)
    })
    .await?;
    Ok(Replay {
        raw,
        prefix: VecDeque::new(),
    })
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

    impl tokio::io::AsyncRead for Chunks {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            match self.poll_data(cx) {
                Poll::Ready(Ok(Some(mut data))) => {
                    let n = buffer.remaining().min(data.len());
                    buffer.put_slice(&data.split_to(n));
                    if !data.is_empty() {
                        self.0.push_front(Poll::Ready(Ok(Some(data))));
                    }
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Ok(None)) => Poll::Ready(Ok(())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(std::io::Error::other(error))),
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
    }

    #[tokio::test]
    async fn hands_off_prefix_without_waiting_or_losing_partial_chunks() {
        let mut reader = ReplayRecv {
            raw: Chunks(VecDeque::from([
                Poll::Ready(Ok(Some(Bytes::from_static(b"abcdef")))),
                Poll::Ready(Ok(Some(Bytes::from_static(b"ghij")))),
                Poll::Pending,
                Poll::Ready(Ok(Some(Bytes::from_static(b"kl")))),
            ])),
            prefix: VecDeque::from([Bytes::from_static(b"header-tail")]),
        };
        let mut short = [0; 4];
        assert_eq!(reader.read(&mut short).await.unwrap(), 4);
        assert_eq!(&short, b"head");
        let mut buffer = [0; 64];
        let n = reader.read(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], b"er-tail");
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, b"abcdefghijkl");
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
            prefix: VecDeque::from([Bytes::from_static(b"prefix")]),
        };
        let mut buffer = [0; 64];
        let n = reader.read(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], b"prefix");
        let n = reader.read(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], b"payload");
        assert!(reader.read(&mut buffer).await.is_err());
    }
}
