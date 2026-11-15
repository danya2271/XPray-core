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

pub struct ReplayRecv {
    raw: h3_quinn::RecvStream,
    prefix: VecDeque<Bytes>,
}

impl Replay {
    pub fn split(self) -> (h3_quinn::SendStream<Bytes>, ReplayRecv) {
        let (send, recv) = quic::BidiStream::split(self.raw);
        (
            send,
            ReplayRecv {
                raw: recv,
                prefix: self.prefix,
            },
        )
    }
}

impl quic::RecvStream for ReplayRecv {
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
